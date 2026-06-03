package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trungnb2210/REtcd/store"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
)

// WatchServer implements the etcd v3 Watch gRPC service.
type WatchServer struct {
	pb.UnimplementedWatchServer
	store Store
	disp  *eventDispatcher
}

func NewWatchServer(s Store) *WatchServer {
	disp := newEventDispatcher(s)
	// Seed the revision watermarks, then wire the dispatcher as the store's event
	// sink: every committed write now fans out in-process via disp.ingest, with no
	// XREAD readback. The Redis stream is still written and still backs historical
	// catch-up (catchUp), but live delivery no longer waits on a re-read.
	disp.prime(context.Background())
	s.SetEventSink(disp.ingest)
	return &WatchServer{store: s, disp: disp}
}

// watchID is a global counter for assigning unique IDs to each watch.
var watchID int64

// sender wraps the raw WatchServer stream with a mutex so multiple goroutines
// can safely call Send concurrently (the gRPC stream is not goroutine-safe).
type sender struct {
	mu sync.Mutex
	ws pb.Watch_WatchServer
}

func (s *sender) Send(resp *pb.WatchResponse) error {
	t0 := time.Now()
	s.mu.Lock()
	// Time spent here is time blocked behind another watch's Send on the same
	// stream — the head-of-line-blocking signal.
	watchSendMutexWait.Observe(time.Since(t0).Seconds())
	defer s.mu.Unlock()

	t1 := time.Now()
	err := s.ws.Send(resp)
	watchSendWrite.Observe(time.Since(t1).Seconds())
	return err
}

// Watch is the entry point for the Watch gRPC streaming RPC.
// Each call to Watch represents one long-lived connection from a Kubernetes
// component. Over that connection the client sends WatchCreateRequests and
// WatchCancelRequests; we spawn a goroutine per watch and fan events back.
func (s *WatchServer) Watch(stream pb.Watch_WatchServer) error {
	ctx := stream.Context()
	sender := &sender{ws: stream}

	// cancels maps watchID → cancel function so we can stop individual watches.
	cancels := make(map[int64]func())
	var mu sync.Mutex

	for {
		msg, err := stream.Recv()
		if err != nil {
			// Client disconnected — cancel all active watches.
			mu.Lock()
			for _, cancel := range cancels {
				cancel()
			}
			mu.Unlock()
			return err
		}

		if cr := msg.GetCreateRequest(); cr != nil {
			id := atomic.AddInt64(&watchID, 1)

			mode := "fromnow"
			if cr.StartRevision > 0 {
				mode = "catchup"
			}
			watchCreates.WithLabelValues(watchPrefixLabel(cr.Key), mode).Inc()

			watchCtx, cancel := cancelOnParent(ctx)

			mu.Lock()
			cancels[id] = cancel
			mu.Unlock()

			// Confirm watch creation to the client immediately.
			rev, _ := s.store.CurrentRevision(ctx)
			req := *cr
			req.StartRevision = effectiveStartRevision(req.StartRevision, rev)
			_ = sender.Send(&pb.WatchResponse{
				Header:  &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: rev},
				Created: true,
				WatchId: id,
			})

			// Tail the event stream in a background goroutine.
			go func(id int64, req *pb.WatchCreateRequest) {
				defer func() {
					cancel()
					mu.Lock()
					delete(cancels, id)
					mu.Unlock()
				}()
				s.tailWatch(watchCtx, sender, id, req)
			}(id, &req)
		}

		if cr := msg.GetCancelRequest(); cr != nil {
			mu.Lock()
			if cancel, ok := cancels[cr.WatchId]; ok {
				cancel()
				delete(cancels, cr.WatchId)
			}
			mu.Unlock()

			_ = sender.Send(&pb.WatchResponse{
				Header:   &pb.ResponseHeader{ClusterId: 1, MemberId: 1},
				Canceled: true,
				WatchId:  cr.WatchId,
			})
		}
	}
}

const (
	progressInterval = 1 * time.Second
	// maxSendBatch caps how many fanned-out events are coalesced into a single
	// WatchResponse. Batching bursts into one gRPC Send is what keeps delivery
	// throughput up under write storms.
	maxSendBatch = 1000
)

// tailWatch registers the watch with the shared dispatcher, replays any
// historical events in [startRevision, boundary] straight from the store, then
// delivers the live fan-out feed. Each WatchResponse stamps Header.Revision with
// the cluster's current revision (not the matched event's). An idle watch still
// emits a periodic empty progress notification — without it the apiserver's
// per-resource watch cache for an idle resource type lags and reflectors stall
// with "Too large resource version".
func (s *WatchServer) tailWatch(ctx context.Context, sender *sender, id int64, req *pb.WatchCreateRequest) {
	activeWatches.Inc()
	defer activeWatches.Dec()

	key := string(req.Key)
	rangeEnd := string(req.RangeEnd)
	startRevision := req.StartRevision

	subID, sub, boundary := s.disp.register(key, rangeEnd)
	defer s.disp.deregister(subID)

	// Catch-up: everything with rev > boundary is guaranteed on the live channel,
	// so we only need to replay [startRevision, boundary] from the store.
	if startRevision <= boundary {
		if !s.catchUp(ctx, sender, id, key, rangeEnd, startRevision, boundary) {
			return
		}
	}

	lastSend := time.Now()
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-sub.cancelCh:
			// Dispatcher cut us loose (backend outage, or we lagged too far).
			// Use the in-memory revision: the store is likely the thing that's
			// down, and a failed GET here would just stall the cancel.
			curRev := s.disp.currentRev()
			_ = sender.Send(&pb.WatchResponse{
				Header:       &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: curRev},
				WatchId:      id,
				Canceled:     true,
				CancelReason: "watch backend unavailable",
			})
			return

		case ev := <-sub.ch:
			// Coalesce this event plus anything already queued into one response.
			matched, createdMs := collectEvents(ev, sub.ch, boundary, startRevision, key, rangeEnd)
			if len(matched) == 0 {
				continue
			}
			if err := sender.Send(&pb.WatchResponse{
				Header:  &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: s.disp.currentRev()},
				WatchId: id,
				Events:  matched,
			}); err != nil {
				return
			}
			now := time.Now().UnixMilli()
			for _, ms := range createdMs {
				if ms > 0 {
					watchDelivery.Observe(float64(now-ms) / 1000.0)
				}
			}
			lastSend = time.Now()

		case <-ticker.C:
			if time.Since(lastSend) >= progressInterval {
				// In-memory revision (primed at startup, advanced by every event
				// the shared reader sees) — avoids an O(watches)/sec Redis GET
				// just to stamp idle progress notifications.
				curRev := s.disp.currentRev()
				if err := sender.Send(&pb.WatchResponse{
					Header:  &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: curRev},
					WatchId: id,
				}); err != nil {
					return
				}
				lastSend = time.Now()
			}
		}
	}
}

// collectEvents turns the first event plus any others already buffered on the
// channel into a single batch, applying the watch's revision/range filter.
func collectEvents(first store.Event, ch <-chan store.Event, boundary, startRevision int64, key, rangeEnd string) ([]*mvccpb.Event, []int64) {
	matched := make([]*mvccpb.Event, 0, 1)
	createdMs := make([]int64, 0, 1)
	add := func(ev store.Event) {
		if ev.Rev <= boundary || ev.Rev < startRevision {
			return // already delivered during catch-up, or before the watch window
		}
		if e := eventToProto(ev); e != nil {
			matched = append(matched, e)
			createdMs = append(createdMs, ev.CreatedMs)
		}
	}
	add(first)
	for len(matched) < maxSendBatch {
		select {
		case ev := <-ch:
			add(ev)
		default:
			return matched, createdMs
		}
	}
	return matched, createdMs
}

// catchUp replays historical events in [startRevision, boundary] from the store
// (the dispatcher only feeds events with rev > boundary live). It returns false
// if the watch should terminate (context cancelled).
func (s *WatchServer) catchUp(ctx context.Context, sender *sender, id int64, key, rangeEnd string, startRevision, boundary int64) bool {
	// Seek to the nearest checkpoint at or below startRevision instead of
	// re-scanning the whole stream from "0". Falls back to "0" when the index
	// can't narrow the scan; either way the rev filter below keeps it correct.
	lastID := s.disp.seekStreamID(startRevision)
	var scanned int64
	firstDeliveryDone := false
	consecErr := 0
	for {
		if ctx.Err() != nil {
			return false
		}
		events, newLastID, err := s.store.BlockReadEvents(ctx, lastID, readBatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			// Transient backend error during historical catch-up. Retry from the
			// same lastID (no events skipped); give up — loudly — only if the
			// backend stays unavailable, so the client re-establishes instead of
			// stalling until its resync interval. Live delivery is in-process and
			// has no backend dependency, so this catch-up read is the only place a
			// watch can observe a persistently dead Redis.
			watchReadErrors.Inc()
			consecErr++
			if consecErr >= maxReaderErrors {
				watchCancels.Inc()
				_ = sender.Send(&pb.WatchResponse{
					Header:       &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: s.disp.currentRev()},
					WatchId:      id,
					Canceled:     true,
					CancelReason: "watch backend unavailable",
				})
				return false
			}
			select {
			case <-ctx.Done():
				return false
			case <-time.After(readerErrBackoff):
			}
			continue
		}
		consecErr = 0
		if len(events) == 0 {
			return true // reached the tail; nothing more to catch up
		}
		lastID = newLastID

		var matched []*mvccpb.Event
		var createdMs []int64
		reachedBoundary := false
		for _, ev := range events {
			scanned++
			if ev.Rev > boundary {
				reachedBoundary = true // belongs to the live feed
				break
			}
			if ev.Rev < startRevision || !matchesWatch(ev.Key, key, rangeEnd) {
				continue
			}
			if e := eventToProto(ev); e != nil {
				matched = append(matched, e)
				createdMs = append(createdMs, ev.CreatedMs)
			}
		}

		if len(matched) > 0 {
			if err := sender.Send(&pb.WatchResponse{
				Header:  &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: s.disp.currentRev()},
				WatchId: id,
				Events:  matched,
			}); err != nil {
				return false
			}
			now := time.Now().UnixMilli()
			for _, ms := range createdMs {
				if ms > 0 {
					watchDelivery.Observe(float64(now-ms) / 1000.0)
				}
			}
			if !firstDeliveryDone {
				watchCatchupEvents.Observe(float64(scanned))
				firstDeliveryDone = true
			}
		}
		if reachedBoundary {
			return true
		}
	}
}

// effectiveStartRevision converts etcd's "start from now" default into the
// first revision after the watch creation response. Explicit start revisions
// remain inclusive.
func effectiveStartRevision(requested, current int64) int64 {
	if requested > 0 {
		return requested
	}
	return current + 1
}

// watchPrefixLabel reduces a watched key to a low-cardinality resource label for
// metrics, e.g. "/registry/pods/ns/name" → "/registry/pods". Keeps at most the
// first two path components after the leading slash so the label set stays small.
func watchPrefixLabel(key []byte) string {
	s := string(key)
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			n++
			if n == 3 { // after "/registry/<resource>"
				return s[:i]
			}
		}
	}
	if s == "" {
		return "(empty)"
	}
	return s
}

// matchesWatch returns true if eventKey falls inside the watch request range.
// An empty rangeEnd watches exactly key. A rangeEnd of "\x00" watches all keys
// from key onwards. Other rangeEnd values use etcd's half-open [key, rangeEnd)
// range semantics, including prefix watches such as key="/p/" rangeEnd="/p0".
func matchesWatch(eventKey, key, rangeEnd string) bool {
	if rangeEnd == "" {
		return eventKey == key
	}
	if rangeEnd == "\x00" {
		return eventKey >= key
	}
	return eventKey >= key && eventKey < rangeEnd
}

// eventToProto converts a store.Event to an mvccpb.Event proto.
func eventToProto(ev store.Event) *mvccpb.Event {
	switch ev.Type {
	case "PUT":
		var kv, prevKV *mvccpb.KeyValue
		if ev.KV != nil {
			kv = toProtoKV(ev.KV)
		}
		if ev.PrevKV != nil {
			prevKV = toProtoKV(ev.PrevKV)
		}
		return &mvccpb.Event{Type: mvccpb.PUT, Kv: kv, PrevKv: prevKV}
	case "DELETE":
		var prevKV *mvccpb.KeyValue
		if ev.PrevKV != nil {
			prevKV = toProtoKV(ev.PrevKV)
		}
		// PrevKv MUST be populated: the apiserver opens storage watches
		// WithPrevKV and treats a DELETE event with PrevKv=nil as a fatal error,
		// terminating its watch-cache for the whole resource and forcing all
		// clients to relist (the cold-start watch-churn root cause).
		return &mvccpb.Event{
			Type: mvccpb.DELETE,
			Kv: &mvccpb.KeyValue{
				Key:         []byte(ev.Key),
				ModRevision: ev.Rev,
			},
			PrevKv: prevKV,
		}
	}
	return nil
}

// cancelOnParent creates a context that is cancelled when the parent is done,
// or when the returned cancel function is called — whichever happens first.
// cancel is safe to call concurrently and is idempotent.
func cancelOnParent(parent context.Context) (context.Context, func()) {
	ch := make(chan struct{})
	var once sync.Once
	cancel := func() {
		once.Do(func() { close(ch) })
	}
	ctx := &cancelCtxImpl{parent: parent, done: ch, merged: make(chan struct{})}
	return ctx, cancel
}

type cancelCtxImpl struct {
	parent context.Context
	done   chan struct{}
	merged chan struct{}
	once   sync.Once
}

func (c *cancelCtxImpl) Done() <-chan struct{} {
	c.once.Do(func() {
		go func() {
			select {
			case <-c.parent.Done():
				close(c.merged)
			case <-c.done:
				close(c.merged)
			}
		}()
	})
	return c.merged
}

func (c *cancelCtxImpl) Deadline() (deadline time.Time, ok bool) { return }
func (c *cancelCtxImpl) Err() error {
	select {
	case <-c.Done():
		return context.Canceled
	default:
		return nil
	}
}
func (c *cancelCtxImpl) Value(key any) any { return c.parent.Value(key) }
