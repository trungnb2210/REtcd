package server

import (
	"context"
	"strings"
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
}

func NewWatchServer(s Store) *WatchServer {
	return &WatchServer{store: s}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ws.Send(resp)
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

			watchCtx, cancel := cancelOnParent(ctx)

			mu.Lock()
			cancels[id] = cancel
			mu.Unlock()

			// Confirm watch creation to the client immediately.
			rev, _ := s.store.CurrentRevision(ctx)
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
			}(id, cr)
		}

		if cr := msg.GetCancelRequest(); cr != nil {
			mu.Lock()
			if cancel, ok := cancels[cr.WatchId]; ok {
				cancel()
				delete(cancels, cr.WatchId)
			}
			mu.Unlock()

			_ = sender.Send(&pb.WatchResponse{
				Header:    &pb.ResponseHeader{ClusterId: 1, MemberId: 1},
				Canceled:  true,
				WatchId:   cr.WatchId,
			})
		}
	}
}

// tailWatch reads events from the Redis Stream and forwards matching ones to
// the client. It first replays historical events since startRevision, then
// blocks waiting for new ones.
func (s *WatchServer) tailWatch(ctx context.Context, sender *sender, id int64, req *pb.WatchCreateRequest) {
	prefix := string(req.Key)
	startRevision := req.StartRevision

	// Start reading from the beginning of the stream and skip events with
	// revision < startRevision. This is simple and correct; a production
	// system would maintain a revision→streamID index to seek directly.
	lastID := "0"

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		events, newLastID, err := s.store.BlockReadEvents(ctx, lastID, 100)
		if err != nil {
			return
		}
		lastID = newLastID

		for _, ev := range events {
			// Skip events before the requested startRevision.
			if ev.Rev < startRevision {
				continue
			}
			// Skip events that don't match the watched key/prefix.
			if !matchesWatch(ev.Key, prefix) {
				continue
			}

			resp := eventToWatchResponse(id, ev)
			if resp == nil {
				continue
			}
			if err := sender.Send(resp); err != nil {
				return
			}
		}
	}
}

// matchesWatch returns true if key matches the watch prefix.
// A single key watch matches exactly; a prefix watch matches any key with that prefix.
func matchesWatch(key, prefix string) bool {
	return key == prefix || strings.HasPrefix(key, prefix)
}

// eventToWatchResponse converts a store.Event to a WatchResponse proto.
func eventToWatchResponse(id int64, ev store.Event) *pb.WatchResponse {
	var eventType mvccpb.Event_EventType
	var kv *mvccpb.KeyValue
	var prevKV *mvccpb.KeyValue

	switch ev.Type {
	case "PUT":
		eventType = mvccpb.PUT
		if ev.KV != nil {
			kv = toProtoKV(ev.KV)
		}
		if ev.PrevKV != nil {
			prevKV = toProtoKV(ev.PrevKV)
		}
	case "DELETE":
		eventType = mvccpb.DELETE
		kv = &mvccpb.KeyValue{
			Key:         []byte(ev.Key),
			ModRevision: ev.Rev,
		}
	default:
		return nil
	}

	return &pb.WatchResponse{
		Header:  &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: ev.Rev},
		WatchId: id,
		Events: []*mvccpb.Event{{
			Type:   eventType,
			Kv:     kv,
			PrevKv: prevKV,
		}},
	}
}

// cancelOnParent creates a context that is cancelled when the parent is done,
// or when the returned cancel function is called — whichever happens first.
func cancelOnParent(parent context.Context) (context.Context, func()) {
	ch := make(chan struct{})
	cancel := func() {
		select {
		case <-ch:
		default:
			close(ch)
		}
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
