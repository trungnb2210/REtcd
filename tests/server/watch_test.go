package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/trungnb2210/REtcd/server"
	"github.com/trungnb2210/REtcd/store"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// mockWatchStream is a minimal in-memory implementation of pb.Watch_WatchServer.
// Watch()/tailWatch only use Send, Recv and Context; the remaining
// grpc.ServerStream methods are never called.
type mockWatchStream struct {
	grpc.ServerStream
	ctx    context.Context
	recvCh chan *pb.WatchRequest
	mu     sync.Mutex
	sent   []*pb.WatchResponse
}

func newMockWatchStream(ctx context.Context) *mockWatchStream {
	return &mockWatchStream{ctx: ctx, recvCh: make(chan *pb.WatchRequest, 8)}
}

func (m *mockWatchStream) Send(r *pb.WatchResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, r)
	return nil
}

func (m *mockWatchStream) Recv() (*pb.WatchRequest, error) {
	select {
	case r := <-m.recvCh:
		return r, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *mockWatchStream) Context() context.Context { return m.ctx }

func (m *mockWatchStream) deliveredKey(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.sent {
		for _, e := range r.Events {
			if e.Kv != nil && string(e.Kv.Key) == key {
				return true
			}
		}
	}
	return false
}

func (m *mockWatchStream) sawCanceled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.sent {
		if r.Canceled {
			return true
		}
	}
	return false
}

func (m *mockWatchStream) sawCreated() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.sent {
		if r.Created {
			return true
		}
	}
	return false
}

func watchCreateReq(key string, startRev int64) *pb.WatchRequest {
	return &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key:           []byte(key),
				StartRevision: startRev,
			},
		},
	}
}

func waitFor(cond func() bool, d time.Duration) bool {
	deadline := time.After(d)
	for {
		if cond() {
			return true
		}
		select {
		case <-deadline:
			return cond()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// A watch must survive transient backend errors: the first few BlockReadEvents
// calls fail, but the event is still delivered once the backend recovers,
// rather than the watch dying silently.
func TestWatchRetriesTransientErrors(t *testing.T) {
	fs := newFakeStore()
	fs.failFirst = 3
	fs.rev = 1
	fs.events = append(fs.events, store.Event{
		Type: "PUT", Key: "foo", Rev: 1,
		KV: &store.KeyValue{Key: "foo", Value: []byte("bar"), ModRevision: 1},
	})

	ws := server.NewWatchServer(fs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockWatchStream(ctx)
	stream.recvCh <- watchCreateReq("foo", 1)
	go func() { _ = ws.Watch(stream) }()

	if !waitFor(func() bool { return stream.deliveredKey("foo") }, 3*time.Second) {
		t.Fatalf("event for key %q never delivered; watch likely died on a transient error", "foo")
	}
}

// When the backend is persistently unavailable, the watch must tell the client
// (Canceled=true) so it re-establishes, rather than going silent and leaving the
// apiserver's cache stale until a resync interval.
func TestWatchCancelsOnPersistentFailure(t *testing.T) {
	fs := newFakeStore()
	fs.failAll = true

	ws := server.NewWatchServer(fs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockWatchStream(ctx)
	stream.recvCh <- watchCreateReq("foo", 1)
	go func() { _ = ws.Watch(stream) }()

	// maxConsecErrors (30) * 50ms backoff ≈ 1.5s before the cancel is emitted.
	if !waitFor(stream.sawCanceled, 5*time.Second) {
		t.Fatalf("watch did not emit Canceled on persistent backend failure (would stall the client until resync)")
	}
}

// A "from now" watch must receive a key written AFTER it was created, delivered
// through the in-process sink→ingest→fan-out path (no XREAD readback). The store
// is seeded with prior history so the new write's revision sits clearly above the
// watch's catch-up boundary, exercising live delivery.
func TestWatchDeliversWriteAfterCreate(t *testing.T) {
	fs := newFakeStore()
	fs.rev = 5 // pretend the cluster already has revision history

	ws := server.NewWatchServer(fs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockWatchStream(ctx)
	stream.recvCh <- watchCreateReq("foo", 0) // StartRevision 0 = watch from now
	go func() { _ = ws.Watch(stream) }()

	if !waitFor(stream.sawCreated, 2*time.Second) {
		t.Fatal("watch was never created")
	}
	// Let the watch register before writing so the event takes the live path; if
	// it races registration, catch-up still covers it, so delivery is guaranteed.
	time.Sleep(50 * time.Millisecond)

	if _, _, err := fs.Put(ctx, "foo", []byte("bar"), 0); err != nil {
		t.Fatalf("put: %v", err)
	}

	if !waitFor(func() bool { return stream.deliveredKey("foo") }, 2*time.Second) {
		t.Fatal("write after watch-create was not delivered (sink→ingest wiring broken)")
	}
}

// An on-demand progress request must be answered immediately (etcd semantics:
// one empty response on watch ID -1, stamped with the current revision), not
// left to the periodic 1 s notification. The apiserver's watch-cache freshness
// check blocks on this reply.
func TestWatchProgressRequestAnsweredImmediately(t *testing.T) {
	fs := newFakeStore()
	fs.rev = 7

	ws := server.NewWatchServer(fs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockWatchStream(ctx)
	stream.recvCh <- watchCreateReq("foo", 0)
	go func() { _ = ws.Watch(stream) }()

	if !waitFor(stream.sawCreated, 2*time.Second) {
		t.Fatal("watch was never created")
	}

	stream.recvCh <- &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_ProgressRequest{
			ProgressRequest: &pb.WatchProgressRequest{},
		},
	}

	sawProgress := func() bool {
		stream.mu.Lock()
		defer stream.mu.Unlock()
		for _, r := range stream.sent {
			if r.WatchId == -1 && len(r.Events) == 0 && !r.Created && !r.Canceled {
				if r.Header == nil || r.Header.Revision < 7 {
					t.Fatalf("progress reply with stale/missing revision: %+v", r.Header)
				}
				return true
			}
		}
		return false
	}
	// Well under progressInterval (1 s): the reply must come from the
	// on-demand path, not the periodic ticker.
	if !waitFor(sawProgress, 500*time.Millisecond) {
		t.Fatal("progress request was not answered with a WatchId=-1 progress notification")
	}
}
