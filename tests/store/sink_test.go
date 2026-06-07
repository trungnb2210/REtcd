package store_test

// Tests for the in-process event sink (SetEventSink) — the write→watch
// short-circuit. Every committed write must hand a complete event to the sink
// the moment it returns, so the watch dispatcher can fan out without re-reading
// the Redis stream. Requires a live Redis (skipped otherwise, via newTestStore).

import (
	"context"
	"sync"
	"testing"

	"github.com/trungnb2210/REtcd/store"
)

// eventCollector is a thread-safe sink that records every emitted event.
type eventCollector struct {
	mu     sync.Mutex
	events []store.Event
}

func (c *eventCollector) sink(ev store.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, ev)
}

func (c *eventCollector) snapshot() []store.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]store.Event(nil), c.events...)
}

func TestEventSinkEmitsPutCreateOverwriteAndDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	c := &eventCollector{}
	s.SetEventSink(c.sink)

	// Create.
	rev1, _, err := s.Put(ctx, "/k", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("Put create: %v", err)
	}
	// Overwrite.
	rev2, _, err := s.Put(ctx, "/k", []byte("v2"), 0)
	if err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	// Delete.
	rev3, _, err := s.Delete(ctx, "/k")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	evs := c.snapshot()
	if len(evs) != 3 {
		t.Fatalf("expected 3 emitted events, got %d: %+v", len(evs), evs)
	}

	// Event 0: create PUT — KV present, no PrevKv, stream ID set.
	if evs[0].Type != "PUT" || evs[0].Rev != rev1 || evs[0].Key != "/k" {
		t.Fatalf("create event wrong: %+v", evs[0])
	}
	if evs[0].KV == nil || string(evs[0].KV.Value) != "v1" {
		t.Fatalf("create event KV: %+v", evs[0].KV)
	}
	if evs[0].PrevKV != nil {
		t.Fatalf("create event should have no PrevKv, got %+v", evs[0].PrevKV)
	}
	if evs[0].ID == "" {
		t.Fatal("create event missing stream ID (needed for catch-up seek + latency metric)")
	}

	// Event 1: overwrite PUT — new value plus the overwritten value as PrevKv.
	if evs[1].Type != "PUT" || evs[1].Rev != rev2 {
		t.Fatalf("overwrite event wrong: %+v", evs[1])
	}
	if evs[1].KV == nil || string(evs[1].KV.Value) != "v2" {
		t.Fatalf("overwrite event KV: %+v", evs[1].KV)
	}
	if evs[1].PrevKV == nil || string(evs[1].PrevKV.Value) != "v1" {
		t.Fatalf("overwrite event PrevKv: %+v", evs[1].PrevKV)
	}

	// Event 2: DELETE — carries the deleted object as PrevKv (the apiserver
	// requires this, see the v10 fix), stream ID set.
	if evs[2].Type != "DELETE" || evs[2].Rev != rev3 {
		t.Fatalf("delete event wrong: %+v", evs[2])
	}
	if evs[2].PrevKV == nil || string(evs[2].PrevKV.Value) != "v2" {
		t.Fatalf("delete event PrevKv: %+v", evs[2].PrevKV)
	}
	if evs[2].ID == "" {
		t.Fatal("delete event missing stream ID")
	}
}

// An absent delete bumps nothing and must emit no event — otherwise the
// dispatcher's contiguous-revision reorder buffer would see a phantom rev.
func TestEventSinkSilentOnAbsentDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	c := &eventCollector{}
	s.SetEventSink(c.sink)

	if _, _, err := s.Delete(ctx, "/missing"); err != nil {
		t.Fatalf("Delete absent: %v", err)
	}
	if evs := c.snapshot(); len(evs) != 0 {
		t.Fatalf("absent delete emitted %d events, want 0: %+v", len(evs), evs)
	}
}

// Txn (the path almost every Kubernetes write takes) must emit through the sink
// too: create, update-with-PrevKv, and delete-with-PrevKv.
func TestEventSinkEmitsForTxn(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	c := &eventCollector{}
	s.SetEventSink(c.sink)

	// Txn create (compare mod_revision == -1: key must not exist).
	res, err := s.Txn(ctx, "/t", -1, "PUT", []byte("a"), 0)
	if err != nil || !res.Succeeded {
		t.Fatalf("txn create: res=%+v err=%v", res, err)
	}
	// Txn update (compare mod_revision == previous).
	res, err = s.Txn(ctx, "/t", res.Revision, "PUT", []byte("b"), 0)
	if err != nil || !res.Succeeded {
		t.Fatalf("txn update: res=%+v err=%v", res, err)
	}
	// Txn delete.
	res, err = s.Txn(ctx, "/t", res.Revision, "DELETE", nil, 0)
	if err != nil || !res.Succeeded {
		t.Fatalf("txn delete: res=%+v err=%v", res, err)
	}

	evs := c.snapshot()
	if len(evs) != 3 {
		t.Fatalf("expected 3 txn events, got %d: %+v", len(evs), evs)
	}
	if evs[0].Type != "PUT" || evs[0].KV == nil || string(evs[0].KV.Value) != "a" || evs[0].PrevKV != nil {
		t.Fatalf("txn create event wrong: %+v kv=%+v", evs[0], evs[0].KV)
	}
	if evs[1].Type != "PUT" || string(evs[1].KV.Value) != "b" || evs[1].PrevKV == nil || string(evs[1].PrevKV.Value) != "a" {
		t.Fatalf("txn update event wrong: %+v", evs[1])
	}
	if evs[2].Type != "DELETE" || evs[2].PrevKV == nil || string(evs[2].PrevKV.Value) != "b" {
		t.Fatalf("txn delete event wrong: %+v", evs[2])
	}
	// Revisions strictly ascend across the three writes.
	if evs[0].Rev >= evs[1].Rev || evs[1].Rev >= evs[2].Rev {
		t.Fatalf("txn event revisions not ascending: %d %d %d", evs[0].Rev, evs[1].Rev, evs[2].Rev)
	}
}
