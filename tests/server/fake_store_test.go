package server_test

// fakeStore is an in-memory implementation of server.Store used by unit tests.
// No Redis required — all state lives in plain Go maps.

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/trungnb2210/REtcd/store"
)

type fakeStore struct {
	mu     sync.Mutex
	data   map[string]*store.KeyValue
	rev    int64
	events []store.Event // append-only event log
	sink   func(store.Event)

	// Error injection for BlockReadEvents. failFirst causes the first N calls to
	// return a transient error; failAll causes every call to error. blockCalls
	// counts calls (for assertions).
	failFirst  int
	failAll    bool
	blockCalls int
}

func newFakeStore() *fakeStore {
	return &fakeStore{data: make(map[string]*store.KeyValue)}
}

// SetEventSink mirrors RedisStore: the watch dispatcher registers disp.ingest
// here so writes fan out in-process. Reads f.sink without the lock is fine — it
// is set once at server construction, before any write.
func (f *fakeStore) SetEventSink(sink func(store.Event)) { f.sink = sink }

func (f *fakeStore) incrRev() int64 {
	f.rev++
	return f.rev
}

func (f *fakeStore) Put(_ context.Context, key string, value []byte, leaseID int64) (int64, *store.KeyValue, error) {
	f.mu.Lock()

	prev := f.data[key]
	rev := f.incrRev()

	createRev := rev
	version := int64(1)
	if prev != nil {
		createRev = prev.CreateRevision
		version = prev.Version + 1
	}

	kv := &store.KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: createRev,
		ModRevision:    rev,
		Version:        version,
		Lease:          leaseID,
	}
	f.data[key] = kv
	ev := store.Event{Type: "PUT", Key: key, Rev: rev, KV: kv, PrevKV: prev}
	f.events = append(f.events, ev)
	sink := f.sink
	f.mu.Unlock()

	if sink != nil {
		sink(ev)
	}
	return rev, prev, nil
}

func (f *fakeStore) Get(_ context.Context, key string) (*store.KeyValue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data[key], nil
}

func (f *fakeStore) Range(_ context.Context, prefix string) (int64, []*store.KeyValue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keys []string
	for k := range f.data {
		if prefix == "" || k == prefix || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var out []*store.KeyValue
	for _, k := range keys {
		out = append(out, f.data[k])
	}
	return f.rev, out, nil
}

func (f *fakeStore) Delete(_ context.Context, key string) (int64, *store.KeyValue, error) {
	f.mu.Lock()
	prev := f.data[key]
	if prev == nil {
		rev := f.rev
		f.mu.Unlock()
		return rev, nil, nil
	}
	rev := f.incrRev()
	delete(f.data, key)
	ev := store.Event{Type: "DELETE", Key: key, Rev: rev, PrevKV: prev}
	f.events = append(f.events, ev)
	sink := f.sink
	f.mu.Unlock()

	if sink != nil {
		sink(ev)
	}
	return rev, prev, nil
}

func (f *fakeStore) Txn(_ context.Context, key string, expectedModRevision int64, successOp string, newValue []byte, leaseID int64) (*store.TxnResult, error) {
	f.mu.Lock()

	current := f.data[key]
	var currentModRev int64
	if current != nil {
		currentModRev = current.ModRevision
	}

	// Check compare condition.
	if expectedModRevision == -1 {
		if current != nil {
			res := &store.TxnResult{Succeeded: false, Revision: f.rev, Current: current}
			f.mu.Unlock()
			return res, nil
		}
	} else if currentModRev != expectedModRevision {
		res := &store.TxnResult{Succeeded: false, Revision: f.rev, Current: current}
		f.mu.Unlock()
		return res, nil
	}

	rev := f.incrRev()
	var ev store.Event
	if successOp == "DELETE" {
		delete(f.data, key)
		ev = store.Event{Type: "DELETE", Key: key, Rev: rev, PrevKV: current}
	} else {
		createRev := rev
		version := int64(1)
		if current != nil {
			createRev = current.CreateRevision
			version = current.Version + 1
		}
		kv := &store.KeyValue{
			Key:            key,
			Value:          newValue,
			CreateRevision: createRev,
			ModRevision:    rev,
			Version:        version,
			Lease:          leaseID,
		}
		f.data[key] = kv
		ev = store.Event{Type: "PUT", Key: key, Rev: rev, KV: kv, PrevKV: current}
	}
	f.events = append(f.events, ev)
	sink := f.sink
	f.mu.Unlock()

	if sink != nil {
		sink(ev)
	}
	return &store.TxnResult{Succeeded: true, Revision: rev}, nil
}

func (f *fakeStore) CurrentRevision(_ context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rev, nil
}

func (f *fakeStore) BlockReadEvents(_ context.Context, lastID string, maxCount int64) ([]store.Event, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blockCalls++
	if f.failAll || f.blockCalls <= f.failFirst {
		return nil, lastID, fmt.Errorf("injected transient backend error")
	}
	if len(f.events) == 0 {
		return nil, lastID, nil
	}
	// lastID is a simple integer index encoded as a string ("0" = from start).
	start := 0
	if lastID != "0" {
		fmt.Sscanf(lastID, "%d", &start)
	}
	end := len(f.events)
	if int64(end-start) > maxCount {
		end = start + int(maxCount)
	}
	events := f.events[start:end]
	newLastID := fmt.Sprintf("%d", end)
	return events, newLastID, nil
}
