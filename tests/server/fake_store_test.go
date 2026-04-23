package server_test

// fakeStore is an in-memory implementation of server.Store used by unit tests.
// No Redis required — all state lives in plain Go maps.

import (
	"context"
	"fmt"
	"sync"

	"github.com/trungnb2210/REtcd/store"
)

type fakeStore struct {
	mu     sync.Mutex
	data   map[string]*store.KeyValue
	rev    int64
	events []store.Event // append-only event log
}

func newFakeStore() *fakeStore {
	return &fakeStore{data: make(map[string]*store.KeyValue)}
}

func (f *fakeStore) incrRev() int64 {
	f.rev++
	return f.rev
}

func (f *fakeStore) Put(_ context.Context, key string, value []byte, leaseID int64) (int64, *store.KeyValue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

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
	f.events = append(f.events, store.Event{Type: "PUT", Key: key, Rev: rev, KV: kv})
	return rev, prev, nil
}

func (f *fakeStore) Get(_ context.Context, key string) (*store.KeyValue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data[key], nil
}

func (f *fakeStore) Range(_ context.Context, prefix string) ([]*store.KeyValue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*store.KeyValue
	for k, v := range f.data {
		if prefix == "" || k == prefix || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, v)
		}
	}
	return out, nil
}

func (f *fakeStore) Delete(_ context.Context, key string) (int64, *store.KeyValue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prev := f.data[key]
	if prev == nil {
		return f.rev, nil, nil
	}
	rev := f.incrRev()
	delete(f.data, key)
	f.events = append(f.events, store.Event{Type: "DELETE", Key: key, Rev: rev})
	return rev, prev, nil
}

func (f *fakeStore) Txn(_ context.Context, key string, expectedModRevision int64, successOp string, newValue []byte, leaseID int64) (*store.TxnResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	current := f.data[key]
	var currentModRev int64
	if current != nil {
		currentModRev = current.ModRevision
	}

	// Check compare condition.
	if expectedModRevision == -1 {
		if current != nil {
			return &store.TxnResult{Succeeded: false, Revision: f.rev, Current: current}, nil
		}
	} else if currentModRev != expectedModRevision {
		return &store.TxnResult{Succeeded: false, Revision: f.rev, Current: current}, nil
	}

	rev := f.incrRev()
	if successOp == "DELETE" {
		delete(f.data, key)
		f.events = append(f.events, store.Event{Type: "DELETE", Key: key, Rev: rev})
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
		f.events = append(f.events, store.Event{Type: "PUT", Key: key, Rev: rev, KV: kv})
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
