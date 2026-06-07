package server

import (
	"context"

	"github.com/trungnb2210/REtcd/store"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
)

// Store is the interface the server layer depends on.
// *store.RedisStore satisfies it; tests can supply a fake.
type Store interface {
	Put(ctx context.Context, key string, value []byte, leaseID int64) (int64, *store.KeyValue, error)
	Get(ctx context.Context, key string) (*store.KeyValue, error)
	Range(ctx context.Context, prefix string) (int64, []*store.KeyValue, error)
	Delete(ctx context.Context, key string) (int64, *store.KeyValue, error)
	Txn(ctx context.Context, key string, expectedModRevision int64, successOp string, newValue []byte, leaseID int64) (*store.TxnResult, error)
	CurrentRevision(ctx context.Context) (int64, error)
	BlockReadEvents(ctx context.Context, lastID string, maxCount int64) ([]store.Event, string, error)
	// SetEventSink registers a callback the store invokes the moment each write
	// commits, so the watch dispatcher can fan the event out in-process instead
	// of reading it back off the Redis stream.
	SetEventSink(func(store.Event))
}

// toProtoKV converts our internal KeyValue to the protobuf wire type.
// Shared by the KV and Watch services.
func toProtoKV(kv *store.KeyValue) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		Key:            []byte(kv.Key),
		Value:          kv.Value,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		Lease:          kv.Lease,
	}
}

// commonPrefix returns the longest common prefix of two strings.
// Used to derive a Redis scan prefix from an etcd range request.
func commonPrefix(a, b string) string {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return a[:i]
		}
	}
	return a[:n]
}
