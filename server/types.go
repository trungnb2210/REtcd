package server

import (
	"context"

	"github.com/trungnb2210/REtcd/store"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
)

// Store is the interface the server layer depends on.
// *store.RedisStore satisfies it; tests can supply a fake.
type Store interface {
	Put(ctx context.Context, key string, value []byte, leaseID int64) (int64, *store.KeyValue, error)
	Get(ctx context.Context, key string) (*store.KeyValue, error)
	Range(ctx context.Context, prefix string) ([]*store.KeyValue, error)
	Delete(ctx context.Context, key string) (int64, *store.KeyValue, error)
	Txn(ctx context.Context, key string, expectedModRevision int64, successOp string, newValue []byte, leaseID int64) (*store.TxnResult, error)
	CurrentRevision(ctx context.Context) (int64, error)
	BlockReadEvents(ctx context.Context, lastID string, maxCount int64) ([]store.Event, string, error)
}

// KVServer implements the etcd v3 KV gRPC service backed by Redis.
type KVServer struct {
	pb.UnimplementedKVServer
	store Store
}

func NewKVServer(s Store) *KVServer {
	return &KVServer{store: s}
}

// header builds a response header stamped with the current revision.
func (s *KVServer) header(ctx context.Context) *pb.ResponseHeader {
	rev, _ := s.store.CurrentRevision(ctx)
	return &pb.ResponseHeader{
		ClusterId: 1,
		MemberId:  1,
		Revision:  rev,
	}
}

// toProtoKV converts our internal KeyValue to the protobuf wire type.
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
