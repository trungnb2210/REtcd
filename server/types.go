package server

import (
	"context"

	"github.com/trungnb2210/REtcd/store"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
)

// KVServer implements the etcd v3 KV gRPC service backed by Redis.
type KVServer struct {
	pb.UnimplementedKVServer
	store *store.RedisStore
}

func NewKVServer(store *store.RedisStore) *KVServer {
	return &KVServer{store: store}
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
