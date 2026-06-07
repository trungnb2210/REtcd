package server

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// KVServer implements the etcd v3 KV gRPC service backed by Redis.
// Its RPC methods are split across kv_put.go, kv_range.go, kv_delete.go,
// kv_txn.go, and compact.go.
type KVServer struct {
	pb.UnimplementedKVServer
	store Store
}

// NewKVServer returns a KVServer backed by the given Store.
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
