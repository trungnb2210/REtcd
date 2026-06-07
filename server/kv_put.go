package server

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Put stores a key-value pair unconditionally.
// Every Kubernetes resource creation that bypasses Txn comes through here,
// though in practice most writes use Txn (compare-and-swap).
//
// Atomicity: the revision increment, key write, and event append all run inside
// a single Redis Lua script (write.lua), so they apply as one unit — no torn
// state and event-stream order matches revision order even under concurrent
// writers. The remaining durability caveat is Redis-level: a single AOF-backed
// instance can lose up to one fsync window (~1s with appendfsync everysec) of
// acknowledged writes on a host crash. Worth noting as a limitation.
func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// TODO: ignore_value/ignore_lease not supported
	rev, prevKV, err := s.store.Put(ctx, string(req.Key), req.Value, req.Lease)
	if err != nil {
		return nil, err
	}

	resp := &pb.PutResponse{
		Header: &pb.ResponseHeader{
			ClusterId: 1,
			MemberId:  1,
			Revision:  rev,
		},
	}

	if req.PrevKv && prevKV != nil {
		resp.PrevKv = toProtoKV(prevKV)
	}

	return resp, nil
}
