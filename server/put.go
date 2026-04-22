package server

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Put stores a key-value pair unconditionally.
// Every Kubernetes resource creation that bypasses Txn comes through here,
// though in practice most writes use Txn (compare-and-swap).
//
// Correctness note: the revision increment and the store are not perfectly
// atomic (Redis INCR + pipeline). Two concurrent Puts will get distinct
// revisions, but a crash between INCR and SET would leave the revision
// incremented with no matching key written. Acceptable for a single-node FYP;
// worth noting as a limitation in the write-up.
func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
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
