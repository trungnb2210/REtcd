package server

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Compact is a stub. The Kubernetes API server calls this periodically to
// ask etcd to discard old revisions. We acknowledge it but do nothing —
// Redis Streams retain all events until explicitly trimmed, which is
// acceptable for the FYP scope.
func (s *KVServer) Compact(ctx context.Context, req *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	return &pb.CompactionResponse{Header: s.header(ctx)}, nil
}
