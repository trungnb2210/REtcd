package server

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// MaintenanceServer implements the etcd v3 Maintenance gRPC service. Only the
// Status RPC the Kubernetes API server depends on is backed by real data.
type MaintenanceServer struct {
	pb.UnimplementedMaintenanceServer
	store Store
}

// NewMaintenanceServer returns a MaintenanceServer backed by the given Store.
func NewMaintenanceServer(s Store) *MaintenanceServer {
	return &MaintenanceServer{store: s}
}

// Status reports server health and the current revision; other fields are
// synthesized to satisfy clients that expect an etcd cluster.
func (s *MaintenanceServer) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	rev, _ := s.store.CurrentRevision(ctx)
	return &pb.StatusResponse{
		Header:    &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: rev},
		Version:   "3.5.24",
		DbSize:    0,
		Leader:    1,
		RaftIndex: uint64(rev),
		RaftTerm:  1,
	}, nil
}
