package server

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

type MaintenanceServer struct {
	pb.UnimplementedMaintenanceServer
	store Store
}

func NewMaintenanceServer(s Store) *MaintenanceServer {
	return &MaintenanceServer{store: s}
}

func (s *MaintenanceServer) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	rev, _ := s.store.CurrentRevision(ctx)
	return &pb.StatusResponse{
		Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: rev},
		Version: "3.5.0",
		DbSize:  0,
		Leader:  1,
		RaftIndex: uint64(rev),
		RaftTerm:  1,
	}, nil
}
