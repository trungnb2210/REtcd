package server

import (
	"context"

	"github.com/trungnb2210/REtcd/store"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

type LeaseServer struct {
	pb.UnimplementedLeaseServer
	store *store.RedisStore
}

func NewLeaseServer(store *store.RedisStore) *LeaseServer {
	return &LeaseServer{store: store}
}

func (s *LeaseServer) LeaseGrant(ctx context.Context, req *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	// TODO: implement
	return &pb.LeaseGrantResponse{}, nil
}

func (s *LeaseServer) LeaseRevoke(ctx context.Context, req *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	// TODO: implement
	return &pb.LeaseRevokeResponse{}, nil
}

func (s *LeaseServer) LeaseKeepAlive(stream pb.Lease_LeaseKeepAliveServer) error {
	// TODO: implement
	<-stream.Context().Done()
	return nil
}

func (s *LeaseServer) LeaseTimeToLive(ctx context.Context, req *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	// TODO: implement
	return &pb.LeaseTimeToLiveResponse{}, nil
}

func (s *LeaseServer) LeaseLeases(ctx context.Context, req *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error) {
	return &pb.LeaseLeasesResponse{}, nil
}
