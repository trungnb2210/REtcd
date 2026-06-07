package server

import (
	"context"

	"github.com/trungnb2210/REtcd/store"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// LeaseServer implements the etcd v3 Lease gRPC service backed by Redis.
type LeaseServer struct {
	pb.UnimplementedLeaseServer
	store *store.RedisStore
}

// NewLeaseServer returns a LeaseServer backed by the given Redis store.
func NewLeaseServer(store *store.RedisStore) *LeaseServer {
	return &LeaseServer{store: store}
}

// LeaseGrant creates a new lease with the requested TTL (defaulting to 30s).
func (s *LeaseServer) LeaseGrant(ctx context.Context, req *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	ttl := req.TTL
	if ttl <= 0 {
		ttl = 30
	}
	id, err := s.store.LeaseGrant(ctx, ttl)
	if err != nil {
		return nil, err
	}
	return &pb.LeaseGrantResponse{
		Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1},
		ID:     id,
		TTL:    ttl,
	}, nil
}

// LeaseRevoke revokes a lease and deletes all keys attached to it.
func (s *LeaseServer) LeaseRevoke(ctx context.Context, req *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	if err := s.store.LeaseRevoke(ctx, req.ID); err != nil {
		return nil, err
	}
	return &pb.LeaseRevokeResponse{
		Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1},
	}, nil
}

// LeaseKeepAlive is the bidirectional stream that renews leases; it replies with
// TTL=0 when a lease no longer exists to signal expiry to the client.
func (s *LeaseServer) LeaseKeepAlive(stream pb.Lease_LeaseKeepAliveServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		ttl, err := s.store.LeaseRenew(stream.Context(), req.ID)
		if err != nil {
			// Lease not found — signal expiry with TTL=0.
			_ = stream.Send(&pb.LeaseKeepAliveResponse{
				Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1},
				ID:     req.ID,
				TTL:    0,
			})
			continue
		}
		if err := stream.Send(&pb.LeaseKeepAliveResponse{
			Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1},
			ID:     req.ID,
			TTL:    ttl,
		}); err != nil {
			return err
		}
	}
}

// LeaseTimeToLive reports the granted and remaining TTL for a lease.
func (s *LeaseServer) LeaseTimeToLive(ctx context.Context, req *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	granted, remaining, err := s.store.LeaseTimeToLive(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	return &pb.LeaseTimeToLiveResponse{
		Header:     &pb.ResponseHeader{ClusterId: 1, MemberId: 1},
		ID:         req.ID,
		TTL:        remaining,
		GrantedTTL: granted,
	}, nil
}

// LeaseLeases lists all active leases. It is a stub returning an empty response.
func (s *LeaseServer) LeaseLeases(ctx context.Context, req *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error) {
	return &pb.LeaseLeasesResponse{}, nil
}
