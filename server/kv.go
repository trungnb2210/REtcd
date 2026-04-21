package server

import (
	"context"

	"github.com/trungnb2210/REtcd/store"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

type KVServer struct {
	pb.UnimplementedKVServer
	store *store.RedisStore
}

func NewKVServer(store *store.RedisStore) *KVServer {
	return &KVServer{store: store}
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// TODO: implement
	return &pb.PutResponse{}, nil
}

func (s *KVServer) Range(ctx context.Context, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	// TODO: implement
	return &pb.RangeResponse{}, nil
}

func (s *KVServer) DeleteRange(ctx context.Context, req *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	// TODO: implement
	return &pb.DeleteRangeResponse{}, nil
}

func (s *KVServer) Txn(ctx context.Context, req *pb.TxnRequest) (*pb.TxnResponse, error) {
	// TODO: implement
	return &pb.TxnResponse{}, nil
}

func (s *KVServer) Compact(ctx context.Context, req *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	// stub — not implemented
	return &pb.CompactionResponse{}, nil
}
