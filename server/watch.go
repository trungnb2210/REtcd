package server

import (
	"github.com/trungnb2210/REtcd/store"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

type WatchServer struct {
	pb.UnimplementedWatchServer
	store *store.RedisStore
}

func NewWatchServer(store *store.RedisStore) *WatchServer {
	return &WatchServer{store: store}
}

func (s *WatchServer) Watch(stream pb.Watch_WatchServer) error {
	// TODO: implement
	<-stream.Context().Done()
	return nil
}
