package main

import (
	"context"
	"log"
	"net"

	"github.com/trungnb2210/REtcd/server"
	"github.com/trungnb2210/REtcd/store"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":2379")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	rdb := store.NewRedisStore("localhost:6379")
	rdb.StartLeaseReaper(context.Background())

	grpcServer := grpc.NewServer()
	pb.RegisterKVServer(grpcServer, server.NewKVServer(rdb))
	pb.RegisterWatchServer(grpcServer, server.NewWatchServer(rdb))
	pb.RegisterLeaseServer(grpcServer, server.NewLeaseServer(rdb))

	log.Println("REtcd listening on :2379")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
