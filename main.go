package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"github.com/trungnb2210/REtcd/server"
	"github.com/trungnb2210/REtcd/store"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type versionInfo struct {
	EtcdServer  string `json:"etcdserver"`
	EtcdCluster string `json:"etcdcluster"`
}

func versionHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(versionInfo{
		EtcdServer:  "3.5.24",
		EtcdCluster: "3.5.24",
	})
}

func main() {
	listenAddr := envOr("LISTEN_ADDR", ":2379")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	rdb := store.NewRedisStore(redisAddr)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(server.UnaryMetricsInterceptor),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    2 * time.Hour,
			Timeout: 20 * time.Second,
		}),
	)

	// Publish go-redis pool counters every few seconds for the /metrics endpoint.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			st := rdb.PoolStats()
			server.RecordRedisPool(st.TotalConns, st.IdleConns, st.Timeouts, st.Misses)
		}
	}()
	pb.RegisterKVServer(grpcServer, server.NewKVServer(rdb))
	pb.RegisterWatchServer(grpcServer, server.NewWatchServer(rdb))
	pb.RegisterLeaseServer(grpcServer, server.NewLeaseServer(rdb))
	pb.RegisterMaintenanceServer(grpcServer, server.NewMaintenanceServer(rdb))

	// Start the lease reaper after NewWatchServer has registered the event sink,
	// so lease-expiry deletes fan out to live watches in-process rather than being
	// missed during the startup window.
	rdb.StartLeaseReaper(context.Background())

	m := cmux.New(lis)
	grpcL := m.MatchWithWriters(
		cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"),
	)
	httpL := m.Match(cmux.HTTP1Fast())

	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/version", versionHandler)
	httpMux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"health":"true"}`))
	})
	httpMux.Handle("/metrics", promhttp.Handler())
	httpServer := &http.Server{Handler: httpMux}

	go func() {
		if err := grpcServer.Serve(grpcL); err != nil && err != cmux.ErrServerClosed {
			log.Fatalf("grpc serve: %v", err)
		}
	}()
	go func() {
		if err := httpServer.Serve(httpL); err != nil && err != cmux.ErrServerClosed && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	log.Printf("REtcd listening on %s, redis=%s", listenAddr, redisAddr)
	if err := m.Serve(); err != nil {
		log.Fatalf("cmux serve: %v", err)
	}
}
