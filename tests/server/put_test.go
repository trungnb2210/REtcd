package server_test

// Unit tests for server/put.go

import (
	"context"
	"testing"

	"github.com/trungnb2210/REtcd/server"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestPutIncreasesRevision(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	r1, _ := kv.Put(ctx, &pb.PutRequest{Key: []byte("/k"), Value: []byte("a")})
	r2, _ := kv.Put(ctx, &pb.PutRequest{Key: []byte("/k"), Value: []byte("b")})

	if r1.Header.Revision >= r2.Header.Revision {
		t.Errorf("revision should increase: %d -> %d", r1.Header.Revision, r2.Header.Revision)
	}
}

func TestPutReturnsPrevKvWhenRequested(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	kv.Put(ctx, &pb.PutRequest{Key: []byte("/k"), Value: []byte("old")})
	resp, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("/k"), Value: []byte("new"), PrevKv: true})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if resp.PrevKv == nil {
		t.Fatal("expected PrevKv to be populated")
	}
	if string(resp.PrevKv.Value) != "old" {
		t.Errorf("PrevKv.Value: got %q, want %q", resp.PrevKv.Value, "old")
	}
}

func TestPutDoesNotReturnPrevKvWhenNotRequested(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	kv.Put(ctx, &pb.PutRequest{Key: []byte("/k"), Value: []byte("old")})
	resp, _ := kv.Put(ctx, &pb.PutRequest{Key: []byte("/k"), Value: []byte("new"), PrevKv: false})
	if resp.PrevKv != nil {
		t.Error("PrevKv should be nil when not requested")
	}
}

func TestPutFirstWriteHasNoPrevKv(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	resp, _ := kv.Put(ctx, &pb.PutRequest{Key: []byte("/new"), Value: []byte("v"), PrevKv: true})
	if resp.PrevKv != nil {
		t.Error("PrevKv should be nil for a brand-new key")
	}
}
