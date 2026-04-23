package server_test

// Unit tests for server/delete.go

import (
	"context"
	"testing"

	"github.com/trungnb2210/REtcd/server"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestDeleteSingleKeyExisting(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	kv.Put(ctx, &pb.PutRequest{Key: []byte("/del"), Value: []byte("bye")})

	resp, err := kv.DeleteRange(ctx, &pb.DeleteRangeRequest{Key: []byte("/del")})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if resp.Deleted != 1 {
		t.Errorf("expected deleted=1, got %d", resp.Deleted)
	}

	get, _ := kv.Range(ctx, &pb.RangeRequest{Key: []byte("/del")})
	if get.Count != 0 {
		t.Error("key still present after delete")
	}
}

func TestDeleteSingleKeyMissingIsNoOp(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	resp, err := kv.DeleteRange(ctx, &pb.DeleteRangeRequest{Key: []byte("/ghost")})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if resp.Deleted != 0 {
		t.Errorf("expected deleted=0 for missing key, got %d", resp.Deleted)
	}
}

func TestDeleteRangeRemovesOnlyMatchingKeys(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	kv.Put(ctx, &pb.PutRequest{Key: []byte("/dr/a"), Value: []byte("1")})
	kv.Put(ctx, &pb.PutRequest{Key: []byte("/dr/b"), Value: []byte("2")})
	kv.Put(ctx, &pb.PutRequest{Key: []byte("/other"), Value: []byte("3")})

	resp, err := kv.DeleteRange(ctx, &pb.DeleteRangeRequest{
		Key:      []byte("/dr/"),
		RangeEnd: []byte("/dr0"),
	})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if resp.Deleted != 2 {
		t.Errorf("expected deleted=2, got %d", resp.Deleted)
	}

	// /other must be untouched.
	get, _ := kv.Range(ctx, &pb.RangeRequest{Key: []byte("/other")})
	if get.Count != 1 {
		t.Error("/other was incorrectly deleted")
	}
}

func TestDeleteRangeEmptyRangeIsNoOp(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	resp, err := kv.DeleteRange(ctx, &pb.DeleteRangeRequest{
		Key:      []byte("/empty/"),
		RangeEnd: []byte("/empty0"),
	})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if resp.Deleted != 0 {
		t.Errorf("expected deleted=0 for empty range, got %d", resp.Deleted)
	}
}
