package server_test

// Unit tests for server/range.go

import (
	"context"
	"testing"

	"github.com/trungnb2210/REtcd/server"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestRangeSingleKeyFound(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	kv.Put(ctx, &pb.PutRequest{Key: []byte("/a"), Value: []byte("1")})

	resp, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte("/a")})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if resp.Count != 1 || len(resp.Kvs) != 1 {
		t.Fatalf("expected 1 result, got %d", resp.Count)
	}
	if string(resp.Kvs[0].Value) != "1" {
		t.Errorf("value: got %q, want %q", resp.Kvs[0].Value, "1")
	}
}

func TestRangeSingleKeyMissing(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	resp, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte("/ghost")})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("expected 0 results for missing key, got %d", resp.Count)
	}
}

func TestRangePrefixReturnsOnlyMatchingKeys(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	kv.Put(ctx, &pb.PutRequest{Key: []byte("/p/a"), Value: []byte("1")})
	kv.Put(ctx, &pb.PutRequest{Key: []byte("/p/b"), Value: []byte("2")})
	kv.Put(ctx, &pb.PutRequest{Key: []byte("/q/c"), Value: []byte("3")})

	// etcd prefix "/p/" encoded as key="/p/" rangeEnd="/p0"
	resp, err := kv.Range(ctx, &pb.RangeRequest{
		Key:      []byte("/p/"),
		RangeEnd: []byte("/p0"),
	})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if resp.Count != 2 {
		t.Errorf("expected 2 results, got %d", resp.Count)
	}
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if key != "/p/a" && key != "/p/b" {
			t.Errorf("unexpected key in range: %q", key)
		}
	}
}

func TestRangeExcludesRangeEnd(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	kv.Put(ctx, &pb.PutRequest{Key: []byte("/p/a"), Value: []byte("1")})
	kv.Put(ctx, &pb.PutRequest{Key: []byte("/p0"),  Value: []byte("boundary")}) // == rangeEnd, must be excluded

	resp, err := kv.Range(ctx, &pb.RangeRequest{
		Key:      []byte("/p/"),
		RangeEnd: []byte("/p0"),
	})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	for _, kv := range resp.Kvs {
		if string(kv.Key) == "/p0" {
			t.Error("rangeEnd itself must not appear in results")
		}
	}
}

func TestRangeCountOnly(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	kv.Put(ctx, &pb.PutRequest{Key: []byte("/c/1"), Value: []byte("a")})
	kv.Put(ctx, &pb.PutRequest{Key: []byte("/c/2"), Value: []byte("b")})

	resp, err := kv.Range(ctx, &pb.RangeRequest{
		Key:       []byte("/c/"),
		RangeEnd:  []byte("/c0"),
		CountOnly: true,
	})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if resp.Count != 2 {
		t.Errorf("expected count=2, got %d", resp.Count)
	}
	if len(resp.Kvs) != 0 {
		t.Errorf("CountOnly should return no Kvs, got %d", len(resp.Kvs))
	}
}
