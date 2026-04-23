package server_test

// Unit tests for server/txn.go

import (
	"context"
	"testing"

	"github.com/trungnb2210/REtcd/server"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// createReq builds a Kubernetes CREATE Txn: if mod_revision==0 then Put.
func createReq(key, value string) *pb.TxnRequest {
	return &pb.TxnRequest{
		Compare: []*pb.Compare{{
			Target:      pb.Compare_MOD,
			Result:      pb.Compare_EQUAL,
			Key:         []byte(key),
			TargetUnion: &pb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*pb.RequestOp{{
			Request: &pb.RequestOp_RequestPut{
				RequestPut: &pb.PutRequest{Key: []byte(key), Value: []byte(value)},
			},
		}},
	}
}

// updateReq builds a Kubernetes UPDATE Txn: if mod_revision==rev then Put, else Range.
func updateReq(key string, rev int64, value string) *pb.TxnRequest {
	return &pb.TxnRequest{
		Compare: []*pb.Compare{{
			Target:      pb.Compare_MOD,
			Result:      pb.Compare_EQUAL,
			Key:         []byte(key),
			TargetUnion: &pb.Compare_ModRevision{ModRevision: rev},
		}},
		Success: []*pb.RequestOp{{
			Request: &pb.RequestOp_RequestPut{
				RequestPut: &pb.PutRequest{Key: []byte(key), Value: []byte(value)},
			},
		}},
		Failure: []*pb.RequestOp{{
			Request: &pb.RequestOp_RequestRange{
				RequestRange: &pb.RangeRequest{Key: []byte(key)},
			},
		}},
	}
}

func TestTxnCreateSucceedsForNewKey(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	resp, err := kv.Txn(ctx, createReq("/new", "v"))
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("expected Txn to succeed for new key")
	}

	get, _ := kv.Range(ctx, &pb.RangeRequest{Key: []byte("/new")})
	if get.Count != 1 || string(get.Kvs[0].Value) != "v" {
		t.Error("key not written after successful create Txn")
	}
}

func TestTxnCreateFailsIfKeyExists(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	kv.Put(ctx, &pb.PutRequest{Key: []byte("/exists"), Value: []byte("old")})

	resp, err := kv.Txn(ctx, createReq("/exists", "new"))
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if resp.Succeeded {
		t.Fatal("expected Txn to fail when key already exists")
	}

	// Value must remain unchanged.
	get, _ := kv.Range(ctx, &pb.RangeRequest{Key: []byte("/exists")})
	if string(get.Kvs[0].Value) != "old" {
		t.Error("create Txn must not overwrite existing key")
	}
}

func TestTxnUpdateSucceedsWithCorrectRevision(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	putResp, _ := kv.Put(ctx, &pb.PutRequest{Key: []byte("/upd"), Value: []byte("v1")})
	rev := putResp.Header.Revision

	resp, err := kv.Txn(ctx, updateReq("/upd", rev, "v2"))
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("expected update Txn to succeed")
	}

	get, _ := kv.Range(ctx, &pb.RangeRequest{Key: []byte("/upd")})
	if string(get.Kvs[0].Value) != "v2" {
		t.Errorf("value after update: got %q, want v2", get.Kvs[0].Value)
	}
}

func TestTxnUpdateFailsOnStaleRevision(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	putResp, _ := kv.Put(ctx, &pb.PutRequest{Key: []byte("/stale"), Value: []byte("v1")})
	oldRev := putResp.Header.Revision
	kv.Put(ctx, &pb.PutRequest{Key: []byte("/stale"), Value: []byte("v2")}) // bumps revision

	resp, err := kv.Txn(ctx, updateReq("/stale", oldRev, "v3"))
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if resp.Succeeded {
		t.Fatal("expected Txn to fail on stale revision")
	}

	// Failure branch must return current value so caller can retry.
	if len(resp.Responses) == 0 {
		t.Fatal("failure response should carry current key value")
	}
	rangeResp := resp.Responses[0].GetResponseRange()
	if rangeResp == nil || rangeResp.Count == 0 {
		t.Fatal("expected range response in failure branch")
	}
	if string(rangeResp.Kvs[0].Value) != "v2" {
		t.Errorf("failure response: got %q, want v2", rangeResp.Kvs[0].Value)
	}
}

func TestTxnUpdateIncrementsRevision(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	putResp, _ := kv.Put(ctx, &pb.PutRequest{Key: []byte("/k"), Value: []byte("v1")})
	rev := putResp.Header.Revision

	txnResp, _ := kv.Txn(ctx, updateReq("/k", rev, "v2"))
	if txnResp.Header.Revision <= rev {
		t.Errorf("revision should increase after update: was %d, now %d", rev, txnResp.Header.Revision)
	}
}

func TestTxnCreateDoesNotIncrementRevisionOnFailure(t *testing.T) {
	kv := server.NewKVServer(newFakeStore())
	ctx := context.Background()

	kv.Put(ctx, &pb.PutRequest{Key: []byte("/k"), Value: []byte("v")})
	revBefore, _ := kv.Range(ctx, &pb.RangeRequest{Key: []byte("/k")})
	revSnapshot := revBefore.Header.Revision

	kv.Txn(ctx, createReq("/k", "duplicate")) // must fail

	revAfter, _ := kv.Range(ctx, &pb.RangeRequest{Key: []byte("/k")})
	if revAfter.Header.Revision != revSnapshot {
		t.Errorf("failed Txn must not change revision: before=%d after=%d", revSnapshot, revAfter.Header.Revision)
	}
}
