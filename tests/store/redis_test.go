package store_test

// Integration tests for store/redis.go.
// These require a live Redis instance. All tests are skipped automatically
// when Redis is not reachable, so the test suite always passes in CI without Redis.
//
// Run against a local Redis:
//   docker run --rm -p 6379:6379 redis:7
//   go test ./tests/store/...

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/trungnb2210/REtcd/store"
)

// newTestStore connects to Redis DB 15, flushes it, and returns a RedisStore.
// The test is skipped if Redis is not reachable.
func newTestStore(t *testing.T) *store.RedisStore {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 15})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}
	client.FlushDB(ctx)
	client.Close()

	s := store.NewRedisStoreDB("localhost:6379", 15)
	t.Cleanup(func() {
		c := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 15})
		c.FlushDB(context.Background())
		c.Close()
	})
	return s
}

// --- Put / Get ---

func TestPutAndGet(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev, _, err := s.Put(ctx, "/foo", []byte("bar"), 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if rev != 1 {
		t.Errorf("expected revision 1, got %d", rev)
	}

	kv, err := s.Get(ctx, "/foo")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if kv == nil {
		t.Fatal("Get returned nil")
	}
	if string(kv.Value) != "bar" {
		t.Errorf("value: got %q, want %q", kv.Value, "bar")
	}
	if kv.CreateRevision != 1 || kv.ModRevision != 1 || kv.Version != 1 {
		t.Errorf("revisions: create=%d mod=%d version=%d", kv.CreateRevision, kv.ModRevision, kv.Version)
	}
}

func TestGetMissing(t *testing.T) {
	s := newTestStore(t)
	kv, err := s.Get(context.Background(), "/does-not-exist")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if kv != nil {
		t.Errorf("expected nil for missing key, got %+v", kv)
	}
}

func TestPutUpdatesVersionAndRevision(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Put(ctx, "/k", []byte("v1"), 0)
	rev2, _, err := s.Put(ctx, "/k", []byte("v2"), 0)
	if err != nil {
		t.Fatalf("second Put: %v", err)
	}

	kv, _ := s.Get(ctx, "/k")
	if string(kv.Value) != "v2" {
		t.Errorf("value: got %q, want %q", kv.Value, "v2")
	}
	if kv.CreateRevision != 1 {
		t.Errorf("create_revision should stay 1, got %d", kv.CreateRevision)
	}
	if kv.ModRevision != rev2 {
		t.Errorf("mod_revision: got %d, want %d", kv.ModRevision, rev2)
	}
	if kv.Version != 2 {
		t.Errorf("version: got %d, want 2", kv.Version)
	}
}

func TestRevisionMonotonicallyIncreases(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	var prev int64
	for i := range 5 {
		rev, _, err := s.Put(ctx, fmt.Sprintf("/k%d", i), []byte("v"), 0)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		if rev <= prev {
			t.Errorf("revision went backwards: %d -> %d", prev, rev)
		}
		prev = rev
	}
}

// --- Range ---

func TestRange(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Put(ctx, "/a/1", []byte("one"), 0)
	s.Put(ctx, "/a/2", []byte("two"), 0)
	s.Put(ctx, "/b/1", []byte("other"), 0)

	_, kvs, err := s.Range(ctx, "/a/")
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(kvs) != 2 {
		t.Errorf("expected 2 results, got %d", len(kvs))
	}
	for _, kv := range kvs {
		if kv.Key != "/a/1" && kv.Key != "/a/2" {
			t.Errorf("unexpected key in range result: %q", kv.Key)
		}
	}
}

func TestRangeReturnsKeysInLexicographicOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Put(ctx, "/a/2", []byte("two"), 0)
	s.Put(ctx, "/a/10", []byte("ten"), 0)
	s.Put(ctx, "/a/1", []byte("one"), 0)
	s.Put(ctx, "/a0", []byte("boundary"), 0)

	_, kvs, err := s.Range(ctx, "/a/")
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	got := make([]string, len(kvs))
	for i, kv := range kvs {
		got[i] = kv.Key
	}
	want := []string{"/a/1", "/a/10", "/a/2"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("range order = %v, want %v", got, want)
	}
}

// --- Delete ---

func TestDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Put(ctx, "/del", []byte("bye"), 0)
	_, prev, err := s.Delete(ctx, "/del")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if prev == nil {
		t.Fatal("expected previous value, got nil")
	}
	if string(prev.Value) != "bye" {
		t.Errorf("prev value: got %q, want %q", prev.Value, "bye")
	}

	kv, _ := s.Get(ctx, "/del")
	if kv != nil {
		t.Error("key still exists after delete")
	}
}

func TestDeleteMissing(t *testing.T) {
	s := newTestStore(t)
	_, prev, err := s.Delete(context.Background(), "/ghost")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if prev != nil {
		t.Errorf("expected nil prev for missing key, got %+v", prev)
	}
}

// TestDeleteEventCarriesPrevKV guards the cold-start watch-churn fix: a DELETE
// must emit a watch event whose PrevKV is the deleted object. Without it the
// apiserver rejects the event (PrevKv=nil) and tears down its watch-cache.
func TestDeleteEventCarriesPrevKV(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Put(ctx, "/dk", []byte("hello"), 0)
	if _, _, err := s.Delete(ctx, "/dk"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	events, _, err := s.ReadEvents(ctx, "0", 0, 100)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var del *store.Event
	for i := range events {
		if events[i].Type == "DELETE" && events[i].Key == "/dk" {
			del = &events[i]
		}
	}
	if del == nil {
		t.Fatal("no DELETE event for /dk")
	}
	if del.PrevKV == nil {
		t.Fatal("DELETE event PrevKV is nil — apiserver would reject it and relist")
	}
	if string(del.PrevKV.Value) != "hello" {
		t.Errorf("DELETE PrevKV value: got %q, want %q", del.PrevKV.Value, "hello")
	}
}

// --- Txn ---

func TestTxnCreateSucceeds(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	res, err := s.Txn(ctx, "/new", -1, "PUT", []byte("created"), 0)
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !res.Succeeded {
		t.Fatal("expected Txn to succeed for new key")
	}

	kv, _ := s.Get(ctx, "/new")
	if kv == nil || string(kv.Value) != "created" {
		t.Error("key not written correctly after Txn")
	}
}

func TestTxnCreateFailsIfExists(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Put(ctx, "/existing", []byte("old"), 0)

	res, err := s.Txn(ctx, "/existing", -1, "PUT", []byte("new"), 0)
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if res.Succeeded {
		t.Fatal("expected Txn to fail when key already exists")
	}
	if res.Current == nil || string(res.Current.Value) != "old" {
		t.Error("expected Current to hold the existing value on failure")
	}
}

func TestTxnUpdateSucceeds(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _, _ := s.Put(ctx, "/upd", []byte("v1"), 0)

	res, err := s.Txn(ctx, "/upd", rev1, "PUT", []byte("v2"), 0)
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !res.Succeeded {
		t.Fatal("expected Txn to succeed with correct mod_revision")
	}

	kv, _ := s.Get(ctx, "/upd")
	if string(kv.Value) != "v2" {
		t.Errorf("value after update: got %q, want v2", kv.Value)
	}
}

func TestTxnUpdateFailsOnStaleRevision(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _, _ := s.Put(ctx, "/stale", []byte("v1"), 0)
	s.Put(ctx, "/stale", []byte("v2"), 0)

	res, err := s.Txn(ctx, "/stale", rev1, "PUT", []byte("v3"), 0)
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if res.Succeeded {
		t.Fatal("expected Txn to fail on stale revision")
	}
	if res.Current == nil || string(res.Current.Value) != "v2" {
		t.Error("expected Current to hold latest value v2")
	}
}

func TestTxnDeleteSucceeds(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev, _, _ := s.Put(ctx, "/todel", []byte("bye"), 0)

	res, err := s.Txn(ctx, "/todel", rev, "DELETE", nil, 0)
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !res.Succeeded {
		t.Fatal("expected delete Txn to succeed")
	}

	kv, _ := s.Get(ctx, "/todel")
	if kv != nil {
		t.Error("key still present after Txn DELETE")
	}
}

// --- BlockReadEvents ---

func TestBlockReadEventsDeliversEvents(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.Put(ctx, "/ev/a", []byte("1"), 0)
	s.Put(ctx, "/ev/b", []byte("2"), 0)
	s.Delete(ctx, "/ev/a")

	events, _, err := s.BlockReadEvents(ctx, "0", 100)
	if err != nil {
		t.Fatalf("BlockReadEvents: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Type != "PUT" || events[0].Key != "/ev/a" {
		t.Errorf("event[0]: type=%s key=%s", events[0].Type, events[0].Key)
	}
	if events[2].Type != "DELETE" || events[2].Key != "/ev/a" {
		t.Errorf("event[2]: type=%s key=%s", events[2].Type, events[2].Key)
	}
}

func TestBlockReadEventsRevisionsAscend(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := range 5 {
		s.Put(ctx, fmt.Sprintf("/e/%d", i), []byte("v"), 0)
	}

	events, _, err := s.BlockReadEvents(ctx, "0", 100)
	if err != nil {
		t.Fatalf("BlockReadEvents: %v", err)
	}

	var prev int64
	for _, ev := range events {
		if ev.Rev <= prev {
			t.Errorf("event revisions not ascending: %d -> %d", prev, ev.Rev)
		}
		prev = ev.Rev
	}
}
