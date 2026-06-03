package store

import (
	"context"
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

//go:embed txn.lua
var txnLua string

//go:embed write.lua
var writeLua string

// ErrTxnFailed is returned when a Txn compare condition is not satisfied.
var ErrTxnFailed = errors.New("txn compare failed")

// Event represents a single change record read from the Redis Stream.
type Event struct {
	Type      string // "PUT" or "DELETE"
	Key       string
	Rev       int64
	KV        *KeyValue // nil for DELETE events
	PrevKV    *KeyValue // previous value before this PUT; nil for creates
	CreatedMs int64     // unix-ms when the entry was appended (from the Redis stream ID)
	ID        string    // the Redis Stream entry ID ("<ms>-<seq>"); used to seek catch-up
}

// ReadEvents reads up to maxCount events from the Redis Stream after lastID,
// non-blocking. Returns the events and the last stream ID read.
func (r *RedisStore) ReadEvents(ctx context.Context, lastID string, blockMs int, maxCount int64) ([]Event, string, error) {
	results, err := r.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{eventStream, lastID},
		Count:   maxCount,
		Block:   0,
	}).Result()

	if err == redis.Nil {
		return nil, lastID, nil
	}
	if err != nil {
		return nil, lastID, fmt.Errorf("xread: %w", err)
	}

	var events []Event
	newLastID := lastID

	for _, stream := range results {
		for _, msg := range stream.Messages {
			newLastID = msg.ID
			ev, err := parseStreamEvent(msg)
			if err != nil {
				continue
			}
			events = append(events, ev)
		}
	}

	return events, newLastID, nil
}

// BlockReadEvents blocks until at least one new event arrives after lastID,
// or until ctx is cancelled. Uses XREAD BLOCK.
func (r *RedisStore) BlockReadEvents(ctx context.Context, lastID string, maxCount int64) ([]Event, string, error) {
	results, err := r.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{eventStream, lastID},
		Count:   maxCount,
		Block:   500,
	}).Result()

	if err == redis.Nil {
		return nil, lastID, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, lastID, ctx.Err()
		}
		return nil, lastID, fmt.Errorf("xread block: %w", err)
	}

	var events []Event
	newLastID := lastID

	for _, stream := range results {
		for _, msg := range stream.Messages {
			newLastID = msg.ID
			ev, err := parseStreamEvent(msg)
			if err != nil {
				continue
			}
			events = append(events, ev)
		}
	}

	return events, newLastID, nil
}

// streamString extracts a stream field as a string. go-redis returns XADD
// values as strings, so a type assertion avoids the reflection cost of
// fmt.Sprintf("%v", ...) on the watch hot path (every field of every event).
func streamString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

// parseStreamEvent converts a Redis Stream message to an Event.
func parseStreamEvent(msg redis.XMessage) (Event, error) {
	ev := Event{
		Type: streamString(msg.Values["type"]),
		Key:  streamString(msg.Values["key"]),
		ID:   msg.ID,
	}

	// Redis stream IDs are "<unix-ms>-<seq>"; the millisecond prefix is the
	// append time, used to measure watch-delivery latency.
	if dash := strings.IndexByte(msg.ID, '-'); dash > 0 {
		if ms, err := strconv.ParseInt(msg.ID[:dash], 10, 64); err == nil {
			ev.CreatedMs = ms
		}
	}

	rev, err := strconv.ParseInt(streamString(msg.Values["rev"]), 10, 64)
	if err != nil {
		return ev, fmt.Errorf("parse rev: %w", err)
	}
	ev.Rev = rev

	if ev.Type == "PUT" {
		if dataRaw, ok := msg.Values["data"]; ok {
			if kv := decodeKV(ev.Key, []byte(streamString(dataRaw))); kv != nil {
				ev.KV = kv
			}
		}
	}

	// prev_data carries the pre-image for both PUT (the overwritten value) and
	// DELETE (the deleted object). The apiserver needs it on DELETE to avoid a
	// PrevKv=nil watch error that tears down its watch-cache.
	if prevRaw, ok := msg.Values["prev_data"]; ok {
		if prevStr := streamString(prevRaw); prevStr != "" {
			if prevKV := decodeKV(ev.Key, []byte(prevStr)); prevKV != nil {
				ev.PrevKV = prevKV
			}
		}
	}

	return ev, nil
}

const (
	revisionKey = "global:revision"
	eventStream = "events"
	keyIndex    = "keyindex"
)

// KeyValue is what we store in Redis for each etcd key.
type KeyValue struct {
	Key            string
	Value          []byte
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Lease          int64
}

// encodeKV serialises a KeyValue to a compact binary blob:
//
//	bytes  1- 8  create_revision (big-endian int64)
//	bytes  9-16  mod_revision
//	bytes 17-24  version
//	bytes 25-32  lease
//	bytes 33+    raw value
func encodeKV(createRev, modRev, version, lease int64, value []byte) []byte {
	buf := make([]byte, 32+len(value))
	binary.BigEndian.PutUint64(buf[0:], uint64(createRev))
	binary.BigEndian.PutUint64(buf[8:], uint64(modRev))
	binary.BigEndian.PutUint64(buf[16:], uint64(version))
	binary.BigEndian.PutUint64(buf[24:], uint64(lease))
	copy(buf[32:], value)
	return buf
}

// decodeKV deserialises a binary blob written by encodeKV (or txn.lua).
// Returns nil if data is too short to be valid.
func decodeKV(key string, data []byte) *KeyValue {
	if len(data) < 32 {
		return nil
	}
	return &KeyValue{
		Key:            key,
		CreateRevision: int64(binary.BigEndian.Uint64(data[0:8])),
		ModRevision:    int64(binary.BigEndian.Uint64(data[8:16])),
		Version:        int64(binary.BigEndian.Uint64(data[16:24])),
		Lease:          int64(binary.BigEndian.Uint64(data[24:32])),
		Value:          append([]byte(nil), data[32:]...),
	}
}

type RedisStore struct {
	client *redis.Client
	// sink, if set, receives every event the moment a write completes — the
	// in-process watch short-circuit. It lets the dispatcher fan an event out
	// from the write path instead of reading it back off the Redis stream via
	// XREAD, removing one Redis→server round-trip from watch propagation.
	sink func(Event)
}

// SetEventSink registers the in-process event sink. The watch dispatcher wires
// its ordered-ingest here; nil (the default) just means events are only durably
// recorded on the Redis stream and picked up via catch-up, never live.
func (r *RedisStore) SetEventSink(sink func(Event)) { r.sink = sink }

// emit hands a freshly written event to the sink if one is registered. sid is
// the XADD stream ID ("" when no event was appended, e.g. an absent DELETE — in
// which case nothing is emitted, preserving the contiguity of dispatched revs).
func (r *RedisStore) emit(typ, key string, rev int64, newBlob, prevBlob []byte, sid string) {
	if r.sink == nil || sid == "" || rev <= 0 {
		return
	}
	r.sink(makeEvent(typ, key, rev, newBlob, prevBlob, sid))
}

// makeEvent builds a store.Event from the blobs and stream ID a write returns,
// so the watch event is assembled in-process with no extra Redis read. CreatedMs
// is parsed from the stream ID's millisecond prefix (the XADD/append time), the
// basis for the watch-delivery latency metric.
func makeEvent(typ, key string, rev int64, newBlob, prevBlob []byte, sid string) Event {
	ev := Event{Type: typ, Key: key, Rev: rev, ID: sid}
	if dash := strings.IndexByte(sid, '-'); dash > 0 {
		if ms, err := strconv.ParseInt(sid[:dash], 10, 64); err == nil {
			ev.CreatedMs = ms
		}
	}
	if typ == "PUT" && len(newBlob) >= 32 {
		ev.KV = decodeKV(key, newBlob)
	}
	if len(prevBlob) >= 32 {
		ev.PrevKV = decodeKV(key, prevBlob)
	}
	return ev
}

// scriptStr returns element i of a Lua script result as a string, "" if absent.
func scriptStr(res []interface{}, i int) string {
	if i < len(res) {
		if s, ok := res[i].(string); ok {
			return s
		}
	}
	return ""
}

func NewRedisStore(addr string) *RedisStore {
	return NewRedisStoreDB(addr, 0)
}

// NewRedisStoreDB connects to a specific Redis database number.
// DB 0 is the default; use a non-zero DB (e.g. 15) in tests for isolation.
//
// addr supports two forms:
//   - "host:port"          → TCP (default)
//   - "unix:///path/sock"  → Unix domain socket
//
// Unix sockets avoid the kernel TCP stack and shave ~50 µs off every Redis
// round-trip, which compounds across the ~7-8 sequential apiserver writes
// during a Knative scale-from-zero.
func NewRedisStoreDB(addr string, db int) *RedisStore {
	opts := &redis.Options{Addr: addr, DB: db}
	if strings.HasPrefix(addr, "unix://") {
		opts.Network = "unix"
		opts.Addr = strings.TrimPrefix(addr, "unix://")
	}
	client := redis.NewClient(opts)
	// Initialise the revision counter to 0 so the first INCR returns 1.
	client.SetArgs(context.Background(), revisionKey, 0, redis.SetArgs{Mode: "NX"})
	r := &RedisStore{client: client}
	r.migrateKeyIndex(context.Background())
	return r
}

func (r *RedisStore) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// PoolStats exposes the go-redis connection-pool counters for metrics.
func (r *RedisStore) PoolStats() *redis.PoolStats {
	return r.client.PoolStats()
}

// incrRevision atomically increments the global revision and returns the new value.
func (r *RedisStore) incrRevision(ctx context.Context) (int64, error) {
	rev, err := r.client.Incr(ctx, revisionKey).Result()
	if err != nil {
		return 0, fmt.Errorf("incr revision: %w", err)
	}
	return rev, nil
}

// RawRevision returns the underlying revision counter without the ≥1 clamp that
// CurrentRevision applies. The watch dispatcher seeds its reorder buffer's "next
// revision to release" with raw+1: on a fresh store the counter is 0 and the
// first write is rev 1, whereas CurrentRevision reports 1 there and would make
// rev 1 look like an already-released duplicate, dropping the first live event.
func (r *RedisStore) RawRevision(ctx context.Context) (int64, error) {
	val, err := r.client.Get(ctx, revisionKey).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil || n < 0 {
		return 0, nil
	}
	return n, nil
}

// CurrentRevision returns the current global revision without incrementing.
// Always returns ≥ 1 — etcd never exposes revision 0.
func (r *RedisStore) CurrentRevision(ctx context.Context) (int64, error) {
	val, err := r.client.Get(ctx, revisionKey).Result()
	if err == redis.Nil {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	rev, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}
	if rev < 1 {
		return 1, nil
	}
	return rev, nil
}

// redisKey returns the Redis key used to store an etcd key's data.
func redisKey(key string) string {
	return "kv:" + key
}

// migrateKeyIndex upgrades older REtcd databases where keyindex was a Redis
// Set. Newer code stores it as a ZSET so range scans can use ZRANGEBYLEX.
func (r *RedisStore) migrateKeyIndex(ctx context.Context) {
	typ, err := r.client.Type(ctx, keyIndex).Result()
	if err != nil || typ != "set" {
		return
	}

	members, err := r.client.SMembers(ctx, keyIndex).Result()
	if err != nil {
		return
	}

	pipe := r.client.Pipeline()
	pipe.Del(ctx, keyIndex)
	if len(members) > 0 {
		zs := make([]redis.Z, len(members))
		for i, member := range members {
			zs[i] = redis.Z{Score: 0, Member: member}
		}
		pipe.ZAdd(ctx, keyIndex, zs...)
	}
	_, _ = pipe.Exec(ctx)
}

func prefixLexRange(prefix string) (min, max string) {
	if prefix == "" {
		return "-", "+"
	}

	next, ok := lexSuccessor(prefix)
	if !ok {
		return "[" + prefix, "+"
	}
	return "[" + prefix, "(" + next
}

func lexSuccessor(s string) (string, bool) {
	b := []byte(s)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xff {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

// Put stores a key-value pair, increments the revision, and appends to the event
// stream — all in one atomic Redis round-trip via write.lua (was 3 sequential
// round-trips: GET + INCR + pipeline). Returns the new revision and the
// overwritten value (nil on create).
func (r *RedisStore) Put(ctx context.Context, key string, value []byte, leaseID int64) (int64, *KeyValue, error) {
	keys := []string{redisKey(key), revisionKey, eventStream, keyIndex}
	res, err := writeScript.Run(ctx, r.client, keys, "PUT", string(value), leaseID).Slice()
	if err != nil {
		return 0, nil, fmt.Errorf("put script: %w", err)
	}
	rev := scriptRev(res)
	existingBlob := scriptStr(res, 1)
	newBlob := scriptStr(res, 2)
	sid := scriptStr(res, 3)

	r.emit("PUT", key, rev, []byte(newBlob), []byte(existingBlob), sid)

	var existing *KeyValue
	if existingBlob != "" {
		existing = decodeKV(key, []byte(existingBlob))
	}
	return rev, existing, nil
}

// scriptRev parses the revision (element 0, a string) from a write.lua result.
func scriptRev(res []interface{}) int64 {
	if len(res) == 0 {
		return 0
	}
	switch v := res[0].(type) {
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	case int64:
		return v
	}
	return 0
}

// Get retrieves a single key. Returns nil if the key does not exist.
func (r *RedisStore) Get(ctx context.Context, key string) (*KeyValue, error) {
	return r.get(ctx, key)
}

func (r *RedisStore) get(ctx context.Context, key string) (*KeyValue, error) {
	data, err := r.client.Get(ctx, redisKey(key)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	return decodeKV(key, data), nil
}

// Range returns all keys with the given prefix and the current global revision.
// keyindex is a ZSET with score 0 for all keys, so ZRANGEBYLEX gives ordered
// prefix scans without reading the whole keyspace.
func (r *RedisStore) Range(ctx context.Context, prefix string) (int64, []*KeyValue, error) {
	min, max := prefixLexRange(prefix)

	pipe := r.client.Pipeline()
	rangeCmd := pipe.ZRangeByLex(ctx, keyIndex, &redis.ZRangeBy{
		Min: min,
		Max: max,
	})
	revCmd := pipe.Get(ctx, revisionKey)
	pipe.Exec(ctx) //nolint:errcheck

	rev := int64(1)
	if revStr, err := revCmd.Result(); err == nil {
		if v, err := strconv.ParseInt(revStr, 10, 64); err == nil && v >= 1 {
			rev = v
		}
	}
	matched, err := rangeCmd.Result()
	if err != nil {
		return rev, nil, fmt.Errorf("zrangebylex keyindex: %w", err)
	}
	if len(matched) == 0 {
		return rev, nil, nil
	}

	redisKeys := make([]string, len(matched))
	for i, k := range matched {
		redisKeys[i] = redisKey(k)
	}
	vals, err := r.client.MGet(ctx, redisKeys...).Result()
	if err != nil {
		return rev, nil, fmt.Errorf("mget: %w", err)
	}

	var results []*KeyValue
	for i, v := range vals {
		if v == nil {
			continue
		}
		str, ok := v.(string)
		if !ok {
			continue
		}
		kv := decodeKV(matched[i], []byte(str))
		if kv != nil {
			results = append(results, kv)
		}
	}
	return rev, results, nil
}

// Delete removes a key and appends a delete event to the stream — one atomic
// Redis round-trip via write.lua. The DELETE event carries prev_data (the
// deleted object) so the apiserver's WithPrevKV watch doesn't reject it and tear
// down its watch-cache. Returns the new revision and the deleted value (nil if
// the key was absent, in which case the revision is not bumped).
func (r *RedisStore) Delete(ctx context.Context, key string) (int64, *KeyValue, error) {
	keys := []string{redisKey(key), revisionKey, eventStream, keyIndex}
	res, err := writeScript.Run(ctx, r.client, keys, "DELETE", "", 0).Slice()
	if err != nil {
		return 0, nil, fmt.Errorf("delete script: %w", err)
	}
	rev := scriptRev(res)
	prevBlob := scriptStr(res, 1)
	sid := scriptStr(res, 3)

	// sid == "" for an absent delete (no INCR, no event) → emit is a no-op.
	r.emit("DELETE", key, rev, nil, []byte(prevBlob), sid)

	var prev *KeyValue
	if prevBlob != "" {
		prev = decodeKV(key, []byte(prevBlob))
	}
	return rev, prev, nil
}

// txnScript is loaded from txn.lua at compile time via go:embed.
var txnScript = redis.NewScript(txnLua)

// writeScript (write.lua) does an atomic unconditional PUT or DELETE in one
// round-trip; used by Put and Delete.
var writeScript = redis.NewScript(writeLua)

// TxnResult is returned by Txn.
type TxnResult struct {
	Succeeded bool
	Revision  int64
	Current   *KeyValue // populated when compare fails
}

// Txn performs an atomic compare-and-swap on a single key.
//
//   - expectedModRevision == -1  →  key must not exist (create-only)
//   - expectedModRevision >= 0   →  key's mod_revision must equal this value
func (r *RedisStore) Txn(
	ctx context.Context,
	key string,
	expectedModRevision int64,
	successOp string,
	newValue []byte,
	leaseID int64,
) (*TxnResult, error) {
	keys := []string{redisKey(key), revisionKey, eventStream, keyIndex}
	args := []interface{}{expectedModRevision, string(newValue), successOp, leaseID}

	res, err := txnScript.Run(ctx, r.client, keys, args...).Slice()
	if err != nil {
		return nil, fmt.Errorf("txn script: %w", err)
	}

	succeeded := res[0].(int64) == 1
	result := &TxnResult{Succeeded: succeeded}

	if !succeeded {
		if binaryStr, ok := res[1].(string); ok {
			if kv := decodeKV(key, []byte(binaryStr)); kv != nil {
				result.Current = kv
			}
		}
	}

	if len(res) >= 3 {
		if revStr, ok := res[2].(string); ok {
			if v, err2 := strconv.ParseInt(revStr, 10, 64); err2 == nil {
				result.Revision = v
			}
		}
	}
	if result.Revision == 0 {
		result.Revision, _ = r.CurrentRevision(ctx)
	}

	// On success the script returns the pre-image (index 1), the new blob
	// (index 3, "" for DELETE) and the stream ID (index 4) so the watch event is
	// dispatched in-process — no XREAD readback. successOp is "PUT" or "DELETE".
	if succeeded {
		r.emit(successOp, key, result.Revision,
			[]byte(scriptStr(res, 3)), []byte(scriptStr(res, 1)), scriptStr(res, 4))
	}

	return result, nil
}
