package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// ErrTxnFailed is returned when a Txn compare condition is not satisfied.
var ErrTxnFailed = errors.New("txn compare failed")

// Event represents a single change record read from the Redis Stream.
type Event struct {
	Type string // "PUT" or "DELETE"
	Key  string
	Rev  int64
	KV   *KeyValue // nil for DELETE events
}

// StreamStartID returns the Redis Stream ID to start reading from for a given
// etcd revision. We use "0" (beginning of stream) when startRevision <= 1,
// otherwise we use the revision as a stream entry sequence hint.
// Since we don't store a direct revision→streamID mapping, we read from the
// beginning and skip entries with rev < startRevision.
func StreamStartID(startRevision int64) string {
	if startRevision <= 1 {
		return "0"
	}
	return "0" // always read from start and filter — simple and correct
}

// ReadEvents reads up to maxCount events from the Redis Stream after lastID,
// blocking up to blockMs milliseconds if no events are available.
// Returns the events and the last stream ID read (to use as the next lastID).
func (r *RedisStore) ReadEvents(ctx context.Context, lastID string, blockMs int, maxCount int64) ([]Event, string, error) {
	results, err := r.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{eventStream, lastID},
		Count:   maxCount,
		Block:   0, // non-blocking — we handle blocking in the caller loop
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
				continue // skip malformed events
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
		Block:   5000, // block up to 5s then return empty so we can check ctx
	}).Result()

	if err == redis.Nil {
		return nil, lastID, nil // timeout, no new events
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

// parseStreamEvent converts a Redis Stream message to an Event.
func parseStreamEvent(msg redis.XMessage) (Event, error) {
	ev := Event{
		Type: fmt.Sprintf("%v", msg.Values["type"]),
		Key:  fmt.Sprintf("%v", msg.Values["key"]),
	}

	revStr := fmt.Sprintf("%v", msg.Values["rev"])
	rev, err := strconv.ParseInt(revStr, 10, 64)
	if err != nil {
		return ev, fmt.Errorf("parse rev: %w", err)
	}
	ev.Rev = rev

	if ev.Type == "PUT" {
		dataStr := fmt.Sprintf("%v", msg.Values["data"])
		var kv KeyValue
		if err := json.Unmarshal([]byte(dataStr), &kv); err != nil {
			return ev, fmt.Errorf("unmarshal kv: %w", err)
		}
		ev.KV = &kv
	}

	return ev, nil
}

const (
	revisionKey = "global:revision"
	eventStream = "events"
)

// KeyValue is what we store in Redis for each etcd key.
type KeyValue struct {
	Key            string `json:"key"`
	Value          []byte `json:"value"`
	CreateRevision int64  `json:"create_revision"`
	ModRevision    int64  `json:"mod_revision"`
	Version        int64  `json:"version"` // increments on each modification
	Lease          int64  `json:"lease"`
}

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(addr string) *RedisStore {
	return NewRedisStoreDB(addr, 0)
}

// NewRedisStoreDB connects to a specific Redis database number.
// DB 0 is the default; use a non-zero DB (e.g. 15) in tests for isolation.
func NewRedisStoreDB(addr string, db int) *RedisStore {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   db,
	})
	return &RedisStore{client: client}
}

func (r *RedisStore) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// incrRevision atomically increments the global revision and returns the new value.
func (r *RedisStore) incrRevision(ctx context.Context) (int64, error) {
	rev, err := r.client.Incr(ctx, revisionKey).Result()
	if err != nil {
		return 0, fmt.Errorf("incr revision: %w", err)
	}
	return rev, nil
}

// CurrentRevision returns the current global revision without incrementing.
func (r *RedisStore) CurrentRevision(ctx context.Context) (int64, error) {
	val, err := r.client.Get(ctx, revisionKey).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(val, 10, 64)
}

// redisKey returns the Redis key used to store an etcd key's data.
func redisKey(key string) string {
	return "kv:" + key
}

// Put stores a key-value pair, increments the revision, and appends to the event stream.
func (r *RedisStore) Put(ctx context.Context, key string, value []byte, leaseID int64) (int64, *KeyValue, error) {
	// Fetch existing value (for create_revision and version tracking)
	existing, err := r.get(ctx, key)
	if err != nil {
		return 0, nil, err
	}

	rev, err := r.incrRevision(ctx)
	if err != nil {
		return 0, nil, err
	}

	createRevision := rev
	version := int64(1)
	if existing != nil {
		createRevision = existing.CreateRevision
		version = existing.Version + 1
	}

	kv := &KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: createRevision,
		ModRevision:    rev,
		Version:        version,
		Lease:          leaseID,
	}

	data, err := json.Marshal(kv)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal kv: %w", err)
	}

	// Store the key-value and append event in a pipeline for efficiency.
	pipe := r.client.Pipeline()
	pipe.Set(ctx, redisKey(key), data, 0)
	pipe.SAdd(ctx, "keyindex", key) // track all keys for range queries
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: eventStream,
		ID:     "*", // auto-generate stream ID
		Values: map[string]interface{}{
			"type": "PUT",
			"key":  key,
			"rev":  rev,
			"data": string(data),
		},
	})
	// Update lease key-sets: detach from old lease, attach to new one.
	if existing != nil && existing.Lease != 0 && existing.Lease != leaseID {
		pipe.SRem(ctx, leaseKeysSetKey(existing.Lease), key)
	}
	if leaseID != 0 {
		pipe.SAdd(ctx, leaseKeysSetKey(leaseID), key)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, nil, fmt.Errorf("pipeline exec: %w", err)
	}

	return rev, existing, nil
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
	var kv KeyValue
	if err := json.Unmarshal(data, &kv); err != nil {
		return nil, fmt.Errorf("unmarshal kv: %w", err)
	}
	return &kv, nil
}

// Range returns all keys with the given prefix, sorted lexicographically.
func (r *RedisStore) Range(ctx context.Context, prefix string) ([]*KeyValue, error) {
	// Get all tracked keys from the key index
	keys, err := r.client.SMembers(ctx, "keyindex").Result()
	if err != nil {
		return nil, fmt.Errorf("smembers keyindex: %w", err)
	}

	var results []*KeyValue
	for _, k := range keys {
		if len(prefix) == 0 || k == prefix || (len(k) >= len(prefix) && k[:len(prefix)] == prefix) {
			kv, err := r.get(ctx, k)
			if err != nil {
				return nil, err
			}
			if kv != nil {
				results = append(results, kv)
			}
		}
	}
	return results, nil
}

// Delete removes a key and appends a delete event to the stream.
func (r *RedisStore) Delete(ctx context.Context, key string) (int64, *KeyValue, error) {
	existing, err := r.get(ctx, key)
	if err != nil {
		return 0, nil, err
	}
	if existing == nil {
		rev, _ := r.CurrentRevision(ctx)
		return rev, nil, nil
	}

	rev, err := r.incrRevision(ctx)
	if err != nil {
		return 0, nil, err
	}

	pipe := r.client.Pipeline()
	pipe.Del(ctx, redisKey(key))
	pipe.SRem(ctx, "keyindex", key)
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: eventStream,
		ID:     "*",
		Values: map[string]interface{}{
			"type": "DELETE",
			"key":  key,
			"rev":  rev,
		},
	})
	if existing.Lease != 0 {
		pipe.SRem(ctx, leaseKeysSetKey(existing.Lease), key)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, nil, fmt.Errorf("pipeline exec: %w", err)
	}

	return rev, existing, nil
}

// txnScript is a Lua script that atomically:
//  1. Fetches the current value of a key
//  2. Checks whether its mod_revision matches the expected value
//  3. If yes: writes the new value, increments the global revision, appends a PUT event
//  4. Returns 1 (success) or 0 (failure) plus the current raw JSON value
//
// KEYS[1] = redisKey(key)   e.g. "kv:/registry/pods/default/mypod"
// KEYS[2] = revisionKey     "global:revision"
// KEYS[3] = eventStream     "events"
// KEYS[4] = "keyindex"
// ARGV[1] = expected mod_revision (or -1 to mean "key must not exist")
// ARGV[2] = new JSON-serialised KeyValue to write (empty string = delete)
// ARGV[3] = "PUT" or "DELETE"
var txnScript = redis.NewScript(`
local current_raw = redis.call('GET', KEYS[1])

-- Decode current mod_revision (0 if key does not exist)
local current_mod_rev = 0
local current_create_rev = 0
local current_version = 0
if current_raw then
	local ok, decoded = pcall(cjson.decode, current_raw)
	if ok then
		current_mod_rev    = tonumber(decoded['mod_revision'])   or 0
		current_create_rev = tonumber(decoded['create_revision']) or 0
		current_version    = tonumber(decoded['version'])         or 0
	end
end

local expected = tonumber(ARGV[1])

-- Compare: -1 means "key must not exist" (create-only), otherwise match mod_revision
if expected == -1 then
	if current_raw then
		return {0, current_raw or ''}
	end
elseif current_mod_rev ~= expected then
	return {0, current_raw or ''}
end

-- Compare passed — perform the write
local new_rev = redis.call('INCR', KEYS[2])
local op = ARGV[3]

if op == 'DELETE' then
	redis.call('DEL', KEYS[1])
	redis.call('SREM', KEYS[4], string.sub(KEYS[1], 4))
	redis.call('XADD', KEYS[3], '*',
		'type', 'DELETE',
		'key',  string.sub(KEYS[1], 4),
		'rev',  tostring(new_rev))
else
	local new_kv = cjson.decode(ARGV[2])
	if current_raw then
		new_kv['create_revision'] = current_create_rev
		new_kv['version']         = current_version + 1
	else
		new_kv['create_revision'] = new_rev
		new_kv['version']         = 1
	end
	new_kv['mod_revision'] = new_rev
	local new_raw = cjson.encode(new_kv)
	redis.call('SET',  KEYS[1], new_raw)
	redis.call('SADD', KEYS[4], string.sub(KEYS[1], 4))
	redis.call('XADD', KEYS[3], '*',
		'type', 'PUT',
		'key',  string.sub(KEYS[1], 4),
		'rev',  tostring(new_rev),
		'data', new_raw)
end

return {1, ''}
`)

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
	successOp string, // "PUT" or "DELETE"
	newValue []byte,
	leaseID int64,
) (*TxnResult, error) {
	skeleton := &KeyValue{
		Key:   key,
		Value: newValue,
		Lease: leaseID,
	}
	skeletonJSON, err := json.Marshal(skeleton)
	if err != nil {
		return nil, fmt.Errorf("marshal skeleton: %w", err)
	}

	keys := []string{redisKey(key), revisionKey, eventStream, "keyindex"}
	args := []interface{}{expectedModRevision, string(skeletonJSON), successOp}

	res, err := txnScript.Run(ctx, r.client, keys, args...).Slice()
	if err != nil {
		return nil, fmt.Errorf("txn script: %w", err)
	}

	succeeded := res[0].(int64) == 1
	result := &TxnResult{Succeeded: succeeded}

	if !succeeded && res[1] != "" {
		var kv KeyValue
		if err := json.Unmarshal([]byte(res[1].(string)), &kv); err == nil {
			result.Current = &kv
		}
	}

	result.Revision, _ = r.CurrentRevision(ctx)
	return result, nil
}
