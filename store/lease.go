package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	leaseIDCounter = "lease:id_counter"
	leaseSetKey    = "leases"
)

func leaseDataKey(id int64) string    { return fmt.Sprintf("lease:%d", id) }
func leaseKeysSetKey(id int64) string { return fmt.Sprintf("lease:keys:%d", id) }

// LeaseGrant creates a new lease with the given TTL in seconds.
func (r *RedisStore) LeaseGrant(ctx context.Context, ttl int64) (int64, error) {
	id, err := r.client.Incr(ctx, leaseIDCounter).Result()
	if err != nil {
		return 0, fmt.Errorf("incr lease id: %w", err)
	}
	pipe := r.client.Pipeline()
	pipe.Set(ctx, leaseDataKey(id), ttl, time.Duration(ttl)*time.Second)
	pipe.SAdd(ctx, leaseSetKey, id)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("lease grant: %w", err)
	}
	return id, nil
}

// LeaseRevoke revokes a lease and deletes all keys attached to it.
func (r *RedisStore) LeaseRevoke(ctx context.Context, id int64) error {
	keys, err := r.client.SMembers(ctx, leaseKeysSetKey(id)).Result()
	if err != nil {
		return fmt.Errorf("smembers lease keys: %w", err)
	}
	for _, key := range keys {
		if _, _, err := r.Delete(ctx, key); err != nil {
			return fmt.Errorf("delete lease key %s: %w", key, err)
		}
	}
	pipe := r.client.Pipeline()
	pipe.Del(ctx, leaseDataKey(id))
	pipe.Del(ctx, leaseKeysSetKey(id))
	pipe.SRem(ctx, leaseSetKey, id)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("lease revoke: %w", err)
	}
	return nil
}

// LeaseRenew resets the TTL of an existing lease. Returns the granted TTL.
func (r *RedisStore) LeaseRenew(ctx context.Context, id int64) (int64, error) {
	ttlStr, err := r.client.Get(ctx, leaseDataKey(id)).Result()
	if err == redis.Nil {
		return 0, fmt.Errorf("lease %d not found", id)
	}
	if err != nil {
		return 0, fmt.Errorf("get lease: %w", err)
	}
	ttl, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse ttl: %w", err)
	}
	r.client.Expire(ctx, leaseDataKey(id), time.Duration(ttl)*time.Second)
	return ttl, nil
}

// LeaseTimeToLive returns the granted TTL and remaining TTL (in seconds) for a lease.
// remaining == -1 means the lease does not exist or has expired.
func (r *RedisStore) LeaseTimeToLive(ctx context.Context, id int64) (granted, remaining int64, err error) {
	ttlStr, err := r.client.Get(ctx, leaseDataKey(id)).Result()
	if err == redis.Nil {
		return 0, -1, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("get lease: %w", err)
	}
	granted, err = strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse ttl: %w", err)
	}
	dur, err := r.client.TTL(ctx, leaseDataKey(id)).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("ttl: %w", err)
	}
	remaining = int64(dur.Seconds())
	return granted, remaining, nil
}

// StartLeaseReaper starts a background goroutine that deletes keys belonging
// to expired leases. It runs until ctx is cancelled.
func (r *RedisStore) StartLeaseReaper(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.reapExpiredLeases(ctx)
			}
		}
	}()
}

func (r *RedisStore) reapExpiredLeases(ctx context.Context) {
	ids, err := r.client.SMembers(ctx, leaseSetKey).Result()
	if err != nil {
		return
	}
	for _, idStr := range ids {
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			continue
		}
		exists, err := r.client.Exists(ctx, leaseDataKey(id)).Result()
		if err != nil || exists > 0 {
			continue // still alive
		}
		// Lease expired — delete its keys and clean up tracking sets.
		keys, _ := r.client.SMembers(ctx, leaseKeysSetKey(id)).Result()
		for _, key := range keys {
			r.Delete(ctx, key) //nolint:errcheck
		}
		r.client.Del(ctx, leaseKeysSetKey(id))
		r.client.SRem(ctx, leaseSetKey, idStr)
	}
}
