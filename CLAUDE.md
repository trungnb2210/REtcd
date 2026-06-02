# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

REtcd is an etcd v3 gRPC server that uses Redis as its storage backend instead of Raft/bbolt. It is intended to be a drop-in replacement for etcd for running a Kind/Kubernetes cluster, backed by a single Redis instance with AOF persistence.

Module: `github.com/trungnb2210/REtcd`  
Listens on `:2379` (standard etcd port). Requires Redis at `localhost:6379`.

## Commands

```bash
# Build
go build ./...

# Run (requires Redis)
go run .

# Unit tests (no Redis required)
go test ./tests/server/...

# Integration tests (requires Redis on localhost:6379)
docker run --rm -p 6379:6379 redis:7
go test ./tests/store/...

# All tests
go test ./...

# Single test
go test ./tests/server/... -run TestPutIncreasesRevision
go test ./tests/store/... -run TestTxnCreateSucceeds
```

## Architecture

The codebase has two packages wired together in `main.go`:

**`server/`** — implements the etcd v3 gRPC services  
**`store/`** — the Redis backend, accessed only through the `Store` interface

### Store interface (`server/types.go`)

The `Store` interface decouples the server layer from Redis. `*store.RedisStore` is the production implementation; `fakeStore` in `tests/server/fake_store_test.go` is the in-memory test double used by all server unit tests.

### Redis data layout (`store/redis.go`)

| Key | Type | Purpose |
|-----|------|---------|
| `kv:<etcd-key>` | String (binary) | Serialized `KeyValue` per etcd key |
| `keyindex` | Sorted Set | All live keys — used for ordered prefix/range scans |
| `global:revision` | String (int) | Monotonically increasing revision counter |
| `events` | Stream | Ordered event log for Watch |

`Put` and `Delete` use a Redis pipeline to keep the key data, `keyindex`, and `events` stream in sync. `Txn` uses a Lua script (`txnScript`) for atomic compare-and-swap on a single key.

### Watch (`server/watch.go`)

Each `Watch` gRPC stream spawns a goroutine per watch request that tails the `events` Redis Stream via `XREAD BLOCK`. Events are filtered by key/prefix and revision in Go. There is no revision→stream-ID index; all watches start reading from stream ID `"0"` and skip events below `startRevision`. The `sender` struct wraps the stream with a mutex because gRPC streams are not goroutine-safe.

### Range/prefix scan (`server/range.go`)

etcd encodes a prefix scan as `key="/a/"`, `rangeEnd="/a0"` (last byte incremented). The server computes the common prefix of `key` and `rangeEnd`, calls `store.Range(prefix)`, then filters results to the exact `[key, rangeEnd)` window in Go. `store.Range` uses Redis `ZRANGEBYLEX` over `keyindex` so prefix scans are ordered without scanning every live key. A special `rangeEnd="\x00"` means "all keys from key onwards".

### What is not implemented

Leases (`server/lease.go`) are stubbed — `LeaseGrant`, `LeaseRevoke`, `LeaseKeepAlive`, and `LeaseTimeToLive` all return empty responses. Compaction (`server/compact.go`) is also a stub.

## Testing conventions

- Server unit tests live in `tests/server/` (package `server_test`) and use `fakeStore` — no Redis needed.
- Store integration tests live in `tests/store/` (package `store_test`) and use Redis DB 15 for isolation. They auto-skip if Redis is unreachable, so `go test ./...` always passes in CI.
- Use `NewRedisStoreDB(addr, 15)` in any new Redis tests to avoid clobbering the default DB.
