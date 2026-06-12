# REtcd

REtcd is an etcd v3 API server backed by a single vanilla Redis instance. It implements the full etcd v3 gRPC protocol (KV, Watch, Lease, Maintenance) in terms of Redis primitives: atomic Lua scripts for MVCC revisions, an in-process watch dispatcher for prefix-watch fan-out, and Redis AOF for persistence. It runs as a drop-in replacement for etcd with any etcd-v3-compatible client, including the Kubernetes API server and k3s.

The accompanying thesis evaluates REtcd as a controlled instrument for the question: _under what conditions does the choice of control-plane store determine end-to-end latency?_

## Architecture

```
clients (kubectl, kube-apiserver, k3s)
        │  etcd v3 gRPC  (port 2379)
        ▼
  ┌─────────────┐
  │  gRPC layer │  KV · Watch · Lease · Maintenance
  │  (server/)  │
  └──────┬──────┘
         │  Store interface
         ▼
  ┌─────────────┐
  │ RedisStore  │  Lua scripts · AOF persistence
  │  (store/)   │
  └──────┬──────┘
         │
         ▼
      Redis 7+
```

Key design points:
- **Revisions** are a single Redis integer incremented atomically inside the same Lua script that applies each write — no separate counter round-trip.
- **Prefix watches** are served by an in-process dispatcher that fans out events from the write path in strict revision order; Redis keyspace notifications are not used.
- **Leases** are Redis keys with TTL; keepalives refresh the TTL.
- **Durability** defaults to `appendfsync everysec` (at most ~1 s of acknowledged writes lost on power failure). `appendfsync always` matches etcd's per-write fsync semantics.

## Configuration

REtcd is configured via environment variables:

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:2379` | Address REtcd listens on |
| `REDIS_ADDR` | `localhost:6379` | Redis instance address |

## Running locally

**Prerequisites:** Go 1.21+, Redis 7+.

```sh
# Start Redis with AOF persistence
docker run -d --name retcd-redis -p 6379:6379 redis:7 --appendonly yes

# Build and run REtcd
go build -o retcd . && ./retcd
```

REtcd will listen on `:2379` and connect to Redis at `localhost:6379`.

## Docker

```sh
docker pull ghcr.io/trungnb2210/retcd:v12

docker run -d \
  -e REDIS_ADDR=<redis-host>:6379 \
  -p 2379:2379 \
  ghcr.io/trungnb2210/retcd:v12
```

The image is a distroless static binary; no shell.

## Connecting a Kind cluster (macOS / Docker Desktop)

```sh
# Start Redis and REtcd on the host
docker run -d --name retcd-redis -p 6379:6379 redis:7 --appendonly yes
go build -o retcd . && ./retcd &

# Create a Kind cluster patched to use REtcd as its external etcd
kind create cluster --config kind-config.yaml
```

`kind-config.yaml` patches kubeadm to point `etcd.external.endpoints` at `http://192.168.65.254:2379` (the Docker Desktop host gateway).

## Connecting k3s

```sh
k3s server --datastore-endpoint="http://<retcd-host>:2379"
```

No other flags are required. k3s bootstraps and runs its full control plane against REtcd unmodified.

## Development

```sh
# Run tests
go test ./...

# Lint
golangci-lint run ./...
golangci-lint run --fix ./...
```

## Benchmarks

The `load-test/` directory contains the full evaluation harness:

- **`propbench`** — write-latency and watch-propagation microbenchmark (Go binary, drives the etcd v3 gRPC API directly).
- **`lossbench`** — durability microbenchmark: issues writes, triggers a hard reboot (SysRq-b), counts survivors after recovery.
- **`load_test.py`** — orchestrates multi-backend concurrency sweeps.
- **`run_concurrency.sh`** / **`run_propagation.sh`** — CloudLab experiment scripts.
- **`make_figures.py`** / **`analyze.py`** — plot generation from result CSVs.

## Repository layout

```
main.go          entry point; wires gRPC, HTTP /version, cmux
server/          gRPC service implementations (KV, Watch, Lease, Maintenance)
  types.go       Store interface
  kv.go          Range, Put, DeleteRange dispatch
  kv_txn.go      Txn (compare-and-swap)
  watch.go       Watch stream and dispatcher
  lease.go       LeaseGrant/Revoke/KeepAlive
  maintenance.go Status, Defragment stubs
store/
  redis.go       RedisStore: Store interface over go-redis
  write.lua      Atomic put/delete Lua script
  txn.lua        Atomic Txn Lua script
load-test/       Benchmark harness and CloudLab scripts
write-up/        LaTeX thesis source
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
