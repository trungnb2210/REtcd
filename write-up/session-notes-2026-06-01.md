# Session notes — 2026-06-01 (watch propagation microbench + shared-dispatch fix)

Resume-here for this session. Detail/finding also in memory
(`project_propagation_microbench`) and `write-up/watch-dispatch-redesign.md`.

## What got done

1. **Built `load-test/propbench/`** — a Go microbenchmark that measures
   **watch-propagation latency** (Put → matching Watch event) over the raw etcd
   gRPC API, so it runs unchanged against REtcd *and* real etcd. Has:
   - steady (Poisson `--rate`) and `--burst` modes,
   - `--writers N` for concurrent in-flight writes (FaaS 0→N storm) + throughput,
   - `--csv`, optional TLS flags for real etcd. Unit tests pass.
2. **Ran the comparison on a fresh single node (hp127 / `retcd-micro-1`).**
   Stood up Redis + REtcd v7 + single-node etcd 3.5.17 + co-located 3-node etcd.
3. **Found the headline result:** at low concurrency REtcd v7 ≈ etcd (~1ms,
   0% loss); REtcd(unix) beats 3-node etcd. BUT under concurrent-write load
   (`--writers`) REtcd v7 watch propagation **collapsed** (p99 384ms at 8
   writers, 1.8s at 256) — the per-watch XREAD fan-out bottleneck.
4. **Implemented the fix (call it v8):** shared single-reader dispatch.
   - New `server/dispatcher.go` (`eventDispatcher`): one reader tails the stream
     once, fans events to per-watch in-memory channels.
   - Rewrote `tailWatch` + added `catchUp` in `server/watch.go` (register →
     store-backed catch-up on `[startRev, boundary]` → batched live delivery,
     no per-loop CurrentRevision GET).
   - `streamString` parse fix in `store/redis.go` (drop `fmt.Sprintf("%v")`).
   - All server unit tests still pass (transient-retry + persistent-cancel kept).
5. **Re-ran the sweep on v8 → 100–285× better**, collapse gone.

## Numbers (propbench, 20k writes, node0, Redis unix socket)

Propagation p99 (ms):

| writers | REtcd v7 (old) | REtcd v8 (shared) | single-node etcd |
|---------|----------------|-------------------|------------------|
| 1       | 1.13           | 1.07              | 1.06             |
| 8       | 384            | 1.48              | 1.56             |
| 32      | 783            | 2.74              | 2.20             |
| 128     | 1461           | 14.6              | 4.96             |
| 256     | 1860           | 17.8              | 1277             |

Write throughput: REtcd ~24k/s vs etcd ~63k/s (UNCHANGED by this work — that
ceiling is the per-write `SET`+`ZADD`+`XADD` pipeline, a separate concern).

## State to know when resuming

- **Working tree is UNCOMMITTED.** Changed/added: `server/dispatcher.go` (new),
  `server/watch.go`, `store/redis.go`, `load-test/propbench/*` (new),
  `write-up/watch-dispatch-redesign.md` (new), `load-test/results/propagation-20260601/*.csv`.
- **node0 (hp127.utah.cloudlab.us, user jn1122) still has:** Redis (unix socket
  `/run/redis/redis.sock`, appendonly+everysec) and REtcd **v8**
  (`/tmp/retcd-v8-linux`, systemd unit `retcdbench`, on :2379) running.
  etcd units (`etcdbench`, `etcd1/2/3`) are stopped. propbench binary at
  `/tmp/propbench-linux`. **Tear down when done** (reservation will expire).
- The v8 changes are NOT yet baked into a `ghcr.io/.../retcd:v8` image.

## Open decisions / next steps (pick up here)

1. **Commit the v8 work to a branch** (not done — was waiting on user).
2. **Measure real write concurrency/rate of the n=2700 azure-trace run.** If it's
   < ~32 in-flight and << 24k/s (very likely), v8 is already at etcd parity in the
   regime that matters → report as-is; don't chase throughput. Only if the trace
   exceeds 24k/s is the write-coalescer worth building.
3. (If needed) **write-throughput optimization**: coalesce concurrent Puts into
   one pipelined Redis round trip + cut ops/write (3→2). Plausibly 24k→40k/s.
   Structural ceiling remains (single-threaded Redis + double hop).
4. **Bake v8 into an image**; re-run the cluster cold-start (validate no stalls
   at real trace concurrency with the watch fix).
5. **kine+SQLite** as the 4th triangulation point (isolates the network hop).
6. `appendfsync always` REtcd run to durability-match etcd (rigor).

## Thesis framing (reinforced this session)

Don't claim "faster than etcd" unqualified — single-node etcd wins on latency &
throughput (in-process, no hop, single-threaded-Redis ceiling). Defensible
claims: (a) v7 watch is lossless ~1ms, off the cold-start path; (b) v8
shared-dispatch eliminates the propagation collapse → parity with etcd to ~128
writers, beats 3-node etcd; (c) the collapse-and-fix is novelty — a diagnosed
bottleneck + targeted redesign + measured 100×+ improvement.
