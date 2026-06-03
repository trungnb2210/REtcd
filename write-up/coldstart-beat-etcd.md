# Beating etcd on cold start — design note (T1 → T2)

Date: 2026-06-03. New initiative. Goal: push REtcd cold-start latency **below**
native single-node etcd, explicitly trading durability. Builds directly on the
validated parity result below; the watch fan-out infra from v8 is reused.

## Previous work (the validated baseline — do not relose this)

- **v8 shared-dispatch** (`server/dispatcher.go`): one reader tails the Redis
  `events` stream once, fans to per-watch in-memory channels. Eliminated the
  per-watch XREAD collapse (watch-prop p99 384ms→1.48ms at 8 writers; parity with
  single-node etcd to ~128 writers). See `watch-dispatch-redesign.md`.
- **v10 PrevKv-on-DELETE fix**: apiserver opens storage watches WithPrevKV and
  treats a DELETE with `PrevKv=nil` as fatal → tears down the per-resource watch
  cache → relist storm. Lua/event path now always carries `prev_data` on DELETE.
  Validated: 14-node 4-way A/B, cold p99 **93.5s → 3.4s**; REtcd(v10/v11) ≈ native
  etcd on FaaS cold start.
- **v11 lua write path** (`store/write.lua`, this branch `lua-writepath`):
  Put/Delete collapsed to one atomic Redis round-trip (was GET+INCR+pipeline).
- Propbench: ~1ms lossless watch propagation; REtcd(unix socket) beats 3-node etcd.
- Branches: `v10-validated`, `lua-writepath`. Canonical image lineage `:v7`→.
- Honest framing **so far**: warm-equivalent; cold-start gap characterised + fixed;
  *single-node etcd still wins on raw latency* (in-process, no second hop).

## What this initiative changes about that framing

We flip "etcd wins on latency" by removing REtcd's two **structural
disadvantages** while keeping its one **structural advantage**.

| Cost (× per control-plane hop in scale-from-zero) | REtcd now | single-node etcd | note |
|---|---|---|---|
| Write latency | gRPC hop + Redis round-trip (Lua), no fsync | gRPC hop + **WAL fsync** | REtcd's extra hop ≈ cancels etcd's fsync → parity |
| Watch propagation | write → XADD → **XREAD readback** → dispatch → send | commit → **in-proc notify** → send | REtcd pays a Redis readback etcd doesn't |

Advantage to keep: **no Raft, no per-write fsync** (Redis AOF everysec, or async).
Lever: **etcd couples durability to latency (fsync on commit path); REtcd can
decouple them.**

## T1 — in-process watch short-circuit (no durability change)

The write already has `rev`+blob in hand after the Lua call. Feed the event
straight into the in-memory dispatcher instead of waiting for the reader to read
it back from Redis. Removes one Redis→server round-trip from watch propagation.

Mechanism:
- `write.lua` / `txn.lua` now also return **new_blob** + **stream_id** (XADD id),
  so the store builds a complete `store.Event` with no re-GET.
- `store` gains `SetEventSink(func(Event))`; Put/Delete/Txn emit the event after a
  successful write. `RedisStore` and `fakeStore` both implement it.
- `eventDispatcher`: live events arrive via `ingest(ev)` (was: the `run()` XREAD
  loop, now removed). A **reorder buffer** (`pending` map keyed by rev, `nextRev`
  watermark) releases events in contiguous rev order — needed because concurrent
  writers can call the sink out of rev order. Safe because every revision that
  exists = exactly one event (INCR+XADD atomic) ⇒ event revs are contiguous, no
  gaps. `latestRev`/checkpoints now fed from the release path.
- Catch-up (`catchUp`, historical `[startRev, boundary]`) **unchanged**, still
  reads the Redis stream. The register→boundary handoff is unchanged, so no
  gap/dup across catch-up↔live.
- Persistent-backend-failure → `Canceled` moved from the (removed) reader into
  `catchUp` (the only remaining backend dependency on the watch path). A live,
  from-now watch now has no backend dependency — if Redis is down, writes fail at
  the writer and the watch correctly idles.

Constraint: assumes a **single REtcd process** (kubeadm external-etcd = 1
endpoint ✓). Multi-process HA would need the reader back as a remote-write
reconciler. Known T1 tradeoff: catch-up of revisions written *before* this
process started has no checkpoints → scans the stream from "0" (bounded FYP
streams make this cheap; compaction is still stubbed).

Expected: watch propagation ~1ms → ~0.1–0.3ms (below etcd). Reuses the v8
fan-out wholesale.

## T2 — in-memory authoritative + Redis as async WAL (the decisive win)

Serve Get/Range/CurrentRevision from an in-memory MVCC map. A write applies to
memory + bumps an in-memory revision + notifies watchers in-process (the T1
ingest path), then forwards to Redis **asynchronously**. So:
- Write latency = gRPC hop + memory op (~50–100µs), **no synchronous Redis
  round-trip** → strictly below etcd by the fsync cost, on every write.
- Reads from memory: read-your-writes trivial (single process); Redis
  single-thread throughput ceiling stops gating latency.
- **Durability decoupled from latency**: Redis persistence (even fsync=always)
  sits on the async path, off the ack path. Loss window = writes acked but not
  yet forwarded if REtcd crashes; recovery = replay the Redis log/stream on
  restart. K8s cluster state fits memory.

This is "etcd's architecture minus consensus minus fsync, with Redis as the
durable log." Subsumes T1's watch win and adds the write win.

## Validation plan

1. propbench: watch-prop p99 (expect T1 < etcd) + add write-latency measurement
   (expect T2 < etcd by the fsync delta). Sweep `--writers 1..256`.
2. 14-node trace re-run (cold p99) vs native etcd — expect REtcd < etcd.
3. Durability characterisation: crash REtcd mid-write-burst, measure lost-write
   window vs etcd's zero. Report honestly as the explicit tradeoff.
4. Framing: not "faster, free" — **"REtcd decouples durability from latency:
   below-etcd cold start at the cost of a bounded recovery window."**

## Status

**T1 DONE** on branch `lua-writepath` (uncommitted). Files: `store/write.lua`,
`store/txn.lua`, `store/redis.go` (sink + RawRevision + makeEvent), `server/types.go`
(SetEventSink), `server/dispatcher.go` (prime + ingest reorder buffer; live reader
removed), `server/watch.go` (sink wiring; catch-up owns persistent-failure cancel),
`server/metrics.go` (reorder-flush counter), `main.go` (reaper after sink),
`tests/server/*`, `server/dispatcher_test.go` (new), `tests/store/sink_test.go` (new).

All tests pass (`go test -race ./...`), incl. store integration + sink tests vs
real Redis. Local smoke (laptop, REtcd+Redis only, no etcd — NOT the headline
rig): propbench burst, **0% missed at 1/8/32 writers**; propagation latency now
≈ put RTT at every concurrency (the readback hop is gone):

| writers | put RTT p50/p99 (ms) | propagation p50/p99 (ms) |
|---|---|---|
| 1  | 0.20 / 0.37 | 0.20 / 0.40 |
| 8  | 0.42 / 0.94 | 0.44 / 0.99 |
| 32 | 0.86 / 1.51 | 0.92 / 1.64 |

vs the pre-T1 CloudLab baseline where propagation p99 was ~1.07ms at 1 writer and
*diverged above* write latency under load. Headline etcd-vs-REtcd numbers still
come from the CloudLab propbench + 14-node trace re-run (validation plan above).

**T2 NEXT.**
