# Design

This chapter presents the design of REtcd: an etcd v3-compatible gRPC server with Redis as the storage substrate, intended as a drop-in replacement for etcd in single-instance Kubernetes clusters. The chapter motivates the high-level architectural choice in §4.1, walks through the runtime architecture in §4.2, and then drills into the three subsystems where K8s exerts the most pressure on a storage backend: the Redis data layout (§4.3), the Watch protocol (§4.4), and transactional writes (§4.5). §4.6 covers the integration glue Kubernetes assumes from any etcd-compatible system. §4.7 makes explicit the design alternatives considered and §4.8 the non-goals.

Throughout, the recurring theme is *operational simplicity at the cost of per-operation latency*: REtcd treats Redis as a network-attached key-value primitive rather than embedding storage in-process, and the design choices that follow are consequences of that one decision.

---

## 4.1 Goals and requirements

The functional requirement is a single sentence: serve the etcd v3 gRPC API faithfully enough that kube-apiserver, kubelet, kube-scheduler, kube-controller-manager, and a representative add-on (Knative Serving) operate without modification. There is no requirement to be a drop-in for production HA etcd clusters; the target environment is single-instance deployments — kind, k3s, microk8s, edge nodes, ephemeral CI clusters — where the operational uniformity of a familiar substrate is more valuable than the consensus guarantees of a Raft-replicated store.

The non-functional design priorities, in order:

1. **Correctness under Kubernetes' actual access pattern.** Kubernetes is a watch-heavy, transaction-heavy, version-sensitive client; designs that pass a naive `etcdctl put`/`get` test but break on `kube-controller-manager` cache synchronisation are not acceptable.
2. **Operational simplicity.** The deployment story must be substantively simpler than running a Raft-quorate etcd cluster.
3. **Implementation parsimony.** REtcd should be implementable in single-digit thousands of lines of Go; complexity that exists only to mirror production etcd's architecture (Raft state machine, full MVCC compaction, lease semantics across consensus boundaries) is explicitly out of scope.
4. **Measurable performance.** The system need not match etcd's latency; it must be possible to characterise where and why it does not, with enough instrumentation to attribute cost to specific code paths.

These priorities induce the architectural choice: rather than implementing MVCC, write-ahead logging, B+-tree page management, and Raft consensus from scratch (the path real etcd takes), REtcd delegates persistence and atomicity primitives to an external Redis instance and confines its own code to the gRPC façade and the translation layer between etcd's data model and Redis' data structures.

---

## 4.2 Architectural overview

REtcd is a two-process system: a stateless Go gRPC server (`retcd`) and a Redis instance (`redis-server`) running locally on the same control-plane node. Figure 4.1 shows the request and event paths during a typical Kubernetes write.

> [FIGURE 4.1: Architecture diagram. Three boxes left-to-right: `kube-apiserver`, `REtcd`, `Redis`. Arrows: apiserver→REtcd labelled "gRPC :2379"; REtcd→Redis labelled "TCP/Unix socket :6379"; Redis→Redis-disk labelled "AOF (everysec)". A vertical dashed line between apiserver and REtcd labelled "process boundary"; another between REtcd and Redis. Annotation: "REtcd is stateless; all durable state lives in Redis."]

The split is deliberate. The `retcd` process holds no state beyond connection caches and per-watch goroutines, so it can be restarted at any time without data loss; all durable state — keys, the global revision counter, the event log — lives in the Redis instance. Conversely, Redis can be restarted or upgraded independently of REtcd (REtcd's go-redis client transparently reconnects), and an operator can swap one Redis backend for another (local socket → networked instance → managed service) by changing one environment variable.

Within `retcd`, the gRPC service implementations (KV, Watch, Lease, Maintenance) depend on a single `Store` interface (Listing 4.1), with `RedisStore` as the production implementation and an in-memory `fakeStore` used in unit tests. This split serves two purposes: it keeps the gRPC-layer code free of Redis-specific concerns, and it makes the implementation testable without a running Redis.

**Listing 4.1.** The `Store` interface (excerpt from `server/types.go`):

```go
type Store interface {
    Put(ctx, key, value, leaseID) (rev, prev, error)
    Get(ctx, key)                 (kv, error)
    Range(ctx, prefix)            (rev, kvs, error)
    Delete(ctx, key)              (rev, prev, error)
    Txn(ctx, key, expectedModRev, op, value, lease) (TxnResult, error)
    CurrentRevision(ctx)          (rev, error)
    BlockReadEvents(ctx, lastID, max) ([]Event, lastID, error)
}
```

The interface deliberately avoids exposing Redis-specific types (no `redis.Cmd`, no `redis.StreamMessage`). This is what would allow a future variant of REtcd to substitute an embedded key-value store (Pebble, BadgerDB) by providing an alternative implementation of `Store` without touching the gRPC layer; the same interface is the integration seam for an in-process variant in future work.

The two-process design imposes a per-request kernel-IPC cost (~50–100 µs over TCP localhost, ~10–30 µs over a Unix domain socket) for every operation that touches Redis. Section 4.7 discusses why this cost is accepted; the evaluation chapter quantifies its impact on Knative cold-start latency.

---

## 4.3 Storage layer: data layout in Redis

etcd presents a flat key-value namespace with monotonically-increasing revisions; every write to any key bumps a cluster-wide revision counter, and every key carries its create-revision, mod-revision, and version metadata. Redis offers a richer type system but no MVCC primitives. The translation between the two is straightforward.

REtcd uses four Redis keys (Table 4.1):

**Table 4.1.** REtcd's Redis data layout.

| Redis key            | Type          | Purpose                                                                       |
| -------------------- | ------------- | ----------------------------------------------------------------------------- |
| `kv:<etcd-key>`      | String (blob) | Serialised key-value record: 32-byte fixed header + raw value bytes           |
| `keyindex`           | Set           | Membership of every live etcd key — used for prefix and range scans           |
| `global:revision`    | String (int)  | Monotonically-increasing revision counter, atomic via `INCR`                  |
| `events`             | Stream        | Ordered log of write events, consumed by Watch (§4.4)                         |

The 32-byte header of each `kv:` value encodes `create_revision`, `mod_revision`, `version`, and `lease_id` as big-endian int64s (Listing 4.2). Storing these inline with the value avoids a round-trip on Get — a single Redis `GET kv:<key>` retrieves both the value and the etcd metadata.

**Listing 4.2.** `kv:` value encoding (excerpt from `store/redis.go`):

```
bytes  1– 8  create_revision  (big-endian int64)
bytes  9–16  mod_revision
bytes 17–24  version
bytes 25–32  lease
bytes 33+    raw value bytes
```

The `keyindex` set exists because Redis has no built-in "list all keys matching a pattern" operation that is safe at scale (`KEYS` is O(N) over the whole keyspace and blocks the server). Maintaining a dedicated set of live keys reduces a prefix scan to a single `SMEMBERS keyindex` followed by an in-memory prefix filter and a batched `MGET kv:<keys...>` for the matching keys. Two round-trips total per Range, independent of total key count.

The `events` stream is the most consequential design choice in the storage layer; the rationale is unpacked in §4.4.

Every write traverses the same pattern (Listing 4.3). The revision is allocated via `INCR global:revision` to obtain a unique increasing integer; the value is written to `kv:<key>` carrying the new revision in its header; the key is added to `keyindex`; and the event is appended to the `events` stream. The latter three operations are pipelined to amortise the network round-trip.

**Listing 4.3.** Write path (simplified).

```go
rev := INCR(global:revision)
PIPELINE {
    SET kv:<key> = header(rev) || value
    SADD keyindex <key>
    XADD events * type PUT key <key> rev <rev> data <value>
}
```

This costs two round-trips per Put (one for the `INCR`, one for the pipelined block); a future optimisation, discussed in the evaluation chapter, is to fold both into a single Lua script, reducing the per-Put cost by one round-trip.

---

## 4.4 Watch protocol

Watch is the part of the etcd interface that Kubernetes exercises hardest, and the part where naive implementations fail in the least obvious ways. This section therefore describes the design at protocol level, including a non-obvious requirement uncovered during integration testing that is not documented in the etcd v3 protocol specification but is load-bearing for kube-controller-manager.

### 4.4.1 The event log

Every write through REtcd appends an event record to a single Redis Stream named `events`. Each entry carries the event type (`PUT` or `DELETE`), the etcd key, the assigned revision, and (for puts) the serialised value. Redis Streams provide ordered append, monotonic IDs, and the `XREAD` command — including its blocking `XREAD BLOCK` form, which suspends the caller until a new entry arrives or a timeout elapses.

A `Watch` gRPC stream is implemented as a long-lived bidirectional connection between a Kubernetes component and REtcd. When the client sends a `WatchCreateRequest` for a key or range, REtcd spawns a goroutine that tails the events stream from a configurable start position and forwards matching events to the client. The flow is shown in Figure 4.2.

> [FIGURE 4.2: Sequence diagram. Lifelines: KubeClient, REtcd, Redis. KubeClient → REtcd: WatchCreateRequest(key, startRev). REtcd → KubeClient: WatchResponse(Created=true). Loop: REtcd → Redis: XREAD BLOCK events from lastID. Redis → REtcd: events[]. REtcd → KubeClient: WatchResponse(events, headerRev=cluster_current). Annotation: "every response carries Header.Revision = current cluster rev, regardless of which event triggered it"]

The naive implementation — sending one `WatchResponse` per event, stamping `Header.Revision` with the event's own revision — is incorrect for Kubernetes use, as the next subsection explains.

### 4.4.2 Per-resource watch cache divergence

The kube-apiserver maintains a watch cache per resource type: separate caches for `/pods`, `/nodes`, `/services`, and so on. Each cache tracks the highest revision it has observed for *its* resource type, derived from the `Header.Revision` field of incoming `WatchResponse` messages.

When a Kubernetes controller (for example, the garbage collector) issues a watch list at revision N, the apiserver waits for the relevant resource's cache to reach N before serving the response. If the cache never reaches N — because no event for that resource type ever arrives carrying a header revision ≥ N — the request times out with the error `Timeout: Too large resource version: N, current: M`.

This failure mode is invisible under low contention: as soon as any event for a resource type arrives carrying a fresh revision, that cache advances and the wait is released. Under realistic Kubernetes operation, however, several resource types see no writes for seconds at a time (cluster-level CRDs, node-status updates in a stable cluster), and the apiserver's watch cache for those types lags behind the global cluster revision. Controllers attempting WatchList against the lagging cache time out, fail to synchronise, and never make progress. The visible symptom is that the DaemonSet controller never schedules pods, the garbage collector never starts, and the cluster reports `NotReady` indefinitely while the kube-controller-manager log fills with `unable to sync caches for ...` messages.

This requirement — that every `WatchResponse` stamp `Header.Revision` with the *cluster's* current revision, not the event's revision, and that idle resource types receive periodic empty responses to advance their caches — is not stated in the etcd v3 protocol specification. It is, however, the behaviour of production etcd, which implicitly satisfies it because every watch response carries the current MVCC store revision.

### 4.4.3 Progress notifications

REtcd's Watch implementation therefore carries two invariants beyond the protocol specification:

1. The `Header.Revision` of every `WatchResponse` is set to `store.CurrentRevision(ctx)` at the time of send — the cluster's current revision — not the matched event's revision.
2. If a watch's `tailWatch` loop processes a batch from Redis Streams that contains no events matching the watched key range, an empty `WatchResponse` (no events, only a header) is sent every second.

The combination ensures that every watch cache, regardless of the activity rate on its resource type, observes the cluster's revision counter advancing. Listing 4.4 sketches the loop.

**Listing 4.4.** Watch loop with progress notifications (simplified from `server/watch.go`).

```go
for {
    events := BlockReadEvents(lastID, batchSize)  // XREAD BLOCK 500ms
    matched := filter(events, key, rangeEnd, startRev)
    rev := store.CurrentRevision(ctx)
    if len(matched) > 0 {
        send(WatchResponse{Header: rev, WatchId: id, Events: matched})
        lastSend = now()
    } else if now() - lastSend > 1s {
        send(WatchResponse{Header: rev, WatchId: id})   // progress tick
        lastSend = now()
    }
}
```

### 4.4.4 Limitations

The design carries two known costs:

- **No revision→stream-ID index.** A watch from a non-zero start revision is implemented by reading from the beginning of the events stream and filtering by revision in Go. For long-running clusters this becomes O(N) per watch establishment; the evaluation chapter quantifies the threshold at which this matters.
- **One Redis connection per watch goroutine in `BlockReadEvents`.** Each watch holds a connection while blocked. The go-redis default pool size (200 connections on a 20-core machine) bounds the number of concurrent watches before the pool is exhausted. For the workloads measured this was not a limit; for production-scale deployments a single-consumer fan-out architecture would be required.

Both are addressed in the future-work discussion.

---

## 4.5 Transactions

Kubernetes never writes a key directly. Every Create, Update, and Delete is wrapped in an etcd `Txn` request carrying a compare-and-swap predicate over the key's `mod_revision`, as Listing 4.5 illustrates.

**Listing 4.5.** The two transaction shapes Kubernetes emits.

```
CREATE                              UPDATE
Compare: mod_revision == 0          Compare: mod_revision == N
Success: Put(key, value)            Success: Put(key, newValue)
                                    Failure: Range(key)         # return current
```

These two patterns account for the overwhelming majority of writes through the apiserver; both are amenable to a single atomic Redis operation.

### 4.5.1 Single-key compare-and-swap via Lua

REtcd uses a server-side Lua script (`store/txn.lua`) to perform single-key compare-and-swap atomically within Redis. The script reads the current value, compares its `mod_revision` against the caller's expected value, and — if the comparison succeeds — performs the write, the revision increment, and the event-stream append in one indivisible operation. On failure it returns the current value so the caller can observe the conflict without an additional round-trip.

The `Txn` handler in `server/kv_txn.go` pattern-matches the incoming request: if it has the shape of a Create or Update, it is dispatched to `store.Txn` (one Lua call, one round-trip). Any transaction not matching these patterns falls through to a generic interpreter, described next.

### 4.5.2 Generic multi-operation transactions

The generic path (`genericTxn` in `server/kv_txn.go`) evaluates each compare against the current key state and executes each operation independently. Comparison and execution are not atomic across operations — the design here is a deliberate simplification, since the only transactions Kubernetes is known to send through this path are the apiserver compactor's version-based CAS on a single dedicated key, where the lack of multi-key atomicity is benign.

Bundling multiple operations into a single Lua script — collapsing N round-trips into 1 — is a known performance improvement deferred to future work; the evaluation chapter quantifies its potential impact based on the observed control-plane transaction shapes.

---

## 4.6 Kubernetes integration

Three integration details deserve specific mention because they are not part of the etcd protocol but are assumed by Kubernetes tooling.

### 4.6.1 The `/version` HTTP endpoint

`kubeadm`'s `ExternalEtcdVersion` preflight check makes a plain HTTP `GET` against `http://<etcd>:2379/version`, expecting a JSON document of the form `{"etcdserver": "<semver>", "etcdcluster": "<semver>"}` reporting a version not older than the etcd version the kubeadm release was built against. Kubeadm 1.32.13 requires `>= 3.5.24-0`.

REtcd serves this endpoint on the same TCP port as the gRPC service by multiplexing the two protocols using `cmux`. Connections whose first HTTP/2 SETTINGS frame includes `content-type: application/grpc` are dispatched to the gRPC server; HTTP/1.1 connections are dispatched to a small HTTP mux that serves `/version` and `/health`. This is the same multiplexing pattern production etcd uses.

### 4.6.2 gRPC keepalive policy

By default, the Go gRPC server applies an enforcement policy that disconnects clients pinging more often than once every five minutes when no stream is active. The kube-apiserver pings its etcd connections on the order of every 10–30 seconds, which exceeds the default and provokes the server to respond with `GOAWAY: ENHANCE_YOUR_CALM`. The apiserver then reconnects and is GOAWAY'd again, producing a connection storm that prevents any control-plane progress.

REtcd configures `grpc.KeepaliveEnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}` to admit the apiserver's ping rate. The setting is documented in `main.go` and is treated as a non-tunable correctness requirement rather than a performance knob.

### 4.6.3 Lease semantics

Kubernetes uses etcd leases for leader election (per-component lease keys with sub-minute TTLs) and pod liveness signalling (longer leases). REtcd implements leases as Redis keys with `EX` expiry, tracking which etcd keys are attached to which lease via a set per lease, and revoking those etcd keys when the corresponding Redis lease key expires. A background reaper runs once per second to detect expired leases and clean up attached keys; this is a simplification of etcd's lease semantics that is functionally sufficient for the Kubernetes lease patterns observed in integration testing.

---

## 4.7 Design alternatives considered

Several alternative substrates were considered. Each is rejected for reasons that bear on the thesis position.

**Embedded key-value store (Pebble, BadgerDB).** Replaces the Redis hop with in-process function calls; eliminates the per-operation kernel-IPC cost (~50–100 µs) at the cost of reimplementing the event log, the atomic CAS, and the persistence policy in Go. Forecloses the operational properties Redis provides (independent restart, externally tunable persistence, external debuggability via `redis-cli`) that motivated the substrate choice in the first place. Treated as future work for the variant comparison.

**Embedded bbolt (etcd's own storage engine).** Equivalent to building production etcd minus Raft. Maximum prior art; minimum novelty.

**SQL backend (kine).** k3s' kine is precisely this design point: an etcd-compatibility shim that stores its data in a relational database (sqlite, MySQL, PostgreSQL). Kine occupies a real position in the substrate design space, and the evaluation chapter uses it as a fourth comparison point. REtcd is differentiated from kine by being a network-attached *key-value* substrate rather than an in-process *SQL* substrate; the operational and performance properties differ.

**Raft-replicated REtcd.** Three or five REtcd instances coordinating writes via Raft. Would provide etcd-grade HA at the cost of recapitulating etcd's central complexity. Rejected as out of scope for the single-instance target.

**Bypassing the apiserver write path (KUBEDIRECT).** The closest concurrent work [Wang et al., NSDI '26]. KUBEDIRECT couples function-as-a-service-specific controllers directly to sandbox managers, eliminating the chain of sequential apiserver writes that constitute the bulk of Knative cold-start latency. REtcd operates orthogonally: it accepts the apiserver write path as given and explores the substrate beneath it. The two designs are compared in the evaluation chapter as points in a two-dimensional design space (substrate × control-plane path).

---

## 4.8 Non-goals

To delimit the scope of the design, the following are explicit non-goals:

- **High availability.** REtcd is a single-instance store. A failure of the Redis instance or the REtcd process renders the cluster unavailable until restart. This is acceptable in the target environment.
- **Multi-version concurrency control beyond Kubernetes' usage.** Production etcd supports point-in-time queries at arbitrary historical revisions. REtcd retains only the latest value per key and the event log; historical reads are not supported. Kubernetes does not exercise this capability.
- **Compaction.** Production etcd periodically compacts old revisions to bound storage. REtcd's `events` stream grows unbounded; for long-running clusters this would require attention but is irrelevant on the timescales considered in this thesis.
- **Encryption at rest, gRPC TLS.** Kubeadm permits external etcd over plain HTTP/2 for development; production deployments would require these features but they are not part of the substrate evaluation.
- **Network-level Kubernetes features.** Pod-network CNI, service mesh integration, ingress: these are orthogonal to the storage substrate question.

The non-goals make explicit what kind of claim the thesis can support: REtcd is a design point appropriate for single-instance development, edge, CI, and multi-tenant control-plane settings, and the evaluation chapter quantifies its performance against the alternative substrate choices in that regime.
