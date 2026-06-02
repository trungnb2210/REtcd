# REtcd benchmark session — 2026-05-24 to 2026-05-27

End-to-end working REtcd image, first quantitative comparison against native etcd, and an honest map of where the architecture sits in the K8s storage-substrate design space.

## TL;DR

- **REtcd v4** (`ghcr.io/trungnb2210/retcd:v4`) is now the canonical image. It backs a 3-node Kubernetes 1.32.13 + Knative Serving 1.15 + Kourier cluster end-to-end. First time the full pipeline works.
- **First quantitative result:** on the same 3-node CloudLab xl170 hardware with the same Knative + invitro + azure_10 trace + 5-min window:
  - REtcd: 44/44 invocations succeeded, median 73.9 ms, p99 158.6 s.
  - Native etcd: 43/44 (one 502 EOF), median 84.8 ms, p99 10 s.
  - REtcd's body (CDF up to ~0.6) is indistinguishable from etcd; the tail diverges, dominated by `grpcConnEstablish` during Knative scale-from-zero.
- **Load-bearing protocol finding:** REtcd's Watch RPC must stamp every `WatchResponse.Header.Revision` with the *cluster's* current revision (not the event's rev), and must emit periodic empty progress notifications when no matching events fire. Without this, K8s controllers cannot bootstrap. Documented in `memory/feedback_retcd_watch_progress.md`.
- **Outputs:** `~/me/fyp/research/results-retcd-3node-smoke/` has both CSVs, a CDF plot, and a results README.

## Code changes that made v4

All in `~/me/fyp/retcd`. Compared to the previously-deployed v0 image, v4 adds:

| Change | File | Why |
|---|---|---|
| `genericTxn` rewrite — `evalCompares`, `evalCompare`, `executeOp` helpers | `server/kv_txn.go` | v0 nil-dereffed when apiserver's compactor used a Version-based CAS. Fix restructures the fallback Txn path. |
| Removed per-request gRPC logging interceptor | `main.go` | Dominated p99 under any real load. |
| `KeepaliveEnforcementPolicy{MinTime: 5s, PermitWithoutStream: true}` and `KeepaliveParams{Time: 2h, Timeout: 20s}` | `main.go` | Without it, apiserver's pings every ~10s tripped Go gRPC's default 5-minute minimum and got slammed with `ENHANCE_YOUR_CALM` GoAways, causing reconnect storms that bricked controllers. |
| HTTP `/version` and `/health` endpoints on the same `:2379` port via `github.com/soheilhy/cmux` | `main.go` | kubeadm v1.32.13 preflight `ExternalEtcdVersion` requires `GET http://etcd:2379/version` to return JSON with `etcdserver >= 3.5.24-0`. We report `3.5.24`. |
| Watch sends `Header.Revision = store.CurrentRevision(ctx)` (not event rev), and emits empty progress notifications every 1 s | `server/watch.go` | **The critical bug**: K8s controllers track per-resource-type watch caches. If `/nodes` sees no events while `/pods` does, the `/nodes` cache lags. When a controller asks the apiserver to wait for resourceVersion N, the apiserver waits for *its* cache to reach N — which it never does. Symptom: every controller logs `unable to sync caches for garbage collector / resource quota / daemonset / ...`, DaemonSets stay `desired=0`, Flannel never schedules, cluster permanently NotReady. |

`go.mod` gained `github.com/soheilhy/cmux v0.1.5`. Tests at `tests/server/` still pass.

## Image generations and what each was for

| Tag | What's in it | Status |
|---|---|---|
| `:v0` | Pre-fix scaffold; nil-deref in genericTxn, no /version, no keepalive policy. Required `--ignore-preflight-errors=ExternalEtcdVersion` and `--etcd-compaction-interval=0` workarounds on apiserver. | Historical. Don't use. |
| `:v1` | First "fixed" build. Has cmux + /version reporting "3.5.0". Pushed during initial bring-up. | Retired. Version string too low for kubeadm 1.32.13. |
| `:v2` | Same as v1 with version string bumped to 3.5.24. | Retired. Watch bug still present. |
| `:v3` | Bisect attempt: dropped cmux + keepalive to see if those caused the cache-sync failure. They didn't. | Retired. |
| `:v4` | Watch progress notifications added; all other v2 features restored. | Previous canonical. |
| `:v5` | v4 + Unix-socket Redis support. `REDIS_ADDR=unix:///path/sock` triggers `Network: "unix"` in the go-redis client; TCP form unchanged. | Superseded by v6. |
| `:v6` | v5 + Prometheus instrumentation on `/metrics` (same `:2379` port via cmux). Adds a unary RPC-latency interceptor + watch-path metrics. | Superseded by v7. |
| `:v7` | v6 + watch silent-death fix: `tailWatch` retries transient `BlockReadEvents` errors (50 ms backoff, from the same `lastID` so no events are skipped) and emits `Canceled=true` to the client after persistent failure instead of returning silently. Adds `retcd_watch_read_errors_total` / `retcd_watch_cancels_total` counters and the first watch unit tests. | **Current canonical.** |

## v7 → cold-start stall fix (watch silent-death)

**Root-cause hypothesis (strong, not yet cluster-confirmed):** the cold-start overhead distribution is *bimodal* — most invocations complete with ~10 ms overhead, but ~27% (in the 44-sample smoke run) stall for 100–158 s, and those stalls *cluster at discrete values* (~98 s, ~101 s, ~158 s). Discrete clustering implies recovery on a fixed timer (a controller resync interval), not gradual slowness — i.e. a watch event was never delivered and the system only recovered on resync.

The mechanism, visible by code inspection in `server/watch.go`: `tailWatch` did `if err != nil { return }` on any `BlockReadEvents` error. Under cold-start load (write burst + many concurrent watches → go-redis pool pressure), a transient error would kill the per-watch goroutine *silently* — the gRPC stream stayed open, the apiserver was never told, and that resource type's watch cache went stale until a resync re-established it.

**Fix (v7):**
- Transient error → retry from the same `lastID` after 50 ms (no events skipped), incrementing `retcd_watch_read_errors_total`.
- `ctx` cancelled → return (legitimate teardown).
- Persistent failure (≥30 consecutive errors, ~1.5 s) → send `WatchResponse{Canceled: true, CancelReason: ...}` so the client re-establishes in ms, incrementing `retcd_watch_cancels_total`. Never die silently.

**How to confirm on the next reservation:** deploy v7, run the loader, and check (a) the stall rate in the overhead distribution (`responseTime − actualDuration`) collapses from ~27% toward etcd's ~0%, and (b) `retcd_watch_read_errors_total` is non-zero (proves transient errors *were* happening and are now retried) while `retcd_watch_cancels_total` stays low. If stalls persist despite low error counts, the cause is elsewhere (send-side head-of-line blocking — check `retcd_watch_send_mutex_wait_seconds` — or read-side fan-out) and the structural fixes (single-consumer fan-out, per-watch send queues) are next.

**Regression tests added** (`tests/server/watch_test.go`, the first watch unit tests in the repo): `TestWatchRetriesTransientErrors` (3 injected errors → event still delivered) and `TestWatchCancelsOnPersistentFailure` (permanent failure → `Canceled` emitted, not silent). Uses a new error-injection hook on `fakeStore` (`failFirst`, `failAll`).

**Important measurement correction discovered this session:** invitro timing fields are *microseconds*, and `grpcConnEstablish` is time-to-response-headers (includes function execution), **not** connection-establishment time. The scale-up residual must be computed as `responseTime − actualDuration`. Recomputing on that basis is what revealed etcd's apparent "10 s tail" was mostly legitimate 8.5 s busy-loop function execution (only ~1.6 s real cold-start overhead), while REtcd's was genuine 100–158 s stalls. The `results-retcd-3node-smoke/README.md` `responseTime` table conflates execution with overhead and should be redone on the residual.

## v6 → instrumentation for attributing the cold-start tail

v6 exposes Prometheus metrics at `http://<node>:2379/metrics`. Each metric tests a specific hypothesis about where the ~137 s scale-up residual goes (the residual is `responseTime − actualDuration`; note `grpcConnEstablish` is *not* connection time — it is time-to-response-headers and includes function execution, so do not use it for this).

| Metric | Hypothesis it tests |
|---|---|
| `retcd_rpc_duration_seconds{method}` | Is any single write RPC class slow? (write-path cost) |
| `retcd_watch_delivery_seconds` | Latency from event append (Redis stream-entry timestamp) to client delivery — the watch-lag signal |
| `retcd_watch_send_mutex_wait_seconds` | Time blocked acquiring the per-stream `sender` mutex — **the head-of-line-blocking signal** |
| `retcd_watch_send_write_seconds` | Time inside the gRPC stream `Send` — flow-control backpressure from a slow apiserver |
| `retcd_active_watches` | Concurrent watch goroutines (context for pool pressure) |
| `retcd_watch_catchup_events` | Events scanned before a watch's first delivery — quantifies the O(N) `lastID="0"` scan |
| `retcd_redis_pool_{total_conns,idle_conns,timeouts_total,misses_total}` | go-redis pool exhaustion (rising `timeouts_total` = watches starving writes of connections) |

**How to read the result after a loader run:**

- If `retcd_watch_send_mutex_wait_seconds` has a fat tail (p99 in seconds) → head-of-line blocking confirmed; fix is per-watch send queues.
- If `retcd_watch_delivery_seconds` p99 is seconds-to-minutes while `retcd_watch_send_*` are small → events sit unsent in the tail loop (slow `BlockReadEvents` / catch-up scan), not the send path.
- If `retcd_watch_catchup_events` p99 is large (thousands) → the O(N) establishment scan matters; fix is the revision→stream-ID index.
- If `retcd_redis_pool_timeouts_total` climbs during the run → connection exhaustion; fix is the single-consumer fan-out.
- If `retcd_rpc_duration_seconds` is flat and small across all of the above → the tail is *not* in REtcd at all; look at K8s-side scheduling/image-pull/activator.

**Scrape recipe (no Prometheus server needed for a smoke run):** snapshot `/metrics` before and after the loader, diff the histograms.

```bash
# on the laptop, against the control-plane node
curl -s http://10.10.1.1:2379/metrics > /tmp/metrics_before.txt
# ... run the loader ...
curl -s http://10.10.1.1:2379/metrics > /tmp/metrics_after.txt
# eyeball the watch histograms
grep -E 'retcd_watch_(delivery|send_mutex_wait|send_write)_seconds_(bucket|sum|count)' /tmp/metrics_after.txt
grep -E 'retcd_watch_catchup_events_(sum|count)|retcd_redis_pool_(timeouts|misses)_total' /tmp/metrics_after.txt
```

For a proper time-series view, point the existing Prometheus stack (`load-test/setup.sh`) at the node by adding a scrape target for `:2379/metrics`, or run a throwaway `prometheus --config.file=...` on the laptop scraping the node.

**Deployment for v6 is identical to v5** (Unix-socket Redis args unchanged); just bump the image tag in `retcd.service` to `:v6`.

## v5 → Unix socket deployment notes

The TCP-localhost Redis hop costs ~50–100 µs per RPC from kernel TCP machinery (syscalls, buffer copies, context switches). A Unix domain socket skips the TCP stack — same kernel I/O, no sequencing/checksums/window management — and drops the per-RPC overhead to ~10–30 µs. Multiplied across the ~7–8 sequential apiserver writes in a Knative scale-from-zero, that's worth a few hundred µs off the cold-start tail.

Both Redis and REtcd run in containerd's `retcd` namespace with `--net-host`. They share the network namespace but not the filesystem; for the socket to be visible to both, bind-mount a host directory into both containers.

**Updated `retcd-redis.service`** (on N0, replaces the previous version):

```ini
[Service]
ExecStartPre=-/usr/bin/mkdir -p /run/retcd
ExecStartPre=-/usr/bin/ctr -n retcd task kill -s SIGKILL redis
ExecStartPre=-/usr/bin/ctr -n retcd container rm redis
ExecStart=/usr/bin/ctr -n retcd run --rm --net-host \
  --mount type=bind,src=/run/retcd,dst=/run/retcd,options=rbind:rw \
  docker.io/library/redis:7-alpine redis \
  redis-server --port 0 --unixsocket /run/retcd/redis.sock --unixsocketperm 700 \
  --appendonly yes --dir /data
ExecStopPost=-/usr/bin/ctr -n retcd task kill -s SIGKILL redis
```

(`--port 0` disables TCP listener entirely; keep `--port 6379 --bind 127.0.0.1` instead if you want TCP available for redis-cli debugging from outside the container.)

**Updated `retcd.service`** (on N0):

```ini
Environment=LISTEN_ADDR=:2379
Environment=REDIS_ADDR=unix:///run/retcd/redis.sock
ExecStart=/usr/bin/ctr -n retcd run --rm --net-host \
  --mount type=bind,src=/run/retcd,dst=/run/retcd,options=rbind:rw \
  --env LISTEN_ADDR=${LISTEN_ADDR} --env REDIS_ADDR=${REDIS_ADDR} \
  ghcr.io/trungnb2210/retcd:v5 retcd /retcd
```

**Verification after bring-up:**

```bash
sudo ls -la /run/retcd/redis.sock          # exists, owned by container user
sudo ctr -n retcd task exec --exec-id ping redis redis-cli -s /run/retcd/redis.sock ping
curl -s http://127.0.0.1:2379/version       # REtcd still serves /version
```

If REtcd can't connect, it'll log `failed to dial unix /run/retcd/redis.sock` — usually a missing bind mount or socket permissions issue.

**A/B benchmark suggestion:**

Re-run the same azure_10 / 5-min loader on v5 right after the v4 numbers. Same hardware, same trace, only changing the socket type. The delta is your "Unix-socket optimization" data point for the write-up. Expected: small but measurable improvement on the body of the CDF, marginal improvement on the tail (the tail is dominated by Watch-fanout latency, not Redis RTT).

If you ever re-roll: bumping versions just changes the `versionInfo` string in `main.go` and the maintenance Status response in `server/maintenance.go`. Watch progress logic lives entirely in `server/watch.go:tailWatch`.

## Cluster setup recipe that works

3-node CloudLab `emulab-ops/small-lan`, hardware type **xl170**, OS **UBUNTU22-64-STD**, Utah site. Internal addresses 10.10.1.1/2/3.

1. **Per-node install (parallel ssh fan-out from laptop):** `/tmp/node-setup.sh` installs containerd, kubelet/kubeadm/kubectl v1.32, disables swap, configures sysctl. ~5 min for all 3.
2. **Bootstrap Redis + REtcd on N0** as systemd services in containerd's `retcd` namespace. Uses `--net-host` so the container sees the host's loopback. `redis-server --bind 127.0.0.1 --appendonly yes`, env `REDIS_ADDR=127.0.0.1:6379` for REtcd. Verify `curl http://127.0.0.1:2379/version` returns the JSON.
3. **kubeadm init on N0** with `etcd.external.endpoints=[http://127.0.0.1:2379]`. No preflight skip needed for v4. ~2 min.
4. **kubectl + Flannel CNI**. Apply `https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml`. Wait for nodes to become Ready.
5. **Worker join.** Drive from laptop (CloudLab SSH keys aren't deployed inter-node). Each worker becomes Ready within ~30 s once kubeadm join completes.
6. **Knative Serving v1.15.0 + Kourier.** Apply CRDs, core, kourier net layer. Wait for `knative-serving` and `kourier-system` deployments Available.

## Knative configuration knobs (all required for invitro)

Default Knative will reject the invitro-style ksvcs. Patches needed *before* any ksvc deploys:

```bash
# Allow scale-init=0 (invitro deploys with --scale-init 0)
kubectl patch configmap/config-autoscaler -n knative-serving \
  --type=merge -p '{"data":{"allow-zero-initial-scale":"true"}}'

# Allow nodeSelector in pod template (the trace_func_go.yaml uses it)
kubectl patch configmap/config-features -n knative-serving \
  --type=merge -p '{"data":{"kubernetes.podspec-nodeselector":"enabled"}}'

# Route via Kourier
kubectl patch configmap/config-network -n knative-serving \
  --type=merge -p '{"data":{"ingress-class":"kourier.ingress.networking.knative.dev"}}'

# DNS-resolvable wildcard domain pointing at Kourier ClusterIP.
# sslip.io resolves *.W.X.Y.Z.sslip.io to W.X.Y.Z, so URLs like
# myksvc.default.<KOURIER_IP>.sslip.io route directly to Kourier's ClusterIP.
KOURIER_IP=$(kubectl -n kourier-system get svc kourier -o jsonpath="{.spec.clusterIP}")
kubectl patch configmap/config-domain -n knative-serving \
  --type=merge -p "{\"data\":{\"${KOURIER_IP}.sslip.io\":\"\"}}"
kubectl patch configmap/config-domain -n knative-serving \
  --type=json -p='[{"op":"remove","path":"/data/example.com"}]'  # optional cleanup

# Worker-node label that the trace function YAML's nodeSelector matches
kubectl label node node1.<...> loader-nodetype=worker --overwrite
kubectl label node node2.<...> loader-nodetype=worker --overwrite
```

**Do not** `iptables -F` on N0 — it nukes the rules kube-proxy installs. We did this during early debugging and had to restart the kube-proxy DaemonSet to recover.

## invitro patch — `knative.sh` rewrite

`vhive-serverless/invitro` ships `pkg/driver/deployment/knative.sh` that calls `kn service apply ... --scale-init 0 --concurrency-target 1 --wait-timeout 2000000`. With kn 1.15 and `--scale-init 0`, the kn process never returns even though the Knative Service reaches Ready — apparently a kn/Knative interaction bug. The Go loader stays blocked on the kn command and serial deploys take ~5 min/ksvc. We replaced the script with:

```bash
# /tmp/knative.sh.patched — drop-in replacement
# - kubectl apply instead of kn
# - inject autoscaling.knative.dev/initial-scale annotation directly via Python
# - poll for Ready=True AND a successful warm curl before returning
# - emit "Service X applied at URL:\nhttp://..." so invitro's urlRegex matches
```

Without the warm-curl gate, invocations fire before Kourier's routes propagate and the loader logs 100% 404s. With the gate, invocations succeed at ~95–100%.

## Measurement results

```
3-node xl170 · k8s 1.32.13 · Knative 1.15 · Kourier · azure_10 trace · 5 min
                                                       responseTime (ms)
backend       n     min   median     mean      p95      p99      max
REtcd v4     44    5.5     73.9  26125.8  107500   158639   158639
Native etcd  43    5.0     84.8   1358.8    8148    10042    10042
```

The 158-second observation in REtcd's tail isn't a function-execution time — it's `grpcConnEstablish`. The function itself ran fine once invoked. The wait was for Knative + REtcd to scale a scaled-to-zero ksvc back up. That cycle is `autoscaler → SKS → Deployment → scheduler → kubelet → endpointslice`, ~7-8 sequential apiserver writes, each crossing apiserver → REtcd → Redis. Per-RPC RTT × N writes is the gap.

Plot at `~/me/fyp/research/results-retcd-3node-smoke/smoke_responseTime_cdf.png`.

Generated by `~/me/fyp/research/plot_smoke_comparison.py` from the two CSVs in `~/me/fyp/research/results-retcd-3node-smoke/`.

## What this *can't* claim yet

- **44 samples is too few.** The p99 is the single worst observation. Need azure_50+ over 15-30 min for a real CDF.
- **One run per backend.** No variance characterization. At least 3 runs each before claiming "REtcd is X% slower at p99."
- **Single control-plane node.** Stacked etcd was effectively single-instance, so no Raft RTT for the etcd baseline. Fair single-instance comparison, not fair HA comparison.
- **3 nodes is not the paper testbed.** KUBEDIRECT paper §6 uses 80 nodes. Triangulation against their published numbers requires matching scale.

## Architectural map (where REtcd sits)

| Variant | Substrate | Network hops to storage | Raft? | MVCC surface | Comparable prior art |
|---|---|---|---|---|---|
| native etcd | bbolt (in-process) | 0 | yes | full | self |
| REtcd-Redis (current) | Redis (TCP localhost) | 1 | no | minimal | none — this is the artefact |
| REtcd-Pebble (hypothetical) | Pebble (in-process) | 0 | no | minimal | k3s + Kine (uses sqlite/postgres) |
| KUBEDIRECT (paper, no public code) | bypasses apiserver write path | n/a | n/a | n/a | self |

The extra hop is REtcd-Redis's signature cost. Lua scripts can cut RTT-per-Put from 2 to 1, and bundling multi-op `genericTxn` into one Lua script collapses N RTTs into 1, but the floor of "1 RTT per apiserver write" remains. To go below, you'd have to embed the K-V store — at which point you're no longer Redis-backed.

## Thesis framing — three options

1. **Feasibility:** "Redis works as etcd substrate for single-instance K8s." Modest claim, weak novelty.
2. **Negative result:** "Network-attached storage is the wrong substrate for the K8s control plane; here's the evidence." Strong if argued with care.
3. **Design-space triangulation:** "Position REtcd, etcd, kine, and KUBEDIRECT as points in the K8s storage design space; characterize the tradeoffs." Strongest framing. Lets the thesis sentence be:

> *"We built REtcd, the first Redis-backed etcd-compatible store for Kubernetes, and used it to characterize where storage architecture matters in the K8s control plane. The dominant cost we observe is not steady-state throughput but per-RPC latency amplification during Knative scale-from-zero. We also identify a previously-undocumented Watch protocol requirement (cluster-current revision on every response + periodic progress notifications) that any non-etcd backend must satisfy to back K8s."*

Where the actual novelty lives:
- **Artefact:** REtcd-Redis didn't exist before; now it does, with passing functional integration against K8s 1.32 + Knative 1.15.
- **Protocol finding:** the Watch progress-notification requirement is concrete, transferable, useful to anyone building an etcd alternative.
- **Measurement:** the cold-start RTT-amplification mechanism is the empirical contribution.

## Concrete next steps

1. **More samples for a real CDF.** Re-run with azure_50 (or larger), 15-30 min window, both backends. Same 3-node cluster.
2. **Optimisation pass — Lua-ify Put and Delete.** Currently 2 RTTs per write (INCR + pipeline). One Lua script does both in 1 RTT. Probably the biggest single mechanical lever; ~1 day.
3. **Optimisation pass — bundle multi-op `genericTxn` into one Lua call.** Today each op is a separate RTT. ~3 days.
4. **(stretch) Pebble variant** for the future-work / discussion section. Would close most of the remaining gap. ~2 weeks; only if there's time and the thesis would benefit.
5. **80-node final run** matching paper §6 testbed. Requires fresh CloudLab reservation sized to xl170 × 80 in Utah. Bring-up scales but storage and worker-label scripts work as-is.

## Operational lessons (so we don't relearn them)

- CloudLab default expiry is ~16 h. Plan reservations around that. The first reservation (`retcd-dev-2`) expired before we finished; `retcd-dev-3` (hp126/149/137) is the current cluster.
- Per-node bring-up is ~5 min; full pipeline to "loader runs" is ~30-40 min if everything goes smoothly. Allow padding.
- GHCR packages default to private. After first push, manually set visibility to public via the GitHub package settings page.
- Don't `iptables -F` on a node running kube-proxy.
- Don't leave `.bak` files in `/etc/kubernetes/manifests/` — kubelet treats every file there as a static-pod manifest.
- The kubelet config has `fileCheckFrequency: 0s` by default. Edits to static-pod manifests aren't picked up until kubelet restarts. Use `mv` out-and-back to force a content-hash reload.
- `pkill -f loader` from over SSH is sometimes flaky (255 exit code). Retry, or scp a script and `bash /tmp/cleanup.sh`.
