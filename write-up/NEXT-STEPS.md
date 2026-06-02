# REtcd — resume-here / next-steps (as of 2026-05-28)

Single-page state so this can be picked up after clearing chat. Detail lives in
`session-notes-2026-05-26.md` (operational) and `design.md` (thesis chapter).

## Where things stand

- **Canonical image: `ghcr.io/trungnb2210/retcd:v7`** (on GHCR, public). Contains, cumulatively:
  - genericTxn nil-deref fix
  - gRPC keepalive policy (stops `ENHANCE_YOUR_CALM` storms)
  - HTTP `/version` + `/health` on :2379 via cmux (kubeadm preflight; reports 3.5.24)
  - **Watch progress notifications** (cluster-current revision + idle ticks) — without this, controllers never bootstrap
  - Unix-socket Redis support (`REDIS_ADDR=unix:///path`)
  - Prometheus `/metrics` (watch-path instrumentation)
  - **Watch silent-death fix** (retry transient errors, cancel loudly on persistent — the suspected cold-start-stall cause)
- **No live cluster** (reservations expire ~16 h). All CloudLab clusters from this work are gone.
- **First smoke result exists but is thin** (n=44). Headline finding below.
- Tests pass (`go test ./tests/server/...`), including the first watch unit tests.

## The headline finding (from the n=44 smoke run)

- Warm path: REtcd ≈ etcd (~7–9 ms overhead). Substrate is off the warm path.
- Cold-start overhead (`responseTime − actualDuration`):
  - etcd: unimodal ~1.6 s, healthy, no stalls.
  - REtcd: **bimodal** — ~27% of cold starts stall to 100–158 s, clustering at discrete values (~98/101/158 s) = signature of a dropped/late watch event recovered only on a fixed resync interval.
- v7's watch fix targets exactly this. **Not yet cluster-confirmed.**

## Measurement gotchas (do not relearn)

- invitro timing fields are **microseconds**.
- Use **overhead = `responseTime − actualDuration`**, never raw `responseTime` (it includes function execution; Azure trace has 8 s+ functions) and never `grpcConnEstablish` (it's time-to-response-headers, also includes execution).
- **n=44 → "p99" = the max.** Not a real percentile. Do not report p99 from the smoke run.

## Immediate next actions

### Off-cluster (now, no reservation needed)
1. Write Background + Implementation chapters (Design already drafted in `design.md`).
2. (Optional, ~10 min) regenerate `research/results-retcd-3node-smoke/smoke_responseTime_cdf.png` on overhead instead of raw responseTime; update `research/plot_smoke_comparison.py`. README stats already corrected.

### Next reservation (one uninterrupted ~2–3 h block)
Goal: validate v7's watch fix + collect large-n data for a real CDF.
1. Provision 3× xl170, UBUNTU22-64-STD, Utah (CloudLab `emulab-ops/small-lan`).
2. Bring up per `session-notes-2026-05-26.md` (node setup → Redis+REtcd **v7** with Unix-socket args → kubeadm external etcd → Flannel → join workers → Knative 1.15 + Kourier → the 4 config patches → label workers `loader-nodetype=worker`).
3. Trace/loader setup (Go, kn, git-lfs, clone dirigent+invitro, build loader, patched `knative.sh`, subset azure_10).
4. scp `research/invitro-rps-configs/*.json` to `~/invitro/cmd/`.
5. **Dry run** `config_rps_dryrun.json` (1 min) — confirm RPS invocations succeed before committing.
6. Snapshot `curl :2379/metrics > before.txt`; run `config_rps_retcd.json` (15 min, ~2,700 cold starts); snapshot `after.txt`.
7. Tear down → switch to native stacked etcd (kubeadm config WITHOUT `etcd.external`) → redeploy Knative + patches + labels → run `config_rps_etcd.json`.
8. Pull both CSVs + metrics to laptop.

### Off-cluster (after the run)
9. Compute overhead distributions; check: did REtcd's >5 s stall rate collapse toward etcd's ~0%?
10. Check `retcd_watch_read_errors_total` (non-zero = transient errors happened and were retried) and `retcd_watch_cancels_total` (should stay low). If stalls persist with low errors → check `retcd_watch_send_mutex_wait_seconds` (head-of-line blocking) → next fix is per-watch send queues.
11. Write Results + Discussion from real n≈2,700 data.

## Thesis framing (decided)

Design-space triangulation: REtcd-Redis vs native etcd vs kine vs KUBEDIRECT (cited).
Novelty = (a) the artefact (first Redis-backed etcd shim for K8s), (b) the Watch
progress-notification protocol requirement, (c) the cold-start watch-delivery
stall finding + fix. Honest position: warm-equivalent, cold-start gap
characterised and (pending v7 confirmation) fixed. Full azure_500/80-node run is
the paper-matched final; 3-node RPS runs are the dev/validation evaluation.

## Known remaining work (priority order)
1. Big-n RPS run on v7 (above) — validates the fix, gives real percentiles.
2. If stalls remain: single-consumer watch fan-out (removes pool-pressure errors) + per-watch send queues (head-of-line).
3. kine + sqlite as a 4th comparison point (prior art — examiners will ask).
4. 80-node paper-matched final run.
5. (stretch / future work) embedded Pebble variant to isolate the network-hop cost.
