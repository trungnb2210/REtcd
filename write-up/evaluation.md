# Evaluation

This chapter evaluates REtcd against native etcd along the axis the design chapter identified as REtcd's signature cost: per-operation latency. The evaluation is deliberately scoped to two complementary experiments. §5.1 states the questions. §5.2 describes the two testbeds. §5.3 reports the primary result — a controlled microbenchmark that isolates the apiserver→storage write path and shows that, in steady state, REtcd's single Redis hop is not only competitive with etcd's in-process bbolt but exhibits a markedly tighter latency tail. §5.4 reports a preliminary macrobenchmark of Knative cold-start latency under a serverless trace, where the opposite picture emerges in the tail; §5.4 is explicit about why this result is preliminary and what it can and cannot support. §5.5 enumerates the threats to validity.

The headline finding is a two-part one, and the two parts are not in tension: *per isolated operation, REtcd matches or beats etcd; chained across the many sequential writes of a Knative scale-from-zero, REtcd's per-RPC overhead compounds into a tail regression.* The mechanism connecting them — latency amplification across sequential control-plane writes — is the empirical contribution of this chapter.

---

## 5.1 Evaluation questions

The evaluation is organised around three questions:

- **EQ1 — Steady-state write latency.** For the individual write operations that dominate Kubernetes control-plane traffic (pod create, pod delete), how does REtcd's request latency compare to native etcd, across the full distribution from median to tail?
- **EQ2 — End-to-end cold-start latency.** Under a realistic Function-as-a-Service workload (Knative Serving driven by a production-derived invocation trace), how does the choice of storage backend affect the latency a client observes, including the scale-from-zero path?
- **EQ3 — Cost attribution.** Where does any observed difference originate — in the storage substrate's per-operation cost, or in the number and sequencing of operations the control plane issues?

EQ1 is answered with a controlled microbenchmark (§5.3) and is the result the thesis stands on. EQ2 is answered with a single preliminary run per backend (§5.4); its role is to motivate EQ3 and the cost-attribution argument, not to support a precise quantitative claim. EQ3 is addressed by reading the two results together.

---

## 5.2 Experimental setup

Two testbeds are used, chosen to isolate different parts of the system.

### 5.2.1 Microbenchmark: pod-churn against a kwok-backed control plane

The microbenchmark (`load-test/load_test.py`) is an open-loop Poisson load generator that drives pod-lifecycle operations directly against the kube-apiserver. Each *lifecycle* is one pod `CREATE` followed, after a fixed hold time, by a `DELETE`; new lifecycles arrive as a Poisson process with mean rate λ. The generator records the wall-clock latency of every apiserver request to CSV and computes per-trial validity (a trial is invalid if the non-conflict failure rate exceeds 1%).

The critical methodological choice is that pods are scheduled onto **kwok** virtual nodes (`nodeSelector: type=kwok`), not real kubelets. kwok acknowledges pod binding without starting containers, which removes scheduling, image-pull, and kubelet latency from the measurement. What remains on the measured path is exactly the apiserver→storage write: admission, validation, and the etcd `Txn`/`Range`/`DeleteRange` that the backend services. This isolates EQ1's variable — the storage backend — from the rest of the control plane.

**Parameters.** Target rate λ = 30 lifecycles/s; 5 s unmeasured warmup followed by a 30 s measured window; pod hold time 1 s; 3 independent trials per backend. After the measured window the generator drains in-flight lifecycles so that deletes are recorded. These settings yield approximately 2,600 create and 2,600 delete observations per backend in the measured window.

**Backends compared.** Two single-instance configurations, identical in every respect except the storage backend:

1. **etcd** — a stock single-node etcd, the kind cluster default (`kind-etcd-baseline`).
2. **REtcd** — REtcd backed by a single local Redis with AOF persistence (`everysec`).

Both are single-instance: the etcd baseline runs no Raft quorum, so this is a *fair single-instance comparison*, not a comparison against highly-available etcd. The comparison reported in §5.3 uses, for each backend, a run in which all three trials were valid with a zero non-conflict failure rate; earlier REtcd runs that contained an invalid trial (driven into overload, peak in-flight 313) or a configuration fault are excluded and noted in §5.5.

> [FIGURE 5.1: Microbenchmark topology. Boxes: load_test.py (open-loop Poisson generator) → kube-apiserver → {etcd | REtcd → Redis}; a separate box "kwok virtual nodes" attached to the scheduler with annotation "binds pods without running containers — removes kubelet latency from the measured path". Annotation on the generator→apiserver edge: "measured: per-request latency". TODO: state the host hardware (CPU model, cores, RAM) and kind/Kubernetes/etcd/Redis versions on which the microbenchmark ran.]

### 5.2.2 Macrobenchmark: Knative cold-start on CloudLab

The macrobenchmark measures end-to-end client-observed response time for serverless function invocations, including the scale-from-zero path that dominates FaaS cold-start latency. It runs on a 3-node CloudLab cluster (hardware type **xl170**, Ubuntu 22.04) running Kubernetes 1.32.13, Knative Serving 1.15 with the Kourier ingress, and is driven by the `invitro` loader replaying the `azure_10` invocation trace over a 5-minute window. The same cluster, trace, and window are used for both backends; only the storage backend is swapped.

This testbed answers EQ2 but, as §5.4 makes explicit, the run reported here is a single smoke run per backend with a small number of in-window invocations, and is presented as a preliminary observation rather than a characterised distribution.

---

## 5.3 Steady-state write latency

Table 5.1 reports the latency distribution for pod create and delete under the microbenchmark, pooled across the three valid trials of each backend. Figure 5.2 shows the corresponding latency CDFs; Figure 5.3 shows per-percentile bars with error bars across trials.

**Table 5.1.** Apiserver request latency (ms) under kwok-isolated pod churn at λ = 30/s, pooled over three valid trials per backend. Lower is better.

| Backend | Op     |    n | p50  | p90   | p99   | p99.9 | mean | max    |
| ------- | ------ | ---: | ---: | ----: | ----: | ----: | ---: | -----: |
| etcd    | create | 2629 | 6.15 |  8.71 | 12.94 | 41.15 | 6.60 |  92.14 |
| REtcd   | create | 2588 | 5.66 |  8.07 | 10.54 | 16.82 | 5.93 |  24.78 |
| etcd    | delete | 2617 | 7.22 | 10.30 | 14.23 | 62.42 | 7.75 | 130.53 |
| REtcd   | delete | 2597 | 7.88 | 10.45 | 13.34 | 23.53 | 8.07 |  32.13 |

> [FIGURE 5.2: Latency CDF (log-x), one panel for create and one for delete, etcd vs REtcd. Source: load-test/plots/cdf_create.pdf and cdf_delete.pdf. Annotation: "REtcd body tracks etcd; REtcd tail is bounded well below etcd's."]

> [FIGURE 5.3: Percentile bar chart (p50/p90/p99/p99.9), mean ± std across the three trials, etcd vs REtcd, for create and delete. Source: load-test/plots/pXX_bar_create.pdf and pXX_bar_delete.pdf.]

Three observations follow from Table 5.1.

**The single Redis hop does not penalise the median.** On create, REtcd's median is 5.66 ms against etcd's 6.15 ms — REtcd is in fact 8% *faster* at p50. On delete the median favours etcd slightly (7.22 ms vs 7.88 ms, a 9% difference). The design chapter predicted a per-RPC kernel-IPC cost of order 50–100 µs for the Redis hop over TCP localhost; against a median request latency of several milliseconds, that cost is below the noise floor of this experiment, and the two backends are within ±10% of each other at the median for both operations. The extra network hop, in steady state, is not visible at the median.

**REtcd's tail is substantially tighter.** The divergence between the backends is in the tail, and it runs in REtcd's favour. At p99.9, REtcd's create latency is 16.8 ms against etcd's 41.1 ms (59% lower), and its delete latency is 23.5 ms against etcd's 62.4 ms (62% lower). The worst observed latency tells the same story more starkly: REtcd's maximum create/delete latencies (24.8 ms / 32.1 ms) are roughly a quarter of etcd's (92.1 ms / 130.5 ms). REtcd's tail is not merely competitive — it is bounded well below etcd's across both operations.

**A plausible mechanism for the tail difference.** This result is consistent with the two backends' persistence architectures, though the present experiment does not instrument the cause directly. Native etcd commits through a write-ahead log with periodic `fsync`, a bbolt B+-tree whose page rebalancing and freelist management occur in bursts, and a background MVCC compaction cycle; each of these can inject occasional multi-tens-of-milliseconds stalls into individual requests, producing the heavy tail seen in etcd's p99.9 and max. Redis under `appendonly everysec` defers durability to a once-per-second background flush off the request path, so individual writes see comparatively uniform service time. Attributing the tail difference to these mechanisms specifically would require per-request server-side tracing on both backends and is left to future work; the claim made here is the measured one — REtcd's tail distribution is tighter — not a claim about its cause.

Taken together, these answer EQ1: for the isolated write operations that constitute the bulk of control-plane traffic, REtcd is competitive with native etcd at the median and superior in the tail, on a fair single-instance comparison.

---

## 5.4 Cold-start latency (preliminary)

The microbenchmark isolates a single operation. A Knative scale-from-zero does the opposite: it chains a sequence of dependent control-plane writes — autoscaler decision, SKS update, Deployment scale, scheduler binding, kubelet status, EndpointSlice publication — each of which is itself one or more apiserver writes that cross apiserver→backend. Table 5.2 reports a single preliminary run per backend on the §5.2.2 macrobenchmark.

**Table 5.2.** Preliminary end-to-end invocation response time (ms) on the 3-node CloudLab cluster, azure_10 trace, 5-minute window. **This is a single smoke run per backend; see caveats below.**

| Backend     |  n | min | median |    mean |    p95 |    p99 |     max |
| ----------- | -: | --: | -----: | ------: | -----: | -----: | ------: |
| etcd        | 43 | 5.0 |   84.8 |  1358.8 |   8148 |  10042 |   10042 |
| REtcd       | 44 | 5.5 |   73.9 | 26125.8 | 107500 | 158639 |  158639 |

> [FIGURE 5.4: Response-time CDF, etcd vs REtcd, on the macrobenchmark. Source: results-retcd-3node-smoke/smoke_responseTime_cdf.png. Annotation: "bodies coincide to ~0.6; REtcd tail diverges, dominated by grpcConnEstablish during scale-from-zero."]

Read with appropriate caution, two things are visible. First, the **body** of the distribution is indistinguishable between backends: up to roughly the 60th percentile the two CDFs coincide, and REtcd's median (73.9 ms) is actually below etcd's (84.8 ms) — consistent with the steady-state finding of §5.3, since warm invocations exercise the same single-operation path. Second, the **tail** diverges sharply: REtcd's p99 is 158.6 s against etcd's 10.0 s. Inspection of the slow REtcd invocations attributes the time to `grpcConnEstablish` during scale-from-zero — the function executes normally once its pod is running; the wait is for the scaling pipeline to bring a scaled-to-zero service back up. Because that pipeline is a chain of ~7–8 sequential apiserver writes, each crossing apiserver→REtcd→Redis, a per-RPC overhead that is invisible in isolation (§5.3) is multiplied by the chain length and surfaces in the cold-start tail. This is the latency-amplification mechanism named at the start of the chapter, and it is the bridge to EQ3: the cost is not the substrate's per-operation latency in isolation but the interaction of that latency with the sequential, write-heavy structure of the cold-start path.

**Why this result is preliminary.** Four limitations bar any precise quantitative claim from Table 5.2, and they are stated plainly:

1. **Sample size.** With n ≈ 44 in-window invocations per backend, the reported p99 is effectively the single worst observation; it carries no confidence interval. A characterised tail requires a larger trace (azure_50 or larger) over a 15–30 minute window.
2. **One run per backend.** There is no across-run variance characterisation, so the difference between backends cannot be separated from run-to-run noise. At least three runs per backend are required before stating a percentage tail regression.
3. **Scale.** Three nodes is not the reference testbed; the closest comparable work (KUBEDIRECT, §4.7) evaluates at 80 nodes. Triangulating against published numbers requires matching that scale.
4. **What the tail measures.** The 158 s figure is dominated by the autoscaler control loop during scale-from-zero, not by storage service time per se; it is best read as evidence for the *amplification mechanism*, not as a measurement of REtcd's storage latency.

Accordingly, §5.4 is presented as motivating evidence for EQ3 and as a pointer to the work in §5.5 / future work, not as a headline performance number.

---

## 5.5 Threats to validity

**Internal validity.**
The microbenchmark's isolation rests on kwok faithfully removing kubelet-side latency while preserving the apiserver→storage path; if kwok's admission shortcuts alter the apiserver's write pattern relative to a real kubelet, the isolated path would not be representative. The excluded REtcd runs (one trial driven into overload at peak in-flight 313; one run with a 50% failure rate attributable to a configuration fault) indicate that the generator can enter open-loop backlog under stress; the reported run was checked to have bounded in-flight concurrency (peak ≈ 47, comparable to etcd's ≈ 48), so the two reported backends were measured under comparable load, not with REtcd advantaged by a lighter offered load.

**Construct validity.**
EQ1 measures apiserver request latency, which includes admission and validation common to both backends; it is therefore a conservative estimate of the *relative* backend difference, since the shared overhead dilutes it. The §5.3 tail result is reported as a measured distribution, not a causal claim about bbolt vs AOF; the mechanism in §5.3 is a hypothesis pending server-side tracing.

**External validity.**
Both experiments are single-instance; nothing here speaks to highly-available, Raft-quorate etcd, which pays inter-node consensus RTT that REtcd's single-instance design does not attempt to match. The microbenchmark host hardware (TODO, §5.2.1) and the 3-node macrobenchmark scale both differ from the reference FaaS testbed; generalising the cold-start result to production scale is explicitly out of reach of the present data.

**Statistical validity.**
The §5.3 result has three trials per backend and ~2,600 observations per cell, sufficient to read medians and to compare tails up to p99.9 with the per-trial error bars of Figure 5.3. The §5.4 result, with n ≈ 44 and one run per backend, supports no statistical claim and is labelled preliminary throughout.

These threats define the shape of the remaining work: enlarging the macrobenchmark to a characterised distribution (more invocations, multiple runs, larger scale), and instrumenting the server side to confirm the tail-latency mechanism hypothesised in §5.3.
