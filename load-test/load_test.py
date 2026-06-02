#!/usr/bin/env python3
"""
Pod-burst microbenchmark for kube-apiserver storage backends.

Open-loop Poisson load generator. Records per-request latency to CSV and
run metadata to JSON, so results are plottable (CDF, p50/p99) and
comparable across runs.

Each "lifecycle" is one CREATE followed by a DELETE after --pod-lifetime-s.
Arrivals of new lifecycles are Poisson with mean rate --target-rps.

Example:
    # default: 5 trials, 5s warmup + 60s measured per trial at 50/s
    python load_test.py

    # sweep one rate against the current kube-context
    python load_test.py --target-rps 100 --trials 5

Output layout:
    results/<timestamp>-<context>-rps<R>/
        trial-00.csv          per-request data for trial 0
        trial-01.csv          ...
        run.json              args + per-trial summary + validity
"""
import argparse
import asyncio
import csv
import json
import random
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.rest import ApiException


# ---------- pod manifest ----------

def make_pod_manifest(name: str) -> client.V1Pod:
    return client.V1Pod(
        metadata=client.V1ObjectMeta(name=name, labels={"run": "load-test"}),
        spec=client.V1PodSpec(
            node_selector={"type": "kwok"},
            tolerations=[client.V1Toleration(
                key="kwok.x-k8s.io/node",
                operator="Exists",
                effect="NoSchedule",
            )],
            containers=[client.V1Container(
                name="busybox",
                image="busybox",
                command=["sleep", "3600"],
            )],
        ),
    )


# ---------- namespace + cleanup ----------

async def ensure_namespace(v1, namespace: str) -> None:
    try:
        await v1.read_namespace(name=namespace)
    except ApiException as e:
        if e.status == 404:
            await v1.create_namespace(body=client.V1Namespace(
                metadata=client.V1ObjectMeta(name=namespace),
            ))
        else:
            raise
    # The ServiceAccount admission plugin rejects pod creates with 403 until
    # the controller-manager has reconciled the "default" SA into a new
    # namespace. Wait for it.
    for _ in range(30):
        try:
            await v1.read_namespaced_service_account(name="default", namespace=namespace)
            return
        except ApiException as e:
            if e.status != 404:
                raise
        await asyncio.sleep(0.5)
    raise RuntimeError(f"timed out waiting for default ServiceAccount in {namespace}")


async def cleanup_stale(v1, namespace: str) -> int:
    """Delete every pod with label run=load-test in the namespace."""
    try:
        pods = await v1.list_namespaced_pod(
            namespace=namespace, label_selector="run=load-test",
        )
    except ApiException:
        return 0
    n = 0
    for p in pods.items:
        try:
            await v1.delete_namespaced_pod(
                name=p.metadata.name,
                namespace=namespace,
                propagation_policy="Background",
            )
            n += 1
        except ApiException:
            pass
    return n


# ---------- one trial ----------

async def run_trial(args, trial_idx: int, run_id: str, out_dir: Path) -> dict:
    await config.load_kube_config()
    async with client.ApiClient() as api_client:
        v1 = client.CoreV1Api(api_client)
        await ensure_namespace(v1, args.namespace)
        removed = await cleanup_stale(v1, args.namespace)
        if removed:
            print(f"[trial {trial_idx}] cleaned {removed} stale pods", file=sys.stderr)

        # Pre-warm the gRPC channel: TLS + auth happens here, not on the first
        # measured request.
        try:
            await v1.list_namespaced_pod(namespace=args.namespace, limit=1)
        except ApiException:
            pass

        csv_path = out_dir / f"trial-{trial_idx:02d}.csv"
        csv_file = open(csv_path, "w", newline="")
        writer = csv.DictWriter(csv_file, fieldnames=[
            "t_send_ms", "latency_ms", "op", "status", "pod_id",
        ])
        writer.writeheader()

        t0 = time.perf_counter()
        warmup_end_s = args.warmup_s
        total_end_s = args.warmup_s + args.duration_s

        in_flight = 0
        max_in_flight = 0
        pod_id = 0
        # all attempts (incl. warmup)
        counts = {
            "create_ok": 0, "create_conflict": 0, "create_err": 0,
            "delete_ok": 0, "delete_missing": 0, "delete_err": 0,
        }
        # recorded (measure window only), used for achieved-rate reporting
        recorded = {"create": 0, "delete": 0}

        def in_window(t_rel_s: float) -> bool:
            return warmup_end_s <= t_rel_s < total_end_s

        def record(t_rel_s: float, latency_ms: float, op: str, status: str, pid: int) -> None:
            if in_window(t_rel_s):
                writer.writerow({
                    "t_send_ms": round(t_rel_s * 1000, 3),
                    "latency_ms": round(latency_ms, 3),
                    "op": op,
                    "status": status,
                    "pod_id": pid,
                })
                recorded[op] += 1

        async def lifecycle(pid: int) -> None:
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            try:
                name = f"loadtest-{run_id}-{pid}"

                # ---- CREATE ----
                t_send_rel = time.perf_counter() - t0
                ts0 = time.perf_counter()
                try:
                    await v1.create_namespaced_pod(
                        body=make_pod_manifest(name),
                        namespace=args.namespace,
                    )
                    status = "ok"
                    counts["create_ok"] += 1
                except ApiException as e:
                    if e.status == 409:
                        status = "conflict"
                        counts["create_conflict"] += 1
                    else:
                        status = f"err{e.status}"
                        counts["create_err"] += 1
                except Exception as e:
                    status = f"exc:{type(e).__name__}"
                    counts["create_err"] += 1
                lat_ms = (time.perf_counter() - ts0) * 1000
                record(t_send_rel, lat_ms, "create", status, pid)

                # Hold the pod for the configured lifetime, then delete.
                await asyncio.sleep(args.pod_lifetime_s)

                # ---- DELETE ----
                t_send_rel = time.perf_counter() - t0
                ts0 = time.perf_counter()
                try:
                    await v1.delete_namespaced_pod(
                        name=name,
                        namespace=args.namespace,
                        propagation_policy="Background",
                    )
                    status = "ok"
                    counts["delete_ok"] += 1
                except ApiException as e:
                    if e.status == 404:
                        status = "missing"
                        counts["delete_missing"] += 1
                    else:
                        status = f"err{e.status}"
                        counts["delete_err"] += 1
                except Exception as e:
                    status = f"exc:{type(e).__name__}"
                    counts["delete_err"] += 1
                lat_ms = (time.perf_counter() - ts0) * 1000
                record(t_send_rel, lat_ms, "delete", status, pid)
            finally:
                in_flight -= 1

        # ---- Open-loop Poisson producer ----
        # random.expovariate(rate) returns inter-arrival times with mean 1/rate.
        tasks = []
        last_log = t0
        print(f"[trial {trial_idx}] start: warmup={args.warmup_s}s "
              f"measure={args.duration_s}s rate={args.target_rps}/s",
              file=sys.stderr)
        while True:
            elapsed = time.perf_counter() - t0
            if elapsed >= total_end_s:
                break
            tasks.append(asyncio.create_task(lifecycle(pod_id)))
            pod_id += 1
            await asyncio.sleep(random.expovariate(args.target_rps))

            now = time.perf_counter()
            if now - last_log >= 5.0:
                phase = "warmup" if elapsed < warmup_end_s else "measure"
                print(f"[trial {trial_idx}] t={elapsed:6.1f}s phase={phase:7s} "
                      f"sent={pod_id} in_flight={in_flight} peak={max_in_flight}",
                      file=sys.stderr)
                last_log = now

        # ---- Drain remaining lifecycles so deletes get recorded ----
        print(f"[trial {trial_idx}] draining {in_flight} in-flight "
              f"(max wait {args.drain_s}s)", file=sys.stderr)
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=args.drain_s,
            )
        except asyncio.TimeoutError:
            print(f"[trial {trial_idx}] WARN: drain timed out, "
                  f"{in_flight} still in flight", file=sys.stderr)

        csv_file.close()

        # Final cleanup so trial N+1 starts with an empty namespace.
        await cleanup_stale(v1, args.namespace)

        # Validity check: non-conflict, non-missing errors must be under threshold.
        total_attempts = (counts["create_ok"] + counts["create_conflict"] + counts["create_err"]
                         + counts["delete_ok"] + counts["delete_missing"] + counts["delete_err"])
        total_failures = counts["create_err"] + counts["delete_err"]
        failure_rate = total_failures / max(total_attempts, 1)
        achieved_create_rate = recorded["create"] / max(args.duration_s, 1e-9)
        achieved_delete_rate = recorded["delete"] / max(args.duration_s, 1e-9)
        valid = failure_rate < args.max_failure_rate

        summary = {
            "trial_idx": trial_idx,
            "csv": csv_path.name,
            "pod_ids_attempted": pod_id,
            "counts_all": counts,
            "recorded_in_window": recorded,
            "failure_rate": failure_rate,
            "achieved_create_rate_per_s": achieved_create_rate,
            "achieved_delete_rate_per_s": achieved_delete_rate,
            "max_in_flight": max_in_flight,
            "valid": valid,
        }
        verdict = "valid" if valid else "INVALID"
        print(f"[trial {trial_idx}] done: {verdict} "
              f"failure_rate={failure_rate:.3%} "
              f"achieved_create={achieved_create_rate:.1f}/s "
              f"achieved_delete={achieved_delete_rate:.1f}/s "
              f"peak_in_flight={max_in_flight}",
              file=sys.stderr)
        return summary


# ---------- run wrapper ----------

def get_kube_context() -> Optional[str]:
    try:
        r = subprocess.run(
            ["kubectl", "config", "current-context"],
            capture_output=True, text=True, check=False,
        )
        return r.stdout.strip() if r.returncode == 0 else None
    except FileNotFoundError:
        return None


async def main_async(args) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    run_id = uuid.uuid4().hex[:8]
    started_at = datetime.now(timezone.utc).isoformat()

    print(f"[run {run_id}] out_dir={out_dir}", file=sys.stderr)

    trial_summaries = []
    for trial_idx in range(args.trials):
        summary = await run_trial(args, trial_idx, run_id, out_dir)
        trial_summaries.append(summary)

    metadata = {
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "kube_context": get_kube_context(),
        "args": vars(args),
        "trials": trial_summaries,
    }
    (out_dir / "run.json").write_text(json.dumps(metadata, indent=2))

    n_valid = sum(1 for t in trial_summaries if t["valid"])
    print(f"[run {run_id}] {n_valid}/{len(trial_summaries)} trials valid", file=sys.stderr)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Open-loop Poisson pod-burst benchmark for kube-apiserver.",
    )
    ap.add_argument("--target-rps", type=float, default=50.0,
                    help="Mean Poisson arrival rate of new pod lifecycles per second.")
    ap.add_argument("--duration-s", type=float, default=60.0,
                    help="Length of the measured window in seconds.")
    ap.add_argument("--warmup-s", type=float, default=5.0,
                    help="Seconds of unmeasured load before measurement starts.")
    ap.add_argument("--trials", type=int, default=5,
                    help="Number of independent trial runs.")
    ap.add_argument("--pod-lifetime-s", type=float, default=1.0,
                    help="Seconds each pod stays before being deleted.")
    ap.add_argument("--namespace", default="pods-namespace",
                    help="Kubernetes namespace for pods.")
    ap.add_argument("--out-dir", default=None,
                    help="Output directory (default: results/<timestamp>-<context>-rps<R>).")
    ap.add_argument("--max-failure-rate", type=float, default=0.01,
                    help="Trial marked invalid if non-conflict failure rate exceeds this.")
    ap.add_argument("--drain-s", type=float, default=60.0,
                    help="Max seconds to wait for in-flight lifecycles after the measure window.")
    args = ap.parse_args()

    if args.target_rps <= 0:
        ap.error("--target-rps must be > 0")
    if args.duration_s <= 0:
        ap.error("--duration-s must be > 0")
    if args.warmup_s < 0:
        ap.error("--warmup-s must be >= 0")
    if args.trials <= 0:
        ap.error("--trials must be > 0")

    if args.out_dir is None:
        ctx = (get_kube_context() or "unknown").replace("/", "_")
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        args.out_dir = f"results/{ts}-{ctx}-rps{int(args.target_rps)}"
    return args


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\nStopped by user.", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
