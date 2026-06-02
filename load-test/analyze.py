#!/usr/bin/env python3
"""
analyze.py — produce comparison plots and a summary table from load_test.py runs.

Usage:
    python analyze.py results/RUN_A results/RUN_B [...] [--out plots] [--label A=etcd B=REtcd]

Produces (in --out):
    cdf_create.png/pdf      latency CDF for create ops, log-x
    cdf_delete.png/pdf      latency CDF for delete ops, log-x
    ts_create.png/pdf       per-request latency over time (scatter)
    pXX_bar.png/pdf         p50/p90/p99/p99.9 bar chart with error bars across trials
    summary.csv             per-(system, op) percentile table
"""
import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def load_run(run_dir: Path, fallback_label: str) -> tuple[str, pd.DataFrame, dict]:
    meta = json.load(open(run_dir / "run.json"))
    frames = []
    for trial in meta["trials"]:
        if not trial["valid"]:
            continue
        df = pd.read_csv(run_dir / trial["csv"])
        df["trial"] = trial["trial_idx"]
        frames.append(df)
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    label = fallback_label or meta.get("kube_context") or run_dir.name
    return label, df, meta


def cdf_xy(latencies: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if len(latencies) == 0:
        return np.array([]), np.array([])
    s = np.sort(latencies)
    y = np.arange(1, len(s) + 1) / len(s)
    return s, y


def plot_cdf(runs, op: str, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(6.5, 4))
    for label, df, _ in runs:
        sub = df[(df["op"] == op) & (df["status"] == "ok")]["latency_ms"].values
        x, y = cdf_xy(sub)
        if len(x) == 0:
            continue
        ax.plot(x, y, label=f"{label} (n={len(sub)})", linewidth=1.8)
        # mark p50, p99 with vertical ticks
        for pct in (50, 99):
            v = float(np.percentile(sub, pct))
            ax.axvline(v, ymin=0, ymax=0.04, color=ax.lines[-1].get_color(), alpha=0.6)
    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("CDF")
    ax.set_title(f"Pod {op}: apiserver request latency")
    ax.set_xscale("log")
    ax.set_ylim(0, 1.0)
    ax.grid(True, alpha=0.3, which="both")
    ax.legend(loc="lower right", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    fig.savefig(out_path.with_suffix(".pdf"))
    plt.close(fig)


def plot_timeseries(runs, op: str, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 4))
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    for i, (label, df, _) in enumerate(runs):
        sub = df[df["op"] == op]
        if len(sub) == 0:
            continue
        ax.scatter(
            sub["t_send_ms"] / 1000.0,
            sub["latency_ms"],
            s=4,
            alpha=0.4,
            color=colors[i % len(colors)],
            label=label,
        )
    ax.set_xlabel("Time since trial start (s)")
    ax.set_ylabel("Latency (ms)")
    ax.set_title(f"Pod {op}: per-request latency over time (all trials)")
    ax.set_yscale("log")
    ax.grid(True, alpha=0.3, which="both")
    ax.legend(loc="upper right", markerscale=3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    fig.savefig(out_path.with_suffix(".pdf"))
    plt.close(fig)


def plot_percentile_bars(runs, op: str, out_path: Path) -> None:
    """One bar group per percentile, one bar per system, error bars across trials."""
    percentiles = [50, 90, 99, 99.9]
    n_sys = len(runs)
    width = 0.8 / n_sys
    fig, ax = plt.subplots(figsize=(7, 4))

    for i, (label, df, _) in enumerate(runs):
        means, errs = [], []
        for pct in percentiles:
            per_trial = []
            for tid in sorted(df["trial"].unique()):
                sub = df[(df["trial"] == tid) & (df["op"] == op) & (df["status"] == "ok")]["latency_ms"].values
                if len(sub) > 0:
                    per_trial.append(np.percentile(sub, pct))
            if per_trial:
                means.append(np.mean(per_trial))
                errs.append(np.std(per_trial))
            else:
                means.append(0.0)
                errs.append(0.0)
        x = np.arange(len(percentiles)) + (i - (n_sys - 1) / 2) * width
        ax.bar(x, means, width=width * 0.95, yerr=errs, capsize=3, label=label)

    ax.set_xticks(np.arange(len(percentiles)))
    ax.set_xticklabels([f"p{p}" for p in percentiles])
    ax.set_ylabel("Latency (ms)")
    ax.set_title(f"Pod {op}: latency percentiles (mean ± std across trials)")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    fig.savefig(out_path.with_suffix(".pdf"))
    plt.close(fig)


def summary_table(runs) -> pd.DataFrame:
    rows = []
    for label, df, _ in runs:
        for op in ("create", "delete"):
            sub = df[(df["op"] == op) & (df["status"] == "ok")]["latency_ms"].values
            if len(sub) == 0:
                continue
            rows.append({
                "system": label,
                "op": op,
                "n": len(sub),
                "p50": np.percentile(sub, 50),
                "p90": np.percentile(sub, 90),
                "p99": np.percentile(sub, 99),
                "p99.9": np.percentile(sub, 99.9),
                "mean": float(np.mean(sub)),
                "max": float(np.max(sub)),
            })
    return pd.DataFrame(rows)


def parse_labels(label_args: list[str]) -> dict[str, str]:
    """--label name=value name=value ... where name is the basename of a run dir."""
    out = {}
    for item in label_args or []:
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("runs", nargs="+", type=Path, help="run directories from load_test.py")
    ap.add_argument("--out", type=Path, default=Path("plots"), help="output directory")
    ap.add_argument("--label", action="append", default=[], metavar="DIRNAME=LABEL",
                    help="rename a run for the legend; repeatable")
    args = ap.parse_args()

    overrides = parse_labels(args.label)
    runs = []
    for d in args.runs:
        runs.append(load_run(d, overrides.get(d.name, "")))
    runs = [(l, df, m) for l, df, m in runs if not df.empty]
    if not runs:
        print("no valid trials in any run", file=sys.stderr)
        sys.exit(1)

    args.out.mkdir(parents=True, exist_ok=True)

    plot_cdf(runs, "create", args.out / "cdf_create.png")
    plot_cdf(runs, "delete", args.out / "cdf_delete.png")
    plot_timeseries(runs, "create", args.out / "ts_create.png")
    plot_percentile_bars(runs, "create", args.out / "pXX_bar_create.png")
    plot_percentile_bars(runs, "delete", args.out / "pXX_bar_delete.png")

    summary = summary_table(runs)
    summary.to_csv(args.out / "summary.csv", index=False)

    print("=== summary ===")
    with pd.option_context("display.max_columns", None, "display.width", 140):
        print(summary.to_string(index=False, float_format=lambda x: f"{x:8.2f}"))
    print(f"\nFigures + summary.csv saved to {args.out}/")


if __name__ == "__main__":
    main()
