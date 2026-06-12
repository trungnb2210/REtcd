#!/usr/bin/env python3
"""Generate thesis evaluation figures from load-test results.

Reproducible: reads load-test/results/*, writes vector PDFs into
write-up/figures/. Run: python3 load-test/make_figures.py
"""
import pathlib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = pathlib.Path(__file__).resolve().parent.parent
FIGDIR = ROOT / "write-up" / "figures"
FIGDIR.mkdir(parents=True, exist_ok=True)

# Thesis-consistent style: serif, modest sizes, vector output.
plt.rcParams.update({
    "font.family": "serif",
    "font.size": 9,
    "axes.titlesize": 9,
    "axes.labelsize": 9,
    "legend.fontsize": 8,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "figure.dpi": 150,
    "savefig.bbox": "tight",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linewidth": 0.5,
})

# Distinct, grayscale-distinguishable styles per backend.
STYLE = {
    "REtcd":          dict(color="#1f77b4", marker="o", lw=2.0, ls="-"),
    "etcd":           dict(color="#d62728", marker="s", lw=1.6, ls="--"),
    "kine + SQLite":  dict(color="#2ca02c", marker="^", lw=1.6, ls="-."),
    "kine + Postgres":dict(color="#9467bd", marker="D", lw=1.6, ls=":"),
}

writers = [1, 16, 64]

# ---- Figure 5.5: throughput vs concurrency (txn/s), log-y --------------------
# Bare-metal xl170 (CloudLab): load-test/results/cloudlab-hp109/concurrency-20260609.
throughput = {
    "REtcd":          [1230, 9968, 13019],
    "kine + SQLite":  [ 378,   50,    25],
    "etcd":           [1181, 14969, 23971],
}
# 5.0x3.2 with the legend inside (lower left is empty): keeps the saved canvas
# the same size as the other single-panel eval figures, so they all print at
# 0.72\linewidth with identical font sizes
fig, ax = plt.subplots(figsize=(5.0, 3.2))
for name, ys in throughput.items():
    ax.plot(writers, ys, label=name, markersize=5, **STYLE[name])
ax.set_xscale("log", base=2); ax.set_yscale("log")
ax.set_xticks(writers); ax.set_xticklabels(writers)
ax.set_xlabel("concurrent writers"); ax.set_ylabel("throughput (txn/s)")
ax.legend(frameon=False, loc="lower left")
ax.annotate("collapses", xy=(64, 25), xytext=(20, 60),
            fontsize=7.5, color=STYLE["kine + SQLite"]["color"])
ax.annotate("scale (etcd best)", xy=(64, 23971), xytext=(3.5, 20000),
            fontsize=7.5, color=STYLE["etcd"]["color"])
fig.savefig(FIGDIR / "eval-concurrency.pdf"); plt.close(fig)
print("wrote eval-concurrency.pdf")

# ---- Figure 5.4: propagation p99 (ms) vs concurrent writers, log-log ---------
# Values from propagation-20260607-3c8eb40/summary.txt (propagation ms p99).
burst_writers = [8, 32, 128, 256]
# Bare-metal xl170 (CloudLab): cloudlab-hp109/propagation-20260609 (prop ms p99).
prop_p99 = {
    "REtcd": [1.57, 4.02, 15.93, 27.02],
    "etcd":  [1.66, 3.27,  9.12, 16.97],
}
fig, ax = plt.subplots(figsize=(5.0, 3.2))
for name, ys in prop_p99.items():
    ax.plot(burst_writers, ys, label=name, markersize=5, **STYLE[name])
ax.set_xscale("log", base=2); ax.set_yscale("log")
ax.set_xticks(burst_writers); ax.set_xticklabels(burst_writers)
ax.set_xlabel("concurrent writers (burst)")
ax.set_ylabel("watch propagation p99 (ms)")
ax.legend(frameon=False, loc="upper left")
fig.savefig(FIGDIR / "eval-prop.pdf"); plt.close(fig)
print("wrote eval-prop.pdf")

# ---- Figure: slow-storage axis — write latency vs injected disk latency W ----
# Single-writer Txn-create p50 (ms) from the dm-delay sweep (eval table).
W = [0, 1, 5, 20]
# Bare-metal xl170 (CloudLab) dm-delay sweep: cloudlab-hp118/slowdisk-20260609.
slowstorage = {
    "REtcd":         [0.67, 0.67, 0.69, 0.65],
    "etcd":          [0.71, 16.0, 24.0, 48.1],
    "kine + SQLite": [3.02, 64.0, 96.0, 192.0],
}
fig, ax = plt.subplots(figsize=(5.0, 3.2))
for name, ys in slowstorage.items():
    ax.plot(W, ys, label=name, markersize=5, **STYLE[name])
ax.set_yscale("log")
ax.set_xlabel("injected disk write-latency $W$ (ms)")
ax.set_ylabel("Txn-create p50 latency (ms)")
ax.set_xticks(W)
ax.legend(frameon=False, loc="center right")
ax.annotate("flat (off the fsync path)", xy=(10, 0.9),
            fontsize=7.5, color=STYLE["REtcd"]["color"], ha="center")
fig.savefig(FIGDIR / "eval-slowstorage.pdf"); plt.close(fig)
print("wrote eval-slowstorage.pdf")

# ---- Figure: durability decomposition — same engine, everysec vs always ------
# Single-writer Txn-create p50 (ms) vs W (eval control table).
STYLE_ALWAYS = dict(color="#ff7f0e", marker="v", lw=1.8, ls="-")
# Bare-metal xl170 (CloudLab): cloudlab-hp118/slowdisk-20260609.
durab = {
    "REtcd everysec": ("REtcd",         [0.67, 0.67, 0.69, 0.65]),
    "REtcd always":   ("__always__",    [1.16, 32.0, 48.0, 96.0]),
    "etcd":           ("etcd",          [0.71, 16.0, 24.0, 48.1]),
    "kine + SQLite":  ("kine + SQLite", [3.02, 64.0, 96.0, 192.0]),
}
fig, ax = plt.subplots(figsize=(5.0, 3.2))
for label, (skey, ys) in durab.items():
    st = STYLE_ALWAYS if skey == "__always__" else STYLE[skey]
    ax.plot(W, ys, label=label, markersize=5, **st)
ax.set_yscale("log")
ax.set_xlabel("injected disk write-latency $W$ (ms)")
ax.set_ylabel("Txn-create p50 latency (ms)")
ax.set_xticks(W)
ax.legend(frameon=False, loc="upper left", ncol=2)
fig.savefig(FIGDIR / "eval-durability.pdf"); plt.close(fig)
print("wrote eval-durability.pdf")

# ---- Figure: EQ3 per-function cold-cost CDF, 40-node three-arm replay --------
# Derived data: results/trace40-20260608-afc9956/perfn.csv (per-function mean
# scheduling/cold cost over the azure_240 30-min replay; raw invitro CSVs are
# archived outside the repo, see summary.txt there). Mean per function, then
# CDF across functions — the KUBEDIRECT Fig 13 convention.
import csv
perfn = {}
with open(ROOT / "load-test" / "results" / "trace40-20260608-afc9956" / "perfn.csv") as fh:
    for row in csv.DictReader(r for r in fh if not r.startswith("#")):
        perfn.setdefault(row["backend"], []).append(float(row["sched_ms_mean"]))
fig, ax = plt.subplots(figsize=(5.0, 3.2))
for name in ["REtcd", "etcd", "REtcd always"]:
    st = STYLE_ALWAYS if name == "REtcd always" else STYLE[name]
    xs = sorted(perfn[name])
    ys = [(i + 1) / len(xs) for i in range(len(xs))]
    ax.plot(xs, ys, label=name, lw=st["lw"], ls=st["ls"], color=st["color"])
ax.set_xscale("log")
ax.set_xlabel("per-function mean scheduling/cold cost (ms)")
ax.set_ylabel("CDF (fraction of functions)")
ax.legend(frameon=False, loc="upper left")
fig.savefig(FIGDIR / "eval-coldstart-cdf.pdf"); plt.close(fig)
print("wrote eval-coldstart-cdf.pdf")

# ---- Figure: EQ1 latency CDFs (create / delete), etcd vs REtcd ---------------
# DISABLED: the only source for this is the May-2026 kind-* runs on a Mac (slow
# fsync). Per the "no Mac data" decision the steady-state kwok microbench must be
# re-collected on bare metal or dropped; this figure is not regenerated meanwhile.
import sys as _sys
print("SKIP eval-steady-cdf.pdf (Mac-only kwok data; pending bare-metal re-run)")
_sys.exit(0)
import csv
def pooled_latencies(run_dir, op):
    """Pool per-event latency_ms across trial CSVs for one op (status ok)."""
    out = []
    d = ROOT / "load-test" / "results" / run_dir
    for csvf in sorted(d.glob("trial-*.csv")):
        with open(csvf) as fh:
            for row in csv.DictReader(fh):
                if row["op"] == op and row["status"] == "ok":
                    out.append(float(row["latency_ms"]))
    return sorted(out)

EQ1_DIRS = {"etcd": "20260511-140702-kind-etcd-baseline-rps30",
            "REtcd": "20260511-142354-kind-kind-rps30"}  # matches Table 5.2 (n=2588)
fig, axes = plt.subplots(1, 2, figsize=(6.4, 2.9), sharey=True)
for ax, op in zip(axes, ["create", "delete"]):
    for name, d in EQ1_DIRS.items():
        xs = pooled_latencies(d, op)
        if not xs:
            continue
        ys = [(i + 1) / len(xs) for i in range(len(xs))]
        ax.plot(xs, ys, label=name, lw=1.8,
                color=STYLE[name]["color"], ls=STYLE[name]["ls"])
    ax.set_xscale("log")
    ax.set_title(f"pod {op}")
    ax.set_xlabel("apiserver request latency (ms)")
    ax.grid(True, which="both", alpha=0.3, lw=0.5)
axes[0].set_ylabel("CDF")
axes[0].legend(frameon=False, loc="upper left")
fig.savefig(FIGDIR / "eval-steady-cdf.pdf"); plt.close(fig)
print("wrote eval-steady-cdf.pdf")
