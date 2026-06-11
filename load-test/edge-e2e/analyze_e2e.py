#!/usr/bin/env python3
"""Summarise an edge-e2e results dir: per cell (arm x W), per-pod scheduling and
running latencies relative to the round's scale-up command, plus round makespan
and scale-down drain. Round 0 (warm-up) is excluded.

Usage: ./analyze_e2e.py ~/bench/results/e2e-YYYYMMDD[-HHMM]/
"""
import sys, re, glob, os
from statistics import median

def pct(xs, p):
    if not xs: return float("nan")
    xs = sorted(xs); k = max(0, min(len(xs) - 1, round(p / 100 * len(xs)) - 1))
    return xs[k]

def load_rounds(path):
    rounds = {}
    for line in open(path):
        parts = line.split()
        if len(parts) < 3: continue
        ev, r, ts = parts[0], int(parts[1]), float(parts[2])
        rounds.setdefault(r, {})[ev] = ts
        if ev == "FAIL": rounds[r].setdefault("fails", []).append(" ".join(parts[3:]))
    return rounds

def load_watch(path):
    """-> list of (ts, name, phase, node)"""
    out = []
    for line in open(path):
        m = re.match(r"([\d.]+)\s+(\S+)\s+(\S+)\s+(\S+)", line)
        if m: out.append((float(m.group(1)), m.group(2), m.group(3), m.group(4)))
    return out

def analyse_cell(prefix):
    rounds = load_rounds(prefix + ".rounds")
    watch = load_watch(prefix + ".watch")
    sched, running, makespan, drain, fails = [], [], [], [], 0
    for r, ev in sorted(rounds.items()):
        if r == 0 or "UP" not in ev: continue            # skip warm-up
        if "fails" in ev: fails += len(ev["fails"])
        t_up, t_down = ev["UP"], ev.get("DOWN")
        if "ALLRUN" in ev: makespan.append(ev["ALLRUN"] - t_up)
        if "ALLGONE" in ev and t_down: drain.append(ev["ALLGONE"] - t_down)
        end = t_down or float("inf")
        first_node, first_run = {}, {}
        for ts, name, phase, node in watch:
            if not (t_up <= ts <= end): continue
            if node not in ("<none>", "") and name not in first_node:
                first_node[name] = ts - t_up
            if phase == "Running" and name not in first_run:
                first_run[name] = ts - t_up
        sched += list(first_node.values())
        running += list(first_run.values())
    return sched, running, makespan, drain, fails

def main(d):
    cells = sorted(set(p[:-7] for p in glob.glob(os.path.join(d, "*.rounds"))))
    print(f"{'cell':<16}{'n':>5} {'sched p50/p99 (s)':>20} {'run p50/p99 (s)':>20} "
          f"{'makespan p50 (s)':>17} {'drain p50 (s)':>14} {'fails':>6}")
    for c in cells:
        s, r, mk, dr, f = analyse_cell(c)
        name = os.path.basename(c)
        print(f"{name:<16}{len(r):>5} "
              f"{pct(s,50):>9.2f}/{pct(s,99):<9.2f} "
              f"{pct(r,50):>9.2f}/{pct(r,99):<9.2f} "
              f"{(median(mk) if mk else float('nan')):>17.2f} "
              f"{(median(dr) if dr else float('nan')):>14.2f} {f:>6}")
    print("\nsched = scale-cmd -> pod bound to node (watch-observed); "
          "run = scale-cmd -> phase Running; round 0 (warm-up) excluded")

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
