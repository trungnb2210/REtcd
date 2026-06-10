#!/usr/bin/env bash
# Run ON node0 (client). Watch-propagation sweep vs REtcd (:2379) and single-node
# etcd (:2389) on s1. Mirrors load-test/run_propagation.sh flags. Bare metal.
# Usage: sweep_prop.sh <s1-name>   e.g. node1
set -u
BIN=~/bench/bin; S1=${1:-node1}
OUT=~/bench/results/propagation-$(date +%Y%m%d); mkdir -p "$OUT"
echo "BARE-METAL xl170 CloudLab; client=node0 server=$S1" | tee "$OUT/_meta.txt"
run() { local name=$1 ep=$2; shift 2
  echo "=== $name ($ep) ===" | tee -a "$OUT/summary.txt"
  "$BIN/propbench" --endpoint "$ep" --prefix "pb-$name-$(date +%s)/" --csv "$OUT/$name.csv" "$@" 2>&1 \
    | grep -E 'throughput|propagated|txn RTT|propagation ms|missed' | tee -a "$OUT/summary.txt"
  echo | tee -a "$OUT/summary.txt"; }
run retcd-steady "$S1:2379" --rate 50 --count 3000
run etcd-steady  "$S1:2389" --rate 50 --count 3000
for W in 8 32 128 256; do
  run "retcd-burst-w$W" "$S1:2379" --burst --writers "$W" --count 20000 --drain-s 30
  run "etcd-burst-w$W"  "$S1:2389" --burst --writers "$W" --count 20000 --drain-s 30
done
echo "DONE -> $OUT"
