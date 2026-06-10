#!/usr/bin/env bash
# Run ON node0 (client). Concurrency sweep against the single-instance backends on s1.
# Mirrors load-test/run_concurrency.sh flags exactly (Txn-create, burst, count 2000).
# Usage: sweep_concurrency.sh <s1-name>   e.g. node1
set -u
BIN=~/bench/bin
S1=${1:-node1}
OUT=~/bench/results/concurrency-$(date +%Y%m%d)
mkdir -p "$OUT"

ep_for() { case "$1" in
  retcd)       echo "$S1:2379" ;;
  etcd)        echo "$S1:2389" ;;
  kine-sqlite) echo "$S1:2382" ;;
esac; }

{
  echo "BARE-METAL xl170 CloudLab; client=node0 server=$S1"
  echo "backends: REtcd(redis everysec) :2379  etcd :2389  kine+sqlite :2382"
} | tee "$OUT/_meta.txt"

COUNT=2000
for b in retcd etcd kine-sqlite; do
  for W in 1 16 64; do
    name="${b}-w${W}"; ep=$(ep_for "$b")
    echo "=== $name ($ep) ===" | tee -a "$OUT/summary.txt"
    "$BIN/propbench" --endpoint "$ep" --prefix "cc-${name}-$(date +%s)/" \
      --burst --writers "$W" --count "$COUNT" --warmup 100 --drain-s 30 \
      --csv "$OUT/$name.csv" 2>&1 \
      | grep -E 'throughput|propagated|txn RTT|propagation ms|txn error' | tee -a "$OUT/summary.txt"
    echo | tee -a "$OUT/summary.txt"
  done
done
echo "DONE -> $OUT"
