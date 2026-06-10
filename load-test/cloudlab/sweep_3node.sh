#!/usr/bin/env bash
# Run ON node0 (client). Same concurrency sweep as phase 1, but against the 3-node
# Raft etcd cluster (client port 23791 on s1). This is the HA-cost baseline: identical
# client->server hop, only the server-side quorum differs from single-node etcd.
# Usage: sweep_3node.sh <member-name>   e.g. node1
set -u
BIN=~/bench/bin
EP="${1:-node1}:23791"
OUT=~/bench/results/concurrency3node-$(date +%Y%m%d)
mkdir -p "$OUT"

# health
"$BIN/etcdctl" --endpoints="node1:23791,node2:23791,node3:23791" endpoint health 2>&1 | tee "$OUT/_meta.txt"
echo "BARE-METAL xl170 3-node Raft (real LAN RTT); endpoint=$EP" | tee -a "$OUT/_meta.txt"

COUNT=2000
for W in 1 16 64; do
  name="etcd3-w${W}"
  echo "=== $name ($EP) ===" | tee -a "$OUT/summary.txt"
  "$BIN/propbench" --endpoint "$EP" --prefix "cc-${name}-$(date +%s)/" \
    --burst --writers "$W" --count "$COUNT" --warmup 100 --drain-s 30 \
    --csv "$OUT/$name.csv" 2>&1 \
    | grep -E 'throughput|propagated|txn RTT|propagation ms|txn error' | tee -a "$OUT/summary.txt"
  echo | tee -a "$OUT/summary.txt"
done
echo "DONE -> $OUT"
