#!/usr/bin/env bash
# 4-backend concurrency sweep on current REtcd HEAD. Txn-create (kine-faithful).
# Backends: REtcd :2379  etcd :2389  kine+SQLite :2382  kine+PG :2383
set -u
cd "$(dirname "$0")/.."
HASH=$(git rev-parse --short HEAD)
OUT="load-test/results/concurrency-$(date +%Y%m%d)-${HASH}"
mkdir -p "$OUT"
PB=/tmp/propbench
go build -o "$PB" ./load-test/propbench || exit 1

{
  echo "REtcd HEAD=$HASH"
  echo "redis appendfsync=$(docker exec lua-redis redis-cli config get appendfsync | tail -1)"
  echo "kine version: $(/tmp/kine --version 2>&1 | tail -1)"
  echo "TESTBED: macOS (Darwin) — absolute numbers NOT comparable to CloudLab/Linux; relative cross-backend shape is valid"
} | tee "$OUT/_meta.txt"

ep_for() { # bash 3.2 (macOS) has no associative arrays
  case "$1" in
    retcd)       echo localhost:2379 ;;
    etcd)        echo localhost:2389 ;;
    kine-sqlite) echo localhost:2382 ;;
    kine-pg)     echo localhost:2383 ;;
  esac
}
COUNT=2000

for b in retcd etcd kine-sqlite kine-pg; do
  for W in 1 16 64; do
    name="${b}-w${W}"
    ep=$(ep_for "$b")
    echo "=== $name ($ep) ===" | tee -a "$OUT/summary.txt"
    "$PB" --endpoint "$ep" --prefix "cc-${name}-$(date +%s)/" \
      --burst --writers "$W" --count "$COUNT" --warmup 100 --drain-s 30 \
      --csv "$OUT/$name.csv" 2>&1 | grep -E 'throughput|propagated|txn RTT|propagation ms|txn error' | tee -a "$OUT/summary.txt"
    echo | tee -a "$OUT/summary.txt"
  done
done
echo "DONE -> $OUT" | tee -a "$OUT/summary.txt"
