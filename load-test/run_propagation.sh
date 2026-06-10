#!/usr/bin/env bash
# Re-run watch-propagation sweep on current REtcd HEAD vs local etcd.
# REtcd  :2379 (insecure)   etcd :2389 (insecure)
set -u
cd "$(dirname "$0")/.."
HASH=$(git rev-parse --short HEAD)
OUT="load-test/results/propagation-$(date +%Y%m%d)-${HASH}"
mkdir -p "$OUT"
PB=/tmp/propbench
go build -o "$PB" ./load-test/propbench || exit 1

echo "REtcd HEAD=$HASH  out=$OUT" | tee "$OUT/_meta.txt"
echo "redis appendfsync=$(docker exec lua-redis redis-cli config get appendfsync | tail -1)" | tee -a "$OUT/_meta.txt"
etcd --version | head -1 | tee -a "$OUT/_meta.txt"

run() { # name endpoint args...
  local name=$1 ep=$2; shift 2
  echo "=== $name ($ep) $* ===" | tee -a "$OUT/summary.txt"
  "$PB" --endpoint "$ep" --prefix "pb-$name/" --csv "$OUT/$name.csv" "$@" 2>&1 | tee -a "$OUT/summary.txt"
  echo | tee -a "$OUT/summary.txt"
}

# --- steady 50/s, 3000 writes ---
run retcd-steady localhost:2379 --rate 50 --count 3000
run etcd-steady  localhost:2389 --rate 50 --count 3000

# --- burst concurrency sweep, 20000 writes ---
for W in 8 32 128 256; do
  run "retcd-burst-w$W" localhost:2379 --burst --writers "$W" --count 20000 --drain-s 30
  run "etcd-burst-w$W"  localhost:2389 --burst --writers "$W" --count 20000 --drain-s 30
done

echo "DONE -> $OUT" | tee -a "$OUT/summary.txt"
