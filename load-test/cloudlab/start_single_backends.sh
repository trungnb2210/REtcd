#!/usr/bin/env bash
# Run ON s1 (node1). Brings the single-instance backends up/down for the concurrency
# (phase 1) and loss (phase 3) sweeps. Endpoints are bound on 0.0.0.0 so node0 reaches
# them over the experiment LAN.
#   REtcd :2379 (redis everysec)   etcd :2389   kine+sqlite :2382
# Usage: start_single_backends.sh up | retcd-only | down
set -u
BIN=~/bench/bin
DATA=~/bench/data
MODE=${1:-up}

stop() {
  pkill -f "$BIN/retcd" 2>/dev/null || true
  pkill -f "redis-server .*:6379" 2>/dev/null || true
  pkill -f "$BIN/etcd " 2>/dev/null || true
  pkill -f "$BIN/kine" 2>/dev/null || true
  sleep 1
}

start_redis_retcd() {
  rm -rf "$DATA/redis"; mkdir -p "$DATA/redis"
  redis-server --port 6379 --bind 127.0.0.1 --dir "$DATA/redis" \
    --appendonly yes --appendfsync everysec --save '' \
    --daemonize yes --logfile "$DATA/redis.log"
  REDIS_ADDR=127.0.0.1:6379 LISTEN_ADDR=0.0.0.0:2379 \
    nohup "$BIN/retcd" >"$DATA/retcd.log" 2>&1 &
}

start_etcd() {
  rm -rf "$DATA/etcd1"; mkdir -p "$DATA/etcd1"
  nohup "$BIN/etcd" --name solo --data-dir "$DATA/etcd1" \
    --listen-client-urls http://0.0.0.0:2389 --advertise-client-urls http://node1:2389 \
    --listen-peer-urls http://0.0.0.0:2390 --initial-advertise-peer-urls http://node1:2390 \
    --initial-cluster solo=http://node1:2390 --initial-cluster-state new \
    --logger zap --log-level error >"$DATA/etcd1.log" 2>&1 &
}

start_kine() {
  # TODO: confirm this matches your known-good Mac kine invocation before trusting
  # the kine columns. kine speaks the etcd v3 gRPC subset the apiserver uses.
  rm -f "$DATA/kine.db"*
  nohup "$BIN/kine" --listen-address=tcp://0.0.0.0:2382 \
    --endpoint="sqlite://$DATA/kine.db" >"$DATA/kine.log" 2>&1 &
}

case "$MODE" in
  up)         stop; start_redis_retcd; start_etcd; start_kine ;;
  retcd-only) stop; start_redis_retcd ;;
  down)       stop; echo "backends down"; exit 0 ;;
  *) echo "usage: $0 up|retcd-only|down"; exit 1 ;;
esac

sleep 2
echo "listening:"; ss -ltnp 2>/dev/null | grep -E ':2379|:2389|:2382' || true
echo "started ($MODE) on $(hostname)"
