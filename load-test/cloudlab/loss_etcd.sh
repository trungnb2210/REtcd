#!/usr/bin/env bash
# Run ON s1. etcd loss-control: single-node etcd on :2379 so lossbench uses the same
# endpoint. `up` wipes + fresh; `recover` restarts on the existing data dir (WAL replay,
# no wipe). etcd fsyncs its WAL per commit, so it should lose ZERO acked writes.
set -u
BIN=~/bench/bin; DATA=~/bench/data; MODE=${1:-up}
pkill -f "$BIN/etcd" 2>/dev/null || true
pkill -f "$BIN/retcd" 2>/dev/null || true
pkill -f "redis-server" 2>/dev/null || true
sleep 1
[ "$MODE" = "up" ] && rm -rf "$DATA/etcdloss"
mkdir -p "$DATA/etcdloss"
nohup "$BIN/etcd" --name lossnode --data-dir "$DATA/etcdloss" \
  --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://node1:2379 \
  --listen-peer-urls http://0.0.0.0:2390 --initial-advertise-peer-urls http://node1:2390 \
  --initial-cluster lossnode=http://node1:2390 --initial-cluster-state new \
  --logger zap --log-level error >"$DATA/etcdloss.log" 2>&1 &
for i in $(seq 1 20); do ss -ltn 2>/dev/null | grep -q ":2379" && break; sleep 1; done
ss -ltn 2>/dev/null | grep -q ":2379" && echo "etcd up ($MODE)" || { echo "etcd FAILED"; tail -5 "$DATA/etcdloss.log"; }
