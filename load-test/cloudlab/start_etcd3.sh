#!/usr/bin/env bash
# Run ON each etcd-cluster member (n1=node1, n2=node2, n3=node3). Brings one member
# of the 3-node Raft cluster up/down. Peers talk over the experiment LAN (node1/2/3),
# so this pays REAL inter-node consensus RTT (unlike the loopback local run).
# Client port 23791, peer port 23801 (avoids the single-node :2389/:2390 + kine ports).
# Usage: start_etcd3.sh <name> <self-lan-name> up|down   e.g.  start_etcd3.sh n1 node1 up
set -u
BIN=~/bench/bin
DATA=~/bench/data/etcd3
NAME=${1:?name n1|n2|n3}
SELF=${2:?self lan name node1|node2|node3}
MODE=${3:-up}

CLUSTER="n1=http://node1:23801,n2=http://node2:23801,n3=http://node3:23801"

if [ "$MODE" = "down" ]; then
  pkill -f "$BIN/etcd .*$NAME" 2>/dev/null || true
  echo "$NAME down on $(hostname)"; exit 0
fi

rm -rf "$DATA"; mkdir -p "$DATA"
nohup "$BIN/etcd" --name "$NAME" --data-dir "$DATA" \
  --listen-client-urls http://0.0.0.0:23791 --advertise-client-urls "http://$SELF:23791" \
  --listen-peer-urls "http://0.0.0.0:23801" --initial-advertise-peer-urls "http://$SELF:23801" \
  --initial-cluster "$CLUSTER" --initial-cluster-state new --initial-cluster-token bench3 \
  --logger zap --log-level error >"$DATA.log" 2>&1 &
sleep 2
echo "$NAME ($SELF) started; client :23791 peer :23801"
ss -ltnp 2>/dev/null | grep -E ':2379[1]|:2380[1]' || true
