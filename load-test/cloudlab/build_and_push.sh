#!/usr/bin/env bash
# LOCAL (run on your Mac, repo root). Cross-compiles linux/amd64 binaries, fetches
# etcd + kine, and scp's everything + the scripts to all 4 CloudLab nodes.
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

USER=jn1122
DOM=utah.cloudlab.us
HOSTS=(hp109 hp118 hp111 hp087)   # node0(client) node1(s1) node2(s2) node3(s3)
ETCD_VER=${ETCD_VER:-v3.5.24}
KINE_VER=${KINE_VER:-v0.13.0}

HASH=$(git rev-parse --short HEAD)
STAGE=/tmp/clbin; rm -rf "$STAGE"; mkdir -p "$STAGE"
echo "HEAD=$HASH  etcd=$ETCD_VER  kine=$KINE_VER" | tee "$STAGE/VERSIONS.txt"

echo "== cross-compiling linux/amd64 (retcd embeds write.lua/txn.lua, self-contained) =="
GOOS=linux GOARCH=amd64 go build -o "$STAGE/retcd" .
GOOS=linux GOARCH=amd64 go build -o "$STAGE/propbench" ./load-test/propbench
GOOS=linux GOARCH=amd64 go build -o "$STAGE/lossbench" ./load-test/lossbench

echo "== fetching etcd $ETCD_VER =="
curl -fsSL "https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz" \
  | tar -xz -C "$STAGE" --strip-components=1 \
      "etcd-${ETCD_VER}-linux-amd64/etcd" "etcd-${ETCD_VER}-linux-amd64/etcdctl"

echo "== fetching kine $KINE_VER (optional; comparison backend) =="
curl -fsSL "https://github.com/k3s-io/kine/releases/download/${KINE_VER}/kine-amd64" -o "$STAGE/kine" || \
  echo "WARN: kine download failed — kine columns will be skipped unless you supply a binary"

chmod +x "$STAGE"/* 2>/dev/null || true

for h in "${HOSTS[@]}"; do
  echo "== push -> $h =="
  ssh "${USER}@${h}.${DOM}" 'mkdir -p ~/bench/bin ~/bench/scripts ~/bench/results'
  scp -q "$STAGE"/* "${USER}@${h}.${DOM}:~/bench/bin/"
  scp -q load-test/cloudlab/*.sh "${USER}@${h}.${DOM}:~/bench/scripts/"
done
echo "DONE. Next: run setup_server.sh on node1/node2/node3 (see README)."
