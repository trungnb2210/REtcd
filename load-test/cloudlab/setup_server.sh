#!/usr/bin/env bash
# Run ON each server node (node1/node2/node3). Installs redis + tooling, frees the
# standard ports, and records versions. Idempotent.
set -u
sudo apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y redis-server jq sqlite3 >/dev/null

# We launch our own redis/etcd per phase with explicit configs, so stop the system
# services that would otherwise hold :6379 / default ports.
sudo systemctl disable --now redis-server 2>/dev/null || true
sudo systemctl disable --now etcd 2>/dev/null || true

chmod +x ~/bench/bin/* 2>/dev/null || true
mkdir -p ~/bench/data ~/bench/results

echo "== versions on $(hostname) =="
redis-server --version
~/bench/bin/etcd --version | head -1
~/bench/bin/retcd --help 2>&1 | head -1 || echo "retcd binary present"
echo "experiment-LAN names:"; getent hosts node0 node1 node2 node3 2>/dev/null || true
echo "setup_server done."
