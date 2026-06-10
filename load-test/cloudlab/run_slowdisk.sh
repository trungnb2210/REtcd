#!/usr/bin/env bash
# Run ON s1 (node1). Phase 2/3 of the edge story: slow-storage sweep + durability
# control. A dm-delay device injects a fixed WRITE latency W; each backend's data dir
# lives on it; single-writer propbench measures p50/throughput. Needs sudo.
#   columns: REtcd everysec | REtcd always | etcd | kine+sqlite   (matches Table 6.2/6.3)
set -u
BIN=~/bench/bin
OUT=~/bench/results/slowdisk-$(date +%Y%m%d); mkdir -p "$OUT"
MNT=/mnt/slowdisk
IMG=~/delay.img
COUNT=${COUNT:-1000}
WLIST=(0 1 5 20)

echo "BARE-METAL xl170 dm-delay write-latency sweep; single writer; count=$COUNT" | tee "$OUT/_meta.txt"

# --- one-time dm-delay device over a loop-backed file on the SSD ---
sudo umount "$MNT" 2>/dev/null || true
sudo dmsetup remove delayed 2>/dev/null || true
LOOP=$(losetup -j "$IMG" | cut -d: -f1)
[ -n "$LOOP" ] && sudo losetup -d "$LOOP" 2>/dev/null || true
truncate -s 20G "$IMG"
LOOP=$(sudo losetup -f --show "$IMG")
SZ=$(sudo blockdev --getsz "$LOOP")
echo "0 $SZ delay $LOOP 0 0 $LOOP 0 0" | sudo dmsetup create delayed
sudo mkfs.ext4 -q -F /dev/mapper/delayed
sudo mkdir -p "$MNT"; sudo mount /dev/mapper/delayed "$MNT"
sudo chown "$USER" "$MNT"

set_w() { # reload the dm table with write-delay = $1 ms (read delay 0)
  sync; sudo dmsetup suspend delayed
  echo "0 $SZ delay $LOOP 0 0 $LOOP 0 $1" | sudo dmsetup reload delayed
  sudo dmsetup resume delayed
}

stop_all() {
  pkill -f "$BIN/retcd" 2>/dev/null||true; pkill -f "redis-server" 2>/dev/null||true
  pkill -f "$BIN/etcd " 2>/dev/null||true; pkill -f "$BIN/kine" 2>/dev/null||true; sleep 1
}

run_one() { # backend W
  local b=$1 W=$2 name="${1}-w${2}"
  echo "=== $name ===" | tee -a "$OUT/summary.txt"
  "$BIN/propbench" --endpoint 127.0.0.1:2379 --prefix "sd-${name}-$(date +%s)/" \
    --burst --writers 1 --count "$COUNT" --warmup 100 --drain-s 5 \
    --csv "$OUT/$name.csv" 2>&1 \
    | grep -E 'throughput|txn RTT' | tee -a "$OUT/summary.txt"
  echo | tee -a "$OUT/summary.txt"
}

start_retcd() { # fsync-mode
  rm -rf "$MNT/redis"; mkdir -p "$MNT/redis"
  redis-server --port 6379 --bind 127.0.0.1 --dir "$MNT/redis" \
    --appendonly yes --appendfsync "$1" --save '' --daemonize yes --logfile "$MNT/redis.log"
  REDIS_ADDR=127.0.0.1:6379 LISTEN_ADDR=0.0.0.0:2379 nohup "$BIN/retcd" >"$MNT/retcd.log" 2>&1 &
  sleep 2
}
start_etcd() {
  rm -rf "$MNT/etcd"; mkdir -p "$MNT/etcd"
  nohup "$BIN/etcd" --name solo --data-dir "$MNT/etcd" \
    --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379 \
    --listen-peer-urls http://127.0.0.1:2390 --initial-advertise-peer-urls http://127.0.0.1:2390 \
    --initial-cluster solo=http://127.0.0.1:2390 --initial-cluster-state new \
    --logger zap --log-level error >"$MNT/etcd.log" 2>&1 &
  sleep 2
}
start_kine() {
  rm -f "$MNT/kine.db"*
  nohup "$BIN/kine" --listen-address=tcp://127.0.0.1:2379 --endpoint="sqlite://$MNT/kine.db" >"$MNT/kine.log" 2>&1 &
  sleep 2
}

for W in "${WLIST[@]}"; do
  set_w "$W"
  stop_all; start_retcd everysec; run_one retcd-everysec "$W"
  stop_all; start_retcd always;   run_one retcd-always   "$W"
  stop_all; start_etcd;           run_one etcd           "$W"
  stop_all; start_kine;           run_one kine-sqlite    "$W"
done
stop_all
sudo umount "$MNT" 2>/dev/null || true
sudo dmsetup remove delayed 2>/dev/null || true
sudo losetup -d "$LOOP" 2>/dev/null || true
echo "DONE -> $OUT"
