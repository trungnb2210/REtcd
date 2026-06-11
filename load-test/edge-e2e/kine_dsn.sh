#!/usr/bin/env bash
# Component-level kine+SQLite rerun under the DSN k3s ACTUALLY ships.
# kine only applies its DefaultParams (_journal_mode=WAL&_synchronous=NORMAL&...)
# when no endpoint is given (the k3s-embedded path); a bare sqlite://path gets
# SQLite stock defaults (rollback journal + synchronous=FULL). The thesis's
# component tables used the bare DSN, so this measures both side by side:
#   slow-disk sweep (single writer, W in 0/1/5/20) x {bare, k3s-params}
#   concurrency  (w 1/16/64, W=0)                  x {k3s-params}   (bare = existing data)
# Run with sudo -E. Results -> ~/bench/results/kine-dsn-*/
set -u
REAL_USER=${SUDO_USER:-$USER}
HOME_DIR=$(eval echo "~$REAL_USER")
BIN="$HOME_DIR/bench/bin"
OUT="$HOME_DIR/bench/results/kine-dsn-$(date +%Y%m%d-%H%M)"
MNT=/mnt/dstore
IMG="$HOME_DIR/dstore.img"
KINE_VERSION=${KINE_VERSION:-v0.13.0}
COUNT=${COUNT:-800}
CCOUNT=${CCOUNT:-1000}

mkdir -p "$OUT"
if [ ! -x "$BIN/kine" ]; then
  curl -fsSL -o "$BIN/kine" "https://github.com/k3s-io/kine/releases/download/${KINE_VERSION}/kine-amd64"
  chmod +x "$BIN/kine"
fi
"$BIN/kine" --version 2>&1 | head -1 | tee "$OUT/_meta.txt"

# dm-delay (same pattern as run_e2e.sh, incl. robust teardown)
mountpoint -q "$MNT" && { fuser -km "$MNT" 2>/dev/null || true; sleep 1; }
umount "$MNT" 2>/dev/null || umount -l "$MNT" 2>/dev/null || true
for i in 1 2 3; do dmsetup remove delayed 2>/dev/null && break; sleep 2; done
LOOP=$(losetup -j "$IMG" | cut -d: -f1); [ -n "$LOOP" ] && losetup -d "$LOOP" 2>/dev/null || true
truncate -s 10G "$IMG"
LOOP=$(losetup -f --show "$IMG")
SZ=$(blockdev --getsz "$LOOP")
echo "0 $SZ delay $LOOP 0 0 $LOOP 0 0" | dmsetup create delayed
mkfs.ext4 -q -F /dev/mapper/delayed
mkdir -p "$MNT"; mount /dev/mapper/delayed "$MNT"; chown "$REAL_USER" "$MNT"

set_w() { sync; dmsetup suspend delayed
  echo "0 $SZ delay $LOOP 0 0 $LOOP 0 $1" | dmsetup reload delayed; dmsetup resume delayed; }

DSN_PARAMS='_journal_mode=WAL&_busy_timeout=30000&_synchronous=NORMAL&_txlock=immediate&cache=shared'
dsn_for() { case "$1" in
  bare) echo "sqlite://$MNT/kine.db" ;;
  k3s)  echo "sqlite://$MNT/kine.db?$DSN_PARAMS" ;;
esac; }

start_kine() { # $1 = dsn-name
  pkill -f "$BIN/kine" 2>/dev/null || true; sleep 1
  rm -f "$MNT"/kine.db*
  nohup "$BIN/kine" --listen-address=tcp://127.0.0.1:2379 \
    --endpoint="$(dsn_for "$1")" >"$MNT/kine-$1.log" 2>&1 &
  sleep 2
}

run_pb() { # name writers count
  "$BIN/propbench" --endpoint 127.0.0.1:2379 --prefix "kd-$1-$(date +%s)/" \
    --burst --writers "$2" --count "$3" --warmup 100 --drain-s 5 \
    --csv "$OUT/$1.csv" 2>&1 | grep -E 'throughput|txn RTT' | tee -a "$OUT/summary.txt"
}

echo "== slow-disk sweep (single writer, count=$COUNT)" | tee -a "$OUT/summary.txt"
for W in 0 1 5 20; do
  set_w "$W"
  for d in k3s bare; do
    echo "--- kine-$d w${W}ms" | tee -a "$OUT/summary.txt"
    start_kine "$d"
    timeout 1200 sudo -u "$REAL_USER" -- true # noop; keep ownership sane
    run_pb "slow-$d-w$W" 1 "$COUNT"
  done
done

echo "== concurrency at W=0 (k3s-params DSN, count=$CCOUNT/writer)" | tee -a "$OUT/summary.txt"
set_w 0
for w in 1 16 64; do
  echo "--- kine-k3s writers=$w" | tee -a "$OUT/summary.txt"
  start_kine k3s
  run_pb "conc-k3s-w$w" "$w" "$CCOUNT"
done

pkill -f "$BIN/kine" 2>/dev/null || true
umount "$MNT" 2>/dev/null || true
dmsetup remove delayed 2>/dev/null || true
losetup -d "$LOOP" 2>/dev/null || true
chown -R "$REAL_USER" "$OUT"
echo "DONE -> $OUT"
