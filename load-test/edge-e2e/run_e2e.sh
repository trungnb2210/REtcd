#!/usr/bin/env bash
# End-to-end k3s churn matrix: {sqlite, retcd, etcd} x WLIST write-delays.
# Run with sudo -E (k3s needs root). Results -> ~/bench/results/e2e-YYYYMMDD/.
# Only the DATASTORE lives on the dm-delay device; k3s/kubelet/containerd state
# stays on the fast disk, so the delayed writes are exactly the store's.
set -u

REAL_USER=${SUDO_USER:-$USER}
HOME_DIR=$(eval echo "~$REAL_USER")
BIN="$HOME_DIR/bench/bin"
OUT="$HOME_DIR/bench/results/e2e-$(date +%Y%m%d-%H%M)"
MNT=/mnt/dstore
IMG="$HOME_DIR/dstore.img"
KDATA="$HOME_DIR/bench/k3s-data"
KUBECONFIG_PATH="$KDATA/kubeconfig.yaml"

N=${N:-30}                  # pods per round
ROUNDS=${ROUNDS:-5}         # measured rounds per cell
WLIST=${WLIST:-"0 20"}      # dm-delay write latency (ms)
ARMS=${ARMS:-"retcd etcd sqlite"}
BRINGUP_TIMEOUT=${BRINGUP_TIMEOUT:-900}
ROUND_TIMEOUT=${ROUND_TIMEOUT:-600}

mkdir -p "$OUT"
echo "edge-e2e: N=$N ROUNDS=$ROUNDS WLIST=($WLIST) arms=($ARMS) host=$(hostname) arch=$(uname -m)" | tee "$OUT/_meta.txt"
"$BIN/k3s" --version | head -1 >> "$OUT/_meta.txt"

K="$BIN/k3s kubectl --kubeconfig=$KUBECONFIG_PATH"
now_ms() { date +%s.%3N; }

# ---------- dm-delay device (same pattern as cloudlab/run_slowdisk.sh) ----------
# robust teardown of leftovers from a killed previous run: lazy-umount, kill
# holders, retry the dm remove
# fuser ONLY if the mount is live: on a bare path it would target the root fs
mountpoint -q "$MNT" && { fuser -km "$MNT" 2>/dev/null || true; sleep 1; }
umount "$MNT" 2>/dev/null || umount -l "$MNT" 2>/dev/null || true
for i in 1 2 3; do dmsetup remove delayed 2>/dev/null && break; sleep 2; done
dmsetup remove -f delayed 2>/dev/null || true
LOOP=$(losetup -j "$IMG" | cut -d: -f1); [ -n "$LOOP" ] && losetup -d "$LOOP" 2>/dev/null || true
truncate -s 10G "$IMG"
LOOP=$(losetup -f --show "$IMG")
SZ=$(blockdev --getsz "$LOOP")
echo "0 $SZ delay $LOOP 0 0 $LOOP 0 0" | dmsetup create delayed
mkfs.ext4 -q -F /dev/mapper/delayed
mkdir -p "$MNT"; mount /dev/mapper/delayed "$MNT"

set_w() {
  sync; dmsetup suspend delayed
  echo "0 $SZ delay $LOOP 0 0 $LOOP 0 $1" | dmsetup reload delayed
  dmsetup resume delayed
}

# ---------- backends ----------
stop_backends() {
  pkill -f "$BIN/retcd" 2>/dev/null || true
  pkill -f "redis-server" 2>/dev/null || true
  pkill -f "$BIN/etcd " 2>/dev/null || true
  sleep 1
}

start_retcd() {
  rm -rf "$MNT/redis"; mkdir -p "$MNT/redis"
  redis-server --port 6379 --bind 127.0.0.1 --dir "$MNT/redis" \
    --appendonly yes --appendfsync everysec --save '' --daemonize yes --logfile "$MNT/redis.log"
  REDIS_ADDR=127.0.0.1:6379 LISTEN_ADDR=0.0.0.0:2379 nohup "$BIN/retcd" >"$MNT/retcd.log" 2>&1 &
  sleep 2
}

start_etcd() {
  rm -rf "$MNT/etcd"; mkdir -p "$MNT/etcd"
  nohup "$BIN/etcd" --name solo --data-dir "$MNT/etcd" \
    --listen-client-urls http://127.0.0.1:2389 --advertise-client-urls http://127.0.0.1:2389 \
    --listen-peer-urls http://127.0.0.1:2390 --initial-advertise-peer-urls http://127.0.0.1:2390 \
    --initial-cluster solo=http://127.0.0.1:2390 --initial-cluster-state new \
    --logger zap --log-level error >"$MNT/etcd.log" 2>&1 &
  sleep 2
}

# ---------- k3s lifecycle ----------
stop_k3s() {
  pkill -f "$BIN/k3s server" 2>/dev/null || true
  sleep 2
  # kill leftover pods' shims/containers
  pkill -f "containerd-shim" 2>/dev/null || true
  pkill -f "k3s" 2>/dev/null || true
  sleep 2
  # unmount kubelet/netns leftovers under the data dir, then wipe it
  mount | awk -v d="$KDATA" '$3 ~ d {print $3}' | sort -r | xargs -r -n1 umount 2>/dev/null || true
  ip netns list 2>/dev/null | awk '{print $1}' | xargs -r -n1 ip netns delete 2>/dev/null || true
  rm -rf "$KDATA"
}

start_k3s() { # $1 = arm
  local arm=$1 ds_flag=""
  mkdir -p "$KDATA/server"
  case "$arm" in
    sqlite) rm -rf "$MNT/k3sdb"; mkdir -p "$MNT/k3sdb"
            chown "$REAL_USER" "$MNT/k3sdb"
            ln -s "$MNT/k3sdb" "$KDATA/server/db" ;;
    retcd)  ds_flag="--datastore-endpoint=http://127.0.0.1:2379" ;;
    etcd)   ds_flag="--datastore-endpoint=http://127.0.0.1:2389" ;;
  esac
  nohup "$BIN/k3s" server \
    --data-dir "$KDATA" \
    --write-kubeconfig "$KUBECONFIG_PATH" --write-kubeconfig-mode 644 \
    --node-name bench \
    --disable traefik --disable servicelb --disable metrics-server --disable local-storage \
    --disable-helm-controller --disable-cloud-controller \
    $ds_flag >"$OUT/k3s-$arm.log" 2>&1 &
}

wait_ready() { # waits node Ready + coredns Running; echoes seconds taken (or TIMEOUT)
  local t0 t1 deadline
  t0=$(now_ms); deadline=$(( $(date +%s) + BRINGUP_TIMEOUT ))
  while :; do
    if $K get node bench 2>/dev/null | grep -q ' Ready'; then
      if $K -n kube-system get pods 2>/dev/null | grep coredns | grep -q Running; then
        t1=$(now_ms); echo "$t0 $t1"; return 0
      fi
    fi
    [ "$(date +%s)" -ge "$deadline" ] && { echo "TIMEOUT"; return 1; }
    sleep 2
  done
}

# ---------- workload ----------
deploy_churn() {
  $K create namespace bench >/dev/null 2>&1 || true
  cat <<EOF | $K apply -f - >/dev/null
apiVersion: apps/v1
kind: Deployment
metadata: {name: churn, namespace: bench}
spec:
  replicas: 0
  selector: {matchLabels: {app: churn}}
  template:
    metadata: {labels: {app: churn}}
    spec:
      terminationGracePeriodSeconds: 0
      hostNetwork: true        # keep CNI sandbox-network setup (and its retry
                               # backoff on failure) out of the measured path
      containers:
      - {name: p, image: registry.k8s.io/pause:3.9, imagePullPolicy: IfNotPresent}
EOF
}

count_phase() { $K -n bench get pods --no-headers 2>/dev/null | awk -v p="$1" '$3==p' | wc -l; }
count_pods()  { $K -n bench get pods --no-headers 2>/dev/null | wc -l; }

wait_count() { # $1 = predicate fn, $2 = target, $3 = timeout-s; returns 0/1
  local deadline=$(( $(date +%s) + $3 ))
  while [ "$($1 "$2")" -ne "${4:-$2}" ] 2>/dev/null; do
    [ "$(date +%s)" -ge "$deadline" ] && return 1
    sleep 1
  done; return 0
}

run_cell() { # $1=arm $2=W
  local arm=$1 W=$2 cell="${1}-w${2}" rc=0
  echo "=== $cell ===" | tee -a "$OUT/summary.txt"
  set_w "$W"
  stop_k3s; stop_backends
  case "$arm" in retcd) start_retcd ;; etcd) start_etcd ;; esac
  start_k3s "$arm"

  local bringup; bringup=$(wait_ready) || {
    echo "$cell BRINGUP TIMEOUT (${BRINGUP_TIMEOUT}s)" | tee -a "$OUT/summary.txt"; return 1; }
  local b0=${bringup% *} b1=${bringup#* }
  echo "bringup_s=$(echo "$b1 $b0" | awk '{printf "%.1f", $1-$2}')" | tee -a "$OUT/summary.txt"

  deploy_churn

  # watcher: ms-timestamped pod events for the whole cell
  $K -n bench get pods -w --no-headers \
     -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,NODE:.spec.nodeName \
     | perl -MTime::HiRes=time -ne 'BEGIN{$|=1} printf "%.3f %s", time, $_' \
     > "$OUT/$cell.watch" 2>/dev/null &
  local WPID=$!

  : > "$OUT/$cell.rounds"
  for r in $(seq 0 "$ROUNDS"); do          # round 0 = warm-up (image pull), not analysed
    echo "UP $r $(now_ms)" >> "$OUT/$cell.rounds"
    $K -n bench scale deploy/churn --replicas="$N" >/dev/null
    if ! wait_count count_phase Running "$ROUND_TIMEOUT" "$N"; then
      echo "FAIL $r $(now_ms) up-timeout running=$(count_phase Running)" >> "$OUT/$cell.rounds"
      rc=1
    else
      echo "ALLRUN $r $(now_ms)" >> "$OUT/$cell.rounds"
    fi
    echo "DOWN $r $(now_ms)" >> "$OUT/$cell.rounds"
    $K -n bench scale deploy/churn --replicas=0 >/dev/null
    if ! wait_count count_pods 0 "$ROUND_TIMEOUT" 0; then
      echo "FAIL $r $(now_ms) down-timeout left=$(count_pods)" >> "$OUT/$cell.rounds"
      rc=1
    else
      echo "ALLGONE $r $(now_ms)" >> "$OUT/$cell.rounds"
    fi
    sleep 3
  done

  # end the watcher pipeline: killing only perl leaves `kubectl -w` alive on an
  # idle watch (nothing to write -> no SIGPIPE -> never exits) and wait blocks
  # on it for ~30min+. Kill both ends explicitly, never wait.
  kill "$WPID" 2>/dev/null || true
  pkill -f "[g]et pods -w" 2>/dev/null || true
  $K -n bench get events --sort-by=.lastTimestamp > "$OUT/$cell.events" 2>/dev/null || true
  echo "cell done rc=$rc" | tee -a "$OUT/summary.txt"
  return $rc
}

for W in $WLIST; do
  for arm in $ARMS; do
    run_cell "$arm" "$W" || echo "CELL FAILED: $arm w$W" | tee -a "$OUT/summary.txt"
  done
done

stop_k3s; stop_backends
umount "$MNT" 2>/dev/null || true
dmsetup remove delayed 2>/dev/null || true
losetup -d "$LOOP" 2>/dev/null || true
chown -R "$REAL_USER" "$OUT"
echo "DONE -> $OUT"
