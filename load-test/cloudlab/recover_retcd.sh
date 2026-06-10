#!/usr/bin/env bash
# Run ON s1 after a crash. Restarts Redis on the EXISTING data dir (AOF replay) +
# REtcd, WITHOUT wiping — for the lossbench recovery count. Same detach pattern as
# start_single_backends.sh (redis --daemonize, nohup retcd), which survives ssh close.
set -u
BIN=~/bench/bin; DATA=~/bench/data
pkill -f "$BIN/retcd" 2>/dev/null || true
pkill -f "redis-server .*6379" 2>/dev/null || true
sleep 1
# NO rm -rf: replay the on-disk AOF that survived the crash.
redis-server --port 6379 --bind 127.0.0.1 --dir "$DATA/redis" \
  --appendonly yes --appendfsync everysec --save '' \
  --daemonize yes --logfile "$DATA/redis-recover.log"
for i in $(seq 1 30); do redis-cli -p 6379 ping 2>/dev/null | grep -q PONG && break; sleep 1; done
echo "redis dbsize=$(redis-cli -p 6379 dbsize 2>/dev/null)"
REDIS_ADDR=127.0.0.1:6379 LISTEN_ADDR=0.0.0.0:2379 nohup "$BIN/retcd" >"$DATA/retcd-recover.log" 2>&1 &
for i in $(seq 1 15); do ss -ltn 2>/dev/null | grep -q ":2379" && break; sleep 1; done
ss -ltn 2>/dev/null | grep -q ":2379" && echo "RECOVERED retcd up" || { echo "FAILED"; tail -5 "$DATA/retcd-recover.log"; }
