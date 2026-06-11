# edge-e2e: end-to-end k3s validation of the edge claims

Closes the "no end-to-end edge deployment" threat: runs **k3s** (the actual edge
distribution) against three datastores and measures end-to-end pod churn, with
only the datastore directory on a dm-delay device.

## Arms

| arm | datastore | how |
|---|---|---|
| `sqlite` | k3s default (embedded kine + SQLite) | `server/db` symlinked onto the delayed mount |
| `retcd` | REtcd (redis `appendfsync everysec`) | `k3s server --datastore-endpoint=http://127.0.0.1:2379` |
| `etcd` | stock single-node etcd | `k3s server --datastore-endpoint=http://127.0.0.1:2389` |

k3s treats an `http(s)://` datastore endpoint as etcd and talks etcd v3 gRPC
directly (no kine in the path) — so the `retcd` arm is k3s's full control plane
on REtcd, end to end.

## Workload and metrics

Per arm × per write-delay W ∈ {0, 20} ms:

1. fresh k3s data dir; backend data dir on the delayed mount; start backend + k3s
2. wait node Ready + coredns Running (records **bring-up time** — itself a result)
3. one warm-up round (absorbs the pause-image pull; never measured)
4. R measured rounds of `scale deploy/churn 0 -> N -> 0` (default N=30, R=5)

A `kubectl get pods -w` watcher timestamps every pod event with millisecond
client wall-clock. Per pod per round:
- **sched** = first event with a nodeName (binding visible to a watch client)
- **running** = first event with phase Running
both relative to the scale command. Also: round makespan (all N Running),
scale-down drain time, and pods that never arrive (timeout).

This is the KUBEDIRECT-style per-instance view, but end to end through k3s.

## Usage (on the Linux box: multipass VM or CloudLab node)

```sh
./setup_e2e.sh                      # downloads k3s + etcd, checks redis, expects retcd in ~/bench/bin
sudo ./run_e2e.sh                   # full matrix -> ~/bench/results/e2e-YYYYMMDD/
N=30 ROUNDS=5 WLIST="0 20" sudo -E ./run_e2e.sh   # tweak
./analyze_e2e.py ~/bench/results/e2e-*/            # summary table
```

`retcd` binary: cross-compile on the Mac at the pinned commit and scp to
`~/bench/bin/retcd` (`GOOS=linux GOARCH=arm64|amd64 CGO_ENABLED=0 go build`).

## Notes / gotchas

- k3s needs root; the script sudo-runs `k3s server` directly (no systemd).
- Disabled k3s extras: traefik, servicelb, metrics-server, local-storage,
  helm-controller, cloud-controller — keeps bring-up writes comparable.
- Between arms: `k3s-killall` equivalent (pkill) + wipe of the k3s data dir, so
  every arm bootstraps a fresh cluster. The warm-up round re-pulls the pause
  image after each wipe; measured rounds always run warm.
- REtcd has no compaction; a single arm writes few enough revisions that the
  event stream stays small.
- At W=20 the sqlite arm's *bring-up* may take minutes (every bootstrap write
  pays the fsync) — the script waits up to BRINGUP_TIMEOUT (default 900 s) and
  records the duration rather than failing.
