# CloudLab bare-metal edge re-run + 3-node Raft baseline

Converts the edge chapter from VM/Mac → bare metal, and adds the 3-node Raft etcd
baseline. One session, five phases. Pinned to REtcd `afc9956` (v12).

## Node map (this allocation)

| Role   | node  | host                    |
|--------|-------|-------------------------|
| client | node0 | hp109.utah.cloudlab.us  |
| s1     | node1 | hp118.utah.cloudlab.us  |
| s2     | node2 | hp111.utah.cloudlab.us  |
| s3     | node3 | hp087.utah.cloudlab.us  |

User `jn1122`. Hardware **xl170** (SATA SSD — note in meta; absolute numbers now
bare-metal-citable, unlike the Mac runs). Within the cluster nodes resolve each
other as `node0..node3` over the experiment LAN (used for etcd peers + client→server).

## What runs where

- **Phase 1 — concurrency** (§6.3): backends on **s1**, propbench sweep from **node0**.
- **Phase 2 — slow-storage dm-delay** (§6.4/6.5): all on **s1** (dm-delay loop device).
- **Phase 3 — durability/loss** (§6.6): lossbench from **node0**, hard-reboot **s1**.
- **Phase 4 — 3-node Raft** (baseline): cluster on **s1/s2/s3**, sweep from **node0**.
- **Phase 5 — sustained-update** (optional): from **node0** against **s1**.

## Sequence

```bash
# 0. LOCAL (Mac, repo root): cross-compile + push binaries & scripts to all 4 nodes
bash load-test/cloudlab/build_and_push.sh

# 1. On EACH server node (node1, node2, node3): install deps, place binaries
ssh jn1122@hp118.utah.cloudlab.us 'bash ~/bench/scripts/setup_server.sh'   # node1
ssh jn1122@hp111.utah.cloudlab.us 'bash ~/bench/scripts/setup_server.sh'   # node2
ssh jn1122@hp087.utah.cloudlab.us 'bash ~/bench/scripts/setup_server.sh'   # node3
# client only needs binaries (pushed in step 0); optionally:
ssh jn1122@hp109.utah.cloudlab.us 'chmod +x ~/bench/bin/*'

# 2. PHASE 1 — concurrency
ssh jn1122@hp118.utah.cloudlab.us 'bash ~/bench/scripts/start_single_backends.sh up'   # s1: redis+retcd, etcd, kine
ssh jn1122@hp109.utah.cloudlab.us 'bash ~/bench/scripts/sweep_concurrency.sh node1'    # node0 drives
ssh jn1122@hp118.utah.cloudlab.us 'bash ~/bench/scripts/start_single_backends.sh down'

# 3. PHASE 4 — 3-node Raft (start a member on each, then sweep)
ssh jn1122@hp118.utah.cloudlab.us 'bash ~/bench/scripts/start_etcd3.sh n1 node1 up'
ssh jn1122@hp111.utah.cloudlab.us 'bash ~/bench/scripts/start_etcd3.sh n2 node2 up'
ssh jn1122@hp087.utah.cloudlab.us 'bash ~/bench/scripts/start_etcd3.sh n3 node3 up'
ssh jn1122@hp109.utah.cloudlab.us 'bash ~/bench/scripts/sweep_3node.sh node1'          # node0 drives leader endpoint
ssh jn1122@hp118.utah.cloudlab.us 'bash ~/bench/scripts/start_etcd3.sh n1 node1 down'  # (repeat down on n2,n3)

# 4. PHASE 2 — slow-storage (self-contained on s1; needs sudo for dm-delay)
ssh jn1122@hp118.utah.cloudlab.us 'bash ~/bench/scripts/run_slowdisk.sh'

# 5. PHASE 3 — durability/loss
ssh jn1122@hp118.utah.cloudlab.us 'bash ~/bench/scripts/start_single_backends.sh retcd-only'
ssh jn1122@hp109.utah.cloudlab.us 'timeout 6 ~/bench/bin/lossbench --endpoint node1:2379 --mode=write | tee ~/bench/results/loss-retcd-acked.txt'
#   --> HARD power-cycle s1 from the CloudLab portal (Action -> reboot), wait for ready
ssh jn1122@hp118.utah.cloudlab.us 'bash ~/bench/scripts/start_single_backends.sh retcd-only'   # redis recovers AOF
ssh jn1122@hp109.utah.cloudlab.us '~/bench/bin/lossbench --endpoint node1:2379 --mode=count | tee ~/bench/results/loss-retcd-present.txt'
#   lost = acked - present. Repeat for etcd (expect 0) as the control.

# 6. PULL results back to Mac for analysis
for h in hp118 hp111 hp087 hp109; do
  scp -r jn1122@$h.utah.cloudlab.us:~/bench/results "load-test/results/cloudlab-$h" || true
done
```

## Notes / gotchas

- **etcd version pinned** in `build_and_push.sh` (`ETCD_VER`); all phases use the
  same one so the tables are internally consistent. Default `v3.5.24`.
- **kine**: `start_single_backends.sh` has a `KINE_*` block — confirm it matches the
  known-good invocation from your Mac run before trusting the kine columns.
- **ssh fan-out**: these are one-node-at-a-time `ssh` calls (no in-loop stdin feed),
  so the truncation gotcha does not apply.
- Results land in `~/bench/results/` on each node; step 6 pulls them back.
</content>
