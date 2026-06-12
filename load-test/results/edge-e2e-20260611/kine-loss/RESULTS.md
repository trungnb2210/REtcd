# kine-as-shipped crash trials (2026-06-11, hp109 xl170, kine v0.13.0, k3s DSN: WAL+synchronous=NORMAL)
Method: lossbench sequential Txn-creates; ack record survives off-host; SysRq-b mid-write; recover on surviving db; --mode count.

| trial | rate | acked | recovered | lost |
|---|---|---|---|---|
| low-rate (client = Mac over WAN, RTT 131ms) | ~8/s | 281 (exact, off-host) | 281 | 0 |
| mid-rate (writer local, acks ssh-streamed) | ~350/s | ~6,376 (last milestone 6,000; survivor-bounded) | 6,376 | 0 |

ZERO acknowledged-write loss in both trials. WAL at crash #2: 4,157,112 B ~ 1,015 pages,
i.e. just past the 1,000-page checkpoint (checkpoint fsync fired moments pre-crash).
Interpretation (written into eval §loss): survival is UNCONTRACTED — rests on ext4 5s
journal commits / kernel writeback / server-grade SSD; SQLite documents WAL+NORMAL may
lose recent commits. REtcd everysec = enforced ~1s bound (measured 1,050/17,683 same rig
class); etcd = 0 by construction. Distinction = contract, not observed magnitude.
