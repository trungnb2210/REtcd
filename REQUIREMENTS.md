In scope
  - etcd v3 gRPC API: Put, Get, Range, Delete, Txn, Watch, LeaseGrant, LeaseKeepAlive, LeaseRevoke
  - Single-node Redis backend with AOF persistence
  - Sufficient to boot a Kind cluster and run pod workloads
  - Benchmark against native etcd using Prometheus 
  
Out of scope 
  - TLS / client auth
  - Compaction                                                  
  - Multi-node Redis (HA)
  - etcd maintenance APIs (Defragment, Snapshot)
  - Kubernetes conformance tests

Acceptance Criteria:
  1. kubectl get nodes returns nodes after cluster start
  2. Pod create → schedule → Running works end-to-end 
  3. kubectl delete pod is reflected immediately on re-read
  4. Restarting the API server reconnects and resumes (Watch re-established)
  5. Restarting Redis — data survives (with persistence enabled)
  6. kube-scheduler and kube-controller-manager both elect a leader and stay healthy (lease requirement)
  7. Run your existing load test at 50 RPS for 1 minute without errors 