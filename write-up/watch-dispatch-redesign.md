# Watch dispatch redesign — per-watch XREAD → shared single-reader fan-out

Date: 2026-06-01. Companion to `project_propagation_microbench` and the
cold-start watch-delivery finding (thesis novelty c).

## Why this changed

The propbench concurrency sweep (FaaS 0→N scale-storm: `--burst --writers N`)
showed REtcd's watch **propagation collapsing** under concurrent write load,
while write throughput kept scaling:

| writers | REtcd thrpt | REtcd prop p99 | etcd thrpt | etcd prop p99 |
|---------|-------------|----------------|------------|---------------|
| 1       | 1.3k/s      | 1.1 ms         | 1.4k/s     | 1.1 ms        |
| 8       | 11k/s       | **384 ms**     | 11k/s      | 1.6 ms        |
| 32      | 18k/s       | 783 ms         | 38k/s      | 2.2 ms        |
| 128     | 22k/s       | 1461 ms        | 59k/s      | 5.0 ms        |
| 256     | 24k/s       | 1860 ms        | 63k/s      | 1277 ms       |

(0% events lost in every run — events were *delayed*, not dropped.)

At 8 concurrent writers (same ~11k/s throughput as etcd) REtcd's watch p99 was
**250× worse**. Root cause: each watch ran its own reader loop that could not
drain the shared Redis stream fast enough; events queued, so propagation latency
= backlog depth. This is the storage-side contributor to FaaS cold start, so the
collapse matters.

## OLD design — one XREAD loop per watch (the bottleneck)

Each `Watch` create spawned a goroutine running `tailWatch`, and **every** such
goroutine independently:

- tailed the single shared `events` Redis stream from `"0"` via its own
  `BlockReadEvents` (XREAD, batch of **100**),
- filtered every event by key/range in Go (so each event was parsed once *per
  watch*),
- issued a `CurrentRevision` GET **every loop iteration** (an extra Redis round
  trip per ~100 events) just to stamp `Header.Revision`,
- sent through a per-stream mutex (`sender`), serialising sends across all
  watches on one gRPC connection.

So the cost was **O(watches × events)** Redis work, plus a per-loop GET, plus
read-and-send serialised in the same loop (no pipelining). Under a write storm
the per-loop overhead (XREAD round trip + `CurrentRevision` GET + `fmt.Sprintf`
parsing of 100 events + gRPC send) made a single watch's drain rate fall below
the arrival rate, and the backlog — hence latency — grew without bound.

The old `tailWatch`, preserved verbatim:

```go
func (s *WatchServer) tailWatch(ctx context.Context, sender *sender, id int64, req *pb.WatchCreateRequest) {
	activeWatches.Inc()
	defer activeWatches.Dec()

	key := string(req.Key)
	rangeEnd := string(req.RangeEnd)
	startRevision := req.StartRevision

	lastID := "0"
	lastSend := time.Now()

	// Count events scanned before this watch delivers anything, to quantify the
	// O(N) cost of scanning the stream from "0" on every watch establishment.
	var scannedBeforeFirst int64
	var firstDeliveryDone bool

	const (
		progressInterval = 1 * time.Second
		// After this many consecutive read failures (~1.5s of solid failure at
		// the 50ms backoff) we stop retrying and tell the client to re-establish,
		// rather than dying silently and leaving its watch cache stale until a
		// resync interval fires.
		maxConsecErrors = 30
	)
	consecErrors := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		events, newLastID, err := s.store.BlockReadEvents(ctx, lastID, 100)
		if err != nil {
			// Legitimate teardown — the watch is being cancelled.
			if ctx.Err() != nil {
				return
			}
			// Transient backend error (pool timeout, redis blip). A silent
			// return here would leave the apiserver's per-resource watch cache
			// stale until its resync interval, producing multi-second stalls.
			// Instead retry from the SAME lastID (no events skipped); only give
			// up — loudly — if the backend stays unavailable.
			watchReadErrors.Inc()
			consecErrors++
			if consecErrors >= maxConsecErrors {
				curRev, _ := s.store.CurrentRevision(ctx)
				watchCancels.Inc()
				_ = sender.Send(&pb.WatchResponse{
					Header:       &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: curRev},
					WatchId:      id,
					Canceled:     true,
					CancelReason: "watch backend unavailable",
				})
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		consecErrors = 0
		lastID = newLastID

		var matched []*mvccpb.Event
		var createdMs []int64
		for _, ev := range events {
			if !firstDeliveryDone {
				scannedBeforeFirst++
			}
			if ev.Rev < startRevision {
				continue
			}
			if !matchesWatch(ev.Key, key, rangeEnd) {
				continue
			}
			if e := eventToProto(ev); e != nil {
				matched = append(matched, e)
				createdMs = append(createdMs, ev.CreatedMs)
			}
		}

		curRev, _ := s.store.CurrentRevision(ctx)

		if len(matched) > 0 {
			if err := sender.Send(&pb.WatchResponse{
				Header:  &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: curRev},
				WatchId: id,
				Events:  matched,
			}); err != nil {
				return
			}
			// Delivery latency: event stream-entry timestamp → now (post-send).
			now := time.Now().UnixMilli()
			for _, ms := range createdMs {
				if ms > 0 {
					watchDelivery.Observe(float64(now-ms) / 1000.0)
				}
			}
			if !firstDeliveryDone {
				watchCatchupEvents.Observe(float64(scannedBeforeFirst))
				firstDeliveryDone = true
			}
			lastSend = time.Now()
			continue
		}

		if time.Since(lastSend) >= progressInterval {
			if err := sender.Send(&pb.WatchResponse{
				Header:  &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: curRev},
				WatchId: id,
			}); err != nil {
				return
			}
			lastSend = time.Now()
		}
	}
}
```

## NEW design — one shared reader, in-memory fan-out (`server/dispatcher.go`)

A single `eventDispatcher` per server tails the stream **once** and fans matching
events out to each watch's in-memory channel:

- **One** `BlockReadEvents` loop for the whole process (batch of **1000**),
  regardless of how many watches are open → O(events), not O(watches×events).
- `CurrentRevision` GET removed from the hot path: the reader tracks
  `latestRev` from the events it dispatches; watches read it with an atomic load.
- Read and send are **decoupled** — the reader keeps draining into buffered
  channels while each watch's goroutine sends independently (pipelining).
- Live sends are **batched**: a watch coalesces its first event plus anything
  already queued into one `WatchResponse` (`collectEvents`, up to
  `maxSendBatch`), cutting gRPC send count under bursts.
- Correctness on the catch-up/live handoff: `register` returns a **boundary**
  revision under the same lock the reader dispatches under. Everything with
  `rev > boundary` is guaranteed on the live channel; the watch replays only
  `[startRevision, boundary]` from the store (`catchUp`). No gap, no duplicate.
- Backpressure: a watch that can't keep up fills its buffered channel and is cut
  loose (`Canceled`, client re-establishes) rather than stalling the shared feed.
- Outage handling preserved: the shared reader retries transient errors and,
  after `maxReaderErrors`, cancels live watches loudly (`signalOutage`) so
  clients re-establish instead of stalling until a resync interval.

## Expected effect

The shared reader drains the stream at full speed into channels, and batched
sends keep gRPC throughput up, so propagation should stay low as concurrency
rises instead of collapsing at 8 writers. Re-run the propbench sweep
(`--burst --writers 1,8,32,128,256`) against the new build to fill in the
"after" column and compare to the table above.

Measured (propbench sweep, 20k writes, node0, Redis unix socket):

| writers | OLD prop p99 | NEW prop p99 | improvement | single-node etcd p99 |
|---------|--------------|--------------|-------------|----------------------|
| 1       | 1.13 ms      | 1.07 ms      | ~same       | 1.06 ms              |
| 8       | 384 ms       | **1.48 ms**  | 260×        | 1.56 ms              |
| 32      | 783 ms       | 2.74 ms      | 285×        | 2.20 ms              |
| 128     | 1461 ms      | 14.6 ms      | 100×        | 4.96 ms              |
| 256     | 1860 ms      | **17.8 ms**  | 105×        | **1277 ms**          |

Outcome: the collapse is eliminated. REtcd's watch propagation is now
competitive with single-node etcd through ~128 concurrent writers, and at 256
writers (where etcd's own watch path degrades to 1.3 s p99) REtcd holds 17.8 ms.

NOT changed by this work: write **throughput** still caps ~24k writes/s (etcd:
63k) — that ceiling is the per-write Redis pipeline (`SET`+`ZADD`+`XADD`), a
separate write-path concern, not the watch fan-out fixed here.
