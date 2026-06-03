package server

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trungnb2210/REtcd/store"
)

const (
	// subChanBuffer bounds how far a single watcher may lag before it is cut
	// loose (forcing the client to re-establish) rather than stalling the feed.
	subChanBuffer = 8192
	// readBatchSize is how many stream entries a catch-up read pulls per XREAD.
	readBatchSize = 1000
	// maxReaderErrors before a catch-up read declares an outage and cancels the
	// watch (so the client re-establishes instead of stalling until resync).
	maxReaderErrors  = 30
	readerErrBackoff = 50 * time.Millisecond
	// checkpointEvery is how often (in events) the dispatcher records a
	// rev→streamID checkpoint. A catch-up for a historical startRevision seeks
	// to the nearest checkpoint instead of re-scanning the stream from "0",
	// turning an O(stream length) scan into O(checkpointEvery).
	checkpointEvery = 512
	// maxPendingEvents caps the reorder buffer. It only fills if a revision's
	// event never arrives (a bug — every committed write emits exactly one
	// event); past the cap we flush to preserve liveness. Far above any
	// realistic count of concurrently in-flight writes.
	maxPendingEvents = 4096
)

// checkpoint maps an etcd revision to the Redis stream ID at which it was seen.
type checkpoint struct {
	rev int64
	id  string
}

// watchSub is one registered watcher's view onto the shared event feed.
type watchSub struct {
	key        string
	rangeEnd   string
	ch         chan store.Event // live events with rev > the registration boundary
	cancelCh   chan struct{}    // closed by the dispatcher on lag
	cancelOnce sync.Once
}

func (s *watchSub) signalCancel() { s.cancelOnce.Do(func() { close(s.cancelCh) }) }

// eventDispatcher fans every write's event out to matching watchers entirely
// in-memory. Events arrive via ingest() straight from the write path (the
// store's event sink) — NOT by reading them back off the Redis stream. This is
// the in-process watch short-circuit: it removes the XADD→XREAD readback
// round-trip from watch propagation, so a watcher sees a change as soon as the
// write commits rather than after the server re-reads it from Redis.
//
// The Redis stream is still written (by the Lua scripts) and is still the source
// for historical catch-up (catchUp in watch.go); only LIVE delivery is now
// in-process. This assumes a single REtcd process per Redis — the only writer,
// so its in-process feed is the complete live feed.
type eventDispatcher struct {
	store Store

	mu        sync.Mutex
	subs      map[int64]*watchSub
	nextID    int64
	latestRev int64 // highest revision released so far (a registration's boundary)
	ckCounter int   // events since the last checkpoint (guarded by mu)

	// Reorder buffer: concurrent write handlers can call ingest out of revision
	// order (writer B's Lua may return to Go before writer A's even though A got
	// the lower INCR). We release to subscribers strictly in contiguous rev
	// order so a watch never sees revisions go backwards. Safe and gap-free
	// because every revision that exists corresponds to exactly one event
	// (INCR+XADD are atomic within one Lua script).
	nextRev int64                 // next revision to release (guarded by mu)
	pending map[int64]store.Event // out-of-order events awaiting release (guarded by mu)

	// ckMu guards the sparse rev→streamID index. Kept separate from mu so
	// catch-up seeks (read) don't contend with live release (write).
	ckMu        sync.RWMutex
	checkpoints []checkpoint // sorted by rev ascending
}

func newEventDispatcher(s Store) *eventDispatcher {
	return &eventDispatcher{
		store:   s,
		subs:    make(map[int64]*watchSub),
		pending: make(map[int64]store.Event),
	}
}

// prime initialises the revision watermarks from the store before the dispatcher
// accepts any writes, so currentRev() is accurate immediately and the reorder
// buffer releases the first live event without waiting for a phantom predecessor.
func (d *eventDispatcher) prime(ctx context.Context) {
	rev, err := d.store.CurrentRevision(ctx)

	// latestRev is the externally visible revision: clamp to ≥1 (etcd never
	// exposes 0). It seeds progress-notification headers and the catch-up boundary.
	latest := rev
	if err != nil || latest < 1 {
		latest = 1
	}
	atomic.StoreInt64(&d.latestRev, latest)

	// nextRev = (revision of the last write) + 1 — the rev the first live event
	// will carry. This must use the RAW, unclamped counter: on a fresh store the
	// counter is 0 and the first write is rev 1, so a clamped value of 1 would
	// seed nextRev=2 and drop that first event as a duplicate. RedisStore clamps
	// CurrentRevision, so prefer its RawRevision; the in-memory test double does
	// not clamp, so its CurrentRevision is already raw.
	next := rev + 1
	if err != nil {
		next = 1
	}
	if rr, ok := d.store.(interface {
		RawRevision(context.Context) (int64, error)
	}); ok {
		if raw, rerr := rr.RawRevision(ctx); rerr == nil {
			next = raw + 1
		}
	}
	if next < 1 {
		next = 1
	}
	d.mu.Lock()
	d.nextRev = next
	d.mu.Unlock()
}

// register adds a subscriber and returns its id, handle, and the boundary
// revision. Every event with rev > boundary is guaranteed to arrive on the
// subscriber's channel; the caller catches up on [startRevision, boundary] from
// the store directly. Because registration takes the same lock release
// dispatches under, the boundary is exact — no event is dropped or duplicated
// across the catch-up/live handoff.
func (d *eventDispatcher) register(key, rangeEnd string) (int64, *watchSub, int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextID++
	id := d.nextID
	sub := &watchSub{
		key:      key,
		rangeEnd: rangeEnd,
		ch:       make(chan store.Event, subChanBuffer),
		cancelCh: make(chan struct{}),
	}
	d.subs[id] = sub
	return id, sub, d.latestRev
}

func (d *eventDispatcher) deregister(id int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.subs, id)
}

func (d *eventDispatcher) currentRev() int64 { return atomic.LoadInt64(&d.latestRev) }

// ingest is the store's event sink: every committed write delivers its event
// here. Out-of-order arrivals are buffered and released in contiguous revision
// order. It is safe to call concurrently from many write handlers.
func (d *eventDispatcher) ingest(ev store.Event) {
	if ev.Rev <= 0 {
		return
	}
	d.mu.Lock()
	if d.nextRev == 0 {
		d.nextRev = ev.Rev // unprimed (should not happen post-prime); start here
	}
	if ev.Rev < d.nextRev {
		d.mu.Unlock()
		return // already released — a duplicate
	}
	d.pending[ev.Rev] = ev
	for {
		next, ok := d.pending[d.nextRev]
		if !ok {
			break
		}
		delete(d.pending, d.nextRev)
		d.releaseLocked(next)
		d.nextRev++
	}
	if len(d.pending) > maxPendingEvents {
		d.flushPendingLocked()
	}
	d.mu.Unlock()
}

// releaseLocked fans one event out to matching subscribers and advances the
// watermark/checkpoint index. Sends are non-blocking: a subscriber that cannot
// keep up is cut loose rather than stalling delivery for everyone else. Caller
// holds d.mu.
func (d *eventDispatcher) releaseLocked(ev store.Event) {
	if ev.Rev > d.latestRev {
		atomic.StoreInt64(&d.latestRev, ev.Rev)
	}
	d.maybeCheckpoint(ev)
	for id, sub := range d.subs {
		if !matchesWatch(ev.Key, sub.key, sub.rangeEnd) {
			continue
		}
		select {
		case sub.ch <- ev:
		default:
			sub.signalCancel()
			delete(d.subs, id)
		}
	}
}

// flushPendingLocked is a liveness safety valve. If a revision's event never
// arrives (a bug — every committed write emits its event), the buffer would grow
// and live delivery would stall behind the missing rev forever. Past the cap we
// release everything we hold in revision order and skip the gap, trading strict
// ordering for liveness and bounded memory. It increments a counter so a real
// occurrence is visible. Caller holds d.mu.
func (d *eventDispatcher) flushPendingLocked() {
	revs := make([]int64, 0, len(d.pending))
	for r := range d.pending {
		revs = append(revs, r)
	}
	sort.Slice(revs, func(i, j int) bool { return revs[i] < revs[j] })
	for _, r := range revs {
		ev := d.pending[r]
		delete(d.pending, r)
		d.releaseLocked(ev)
	}
	if n := len(revs); n > 0 {
		d.nextRev = revs[n-1] + 1
	}
	watchReorderFlushes.Inc()
}

// maybeCheckpoint records a sparse rev→streamID checkpoint as events are
// released. Called under d.mu (so ckCounter is safe); it only grabs ckMu on the
// rare append. Checkpoints are kept strictly increasing in rev so the index
// stays sorted for binary search.
func (d *eventDispatcher) maybeCheckpoint(ev store.Event) {
	if ev.ID == "" {
		return // backend doesn't expose stream IDs (e.g. test double) → no index
	}
	d.ckCounter++
	if len(d.checkpoints) > 0 && d.ckCounter < checkpointEvery {
		return
	}
	d.ckCounter = 0
	d.ckMu.Lock()
	if n := len(d.checkpoints); n == 0 || ev.Rev > d.checkpoints[n-1].rev {
		d.checkpoints = append(d.checkpoints, checkpoint{rev: ev.Rev, id: ev.ID})
	}
	d.ckMu.Unlock()
}

// seekStreamID returns a stream ID to start a catch-up XREAD from so that no
// event with rev >= startRev is skipped. It picks a checkpoint with rev strictly
// below startRev and steps back one more as a safety margin against intra-batch
// stream reordering, then lets the caller filter the (few) replayed events that
// fall below startRev. Returns "0" (scan from the beginning) when the index
// can't safely narrow the scan — always correct, just unoptimised.
func (d *eventDispatcher) seekStreamID(startRev int64) string {
	d.ckMu.RLock()
	defer d.ckMu.RUnlock()
	// Largest index whose checkpoint rev is strictly < startRev.
	lo, hi, idx := 0, len(d.checkpoints), -1
	for lo < hi {
		mid := (lo + hi) / 2
		if d.checkpoints[mid].rev < startRev {
			idx = mid
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx-- // one-checkpoint safety margin
	if idx < 0 {
		return "0"
	}
	return d.checkpoints[idx].id
}
