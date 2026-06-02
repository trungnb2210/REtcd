package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trungnb2210/REtcd/store"
)

const (
	// subChanBuffer bounds how far a single watcher may lag before it is cut
	// loose (forcing the client to re-establish) rather than stalling the feed.
	subChanBuffer = 8192
	// readBatchSize is how many stream entries the shared reader pulls per XREAD.
	// Larger batches amortise the round trip under write-storm load.
	readBatchSize = 1000
	// maxReaderErrors before the shared reader declares an outage and cancels
	// live watches (so clients re-establish instead of stalling until resync).
	maxReaderErrors  = 30
	readerErrBackoff = 50 * time.Millisecond
	// checkpointEvery is how often (in events) the shared reader records a
	// rev→streamID checkpoint. A catch-up for a historical startRevision seeks
	// to the nearest checkpoint instead of re-scanning the stream from "0",
	// turning an O(stream length) scan into O(checkpointEvery). Smaller = finer
	// seeks but more memory; ~512 keeps the index tiny (a few KB per 256k events).
	checkpointEvery = 512
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
	cancelCh   chan struct{}    // closed by the dispatcher on lag or backend outage
	cancelOnce sync.Once
}

func (s *watchSub) signalCancel() { s.cancelOnce.Do(func() { close(s.cancelCh) }) }

// eventDispatcher tails the Redis event stream ONCE and fans matching events out
// to every registered watcher in memory. This replaces the previous model where
// each watch ran its own XREAD loop against the shared stream — collapsing
// O(watches) Redis readers (and their per-loop CurrentRevision GETs) down to a
// single reader, which is what lets watch propagation keep up under the
// concurrent-write load of FaaS scale storms.
type eventDispatcher struct {
	store Store

	mu        sync.Mutex
	subs      map[int64]*watchSub
	nextID    int64
	latestRev int64 // highest revision dispatched so far (a registration's boundary)
	ckCounter int   // events since the last checkpoint (guarded by mu)

	// ckMu guards the sparse rev→streamID index. Kept separate from mu so
	// catch-up seeks (read) don't contend with live dispatch (write) on the hot
	// fan-out lock. Lock order is always mu → ckMu; seek takes only ckMu.
	ckMu        sync.RWMutex
	checkpoints []checkpoint // sorted by rev ascending
}

func newEventDispatcher(s Store) *eventDispatcher {
	return &eventDispatcher{store: s, subs: make(map[int64]*watchSub)}
}

// register adds a subscriber and returns its id, handle, and the boundary
// revision. Every event with rev > boundary is guaranteed to arrive on the
// subscriber's channel; the caller catches up on [startRevision, boundary] from
// the store directly. Because registration takes the same lock the reader
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

// run is the single shared reader loop. It lives for the lifetime of the server.
func (d *eventDispatcher) run(ctx context.Context) {
	// Prime latestRev from the store so currentRev() (used for progress
	// notifications and live-event headers) is accurate immediately, rather than
	// reading 0 until the reader has drained the historical stream. Runs before
	// the loop, so no concurrent dispatch can race this write.
	if rev, err := d.store.CurrentRevision(ctx); err == nil {
		atomic.StoreInt64(&d.latestRev, rev)
	}

	lastID := "0"
	consecErr := 0
	for {
		if ctx.Err() != nil {
			return
		}
		events, newLastID, err := d.store.BlockReadEvents(ctx, lastID, readBatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Transient backend error. A silent stop would leave every watch's
			// cache stale; instead retry from the same lastID (no events skipped)
			// and, only if the backend stays down, cancel live watches loudly.
			watchReadErrors.Inc()
			consecErr++
			if consecErr >= maxReaderErrors {
				d.signalOutage()
				consecErr = 0 // keep retrying so watches work again on recovery
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(readerErrBackoff):
			}
			continue
		}
		consecErr = 0
		lastID = newLastID
		if len(events) > 0 {
			d.dispatch(events)
		}
	}
}

// dispatch fans a batch of events out to matching subscribers. Sends are
// non-blocking: a subscriber that cannot keep up is cut loose rather than
// stalling delivery for everyone else.
func (d *eventDispatcher) dispatch(events []store.Event) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, ev := range events {
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
}

// maybeCheckpoint records a sparse rev→streamID checkpoint as the reader tails
// the stream. Called from dispatch under d.mu (so ckCounter is safe); it only
// grabs ckMu on the rare append. Checkpoints are kept strictly increasing in
// rev so the index stays sorted for binary search; an out-of-order lower rev
// (possible when concurrent writers' XADDs land out of INCR order) is skipped.
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

// signalOutage cancels all current subscribers (used when the backend is
// persistently unavailable) so their clients re-establish.
func (d *eventDispatcher) signalOutage() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, sub := range d.subs {
		watchCancels.Inc()
		sub.signalCancel()
		delete(d.subs, id)
	}
}
