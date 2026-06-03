package server

import (
	"testing"

	"github.com/trungnb2210/REtcd/store"
)

// drainRev returns the revision of the next buffered event on the sub, or 0 if
// none is immediately available.
func drainRev(sub *watchSub) int64 {
	select {
	case ev := <-sub.ch:
		return ev.Rev
	default:
		return 0
	}
}

// ingest must release events to subscribers in contiguous revision order even
// when the write path delivers them out of order (concurrent writers can call
// the sink in any order relative to their INCR-assigned revisions).
func TestIngestReleasesInRevisionOrder(t *testing.T) {
	d := newEventDispatcher(nil)
	d.nextRev = 1
	_, sub, _ := d.register("k", "\x00") // watch all keys >= "k"

	// rev 3 arrives first — it must NOT be released until 1 and 2 have gone.
	d.ingest(store.Event{Type: "PUT", Key: "k3", Rev: 3})
	if got := drainRev(sub); got != 0 {
		t.Fatalf("released rev %d before rev 1 arrived; reorder buffer leaked", got)
	}

	d.ingest(store.Event{Type: "PUT", Key: "k1", Rev: 1})
	d.ingest(store.Event{Type: "PUT", Key: "k2", Rev: 2})

	for want := int64(1); want <= 3; want++ {
		if got := drainRev(sub); got != want {
			t.Fatalf("out-of-order release: got rev %d, want %d", got, want)
		}
	}
}

// An event whose revision is at or below the watermark (already released) is a
// duplicate and must be dropped, not delivered again.
func TestIngestDropsAlreadyReleasedRevision(t *testing.T) {
	d := newEventDispatcher(nil)
	d.nextRev = 5
	_, sub, _ := d.register("k", "\x00")

	d.ingest(store.Event{Type: "PUT", Key: "k1", Rev: 3}) // below the watermark
	if got := drainRev(sub); got != 0 {
		t.Fatalf("delivered already-released rev %d", got)
	}
}

// A persistent gap (a revision that never arrives) must not stall delivery
// forever: past the cap the buffer force-flushes in revision order so live
// delivery resumes.
func TestIngestFlushesOnPersistentGap(t *testing.T) {
	d := newEventDispatcher(nil)
	d.nextRev = 1
	_, sub, _ := d.register("k", "\x00")

	// Feed maxPendingEvents+1 events all above the missing rev 1. None can be
	// released contiguously, so the buffer fills and force-flushes.
	for i := 0; i <= maxPendingEvents; i++ {
		d.ingest(store.Event{Type: "PUT", Key: "k", Rev: int64(2 + i)})
	}
	if got := drainRev(sub); got == 0 {
		t.Fatal("buffer never flushed despite a persistent gap; delivery would stall forever")
	}
}
