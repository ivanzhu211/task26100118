package mirrordesk

import (
	"bytes"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

// limiterWaits reports how often the session's sender parked for credit.
func limiterWaits(h *Hub, id string) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s := h.sessions[id]; s != nil {
		return s.lim.waits.Load()
	}
	return -1
}

// waitParked returns once the sender has re-entered a credit wait since
// the returned previous count, proving it settled after a rate change.
func waitParked(t *testing.T, h *Hub, id string, since int64) {
	t.Helper()
	waitFor(t, "sender to park for credit", func() bool {
		return limiterWaits(h, id) > since
	})
}

// The sink alternates partial receives and 0-returns; the rate is lowered
// and raised while credit is exhausted. Verifies: the byte bound holds
// across rate changes, unsent bytes are never charged, and raising the
// rate resumes progress promptly.
func TestRateLimitAcrossSetRate(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	clock := NewManualClock(t0)
	hub := NewHub(clock)

	sink := newTestSink(clock, true, 777) // partial writes
	var stalled atomic.Bool
	sink.onWrite = func(s *testSink) {
		if s.Len() >= 40000 && stalled.CompareAndSwap(false, true) {
			s.SetWritable(false) // switch to 0-returns mid-burst
		}
	}
	if err := hub.Attach("S", 50, sink); err != nil {
		t.Fatalf("attach: %v", err)
	}

	payload := makeFrame(0x11, 200000) // 200004 bytes on the wire
	if !hub.Offer("S", payload) {
		t.Fatal("offer must be accepted")
	}

	// 0-return phase: the sender blocks in WaitWritable, no progress.
	waitFor(t, "sender to wait for writable", func() bool { return sink.WaitCount() >= 1 })
	if n := sink.Len(); n < 40000 {
		t.Fatalf("stalled too early at %d", n)
	}
	sink.SetWritable(true)

	// The initial burst is exactly 65536 bytes; then credit is exhausted.
	waitFor(t, "burst exhausted", func() bool { return sink.Len() >= burstBytes })
	if got := sink.Len(); got != burstBytes {
		t.Fatalf("expected exactly the burst, got %d", got)
	}

	type rateChange struct {
		at   time.Time
		mbps float64
	}
	changes := []rateChange{{t0, 50}}
	setRate := func(mbps float64) {
		t.Helper()
		w := limiterWaits(hub, "S")
		if !hub.SetRate("S", mbps) {
			t.Fatalf("SetRate(%v) not applied", mbps)
		}
		changes = append(changes, rateChange{clock.Now(), mbps})
		waitParked(t, hub, "S", w)
	}

	// Lower the rate repeatedly while credit is exhausted.
	setRate(5)
	setRate(10)
	setRate(5)
	// 10ms at 5Mbps accrues 6250 bytes of credit, but the sender is
	// parked for a full 65536 target ~105ms out: no writes may happen.
	clock.Advance(10 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if got := sink.Len(); got != burstBytes {
		t.Fatalf("wrote %d bytes without sufficient credit", got-burstBytes)
	}

	// Raising the rate wakes the parked sender, which immediately
	// consumes the 6250 bytes of accrued credit, then parks again.
	setRate(50)
	waitFor(t, "accrued credit consumed", func() bool { return sink.Len() >= burstBytes+6250 })
	if got := sink.Len(); got != burstBytes+6250 {
		t.Fatalf("expected exactly the accrued credit, got %d extra", got-burstBytes)
	}

	// 11ms at 50Mbps refills a full burst: progress resumes promptly.
	clock.Advance(11 * time.Millisecond)
	waitFor(t, "second burst", func() bool { return sink.Len() >= burstBytes+6250+burstBytes })
	if got := sink.Len(); got != burstBytes+6250+burstBytes {
		t.Fatalf("expected %d, got %d", burstBytes+6250+burstBytes, got)
	}

	// The remaining 200004-71786-65536=62682 bytes need ~10.03ms.
	clock.Advance(11 * time.Millisecond)
	want := framed(t, payload)
	waitFor(t, "frame complete", func() bool { return sink.Len() >= len(want) })
	if got := sink.Len(); got != len(want) {
		t.Fatalf("expected %d, got %d", len(want), got)
	}
	if got := sink.Bytes(); !bytes.Equal(got, want) {
		t.Fatal("output does not byte-match the original frame")
	}

	// The next offer is accepted only now that the frame is fully out.
	next := makeFrame(0x22, 1000)
	if !hub.Offer("S", next) {
		t.Fatal("offer after completed frame must be accepted")
	}
	clock.Advance(time.Millisecond) // 6250 bytes of credit, frame needs 1004
	wantNext := framed(t, next)
	waitFor(t, "next frame complete", func() bool { return sink.Len() >= len(want)+len(wantNext) })
	if got := sink.Bytes(); !bytes.Equal(got, append(want, wantNext...)) {
		t.Fatal("second frame output corrupted")
	}

	// Replay a token bucket (capacity 65536, initial 65536) over the
	// recorded rate changes and writes. tokens >= 0 at every step is
	// equivalent to: bytes in any interval <= 65536 + integral of rate.
	type item struct {
		at       time.Time
		isChange bool
		mbps     float64
		n        int
	}
	var items []item
	for _, c := range changes {
		items = append(items, item{at: c.at, isChange: true, mbps: c.mbps})
	}
	for _, e := range sink.Events() {
		items = append(items, item{at: e.at, n: e.n})
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].at.Before(items[j].at) })

	tokens := float64(burstBytes)
	last := t0
	rate := 50.0
	refill := func(to time.Time) {
		if to.After(last) {
			tokens += mbpsToBytesPerSec(rate) * to.Sub(last).Seconds()
			if tokens > burstBytes {
				tokens = burstBytes
			}
			last = to
		}
	}
	total := 0
	for _, it := range items {
		refill(it.at)
		if it.isChange {
			rate = it.mbps
			continue
		}
		tokens -= float64(it.n)
		total += it.n
		if tokens < -1e-6 {
			t.Fatalf("rate limit exceeded at %v: tokens=%f", it.at, tokens)
		}
		if tokens > burstBytes+1e-6 {
			t.Fatalf("burst re-granted at %v: tokens=%f", it.at, tokens)
		}
	}
	if total != len(want)+len(wantNext) {
		t.Fatalf("accounted %d bytes, want %d", total, len(want)+len(wantNext))
	}

	hub.Close("S")
	if n := hub.SessionCount(); n != 0 {
		t.Fatalf("sessions left: %d", n)
	}
}
