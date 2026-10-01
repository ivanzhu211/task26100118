package mirrordesk

import (
	"sync"
	"testing"
	"time"
)

// closeWithin runs Close and fails if it does not return promptly.
func closeWithin(t *testing.T, h *Hub, id string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		h.Close(id)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Close(%q) did not return", id)
	}
}

// Close while the sender is blocked waiting for writability.
func TestCloseWhileWaitingWritable(t *testing.T) {
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	hub := NewHub(clock)
	sink := newTestSink(clock, false, 0) // never writable until the test says so

	if err := hub.Attach("A", 5, sink); err != nil {
		t.Fatal(err)
	}
	if !hub.Offer("A", makeFrame(0x01, 1000)) {
		t.Fatal("offer must be accepted")
	}
	waitFor(t, "sender to wait for writable", func() bool { return sink.WaitCount() >= 1 })

	closeWithin(t, hub, "A")

	// No output may appear after Close returned, even if the sink
	// becomes writable again.
	sink.SetWritable(true)
	time.Sleep(30 * time.Millisecond)
	if n := sink.Len(); n != 0 {
		t.Fatalf("wrote %d bytes after Close", n)
	}

	if hub.Offer("A", makeFrame(0x02, 1000)) {
		t.Fatal("offer after Close must be dropped")
	}
	if hub.SetRate("A", 10) {
		t.Fatal("SetRate after Close must not apply")
	}
	closeWithin(t, hub, "A") // idempotent
	if n := hub.SessionCount(); n != 0 {
		t.Fatalf("sessions left: %d", n)
	}
}

// Close while the sender is parked waiting for bandwidth credit.
func TestCloseWhileWaitingCredit(t *testing.T) {
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	hub := NewHub(clock)
	sink := newTestSink(clock, true, 0)

	if err := hub.Attach("B", 5, sink); err != nil {
		t.Fatal(err)
	}
	if !hub.Offer("B", makeFrame(0x03, 200000)) {
		t.Fatal("offer must be accepted")
	}
	waitFor(t, "burst exhausted", func() bool { return sink.Len() >= burstBytes })
	waitParked(t, hub, "B", 0)

	closeWithin(t, hub, "B")

	// Even with plenty of time (and thus credit), nothing more is sent.
	clock.Advance(time.Hour)
	time.Sleep(30 * time.Millisecond)
	if n := sink.Len(); n != burstBytes {
		t.Fatalf("wrote after Close: %d bytes total", n)
	}

	closeWithin(t, hub, "B") // idempotent
	if n := hub.SessionCount(); n != 0 {
		t.Fatalf("sessions left: %d", n)
	}
}

// Offer, SetRate and Close racing each other must stay linearizable:
// Close completes, no output happens afterwards, no work is left.
func TestConcurrentOpsInterleave(t *testing.T) {
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	hub := NewHub(clock)
	sink := newTestSink(clock, true, 512)

	if err := hub.Attach("C", 5, sink); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // hammer offers; each is accepted or dropped, never blocks
		defer wg.Done()
		for i := 0; i < 200; i++ {
			hub.Offer("C", makeFrame(0x04, 1000+i))
		}
	}()
	go func() { // keep re-tuning the rate
		defer wg.Done()
		for i := 0; i < 200; i++ {
			hub.SetRate("C", 5+float64(i%46))
		}
	}()

	time.Sleep(10 * time.Millisecond) // let the racers overlap
	closeWithin(t, hub, "C")
	wg.Wait()

	before := sink.Len()
	clock.Advance(time.Hour)
	sink.SetWritable(false)
	sink.SetWritable(true)
	time.Sleep(30 * time.Millisecond)
	if n := sink.Len(); n != before {
		t.Fatalf("wrote %d more bytes after Close", n-before)
	}
	closeWithin(t, hub, "C") // idempotent
	if n := hub.SessionCount(); n != 0 {
		t.Fatalf("sessions left: %d", n)
	}
}
