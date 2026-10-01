package mirrordesk

import (
	"sync"
	"testing"
	"time"
)

// sessionOf peeks at the live session so tests can verify its goroutine
// has exited after Close.
func (h *Hub) sessionOf(id string) *session { return h.get(id) }

func assertDone(t *testing.T, s *session) {
	t.Helper()
	select {
	case <-s.done:
	default:
		t.Fatal("sender goroutine still running after Close returned")
	}
}

// Close must complete while the sender is blocked in WaitWritable, must
// stop all output, release the goroutine, and tolerate repetition.
func TestCloseWhileWaitingWritable(t *testing.T) {
	clock := NewManualClock()
	h := New(WithClock(clock))

	sink := newFakeSink()
	sink.setWritable(false) // sender will block in WaitWritable immediately
	if err := h.Attach("S", 10, sink); err != nil {
		t.Fatal(err)
	}
	s := h.sessionOf("S")

	frame := makeFrame(0x55, 10_000)
	if !h.Offer("S", frame) {
		t.Fatal("offer must be accepted")
	}
	sink.expectNoWrite(t, 20*time.Millisecond) // blocked on writability

	// Interleave Offer / SetRate / Close; all must terminate.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h.Offer("S", makeFrame(byte(i), 1000))
			_ = h.SetRate("S", 5+float64(i%46))
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	if err := h.Close("S"); err != nil {
		t.Fatal(err)
	}
	<-done

	assertDone(t, s)
	if got := sink.total(); got != 0 {
		t.Fatalf("sink received %d bytes after Close returned", got)
	}
	sink.setWritable(true)
	sink.expectNoWrite(t, 30*time.Millisecond)

	// Close is idempotent.
	if err := h.Close("S"); err != nil {
		t.Fatal(err)
	}
	if err := h.Close("S"); err != nil {
		t.Fatal(err)
	}
	// Late offers are rejected.
	if h.Offer("S", makeFrame(1, 100)) {
		t.Fatal("offer after Close must be rejected")
	}
}

// Close must complete while the sender waits for quota, waking it off the
// old-rate timer, and an Offer accepted just before Close must not produce
// output afterwards.
func TestCloseWhileWaitingQuota(t *testing.T) {
	clock := NewManualClock()
	h := New(WithClock(clock))

	sink := newFakeSink()
	if err := h.Attach("S", 5, sink); err != nil {
		t.Fatal(err)
	}
	s := h.sessionOf("S")

	// Exhaust the burst so the sender parks on the quota timer.
	frame := makeFrame(0x66, 200_000)
	if !h.Offer("S", frame) {
		t.Fatal("offer must be accepted")
	}
	sink.waitTotal(t, burstBytes)
	sink.expectNoWrite(t, 20*time.Millisecond)

	// Interleave SetRate and Close; Close must not hang on the timer.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = h.SetRate("S", 5+float64(i%46))
			h.Offer("S", makeFrame(byte(i), 1000)) // dropped: frame in flight
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	if err := h.Close("S"); err != nil {
		t.Fatal(err)
	}
	<-done

	assertDone(t, s)
	got := sink.total()
	clock.Advance(time.Minute) // quota would flood in if anything survived
	sink.expectNoWrite(t, 30*time.Millisecond)
	if sink.total() != got {
		t.Fatal("output continued after Close")
	}

	if err := h.Close("S"); err != nil {
		t.Fatal(err)
	}
}

// An Offer linearized just before Close may be accepted, but Close still
// prevents any further output.
func TestOfferRacingClose(t *testing.T) {
	clock := NewManualClock()
	h := New(WithClock(clock))

	sink := newFakeSink()
	sink.setWritable(false)
	if err := h.Attach("S", 10, sink); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h.Offer("S", makeFrame(byte(i), 500))
		}(i)
	}
	if err := h.Close("S"); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	sink.setWritable(true)
	sink.expectNoWrite(t, 30*time.Millisecond)
	if got := sink.total(); got != 0 {
		t.Fatalf("got %d bytes after Close, want 0", got)
	}
}
