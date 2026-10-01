package mirrordesk

import (
	"bytes"
	"fmt"
	"testing"
)

// Fifty concurrent sessions each deliver their frame independently.
func TestFiftyConcurrentSessions(t *testing.T) {
	clock := NewManualClock()
	h := New(WithClock(clock))

	const n = 50
	sinks := make([]*fakeSink, n)
	frames := make([][]byte, n)
	for i := 0; i < n; i++ {
		sinks[i] = newFakeSink()
		sinks[i].setChunk(1000 + i) // varied partial-write sizes
		if err := h.Attach(fmt.Sprintf("s%02d", i), 5+float64(i%46), sinks[i]); err != nil {
			t.Fatal(err)
		}
		frames[i] = makeFrame(byte(i), 30_000+i*100)
		if !h.Offer(fmt.Sprintf("s%02d", i), frames[i]) {
			t.Fatalf("offer to session %d must be accepted", i)
		}
	}

	for i := 0; i < n; i++ {
		i := i
		drive(t, clock, func() bool { return sinks[i].total() == len(frames[i])+4 })
		if got := sinks[i].bytes(); !bytes.Equal(got, framed(frames[i])) {
			t.Fatalf("session %d output mismatch", i)
		}
	}
	for i := 0; i < n; i++ {
		if err := h.Close(fmt.Sprintf("s%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
}

// Basic argument and lifecycle validation.
func TestValidation(t *testing.T) {
	h := New(WithClock(NewManualClock()))
	sink := newFakeSink()

	if err := h.Attach("bad", 4, sink); err != ErrRateRange {
		t.Fatalf("want ErrRateRange, got %v", err)
	}
	if err := h.Attach("bad", 51, sink); err != ErrRateRange {
		t.Fatalf("want ErrRateRange, got %v", err)
	}
	if err := h.Attach("x", 10, sink); err != nil {
		t.Fatal(err)
	}
	if err := h.Attach("x", 10, sink); err != ErrSessionExists {
		t.Fatalf("want ErrSessionExists, got %v", err)
	}
	if err := h.SetRate("x", 100); err != ErrRateRange {
		t.Fatalf("want ErrRateRange, got %v", err)
	}
	if err := h.SetRate("ghost", 10); err != ErrNoSuchSession {
		t.Fatalf("want ErrNoSuchSession, got %v", err)
	}
	if h.Offer("ghost", makeFrame(1, 10)) {
		t.Fatal("offer to unknown session must be dropped")
	}
	if h.Offer("x", makeFrame(1, MaxFrameSize+1)) {
		t.Fatal("oversized frame must be dropped")
	}
	if h.Offer("x", nil) {
		t.Fatal("empty frame must be dropped")
	}
	if !h.Offer("x", makeFrame(1, MaxFrameSize)) {
		t.Fatal("max-size frame must be accepted")
	}
	if err := h.Close("x"); err != nil {
		t.Fatal(err)
	}
}
