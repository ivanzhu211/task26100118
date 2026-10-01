package mirrordesk

import (
	"bytes"
	"testing"
	"time"
)

func TestValidation(t *testing.T) {
	hub := NewHub(NewManualClock(time.Unix(1_700_000_000, 0)))
	sink := newTestSink(nil, true, 0)

	if err := hub.Attach("bad-low", 4.9, sink); err != ErrInvalidRate {
		t.Fatalf("rate below range: %v", err)
	}
	if err := hub.Attach("bad-high", 50.1, sink); err != ErrInvalidRate {
		t.Fatalf("rate above range: %v", err)
	}
	if err := hub.Attach("nil-sink", 5, nil); err != ErrNilSink {
		t.Fatalf("nil sink: %v", err)
	}
	if err := hub.Attach("dup", 5, sink); err != nil {
		t.Fatal(err)
	}
	if err := hub.Attach("dup", 5, sink); err != ErrDuplicateID {
		t.Fatalf("duplicate id: %v", err)
	}

	if hub.Offer("unknown", makeFrame(0x01, 10)) {
		t.Fatal("offer to unknown session must be dropped")
	}
	if hub.Offer("dup", make([]byte, MaxFrameBytes+1)) {
		t.Fatal("oversized frame must be dropped")
	}
	if !hub.Offer("dup", make([]byte, MaxFrameBytes)) {
		t.Fatal("max-size frame must be accepted")
	}
	if hub.SetRate("unknown", 10) {
		t.Fatal("SetRate on unknown session")
	}
	if hub.SetRate("dup", 100) {
		t.Fatal("SetRate above range")
	}
	if !hub.SetRate("dup", 50) {
		t.Fatal("SetRate at boundary must apply")
	}
	hub.Close("unknown") // must not panic
	hub.Close("dup")
}

// End-to-end smoke test against the real clock: a frame that fits the
// initial burst flows through without any test-controlled time.
func TestRealClockSmoke(t *testing.T) {
	hub := NewHub(nil) // RealClock
	sink := newTestSink(nil, true, 300)
	if err := hub.Attach("S", 5, sink); err != nil {
		t.Fatal(err)
	}
	payload := makeFrame(0x7E, 10000)
	if !hub.Offer("S", payload) {
		t.Fatal("offer must be accepted")
	}
	want := framed(t, payload)
	waitFor(t, "frame complete", func() bool { return sink.Len() == len(want) })
	if got := sink.Bytes(); !bytes.Equal(got, want) {
		t.Fatal("output corrupted")
	}
	hub.Close("S")
}
