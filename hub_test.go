package mirrordesk

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"
)

func makeFrame(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*31)
	}
	return b
}

func framed(t *testing.T, payload []byte) []byte {
	t.Helper()
	if len(payload) > MaxFrameBytes {
		t.Fatalf("test frame too large: %d", len(payload))
	}
	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out, uint32(len(payload)))
	copy(out[4:], payload)
	return out
}

// A stalls mid-frame; further offers to A are dropped while B completes.
// After A resumes, its output must be the exact original frame.
func TestSlowReceiverDoesNotBlockOthers(t *testing.T) {
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	hub := NewHub(clock)

	const stallAt = 30000
	sinkA := newTestSink(clock, true, 1000)
	var stalled atomic.Bool
	sinkA.onWrite = func(s *testSink) {
		if s.Len() >= stallAt && stalled.CompareAndSwap(false, true) {
			s.SetWritable(false)
		}
	}
	if err := hub.Attach("A", 5, sinkA); err != nil {
		t.Fatalf("attach A: %v", err)
	}

	frameA := makeFrame(0xA5, 60000) // 60004 bytes on the wire, fits the burst
	if !hub.Offer("A", frameA) {
		t.Fatal("first offer to A must be accepted")
	}
	waitFor(t, "A to stall mid-frame", func() bool { return sinkA.WaitCount() >= 1 })
	if got := sinkA.Len(); got < stallAt {
		t.Fatalf("A stalled too early at %d bytes", got)
	}

	// While A is stuck, many offers must be dropped, not queued.
	dropped := 0
	for i := 0; i < 100; i++ {
		if hub.Offer("A", makeFrame(0xEE, 1000+i)) {
			t.Fatalf("offer %d to busy A must be dropped", i)
		}
		dropped++
	}
	if dropped != 100 {
		t.Fatalf("expected 100 dropped offers, got %d", dropped)
	}

	// Session B is unaffected and completes.
	sinkB := newTestSink(clock, true, 0)
	if err := hub.Attach("B", 5, sinkB); err != nil {
		t.Fatalf("attach B: %v", err)
	}
	frameB := makeFrame(0x5A, 5000)
	if !hub.Offer("B", frameB) {
		t.Fatal("offer to B must be accepted")
	}
	waitFor(t, "B to finish", func() bool { return sinkB.Len() == len(frameB)+4 })
	if got := sinkB.Bytes(); !bytes.Equal(got, framed(t, frameB)) {
		t.Fatal("B output corrupted")
	}

	// A must not have made progress while unwritable.
	if got := sinkA.Len(); got > stallAt+1000 {
		t.Fatalf("A advanced while unwritable: %d bytes", got)
	}

	// Resume A: the rest of the original frame, and nothing else, comes out.
	sinkA.SetWritable(true)
	want := framed(t, frameA)
	waitFor(t, "A to finish", func() bool { return sinkA.Len() == len(want) })
	if got := sinkA.Bytes(); !bytes.Equal(got, want) {
		t.Fatal("A output does not byte-match the original frame")
	}

	hub.Close("A")
	hub.Close("B")
	if n := hub.SessionCount(); n != 0 {
		t.Fatalf("sessions left: %d", n)
	}
}

// Fifty concurrently active sessions all make progress independently.
func TestFiftyConcurrentSessions(t *testing.T) {
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	hub := NewHub(clock)

	const n = 50
	sinks := make([]*testSink, n)
	frames := make([][]byte, n)
	for i := 0; i < n; i++ {
		sinks[i] = newTestSink(clock, true, 100+i) // varied partial writes
		id := string(rune('a'+i/26)) + string(rune('a'+i%26))
		if err := hub.Attach(id, 5, sinks[i]); err != nil {
			t.Fatalf("attach %q: %v", id, err)
		}
		frames[i] = makeFrame(byte(i), 30000+100*i)
	}
	if got := hub.SessionCount(); got != n {
		t.Fatalf("expected %d sessions, got %d", n, got)
	}
	for i := 0; i < n; i++ {
		id := string(rune('a'+i/26)) + string(rune('a'+i%26))
		if !hub.Offer(id, frames[i]) {
			t.Fatalf("offer to %q must be accepted", id)
		}
	}
	for i := 0; i < n; i++ {
		want := framed(t, frames[i])
		waitFor(t, "session to finish", func() bool { return sinks[i].Len() == len(want) })
		if got := sinks[i].Bytes(); !bytes.Equal(got, want) {
			t.Fatalf("session %d output corrupted", i)
		}
	}
	for i := 0; i < n; i++ {
		id := string(rune('a'+i/26)) + string(rune('a'+i%26))
		hub.Close(id)
	}
	if got := hub.SessionCount(); got != 0 {
		t.Fatalf("sessions left: %d", got)
	}
}
