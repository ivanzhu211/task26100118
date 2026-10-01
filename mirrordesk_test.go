package mirrordesk

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func makeFrame(seed byte, n int) []byte {
	f := make([]byte, n)
	for i := range f {
		f[i] = seed + byte(i)
	}
	return f
}

func framed(payload []byte) []byte {
	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out, uint32(len(payload)))
	copy(out[4:], payload)
	return out
}

// drive advances the manual clock until cond holds, letting quota-waiting
// senders make progress without long real sleeps.
func drive(t *testing.T, c *ManualClock, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("drive: condition not met in time")
		}
		c.Advance(5 * time.Millisecond)
		time.Sleep(time.Millisecond)
	}
}

// A stalls mid-frame and must not queue or disturb B; once A recovers, its
// output must reconstruct the original frame byte-for-byte.
func TestSlowReceiverIsolation(t *testing.T) {
	clock := NewManualClock()
	h := New(WithClock(clock))

	sinkA, sinkB := newFakeSink(), newFakeSink()
	if err := h.Attach("A", 10, sinkA); err != nil {
		t.Fatal(err)
	}
	if err := h.Attach("B", 10, sinkB); err != nil {
		t.Fatal(err)
	}

	frameA := makeFrame(0x11, 100_000)
	if !h.Offer("A", frameA) {
		t.Fatal("first offer to A must be accepted")
	}
	// Let A write a prefix, then freeze it mid-frame.
	sinkA.setChunk(997)
	sinkA.waitTotal(t, 10_000)
	sinkA.setWritable(false)
	frozen := sinkA.total()

	// Every further offer to A must be dropped while its frame is in flight.
	for i := 0; i < 50; i++ {
		if h.Offer("A", makeFrame(0xEE, 5000)) {
			t.Fatalf("offer %d to busy A must be dropped", i)
		}
	}

	// B is unaffected and completes its own frame.
	frameB := makeFrame(0x22, 80_000)
	if !h.Offer("B", frameB) {
		t.Fatal("first offer to B must be accepted")
	}
	drive(t, clock, func() bool { return sinkB.total() == len(frameB)+4 })
	if got := sinkB.bytes(); !bytes.Equal(got, framed(frameB)) {
		t.Fatal("B output does not match its frame")
	}

	// A stays frozen while unwritable.
	sinkA.expectNoWrite(t, 20*time.Millisecond)
	if sinkA.total() != frozen {
		t.Fatal("A advanced while unwritable")
	}

	// Recover A: the rest of the original frame must come out intact.
	sinkA.setWritable(true)
	drive(t, clock, func() bool { return sinkA.total() == len(frameA)+4 })
	if got := sinkA.bytes(); !bytes.Equal(got, framed(frameA)) {
		t.Fatal("A output corrupted or contains dropped-frame bytes")
	}

	if err := h.Close("A"); err != nil {
		t.Fatal(err)
	}
	if err := h.Close("B"); err != nil {
		t.Fatal(err)
	}
}
