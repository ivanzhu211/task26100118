package mirrordesk

import (
	"bytes"
	"testing"
	"time"
)

// The sink alternates partial writes and zero-write stalls while the rate
// is lowered and raised. The byte bound must hold across every interval,
// unsent bytes must not be charged, and raising the rate must promptly
// resume progress.
func TestRateLimitAcrossSetRate(t *testing.T) {
	clock := NewManualClock()
	h := New(WithClock(clock))

	sink := newFakeSink()
	sink.setChunk(300) // force partial TryWrites
	if err := h.Attach("S", 50, sink); err != nil {
		t.Fatal(err)
	}

	frame := makeFrame(0x33, 200_000)
	if !h.Offer("S", frame) {
		t.Fatal("offer must be accepted")
	}

	// Step 1: only the one-time 65536-byte burst flows; no time has
	// elapsed on the manual clock, so nothing refills.
	sink.waitTotal(t, burstBytes)
	sink.expectNoWrite(t, 20*time.Millisecond)
	if got := sink.total(); got != burstBytes {
		t.Fatalf("burst phase: got %d bytes, want exactly %d", got, burstBytes)
	}

	// Step 2: lower to 5 Mbps (625000 B/s), advance 20ms -> +12500 bytes.
	if err := h.SetRate("S", 5); err != nil {
		t.Fatal(err)
	}
	clock.Advance(20 * time.Millisecond)
	want := burstBytes + 12_500
	sink.waitTotal(t, want)
	sink.expectNoWrite(t, 20*time.Millisecond)
	if got := sink.total(); got != want {
		t.Fatalf("after lowering rate: got %d bytes, want %d", got, want)
	}

	// Step 3: stall the sink; quota accrues but no byte is charged while
	// nothing is accepted.
	sink.setWritable(false)
	clock.Advance(20 * time.Millisecond) // +12500 quota at 5 Mbps, undeliverable
	sink.expectNoWrite(t, 20*time.Millisecond)
	if got := sink.total(); got != want {
		t.Fatalf("unwritable sink must not receive bytes: got %d, want %d", got, want)
	}

	// Step 4: raise to 50 Mbps (6.25 MB/s) and advance 8ms -> +50000.
	// Together with the 12500 quota accrued during the stall the bucket
	// holds 62500, still under the 65536 burst ceiling.
	if err := h.SetRate("S", 50); err != nil {
		t.Fatal(err)
	}
	sink.setWritable(true)
	clock.Advance(8 * time.Millisecond)
	want += 62_500
	sink.waitTotal(t, want) // progress resumes promptly after the raise
	sink.expectNoWrite(t, 20*time.Millisecond)
	if got := sink.total(); got != want {
		t.Fatalf("after raising rate: got %d bytes, want %d", got, want)
	}

	// Step 5: drain the rest; the frame must arrive intact end to end.
	drive(t, clock, func() bool { return sink.total() == len(frame)+4 })
	if got := sink.bytes(); !bytes.Equal(got, framed(frame)) {
		t.Fatal("final output does not reconstruct the frame")
	}

	if err := h.Close("S"); err != nil {
		t.Fatal(err)
	}
}

// SetRate must not re-grant burst allowance: after the initial burst is
// spent, changing the limit alone must not release any new bytes.
func TestSetRateDoesNotRegrantBurst(t *testing.T) {
	clock := NewManualClock()
	h := New(WithClock(clock))

	sink := newFakeSink()
	if err := h.Attach("S", 50, sink); err != nil {
		t.Fatal(err)
	}
	frame := makeFrame(0x44, 150_000)
	if !h.Offer("S", frame) {
		t.Fatal("offer must be accepted")
	}
	sink.waitTotal(t, burstBytes)
	sink.expectNoWrite(t, 20*time.Millisecond)

	for _, mbps := range []float64{5, 50, 5, 50} {
		if err := h.SetRate("S", mbps); err != nil {
			t.Fatal(err)
		}
	}
	sink.expectNoWrite(t, 30*time.Millisecond)
	if got := sink.total(); got != burstBytes {
		t.Fatalf("SetRate re-granted burst: got %d bytes, want %d", got, burstBytes)
	}

	if err := h.Close("S"); err != nil {
		t.Fatal(err)
	}
}
