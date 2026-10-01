package mirrordesk

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeSink is a controllable in-process Sink for tests. It records every
// byte it accepts, can be toggled between writable and blocked, and can
// cap each TryWrite to force partial writes.
type fakeSink struct {
	mu       sync.Mutex
	writable bool
	chunk    int // max bytes accepted per TryWrite; 0 means unlimited
	buf      []byte
	writes   int
	notify   chan struct{} // closed and replaced on every accepted byte
	waitCh   chan struct{} // closed and replaced when becoming writable
}

func newFakeSink() *fakeSink {
	return &fakeSink{
		writable: true,
		notify:   make(chan struct{}),
		waitCh:   make(chan struct{}),
	}
}

func (f *fakeSink) TryWrite(p []byte) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.writable || len(p) == 0 {
		return 0
	}
	n := len(p)
	if f.chunk > 0 && n > f.chunk {
		n = f.chunk
	}
	f.buf = append(f.buf, p[:n]...)
	f.writes++
	close(f.notify)
	f.notify = make(chan struct{})
	return n
}

func (f *fakeSink) WaitWritable(ctx context.Context) error {
	for {
		f.mu.Lock()
		if f.writable {
			f.mu.Unlock()
			return nil
		}
		ch := f.waitCh
		f.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// setWritable toggles writability; becoming writable wakes any waiter.
func (f *fakeSink) setWritable(w bool) {
	f.mu.Lock()
	f.writable = w
	if w {
		close(f.waitCh)
		f.waitCh = make(chan struct{})
	}
	f.mu.Unlock()
}

// setChunk caps how many bytes each TryWrite accepts.
func (f *fakeSink) setChunk(n int) {
	f.mu.Lock()
	f.chunk = n
	f.mu.Unlock()
}

func (f *fakeSink) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.buf)
}

func (f *fakeSink) bytes() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]byte, len(f.buf))
	copy(out, f.buf)
	return out
}

// waitTotal blocks until at least n bytes have been accepted.
func (f *fakeSink) waitTotal(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.mu.Lock()
		got := len(f.buf)
		ch := f.notify
		f.mu.Unlock()
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d bytes, got %d", n, got)
		}
		select {
		case <-ch:
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// expectNoWrite asserts the accepted total stays put for a short window.
func (f *fakeSink) expectNoWrite(t *testing.T, during time.Duration) {
	t.Helper()
	before := f.total()
	time.Sleep(during)
	if got := f.total(); got != before {
		t.Fatalf("expected no further writes, total moved %d -> %d", before, got)
	}
}
