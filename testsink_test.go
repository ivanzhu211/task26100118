package mirrordesk

import (
	"context"
	"sync"
	"testing"
	"time"
)

// writeEvent records one successful TryWrite for rate-limit auditing.
type writeEvent struct {
	at time.Time
	n  int
}

// testSink is a controllable Sink for tests. All state transitions are
// driven explicitly by the test; nothing happens in the background.
type testSink struct {
	mu         sync.Mutex
	clock      *ManualClock // optional, for timestamping writes
	writable   bool
	writableCh chan struct{} // closed exactly while writable
	maxChunk   int           // max bytes accepted per TryWrite; 0 = unlimited
	data       []byte
	events     []writeEvent
	waitCount  int
	onWrite    func(s *testSink) // called after each accepted write, lock-free
}

func newTestSink(clock *ManualClock, writable bool, maxChunk int) *testSink {
	s := &testSink{
		clock:      clock,
		writable:   writable,
		writableCh: make(chan struct{}),
		maxChunk:   maxChunk,
	}
	if writable {
		close(s.writableCh)
	}
	return s
}

func (s *testSink) TryWrite(p []byte) int {
	s.mu.Lock()
	if !s.writable {
		s.mu.Unlock()
		return 0
	}
	n := len(p)
	if s.maxChunk > 0 && n > s.maxChunk {
		n = s.maxChunk
	}
	s.data = append(s.data, p[:n]...)
	if s.clock != nil {
		s.events = append(s.events, writeEvent{at: s.clock.Now(), n: n})
	}
	hook := s.onWrite
	s.mu.Unlock()
	if hook != nil {
		hook(s)
	}
	return n
}

func (s *testSink) WaitWritable(ctx context.Context) {
	s.mu.Lock()
	ch := s.writableCh
	s.waitCount++
	s.mu.Unlock()
	select {
	case <-ch:
	case <-ctx.Done():
	}
}

func (s *testSink) SetWritable(w bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w == s.writable {
		return
	}
	s.writable = w
	if w {
		close(s.writableCh)
	} else {
		s.writableCh = make(chan struct{})
	}
}

func (s *testSink) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

func (s *testSink) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data...)
}

func (s *testSink) Events() []writeEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]writeEvent(nil), s.events...)
}

func (s *testSink) WaitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.waitCount
}

// waitFor polls cond with short sleeps until it holds or fails the test.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
