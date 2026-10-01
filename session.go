package mirrordesk

import (
	"context"
	"encoding/binary"
	"sync"
	"time"
)

// session owns one sender goroutine and at most one in-flight frame.
// All mutable fields are guarded by mu; the lock is never held while
// waiting for writability, quota, or goroutine exit.
type session struct {
	sink  Sink
	clock Clock

	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{} // buffered 1: coalesced signal from Offer/SetRate/Close
	done   chan struct{} // closed when the sender goroutine exits

	mu     sync.Mutex
	closed bool
	frame  []byte // 4-byte big-endian length header + payload; nil when idle
	off    int    // bytes of frame already written
	rate   float64
	bucket float64
	last   time.Time
}

func newSession(sink Sink, mbps float64, clock Clock) *session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &session{
		sink:   sink,
		clock:  clock,
		ctx:    ctx,
		cancel: cancel,
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
		rate:   mbps * 1e6 / 8,
		bucket: burstBytes,
		last:   clock.Now(),
	}
	go s.run()
	return s
}

func (s *session) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// offer installs frame as the in-flight frame. It reports acceptance and
// never blocks on the sink or on quota.
func (s *session) offer(frame []byte) bool {
	if len(frame) == 0 || len(frame) > MaxFrameSize {
		return false
	}
	buf := make([]byte, 4+len(frame))
	binary.BigEndian.PutUint32(buf, uint32(len(frame)))
	copy(buf[4:], frame)

	s.mu.Lock()
	if s.closed || s.frame != nil {
		s.mu.Unlock()
		return false
	}
	s.frame = buf
	s.off = 0
	s.mu.Unlock()
	s.signal()
	return true
}

// setRate changes the bandwidth limit for bytes not yet sent and wakes a
// sender that may be waiting out the old rate.
func (s *session) setRate(mbps float64) {
	s.mu.Lock()
	s.refillLocked(s.clock.Now())
	s.rate = mbps * 1e6 / 8
	s.mu.Unlock()
	s.signal()
}

// shutdown stops the sender and waits for it to exit. The lock is not held
// while waiting. After shutdown returns, no bytes reach the sink.
func (s *session) shutdown() {
	s.mu.Lock()
	already := s.closed
	s.closed = true
	s.frame = nil
	s.mu.Unlock()
	if already {
		return
	}
	s.cancel()
	s.signal()
	<-s.done
}

// refillLocked folds elapsed time into the token bucket, capped at the
// one-time burst allowance.
func (s *session) refillLocked(now time.Time) {
	if elapsed := now.Sub(s.last); elapsed > 0 {
		s.bucket += s.rate * elapsed.Seconds()
		if s.bucket > burstBytes {
			s.bucket = burstBytes
		}
		s.last = now
	}
}

func (s *session) run() {
	defer close(s.done)
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		if s.frame == nil {
			s.mu.Unlock()
			s.waitForWork()
			continue
		}
		s.refillLocked(s.clock.Now())
		allow := int64(s.bucket)
		if allow <= 0 {
			deficit := 1 - s.bucket
			wait := time.Duration(deficit / s.rate * float64(time.Second))
			s.mu.Unlock()
			s.waitForQuota(wait)
			continue
		}
		remaining := int64(len(s.frame) - s.off)
		if allow > remaining {
			allow = remaining
		}
		p := s.frame[s.off : s.off+int(allow)]
		// TryWrite is contractually non-blocking, so holding mu here
		// serializes it against Close without ever waiting on the sink.
		n := s.sink.TryWrite(p)
		if n < 0 || n > len(p) {
			n = 0
		}
		s.bucket -= float64(n)
		s.off += n
		if s.off == len(s.frame) {
			s.frame = nil
			s.off = 0
		}
		partial := n < len(p)
		s.mu.Unlock()
		if partial {
			// The sink could not take everything right now; block
			// outside the lock until it reports writable or Close
			// cancels the context.
			s.sink.WaitWritable(s.ctx)
		}
	}
}

func (s *session) waitForWork() {
	select {
	case <-s.wake:
	case <-s.ctx.Done():
	}
}

func (s *session) waitForQuota(d time.Duration) {
	timer, stop := s.clock.After(d)
	defer stop()
	select {
	case <-timer:
	case <-s.wake:
	case <-s.ctx.Done():
	}
}
