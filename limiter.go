package mirrordesk

import (
	"sync"
	"sync/atomic"
	"time"
)

// burstBytes is the token bucket capacity and the initial burst allowance.
const burstBytes = 65536

// limiter is a token bucket with changeable rate. Only successful output
// bytes are charged; rate changes never re-grant burst credit.
//
// Invariant: bytes written in any interval [t1,t2] never exceed
// burstBytes plus the rate integrated over that interval.
type limiter struct {
	mu     sync.Mutex
	clock  Clock
	rate   float64 // bytes per second
	tokens float64 // <= burstBytes at all times
	last   time.Time
	change chan struct{} // closed and replaced on every setRate
	waits  atomic.Int64  // waitForCredit entries, for tests
}

func newLimiter(clock Clock, mbps float64) *limiter {
	return &limiter{
		clock:  clock,
		rate:   mbpsToBytesPerSec(mbps),
		tokens: burstBytes,
		last:   clock.Now(),
		change: make(chan struct{}),
	}
}

func mbpsToBytesPerSec(mbps float64) float64 { return mbps * 1e6 / 8 }

func (l *limiter) refillLocked() {
	now := l.clock.Now()
	if now.After(l.last) {
		l.tokens += l.rate * now.Sub(l.last).Seconds()
		if l.tokens > burstBytes {
			l.tokens = burstBytes
		}
		l.last = now
	}
}

// avail reports how many bytes may be written right now.
func (l *limiter) avail() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refillLocked()
	if l.tokens < 1 {
		return 0
	}
	return int(l.tokens)
}

// consume charges n actually written bytes (n must not exceed a prior avail).
func (l *limiter) consume(n int) {
	l.mu.Lock()
	l.tokens -= float64(n)
	l.mu.Unlock()
}

// setRate changes the rate in Mbps and wakes any waiters. Accumulated
// credit is preserved, so lowering or raising the rate never grants a
// fresh burst.
func (l *limiter) setRate(mbps float64) {
	l.mu.Lock()
	l.refillLocked()
	l.rate = mbpsToBytesPerSec(mbps)
	close(l.change)
	l.change = make(chan struct{})
	l.mu.Unlock()
}

// waitForCredit blocks until enough credit to write need bytes (capped at
// the burst capacity) may be available, the rate changes, or stop is
// closed. It never holds the limiter lock while waiting.
func (l *limiter) waitForCredit(need int, stop <-chan struct{}) {
	l.mu.Lock()
	l.refillLocked()
	target := float64(need)
	if target > burstBytes {
		target = burstBytes
	}
	if l.tokens >= target {
		l.mu.Unlock()
		return
	}
	// Time until the target credit accrues at the current rate.
	d := time.Duration((target - l.tokens) / l.rate * float64(time.Second))
	change := l.change
	l.mu.Unlock()

	l.waits.Add(1)
	timer, cancel := l.clock.NewTimer(d)
	defer cancel()
	select {
	case <-timer:
	case <-change:
	case <-stop:
	}
}
