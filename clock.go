package mirrordesk

import (
	"sync"
	"time"
)

// realClock implements Clock with the wall clock.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) After(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTimer(d)
	return t.C, func() { t.Stop() }
}

// ManualClock is a Clock driven explicitly by tests. Timers fire only when
// Advance moves the clock past their deadline, so quota waits are
// deterministic and no real sleeping is required.
type ManualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*manualTimer]struct{}
}

type manualTimer struct {
	ch       chan time.Time
	deadline time.Time
	fired    bool
}

// NewManualClock starts a manual clock at an arbitrary fixed instant.
func NewManualClock() *ManualClock {
	return &ManualClock{
		now:    time.Unix(1_700_000_000, 0),
		timers: make(map[*manualTimer]struct{}),
	}
}

// Now returns the current manual time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After registers a timer that fires once Advance moves the clock by d.
func (c *ManualClock) After(d time.Duration) (<-chan time.Time, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &manualTimer{ch: make(chan time.Time, 1), deadline: c.now.Add(d)}
	if d <= 0 {
		t.fired = true
		t.ch <- c.now
	} else {
		c.timers[t] = struct{}{}
	}
	return t.ch, func() {
		c.mu.Lock()
		delete(c.timers, t)
		c.mu.Unlock()
	}
}

// Advance moves the clock forward and fires every timer whose deadline has
// been reached.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	fired := make([]*manualTimer, 0, len(c.timers))
	for t := range c.timers {
		if !t.deadline.After(c.now) {
			t.fired = true
			delete(c.timers, t)
			fired = append(fired, t)
		}
	}
	now := c.now
	c.mu.Unlock()
	for _, t := range fired {
		t.ch <- now
	}
}
