package mirrordesk

import (
	"sync"
	"time"
)

// Clock abstracts time so tests can control credit accumulation.
type Clock interface {
	Now() time.Time
	// NewTimer returns a channel closed once the clock reaches now+d,
	// and a stop function releasing the timer.
	NewTimer(d time.Duration) (<-chan struct{}, func())
}

// RealClock is the production Clock backed by the time package.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) NewTimer(d time.Duration) (<-chan struct{}, func()) {
	if d <= 0 {
		ch := make(chan struct{})
		close(ch)
		return ch, func() {}
	}
	ch := make(chan struct{})
	t := time.AfterFunc(d, func() { close(ch) })
	return ch, func() { t.Stop() }
}

// ManualClock is a test Clock advanced explicitly by the caller.
type ManualClock struct {
	mu      sync.Mutex
	now     time.Time
	nextID  int
	pending map[int]*manualTimer
}

type manualTimer struct {
	deadline time.Time
	ch       chan struct{}
}

func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start, pending: make(map[int]*manualTimer)}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *ManualClock) NewTimer(d time.Duration) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	c.mu.Lock()
	if d <= 0 {
		c.mu.Unlock()
		close(ch)
		return ch, func() {}
	}
	deadline := c.now.Add(d)
	if !deadline.After(c.now) {
		c.mu.Unlock()
		close(ch)
		return ch, func() {}
	}
	id := c.nextID
	c.nextID++
	c.pending[id] = &manualTimer{deadline: deadline, ch: ch}
	c.mu.Unlock()
	stop := func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}
	return ch, stop
}

// Advance moves the clock forward, firing timers whose deadline passed.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var fire []chan struct{}
	for id, t := range c.pending {
		if !t.deadline.After(c.now) {
			fire = append(fire, t.ch)
			delete(c.pending, id)
		}
	}
	c.mu.Unlock()
	for _, ch := range fire {
		close(ch)
	}
}
