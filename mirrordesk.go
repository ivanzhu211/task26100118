// Package mirrordesk implements an in-process screen-frame forwarding
// component. It fans immutable JPEG frames out to per-session sinks with
// per-session bandwidth limits, without queueing pending frames.
package mirrordesk

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MaxFrameSize is the largest accepted JPEG payload (2 MiB).
const MaxFrameSize = 2 << 20

// burstBytes is the one-time burst allowance every session starts with.
// Raising or lowering the rate never re-grants it.
const burstBytes = 65536

const (
	minMbps = 5.0
	maxMbps = 50.0
)

// Sink receives forwarded frame bytes for one session. Both methods are
// supplied by the caller; no real transport is performed by this package.
type Sink interface {
	// TryWrite consumes up to len(p) bytes and returns immediately with
	// the number actually accepted. It may accept a prefix (partial write)
	// or return 0 when not currently writable. It must never block.
	TryWrite(p []byte) (n int)
	// WaitWritable blocks until the sink becomes writable or ctx is
	// cancelled. A writable state that appears before or during the call
	// must not be missed.
	WaitWritable(ctx context.Context) error
}

// Clock abstracts time so tests can control quota refills deterministically.
type Clock interface {
	Now() time.Time
	// After returns a channel that fires once the clock has advanced by d,
	// plus a stop function releasing the timer.
	After(d time.Duration) (<-chan time.Time, func())
}

// Hub manages all forwarding sessions. Its four operations are safe for
// concurrent use; distinct sessions never share a lock.
type Hub struct {
	clock Clock

	mu       sync.RWMutex // guards sessions only; never held across session ops
	sessions map[string]*session
}

// Option customizes a Hub.
type Option func(*Hub)

// WithClock overrides the clock (defaults to real time).
func WithClock(c Clock) Option {
	return func(h *Hub) { h.clock = c }
}

// New creates a Hub.
func New(opts ...Option) *Hub {
	h := &Hub{clock: realClock{}, sessions: make(map[string]*session)}
	for _, o := range opts {
		o(h)
	}
	return h
}

func validMbps(mbps float64) bool { return mbps >= minMbps && mbps <= maxMbps }

func (h *Hub) get(id string) *session {
	h.mu.RLock()
	s := h.sessions[id]
	h.mu.RUnlock()
	return s
}

var (
	// ErrSessionExists is returned by Attach when the id is already in use.
	ErrSessionExists = errors.New("mirrordesk: session id already exists")
	// ErrNoSuchSession is returned when the id has no active session.
	ErrNoSuchSession = errors.New("mirrordesk: no such session")
	// ErrRateRange is returned when mbps falls outside [5, 50].
	ErrRateRange = errors.New("mirrordesk: mbps must be within [5, 50]")
)

// Attach establishes a session with an initial limit of 5-50 Mbps.
func (h *Hub) Attach(id string, mbps float64, sink Sink) error {
	if !validMbps(mbps) {
		return ErrRateRange
	}
	if sink == nil {
		return errors.New("mirrordesk: nil sink")
	}
	h.mu.Lock()
	if _, ok := h.sessions[id]; ok {
		h.mu.Unlock()
		return ErrSessionExists
	}
	s := newSession(sink, mbps, h.clock)
	h.sessions[id] = s
	h.mu.Unlock()
	return nil
}

// Offer submits an immutable JPEG frame. It reports whether the frame was
// accepted and never waits for the sink or for bandwidth quota.
func (h *Hub) Offer(id string, frame []byte) bool {
	s := h.get(id)
	if s == nil {
		return false
	}
	return s.offer(frame)
}

// SetRate adjusts the session's limit; it applies to bytes not yet sent.
func (h *Hub) SetRate(id string, mbps float64) error {
	if !validMbps(mbps) {
		return ErrRateRange
	}
	s := h.get(id)
	if s == nil {
		return ErrNoSuchSession
	}
	s.setRate(mbps)
	return nil
}

// Close idempotently closes a session. After Close returns, no further
// bytes are written to its sink and its frame and goroutine are released.
func (h *Hub) Close(id string) error {
	h.mu.Lock()
	s := h.sessions[id]
	delete(h.sessions, id)
	h.mu.Unlock()
	if s == nil {
		return nil
	}
	s.shutdown()
	return nil
}
