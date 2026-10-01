package mirrordesk

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
)

// MinMbps and MaxMbps bound the per-session rate limit.
const (
	MinMbps = 5.0
	MaxMbps = 50.0
)

// MaxFrameBytes is the largest accepted frame payload (2 MiB).
const MaxFrameBytes = 2 << 20

var (
	ErrInvalidRate = errors.New("mirrordesk: mbps out of range")
	ErrDuplicateID = errors.New("mirrordesk: session id already attached")
	ErrNilSink     = errors.New("mirrordesk: nil sink")
)

// Hub owns all sessions. Use NewHub.
type Hub struct {
	mu       sync.Mutex
	clock    Clock
	sessions map[string]*session
}

// NewHub creates a Hub using the given clock (RealClock if nil).
func NewHub(clock Clock) *Hub {
	if clock == nil {
		clock = RealClock{}
	}
	return &Hub{clock: clock, sessions: make(map[string]*session)}
}

// Attach starts a session with the given id, initial rate and sink.
// The initial rate must be within [MinMbps, MaxMbps].
func (h *Hub) Attach(id string, mbps float64, sink Sink) error {
	if mbps < MinMbps || mbps > MaxMbps {
		return ErrInvalidRate
	}
	if sink == nil {
		return ErrNilSink
	}
	s := newSession(h.clock, sink, mbps)

	h.mu.Lock()
	if _, ok := h.sessions[id]; ok {
		h.mu.Unlock()
		return ErrDuplicateID
	}
	h.sessions[id] = s
	h.mu.Unlock()

	go s.run()
	return nil
}

// Offer submits an immutable JPEG frame. It reports whether the frame was
// accepted; it never waits for the sink or for bandwidth credit. A frame
// is dropped when the session is unknown or closed, when a previous frame
// is still being sent, or when the payload exceeds MaxFrameBytes.
func (h *Hub) Offer(id string, frame []byte) bool {
	if len(frame) > MaxFrameBytes {
		return false
	}
	h.mu.Lock()
	s := h.sessions[id]
	h.mu.Unlock()
	if s == nil {
		return false
	}
	return s.offer(frame)
}

// SetRate adjusts the session's rate limit, affecting unsent bytes.
// It reports whether the new rate was applied.
func (h *Hub) SetRate(id string, mbps float64) bool {
	if mbps < MinMbps || mbps > MaxMbps {
		return false
	}
	h.mu.Lock()
	s := h.sessions[id]
	h.mu.Unlock()
	if s == nil {
		return false
	}
	return s.setRate(mbps)
}

// Close idempotently closes the session. After Close returns, no further
// bytes are written to the sink and the session's frame and sender
// goroutine are released.
func (h *Hub) Close(id string) {
	h.mu.Lock()
	s := h.sessions[id]
	h.mu.Unlock()
	if s == nil {
		return
	}
	s.close()
	h.mu.Lock()
	if h.sessions[id] == s {
		delete(h.sessions, id)
	}
	h.mu.Unlock()
}

// SessionCount reports the number of live sessions (for tests/diagnostics).
func (h *Hub) SessionCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.sessions)
}

// session holds the state of one attached sink. At most one frame is
// in flight; the sender goroutine is the only writer to the sink.
type session struct {
	sink Sink
	lim  *limiter

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	out    []byte // header+payload of the in-flight frame, nil when idle
	closed bool

	wake     chan struct{} // cap 1: new frame offered or closed
	closedCh chan struct{} // closed on close
	done     chan struct{} // closed when the sender goroutine exits
}

func newSession(clock Clock, sink Sink, mbps float64) *session {
	ctx, cancel := context.WithCancel(context.Background())
	return &session{
		sink:     sink,
		lim:      newLimiter(clock, mbps),
		ctx:      ctx,
		cancel:   cancel,
		wake:     make(chan struct{}, 1),
		closedCh: make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (s *session) offer(frame []byte) bool {
	buf := make([]byte, 4+len(frame))
	binary.BigEndian.PutUint32(buf, uint32(len(frame)))
	copy(buf[4:], frame)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.out != nil {
		return false
	}
	s.out = buf
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return true
}

func (s *session) setRate(mbps float64) bool {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return false
	}
	s.lim.setRate(mbps)
	return true
}

func (s *session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-s.done
		return
	}
	s.closed = true
	s.mu.Unlock()

	s.cancel()
	close(s.closedCh)
	select {
	case s.wake <- struct{}{}:
	default:
	}
	<-s.done
}

// run is the per-session sender goroutine started by Attach.
func (s *session) run() {
	defer close(s.done)
	for {
		s.mu.Lock()
		for s.out == nil && !s.closed {
			s.mu.Unlock()
			<-s.wake
			s.mu.Lock()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		buf := s.out
		s.mu.Unlock()

		s.send(buf)

		s.mu.Lock()
		s.out = nil
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return
		}
	}
}

// send writes buf (frame header + payload) to the sink, respecting the
// rate limit. It returns early only when the session is closed.
func (s *session) send(buf []byte) {
	off := 0
	for off < len(buf) {
		s.mu.Lock()
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return
		}

		avail := s.lim.avail()
		if avail <= 0 {
			s.lim.waitForCredit(len(buf)-off, s.closedCh)
			continue
		}
		if avail > len(buf)-off {
			avail = len(buf) - off
		}
		n := s.sink.TryWrite(buf[off : off+avail])
		if n > 0 {
			off += n
			s.lim.consume(n)
			continue
		}
		// The sink accepted nothing: wait for writability (or close)
		// instead of busy polling.
		s.sink.WaitWritable(s.ctx)
	}
}
