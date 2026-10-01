package mirrordesk

import "context"

// Sink is the caller-provided in-process receiver for one session.
//
// TryWrite consumes up to len(p) bytes and returns the number actually
// accepted; it must return immediately and may accept a prefix (including 0).
// WaitWritable blocks until TryWrite might accept bytes again or ctx is
// cancelled; a writable state that appeared before or during the call must
// not be missed.
type Sink interface {
	TryWrite(p []byte) (n int)
	WaitWritable(ctx context.Context)
}
