// Package transport is a minimal request/response transport: one persistent
// connection per peer, one in-flight request per connection, no retries.
// Failure detection belongs to kubelet and recovery from dropped sends to
// repair (T5).
package transport

import (
	"context"
	"errors"

	"github.com/LavishGent/podsync/internal/wire"
)

// Handler serves one request. It returns the response type and message, which
// are framed back to the sender. A returned error closes the connection.
// from is the remote address of the connection (or the sender's address for
// Memory).
type Handler func(ctx context.Context, from string, t wire.MsgType, payload []byte) (wire.MsgType, any, error)

// Transport sends requests to peers and serves requests from them.
type Transport interface {
	// Listen serves h on addr until ctx is cancelled.
	Listen(ctx context.Context, addr string, h Handler) error
	// Send sends msg to addr and returns the raw response frame.
	Send(ctx context.Context, addr string, t wire.MsgType, msg any) (wire.MsgType, []byte, error)
	// Close releases cached peer connections.
	Close() error
}

// Errors common to the transports.
var (
	// ErrSecretTooShort reports a Secret shorter than MinSecretLen.
	ErrSecretTooShort = errors.New("transport: secret must be at least 32 bytes")
	// ErrModeMismatch reports that one side has a secret and the other does not.
	ErrModeMismatch = errors.New("transport: handshake mode mismatch")
	// ErrAuth reports a failed shared-secret handshake.
	ErrAuth = errors.New("transport: handshake authentication failed")
	// ErrBadHello reports a peer that did not speak the PSY1 hello.
	ErrBadHello = errors.New("transport: bad handshake hello")
)

// MinSecretLen is the minimum length of TCP.Secret when set.
const MinSecretLen = 32
