// Package wire defines the messages peers exchange and the framing that
// carries them.
package wire

import (
	"fmt"

	"github.com/LavishGent/podsync/internal/version"
)

// MsgType identifies the message carried by a frame.
type MsgType uint8

// Message types. The numeric values are part of the wire format.
const (
	MsgMutation MsgType = iota + 1
	MsgMutationAck
	MsgStateRequest
	MsgStateResponse
	MsgPing
	MsgPong
)

// String returns the message type's name.
func (t MsgType) String() string {
	switch t {
	case MsgMutation:
		return "Mutation"
	case MsgMutationAck:
		return "MutationAck"
	case MsgStateRequest:
		return "StateRequest"
	case MsgStateResponse:
		return "StateResponse"
	case MsgPing:
		return "Ping"
	case MsgPong:
		return "Pong"
	}
	return fmt.Sprintf("MsgType(%d)", uint8(t))
}

// Entry is the wire form of a storage entry. Key and Value are already
// encoded by the key and value codecs. ExpiresAt is an absolute unix
// nanosecond time (see storage.Entry).
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt int64
	DeletedAt int64
	Deleted   bool
	Version   version.Version
}

// Every message carries Origin so the receiver can Observe the sender's clock
// and log the peer by name.

// Mutation replicates one write or delete.
type Mutation struct {
	Origin string
	Entry  Entry
}

// MutationAck answers a Mutation. Applied reports whether the merge changed
// the receiver's state.
type MutationAck struct {
	Applied bool
}

// StateRequest asks for one shard of the responder's state. Entries carries
// the requester's own entries for that shard (the repair push side) and is
// empty for hydration.
type StateRequest struct {
	Origin    string
	Shard     int
	NumShards int
	Entries   []Entry
}

// StateResponse is one page of state: the responder's entries for Shard. A
// request for a shard the responder does not have gets an empty page.
// NumShards is the responder's shard count.
type StateResponse struct {
	Origin    string
	Shard     int
	NumShards int
	Entries   []Entry
}

// Ping is a liveness probe.
type Ping struct {
	Origin string
}

// Pong answers a Ping and reports the sender's Lamport clock.
type Pong struct {
	Origin string
	Clock  uint64
}
