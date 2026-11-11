package storage

import "github.com/LavishGent/podsync/internal/version"

// Entry holds a single stored value along with metadata describing its current state.
//
// An Entry can be described as "deleted", "expired", or "alive".
type Entry[V any] struct {
	// Value is the value an entry represents.
	Value V
	// ExpiresAt is the time an entry expires at in unix nanoseconds. A zero value
	// means no expiration. The type, int64, is chosen over uint64 as to ensure
	// compatibility with the Go time API.
	//
	// ExpiresAt is an absolute time and is replicated unchanged, so each node
	// compares it against its own clock: an entry expires early or late on a
	// peer by that peer's clock skew. Keep TTLs at least seconds long, well
	// above the skew expected between nodes (normally tens of milliseconds
	// under NTP).
	ExpiresAt int64
	// Version orders writes to the key; the highest version wins a merge.
	Version version.Version
	// Deleted is a tombstone marker. The zero value means the entry has not yet
	// been logically deleted.
	Deleted bool
	// DeletedAt is the originating node's clock in unix nanoseconds at the
	// moment of deletion. Merge keeps it unchanged (filling it with the local
	// time only when it is zero), so a tombstone that arrives late through
	// replication or repair has a shorter local retention window. Only the
	// local tombstone sweeper reads it, so clock skew shifts GC timing and
	// nothing else.
	DeletedAt int64
}

// KeyEntry pairs a key with its raw entry, tombstones included.
type KeyEntry[K comparable, V any] struct {
	Key   K
	Entry Entry[V]
}

// IsExpired returns whether the entry has passed its expiration time.
func (e Entry[V]) IsExpired(now int64) bool {
	return e.ExpiresAt != 0 && now >= e.ExpiresAt
}

// IsAlive reports whether the entry is readable: not deleted and not expired.
func (e Entry[V]) IsAlive(now int64) bool {
	return !e.Deleted && !e.IsExpired(now)
}
