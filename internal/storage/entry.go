package storage

// Entry holds a single stored value along with metadata describing its current state.
//
// An Entry can be described as "deleted", "expired", or "alive".
type Entry[V any] struct {
	// Value is the value an entry represents.
	Value V
	// ExpiresAt is the time an entry expires at in unix nanoseconds. A zero value
	// means no expiration. The type, int64, is chosen over uint64 as to ensure
	// compatibility with the Go time API.
	ExpiresAt int64
	// Version is a placeholder for MS1-B versioning.
	Version uint64
	// Deleted is a tombstone marker. The zero value means the entry has not yet
	// been logically deleted.
	Deleted bool
}

// IsExpired returns whether the entry has passed its expiration time.
func (e Entry[V]) IsExpired(now int64) bool {
	return e.ExpiresAt != 0 && now >= e.ExpiresAt
}

// IsAlive reports whether the entry is readable: not deleted and not expired.
func (e Entry[V]) IsAlive(now int64) bool {
	return !e.Deleted && !e.IsExpired(now)
}
