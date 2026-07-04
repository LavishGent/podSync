package storage

// Entry holds a single stored value with its metadata.
// The Version field is a placeholder; MS1-B will replace it with a proper vector clock type.
type Entry[V any] struct {
	Value     V
	ExpiresAt int64  // Unix nanoseconds; 0 means no expiration
	Deleted   bool   // tombstone marker; true means logically deleted
	Version   uint64 // placeholder for MS1-B versioning
}

// IsExpired reports whether the entry has passed its expiration time.
// An ExpiresAt of 0 never expires.
func (e Entry[V]) IsExpired(now int64) bool {
	return e.ExpiresAt != 0 && now >= e.ExpiresAt
}

// IsAlive reports whether the entry is readable: not deleted and not expired.
func (e Entry[V]) IsAlive(now int64) bool {
	return !e.Deleted && !e.IsExpired(now)
}
