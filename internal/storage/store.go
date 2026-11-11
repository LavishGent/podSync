package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LavishGent/podsync/internal/clock"
	"github.com/LavishGent/podsync/internal/version"
)

// StoreOptions are configurations for a Store.
type StoreOptions struct {
	// NumShards is the total number of shard buckets to allocate.
	// The default total is 64.
	NumShards int
	// SweepInterval is the interval at which to sweep entries that are past their TombstoneTTL window.
	SweepInterval time.Duration
	// TombstoneTTL is how long the sweeper keeps a deleted entry after
	// DeletedAt, and an expired entry after ExpiresAt, before removing it. The
	// default is 1h. Until then the entry's version still wins merges, so an
	// older version cannot be repaired back in. A partition longer than
	// TombstoneTTL can resurrect a deleted or expired key: a peer that missed
	// the newer write still holds the older value, and once the local entry is
	// swept, repair copies that value back.
	TombstoneTTL time.Duration
	// Clock is the time source for Store operations. By default, Clock is set to
	// SystemClock.
	Clock clock.Clock
}

// StoreStats summarizes the current state of a Store.
type StoreStats struct {
	// LiveKeys are entries that are not deleted and not expired.
	LiveKeys int
	// ExpiredKeys are entries that are expired but not yet swept.
	ExpiredKeys int
	// Tombstones are entries marked as deleted, that may also be expired.
	Tombstones int
	// TotalEntries are a total raw entry count across all shards.
	TotalEntries int
	// Conflicts counts merges rejected because the stored version was equal
	// or newer.
	Conflicts uint64
}

// Store is a generic sharded in-memory key-value store with TTL and soft-delete support.
type Store[K comparable, V any] struct {
	shards    []shard[K, V]
	numShards int
	clock     clock.Clock
	opts      StoreOptions
	startOnce sync.Once
	conflicts atomic.Uint64
}

// NewStore creates a Store given StoreOptions.
func NewStore[K comparable, V any](opts StoreOptions) *Store[K, V] {
	if opts.TombstoneTTL <= 0 {
		opts.TombstoneTTL = time.Hour
	}
	if opts.NumShards <= 0 {
		opts.NumShards = 64
	}
	shards := make([]shard[K, V], opts.NumShards)
	for i := range shards {
		shards[i] = newShard[K, V]()
	}
	clk := opts.Clock
	if clk == nil {
		clk = clock.SystemClock{}
	}
	return &Store[K, V]{
		shards:    shards,
		numShards: opts.NumShards,
		clock:     clk,
		opts:      opts,
	}
}

// shardFor returns the shard responsible for key using FNV-1a hashing.
// Common types are fed directly as bytes; all others use fmt.Sprintf.
func (s *Store[K, V]) shardFor(key K) *shard[K, V] {
	h := fnv.New32a()
	switch k := any(key).(type) {
	case string:
		h.Write([]byte(k))
	case int:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(k))
		h.Write(b[:])
	case int32:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(k))
		h.Write(b[:])
	case int64:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(k))
		h.Write(b[:])
	case uint:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(k))
		h.Write(b[:])
	case uint32:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], k)
		h.Write(b[:])
	case uint64:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], k)
		h.Write(b[:])
	default:
		fmt.Fprintf(h, "%v", key)
	}
	idx := h.Sum32() % uint32(s.numShards)
	return &s.shards[idx]
}

// Get returns the value for key, or (zero, false) if the key is missing, expired, or deleted.
func (s *Store[K, V]) Get(key K) (V, bool) {
	now := s.clock.Now()
	e, ok := s.shardFor(key).get(key, now)
	if !ok || !e.IsAlive(now) {
		var zero V
		return zero, false
	}
	return e.Value, true
}

// Set stores value under key with the given expiration time (Unix nanoseconds;
// 0 = no expiry) at version v. It reports whether the write was applied; a
// write whose version is not newer than the stored one is rejected.
func (s *Store[K, V]) Set(key K, value V, expiresAt int64, v version.Version) bool {
	return s.Merge(key, Entry[V]{Value: value, ExpiresAt: expiresAt, Version: v})
}

// Delete soft-deletes key by writing a tombstone at version v, even if the key
// is absent, since a peer may hold a live value. It reports whether the
// tombstone was applied.
func (s *Store[K, V]) Delete(key K, v version.Version) bool {
	return s.Merge(key, Entry[V]{Deleted: true, DeletedAt: s.clock.Now(), Version: v})
}

// Merge applies e to key if its version is newer than the stored entry, and
// reports whether it was applied. It is the single write path: Set, Delete,
// replication, hydration, and repair all end here. e is stored unchanged, so
// identical versions yield identical entries on every node.
func (s *Store[K, V]) Merge(key K, e Entry[V]) bool {
	// A tombstone with no DeletedAt would be swept on the next tick; stamp it
	// with the local time so it keeps its version for the full window.
	if e.Deleted && e.DeletedAt == 0 {
		e.DeletedAt = s.clock.Now()
	}
	if s.shardFor(key).merge(key, e) {
		return true
	}
	s.conflicts.Add(1)
	return false
}

// Snapshot returns every entry, tombstones and expired entries included, in
// no particular order.
func (s *Store[K, V]) Snapshot() []KeyEntry[K, V] {
	var out []KeyEntry[K, V]
	for i := range s.shards {
		out = s.shards[i].snapshot(out)
	}
	return out
}

// NumShards returns the number of shard buckets, for paged state transfer.
func (s *Store[K, V]) NumShards() int { return s.numShards }

// SnapshotShard returns every entry in shard i, tombstones and expired entries
// included, in no particular order. An out-of-range i returns nil.
func (s *Store[K, V]) SnapshotShard(i int) []KeyEntry[K, V] {
	if i < 0 || i >= len(s.shards) {
		return nil
	}
	return s.shards[i].snapshot(nil)
}

// sweep removes expired entries and tombstones past TombstoneTTL.
func (s *Store[K, V]) sweep() {
	now := s.clock.Now()
	ttl := int64(s.opts.TombstoneTTL)
	for i := range s.shards {
		s.shards[i].sweep(now, ttl)
	}
}

// Stats returns a point-in-time snapshot of the store's entry counts.
func (s *Store[K, V]) Stats() StoreStats {
	now := s.clock.Now()
	stats := StoreStats{Conflicts: s.conflicts.Load()}
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for _, e := range sh.data {
			stats.TotalEntries++
			switch {
			case e.Deleted:
				stats.Tombstones++
			case e.IsExpired(now):
				stats.ExpiredKeys++
			default:
				stats.LiveKeys++
			}
		}
		sh.mu.RUnlock()
	}
	return stats
}

// Start launches the background TTL sweeper goroutine. If SweepInterval is 0, Start is
// a no-op. Subsequent calls to Start are safe but have no effect — only one sweeper is
// ever started per Store instance. If the sweeper goroutine exits because ctx is
// cancelled, it cannot be restarted; create a new Store to resume sweeping.
func (s *Store[K, V]) Start(ctx context.Context) {
	if s.opts.SweepInterval == 0 {
		return
	}
	s.startOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(s.opts.SweepInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					s.sweep()
				case <-ctx.Done():
					return
				}
			}
		}()
	})
}
