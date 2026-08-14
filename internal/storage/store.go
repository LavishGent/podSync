package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/LavishGent/podsync/internal/clock"
)

// StoreOptions are configurations for a Store.
type StoreOptions struct {
	NumShards     int           // defaults to 64
	SweepInterval time.Duration // sweep expired entries
	Clock         clock.Clock   // time source; nil defaults to SystemClock
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
}

// Store is a generic sharded in-memory key-value store with TTL and soft-delete support.
type Store[K comparable, V any] struct {
	shards    []shard[K, V]
	numShards int
	clock     clock.Clock
	opts      StoreOptions
	startOnce sync.Once
}

// NewStore creates a Store with the given options.
// NumShards defaults to 64.
func NewStore[K comparable, V any](opts StoreOptions) *Store[K, V] {
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

// Set stores value under key with the given expiration time (Unix nanoseconds; 0 = no expiry).
func (s *Store[K, V]) Set(key K, value V, expiresAt int64) {
	s.shardFor(key).set(key, Entry[V]{
		Value:     value,
		ExpiresAt: expiresAt,
	})
}

// Delete soft-deletes key by marking it as a tombstone. It does not physically
// remove the entry.
func (s *Store[K, V]) Delete(key K) {
	now := s.clock.Now()
	s.shardFor(key).delete(key, now)
}

// Stats returns a point-in-time snapshot of the store's entry counts.
func (s *Store[K, V]) Stats() StoreStats {
	now := s.clock.Now()
	var stats StoreStats
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
					now := s.clock.Now()
					for i := range s.shards {
						s.shards[i].sweep(now)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	})
}
