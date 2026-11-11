package storage

import (
	"sync"

	"github.com/LavishGent/podsync/internal/version"
)

// shard is a sharded bucket. All exported logic lives in Store.
type shard[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]Entry[V]
}

func newShard[K comparable, V any]() shard[K, V] {
	return shard[K, V]{data: make(map[K]Entry[V])}
}

// get returns the raw entry for a key. The caller must check entry.IsAlive.
func (s *shard[K, V]) get(key K, now int64) (Entry[V], bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	return e, ok
}

// merge applies incoming if key is absent or incoming has a strictly newer
// version than the stored entry, and reports whether it was applied. Every
// write to a shard, including tombstones, goes through here.
func (s *shard[K, V]) merge(key K, incoming Entry[V]) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.data[key]; ok && version.Compare(incoming.Version, existing.Version) <= 0 {
		return false
	}
	s.data[key] = incoming
	return true
}

// snapshot appends every raw entry, tombstones included, to dst.
func (s *shard[K, V]) snapshot(dst []KeyEntry[K, V]) []KeyEntry[K, V] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, e := range s.data {
		dst = append(dst, KeyEntry[K, V]{Key: k, Entry: e})
	}
	return dst
}

// sweep removes tombstones whose DeletedAt+tombstoneTTL has passed and expired
// entries whose ExpiresAt+tombstoneTTL has passed. Expired entries keep their
// version for the same window as tombstones so an older version cannot be
// merged back in. Returns the number of entries removed.
func (s *shard[K, V]) sweep(now, tombstoneTTL int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	swept := 0
	for k, e := range s.data {
		if (e.Deleted && e.DeletedAt+tombstoneTTL <= now) ||
			(!e.Deleted && e.ExpiresAt != 0 && e.ExpiresAt+tombstoneTTL <= now) {
			delete(s.data, k)
			swept++
		}
	}
	return swept
}
