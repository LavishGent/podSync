package storage

import "sync"

// shard is an unexported sharded bucket. All exported logic lives in Store.
type shard[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]Entry[V]
}

func newShard[K comparable, V any]() shard[K, V] {
	return shard[K, V]{data: make(map[K]Entry[V])}
}

// get returns the raw entry for key. The caller must check entry.IsAlive.
func (s *shard[K, V]) get(key K, now int64) (Entry[V], bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	return e, ok
}

// set unconditionally writes entry for key.
func (s *shard[K, V]) set(key K, entry Entry[V]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry
}

// delete marks key as a tombstone (Deleted = true). No-op if key does not exist.
func (s *shard[K, V]) delete(key K, now int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.data[key]; ok {
		e.Deleted = true
		s.data[key] = e
	}
}

// sweep removes entries that are expired AND not tombstones.
// Tombstone entries (Deleted=true) are never removed in MS1-A.
// Returns the number of entries removed.
func (s *shard[K, V]) sweep(now int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	swept := 0
	for k, e := range s.data {
		if !e.Deleted && e.IsExpired(now) {
			delete(s.data, k)
			swept++
		}
	}
	return swept
}
