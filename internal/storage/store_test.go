package storage

import (
	"context"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	s.Set("hello", "world", 0)
	v, ok := s.Get("hello")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if v != "world" {
		t.Fatalf("got %q, want %q", v, "world")
	}
}

func TestExpiredKey(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	past := time.Now().Add(-time.Second).UnixNano()
	s.Set("expiring", "value", past)
	_, ok := s.Get("expiring")
	if ok {
		t.Fatal("expected expired key to be missing")
	}
}

func TestResurrectionStaleWrite(t *testing.T) {
	// Version conflict resolution is deferred to MS1-B.
	// For now the second Set unconditionally overwrites the first.
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	s.Set("key", "first", 0)
	s.Set("key", "second", 0)
	v, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist after second Set")
	}
	if v != "second" {
		t.Fatalf("got %q, want %q", v, "second")
	}
}

func TestDeleteGet(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	s.Set("key", "value", 0)
	s.Delete("key")
	_, ok := s.Get("key")
	if ok {
		t.Fatal("expected deleted key to be missing")
	}
}

func TestShardDeterministic(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 16})

	// Same key must always map to the same shard pointer.
	for i := 0; i < 100; i++ {
		sh1 := s.shardFor("consistent-key")
		sh2 := s.shardFor("consistent-key")
		if sh1 != sh2 {
			t.Fatal("shardFor returned different shards for the same key")
		}
	}

	// Different keys should spread across more than one shard.
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
		"iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi"}
	seen := map[*shard[string, string]]struct{}{}
	for _, k := range keys {
		seen[s.shardFor(k)] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatal("all keys mapped to same shard — distribution is broken")
	}
}

func TestStats(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	past := time.Now().Add(-time.Second).UnixNano()

	s.Set("live", "v", 0)
	s.Set("expired", "v", past)
	s.Set("tombstone", "v", 0)
	s.Delete("tombstone")

	stats := s.Stats()
	if stats.LiveKeys != 1 {
		t.Errorf("LiveKeys: got %d, want 1", stats.LiveKeys)
	}
	if stats.ExpiredKeys != 1 {
		t.Errorf("ExpiredKeys: got %d, want 1", stats.ExpiredKeys)
	}
	if stats.Tombstones != 1 {
		t.Errorf("Tombstones: got %d, want 1", stats.Tombstones)
	}
	if stats.TotalEntries != 3 {
		t.Errorf("TotalEntries: got %d, want 3", stats.TotalEntries)
	}
}

func TestSweeper(t *testing.T) {
	s := NewStore[string, string](StoreOptions{
		NumShards:     4,
		SweepInterval: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	past := time.Now().Add(-time.Second).UnixNano()
	s.Set("expiring", "v", past)
	s.Set("live", "v", 0)

	time.Sleep(50 * time.Millisecond) // wait for at least one sweep tick

	stats := s.Stats()
	if stats.ExpiredKeys != 0 {
		t.Errorf("expected expired key to be swept, ExpiredKeys=%d", stats.ExpiredKeys)
	}
	if stats.LiveKeys != 1 {
		t.Errorf("expected live key to remain, LiveKeys=%d", stats.LiveKeys)
	}
}
