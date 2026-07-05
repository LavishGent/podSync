package storage

import (
	"context"
	"testing"
	"time"

	"github.com/LavishGent/podsync/internal/clock"
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
	// Use a FakeClock so we can set an exact "now" without time.Sleep.
	clk := clock.NewFakeClock(1000) // start at t=1000 ns
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk})

	// Key expires at t=500, which is already in the past relative to clk.Now()=1000.
	s.Set("expiring", "value", 500)
	_, ok := s.Get("expiring")
	if ok {
		t.Fatal("expected expired key to be missing")
	}
}

func TestExpiredKeyBecomesExpired(t *testing.T) {
	// Key starts alive, then time advances past its expiration.
	clk := clock.NewFakeClock(100)
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk})

	s.Set("k", "v", 500) // expires at t=500

	// At t=100, the key should be alive.
	v, ok := s.Get("k")
	if !ok {
		t.Fatal("expected key to be alive at t=100")
	}
	if v != "v" {
		t.Fatalf("got %q, want %q", v, "v")
	}

	// Advance past expiration.
	clk.Set(501)
	_, ok = s.Get("k")
	if ok {
		t.Fatal("expected key to be expired at t=501")
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
	clk := clock.NewFakeClock(1000)
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk})

	s.Set("live", "v", 0)      // no expiry → live
	s.Set("expired", "v", 500) // expires at t=500, now=1000 → expired
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
	// Use FakeClock so the sweeper sees entries as expired without time.Sleep.
	clk := clock.NewFakeClock(1000)
	s := NewStore[string, string](StoreOptions{
		NumShards:     4,
		SweepInterval: 10 * time.Millisecond,
		Clock:         clk,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// "expiring" expires at t=500 which is already past (clk=1000).
	s.Set("expiring", "v", 500)
	s.Set("live", "v", 0)

	// Wait for at least one sweep tick to fire.
	time.Sleep(50 * time.Millisecond)

	stats := s.Stats()
	if stats.ExpiredKeys != 0 {
		t.Errorf("expected expired key to be swept, ExpiredKeys=%d", stats.ExpiredKeys)
	}
	if stats.LiveKeys != 1 {
		t.Errorf("expected live key to remain, LiveKeys=%d", stats.LiveKeys)
	}
}

func TestSweeperDoesNotSweepBeforeExpiry(t *testing.T) {
	// Verify that the sweeper uses the clock, not wall time.
	clk := clock.NewFakeClock(100)
	s := NewStore[string, string](StoreOptions{
		NumShards:     4,
		SweepInterval: 10 * time.Millisecond,
		Clock:         clk,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	s.Set("future", "v", 500) // not expired yet at clk=100

	time.Sleep(50 * time.Millisecond) // let sweeper run

	stats := s.Stats()
	if stats.LiveKeys != 1 {
		t.Errorf("expected key to still be live (clk=100, expires=500), LiveKeys=%d", stats.LiveKeys)
	}

	// Now advance past expiry and let sweeper run again.
	clk.Set(501)
	time.Sleep(50 * time.Millisecond)

	stats = s.Stats()
	if stats.ExpiredKeys != 0 {
		t.Errorf("expected key to be swept after clock advance, ExpiredKeys=%d", stats.ExpiredKeys)
	}
}
