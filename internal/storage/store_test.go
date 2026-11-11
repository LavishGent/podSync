package storage

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/LavishGent/podsync/internal/clock"
	"github.com/LavishGent/podsync/internal/version"
)

func ver(c uint64, n string) version.Version { return version.Version{Clock: c, NodeID: n} }

// waitFor polls cond until it holds or a deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSetGet(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	s.Set("hello", "world", 0, ver(1, "a"))
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
	s.Set("expiring", "value", 500, ver(1, "a"))
	_, ok := s.Get("expiring")
	if ok {
		t.Fatal("expected expired key to be missing")
	}
}

func TestExpiredKeyBecomesExpired(t *testing.T) {
	// Key starts alive, then time advances past its expiration.
	clk := clock.NewFakeClock(100)
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk})

	s.Set("k", "v", 500, ver(1, "a")) // expires at t=500

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
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	s.Set("key", "first", 0, ver(1, "a"))
	if !s.Delete("key", ver(5, "a")) {
		t.Fatal("delete with newer version should apply")
	}
	if s.Set("key", "stale", 0, ver(3, "b")) {
		t.Fatal("stale Set must be rejected by the tombstone")
	}
	if _, ok := s.Get("key"); ok {
		t.Fatal("stale write resurrected a deleted key")
	}
}

func TestNewerSetReplacesTombstone(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	s.Delete("key", ver(5, "a"))
	if !s.Set("key", "fresh", 0, ver(6, "b")) {
		t.Fatal("newer Set should replace tombstone")
	}
	if v, ok := s.Get("key"); !ok || v != "fresh" {
		t.Fatalf("got %q, %v", v, ok)
	}
}

func TestDeleteAbsentKeyWritesTombstone(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	if !s.Delete("ghost", ver(2, "a")) {
		t.Fatal("delete of absent key should apply")
	}
	if got := s.Stats().Tombstones; got != 1 {
		t.Fatalf("Tombstones = %d, want 1", got)
	}
	if s.Set("ghost", "v", 0, ver(1, "b")) {
		t.Fatal("older Set must lose to the tombstone")
	}
}

func TestIdenticalVersionIsNoop(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	s.Set("k", "first", 0, ver(1, "a"))
	if s.Set("k", "second", 0, ver(1, "a")) {
		t.Fatal("identical version must be a no-op")
	}
	if v, _ := s.Get("k"); v != "first" {
		t.Fatalf("got %q, want first", v)
	}
}

func TestMergeKeepsIncomingTimestamps(t *testing.T) {
	clk := clock.NewFakeClock(9_000)
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk})
	in := Entry[string]{Value: "v", ExpiresAt: 50_000, Version: ver(3, "x"), Deleted: true, DeletedAt: 1234}
	if !s.Merge("k", in) {
		t.Fatal("merge should apply")
	}
	snap := s.Snapshot()
	if len(snap) != 1 || snap[0].Entry != in {
		t.Fatalf("snapshot %+v, want entry %+v unchanged", snap, in)
	}
}

func TestDeleteSetsDeletedAtFromClock(t *testing.T) {
	clk := clock.NewFakeClock(777)
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk})
	s.Delete("k", ver(1, "a"))
	if got := s.Snapshot()[0].Entry.DeletedAt; got != 777 {
		t.Fatalf("DeletedAt = %d, want 777", got)
	}
}

func TestConflictsCounted(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	s.Set("k", "v", 0, ver(5, "a"))
	s.Set("k", "old", 0, ver(4, "a"))
	s.Set("k", "same", 0, ver(5, "a"))
	s.Delete("k", ver(1, "a"))
	if got := s.Stats().Conflicts; got != 3 {
		t.Fatalf("Conflicts = %d, want 3", got)
	}
}

func TestSnapshotIncludesTombstones(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	s.Set("a", "1", 0, ver(1, "n"))
	s.Delete("b", ver(2, "n"))
	if got := len(s.Snapshot()); got != 2 {
		t.Fatalf("Snapshot len = %d, want 2", got)
	}
}

type mutation struct {
	key   string
	entry Entry[string]
}

func TestConvergence(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	rng := rand.New(rand.NewSource(42))
	muts := make([]mutation, 200)
	for i := range muts {
		e := Entry[string]{
			Value:   fmt.Sprintf("v%d", i),
			Version: ver(uint64(rng.Intn(40)+1), nodes[rng.Intn(len(nodes))]),
		}
		if rng.Intn(4) == 0 {
			e.Deleted, e.DeletedAt = true, int64(e.Version.Clock)
			e.Value = ""
		}
		muts[i] = mutation{key: fmt.Sprintf("k%d", rng.Intn(15)), entry: e}
	}
	// Distinct mutations must not share a version for the same key, or the
	// "same version, same entry" precondition of convergence is violated.
	seen := map[string]bool{}
	uniq := muts[:0]
	for _, m := range muts {
		id := fmt.Sprintf("%s/%v", m.key, m.entry.Version)
		if !seen[id] {
			seen[id] = true
			uniq = append(uniq, m)
		}
	}

	apply := func(seed int64) []KeyEntry[string, string] {
		order := append([]mutation(nil), uniq...)
		rand.New(rand.NewSource(seed)).Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		s := NewStore[string, string](StoreOptions{NumShards: 4})
		for _, m := range order {
			s.Merge(m.key, m.entry)
		}
		snap := s.Snapshot()
		sort.Slice(snap, func(i, j int) bool { return snap[i].Key < snap[j].Key })
		return snap
	}
	a, b := apply(1), apply(2)
	if len(a) == 0 || !reflect.DeepEqual(a, b) {
		t.Fatalf("stores diverged:\n%+v\n%+v", a, b)
	}
}

func TestTombstoneSweep(t *testing.T) {
	clk := clock.NewFakeClock(1_000)
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk, TombstoneTTL: time.Hour})
	ttl := int64(time.Hour)
	s.Delete("old", ver(1, "a"))
	clk.Set(1_000 + ttl/2)
	s.Delete("young", ver(2, "a"))
	s.Set("live", "v", 0, ver(3, "a"))

	clk.Set(1_000 + ttl) // "old" is exactly at its deadline, "young" is not
	s.sweep()
	snap := s.Snapshot()
	got := map[string]bool{}
	for _, ke := range snap {
		got[ke.Key] = true
	}
	if got["old"] || !got["young"] || !got["live"] {
		t.Fatalf("after sweep have %v", got)
	}
}

func TestExpiredEntryKeepsVersionForTombstoneWindow(t *testing.T) {
	clk := clock.NewFakeClock(1_000)
	ttl := int64(time.Hour)
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk, TombstoneTTL: time.Hour})

	s.Set("k", "new", 2_000, ver(5, "a")) // expires at t=2000
	clk.Set(2_000)
	if _, ok := s.Get("k"); ok {
		t.Fatal("expired entry must read as absent")
	}

	// Inside the window a repair delivers an older, non-expiring version.
	clk.Set(2_000 + ttl - 1)
	s.sweep()
	if s.Merge("k", Entry[string]{Value: "old", Version: ver(3, "b")}) {
		t.Fatal("older version accepted while the expired entry is within the window")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("older version resurrected an expired key")
	}
	if got := len(s.Snapshot()); got != 1 {
		t.Fatalf("Snapshot should include the expired entry, len=%d", got)
	}

	clk.Set(2_000 + ttl)
	s.sweep()
	if got := len(s.Snapshot()); got != 0 {
		t.Fatalf("expired entry not swept after the window, len=%d", got)
	}
}

func TestTombstoneTTLDefault(t *testing.T) {
	s := NewStore[string, string](StoreOptions{})
	if s.opts.TombstoneTTL != time.Hour {
		t.Fatalf("default TombstoneTTL = %v", s.opts.TombstoneTTL)
	}
}

func TestDeleteGet(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 4})
	s.Set("key", "value", 0, ver(1, "a"))
	s.Delete("key", ver(2, "a"))
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

	s.Set("live", "v", 0, ver(1, "a"))      // no expiry → live
	s.Set("expired", "v", 500, ver(1, "a")) // expires at t=500, now=1000 → expired
	s.Set("tombstone", "v", 0, ver(1, "a"))
	s.Delete("tombstone", ver(2, "a"))

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
		TombstoneTTL:  100, // ns
		Clock:         clk,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// "expiring" expired at t=500; 500+100 <= 1000, so it is past the window.
	s.Set("expiring", "v", 500, ver(1, "a"))
	s.Set("live", "v", 0, ver(1, "a"))

	waitFor(t, "expired key to be swept", func() bool { return s.Stats().ExpiredKeys == 0 })

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
		TombstoneTTL:  100, // ns
		Clock:         clk,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	s.Set("future", "v", 500, ver(1, "a")) // not expired yet at clk=100

	// Absence of a sweep cannot be polled for; give several ticks to misbehave.
	time.Sleep(50 * time.Millisecond)

	stats := s.Stats()
	if stats.LiveKeys != 1 {
		t.Errorf("expected key to still be live (clk=100, expires=500), LiveKeys=%d", stats.LiveKeys)
	}

	// Expired but still inside the tombstone window: kept.
	clk.Set(550)
	time.Sleep(50 * time.Millisecond)
	if got := s.Stats().TotalEntries; got != 1 {
		t.Errorf("expired entry swept inside the window, TotalEntries=%d", got)
	}

	// Past ExpiresAt + TombstoneTTL: swept.
	clk.Set(600)
	waitFor(t, "key to be swept after clock advance", func() bool { return s.Stats().TotalEntries == 0 })
}

func TestSweepNeverOverflows(t *testing.T) {
	clk := clock.NewFakeClock(1_000_000)
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk, TombstoneTTL: time.Hour})
	s.Set("forever", "v", math.MaxInt64, ver(1, "a"))
	s.sweep()
	if got := len(s.Snapshot()); got != 1 {
		t.Fatalf("entry with ExpiresAt=MaxInt64 swept, len=%d", got)
	}
}

func TestSweepHugeTombstoneTTLKeepsFreshTombstone(t *testing.T) {
	clk := clock.NewFakeClock(1_000_000)
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk, TombstoneTTL: math.MaxInt64})
	s.Delete("k", ver(1, "a"))
	s.Set("e", "v", 500, ver(1, "a"))
	s.sweep()
	if got := len(s.Snapshot()); got != 2 {
		t.Fatalf("entries swept with TombstoneTTL=MaxInt64, len=%d", got)
	}
}

func TestMergeSameTombstoneIdenticalAcrossStores(t *testing.T) {
	in := Entry[string]{Deleted: true, DeletedAt: 4_242, Version: ver(2, "a")}
	var snaps [2][]KeyEntry[string, string]
	for i, now := range []int64{5_000, 9_000_000} {
		s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clock.NewFakeClock(now)})
		if !s.Merge("k", in) {
			t.Fatal("merge should apply")
		}
		snaps[i] = s.Snapshot()
	}
	if !reflect.DeepEqual(snaps[0], snaps[1]) || snaps[0][0].Entry != in {
		t.Fatalf("same version produced different entries: %+v vs %+v", snaps[0], snaps[1])
	}
}

func TestSweepSkippedWhenCutoffUnderflows(t *testing.T) {
	clk := clock.NewFakeClock(math.MinInt64 + 1)
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk, TombstoneTTL: 100})
	s.Merge("tomb", Entry[string]{Deleted: true, DeletedAt: math.MinInt64, Version: ver(1, "a")})
	s.Merge("exp", Entry[string]{Value: "v", ExpiresAt: math.MinInt64, Version: ver(1, "a")})
	s.sweep()
	if got := len(s.Snapshot()); got != 2 {
		t.Fatalf("sweep removed entries despite cutoff underflow, len=%d", got)
	}
}

func TestConcurrentMergeConverges(t *testing.T) {
	clk := clock.NewFakeClock(1_000)
	s := NewStore[string, string](StoreOptions{NumShards: 4, Clock: clk, TombstoneTTL: time.Hour})
	const workers, per = 8, 500
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		best version.Version
	)
	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.Snapshot()
				s.sweep()
			}
		}
	}()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			node := fmt.Sprintf("n%d", w)
			for i := 0; i < per; i++ {
				v := ver(uint64(rng.Intn(10_000)+1), node)
				switch rng.Intn(3) {
				case 0:
					s.Delete("k", v)
				default:
					s.Set("k", "v", 0, v)
				}
				mu.Lock()
				if version.Compare(v, best) > 0 {
					best = v
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	bg.Wait()
	snap := s.Snapshot()
	if len(snap) != 1 || snap[0].Entry.Version != best {
		t.Fatalf("stored %+v, want version %v", snap, best)
	}
}

func TestSnapshotShardUnion(t *testing.T) {
	s := NewStore[string, string](StoreOptions{NumShards: 8})
	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("k%d", i), "v", 0, ver(uint64(i+1), "a"))
	}
	s.Delete("gone", ver(500, "a"))
	if s.NumShards() != 8 {
		t.Fatalf("NumShards = %d", s.NumShards())
	}
	var union []KeyEntry[string, string]
	for i := 0; i < s.NumShards(); i++ {
		union = append(union, s.SnapshotShard(i)...)
	}
	all := s.Snapshot()
	byKey := func(x []KeyEntry[string, string]) {
		sort.Slice(x, func(i, j int) bool { return x[i].Key < x[j].Key })
	}
	byKey(union)
	byKey(all)
	if len(all) != 101 || !reflect.DeepEqual(union, all) {
		t.Fatalf("union (%d) != snapshot (%d)", len(union), len(all))
	}
	if s.SnapshotShard(-1) != nil || s.SnapshotShard(8) != nil {
		t.Fatal("out-of-range shard must return nil")
	}
}
