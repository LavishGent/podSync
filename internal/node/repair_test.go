package node

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LavishGent/podsync/internal/clock"
	"github.com/LavishGent/podsync/internal/storage"
	"github.com/LavishGent/podsync/internal/transport"
	"github.com/LavishGent/podsync/internal/version"
	"github.com/LavishGent/podsync/internal/wire"
)

const testRepair = 50 * time.Millisecond

func repairCfg(c *Config[string, string]) {
	c.RepairInterval = testRepair
	c.FirstRepairDelay = 10 * time.Millisecond
}

// noFanout detaches every peer queue so only repair can move data.
func noFanout(ns ...*N) {
	for _, n := range ns {
		n.mu.Lock()
		for addr, q := range n.queues {
			delete(n.queues, addr)
			q.stop()
		}
		n.mu.Unlock()
	}
}

func TestRepairConvergesAfterPartition(t *testing.T) {
	net, ns := cluster(t, 2, repairCfg)
	a, b := ns[0], ns[1]
	net.Partition("n0:7946", "n1:7946")
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = a.Set(ctx, fmt.Sprintf("a-%d", i), "v", 0)
		_ = b.Set(ctx, fmt.Sprintf("b-%d", i), "v", 0)
	}
	net.Heal("n0:7946", "n1:7946")
	eventually(t, 2*testRepair+time.Second, "convergence", func() bool {
		return len(state(a)) == 400 && equalState(a, b)
	})
}

func TestRepairAloneRecoversDroppedMutations(t *testing.T) {
	net := transport.NewNetwork()
	var cnt atomic.Int64
	net.Drop(func(_, _ string, mt wire.MsgType) bool {
		return mt == wire.MsgMutation && cnt.Add(1)%3 == 0
	})
	all := addrs("n", 3)
	var ns []*N
	for _, a := range all {
		ns = append(ns, build(t, net, a, all, repairCfg))
	}
	startAll(t, ns...)
	for i, n := range ns {
		for j := 0; j < 300; j++ {
			_ = n.Set(context.Background(), fmt.Sprintf("k-%d-%d", i, j), "v", 0)
		}
	}
	eventually(t, 10*time.Second, "convergence via repair", func() bool {
		return len(state(ns[0])) == 900 && equalState(ns...)
	})
}

// tombstoneSetup: Set on A replicated to B, partition, Delete on A.
func tombstoneSetup(t *testing.T, fc *clock.FakeClock) (*transport.Network, *N, *N) {
	t.Helper()
	// Repair is effectively off until the test heals and ticks by hand.
	net, ns := cluster(t, 2, func(c *Config[string, string]) {
		c.RepairInterval = time.Hour
		c.FirstRepairDelay = time.Hour
		c.Clock = fc
		c.Store = storage.StoreOptions{TombstoneTTL: time.Minute, SweepInterval: 5 * time.Millisecond}
	})
	a, b := ns[0], ns[1]
	ctx := context.Background()
	_ = a.Set(ctx, "k", "old", 0)
	eventually(t, time.Second, "B has k", func() bool { _, ok := b.Get("k"); return ok })
	net.Partition("n0:7946", "n1:7946")
	_ = a.Delete(ctx, "k")
	if v, ok := b.Get("k"); !ok || v != "old" {
		t.Fatalf("B should still hold old value, got %q %v", v, ok)
	}
	return net, a, b
}

func TestRepairTombstoneSurvivesWithinTTL(t *testing.T) {
	fc := clock.NewFakeClock(time.Now().UnixNano())
	net, a, b := tombstoneSetup(t, fc)
	net.Heal("n0:7946", "n1:7946")
	b.repairTick(context.Background())
	a.repairTick(context.Background())
	if _, ok := b.Get("k"); ok {
		t.Fatal("B still serves deleted key after repair")
	}
	if _, ok := a.Get("k"); ok {
		t.Fatal("A resurrected deleted key")
	}
	if b.Stats().Tombstones != 1 {
		t.Fatalf("B tombstones = %d, want 1", b.Stats().Tombstones)
	}
}

func TestRepairResurrectsDeletedKeyPastTombstoneTTL(t *testing.T) {
	fc := clock.NewFakeClock(time.Now().UnixNano())
	net, a, b := tombstoneSetup(t, fc)
	fc.Advance(int64(2 * time.Minute))
	eventually(t, time.Second, "A's tombstone swept", func() bool { return a.Stats().TotalEntries == 0 })
	net.Heal("n0:7946", "n1:7946")
	a.repairTick(context.Background())
	if v, ok := a.Get("k"); !ok || v != "old" {
		t.Fatalf("documented resurrection did not occur: %q %v", v, ok)
	}
	_ = b
}

func TestRepairSymmetricOneTick(t *testing.T) {
	_, ns := cluster(t, 2, func(c *Config[string, string]) {
		c.RepairInterval = time.Hour
		c.FirstRepairDelay = time.Hour
	})
	a, b := ns[0], ns[1]
	noFanout(a, b)
	for i := 0; i < 10; i++ {
		_ = a.Set(context.Background(), fmt.Sprintf("a%d", i), "v", 0)
		_ = b.Set(context.Background(), fmt.Sprintf("b%d", i), "v", 0)
	}
	a.repairTick(context.Background())
	if len(state(a)) != 20 || !equalState(a, b) {
		t.Fatalf("not converged after one tick: a=%d b=%d", len(state(a)), len(state(b)))
	}
}

func TestRepairOverEnvelopeWarnsOncePerTick(t *testing.T) {
	buf := &lockedBuf{}
	mod := func(c *Config[string, string]) {
		c.RepairInterval = time.Hour
		c.FirstRepairDelay = time.Hour
		c.MaxStateEntries = 20
		c.Logger = slog.New(slog.NewTextHandler(buf, nil))
	}
	_, ns := cluster(t, 2, mod)
	a, b := ns[0], ns[1]
	noFanout(a, b)
	for i := 0; i < 21; i++ {
		_ = b.Set(context.Background(), fmt.Sprintf("k%d", i), "v", 0)
	}
	a.repairTick(context.Background())
	if got := strings.Count(buf.String(), "operating envelope"); got != 1 {
		t.Fatalf("envelope warnings in one tick = %d, want 1", got)
	}
	if len(state(a)) != 21 {
		t.Fatalf("a has %d entries", len(state(a)))
	}
}

func TestFirstRepairTickUsesFirstRepairDelay(t *testing.T) {
	net := transport.NewNetwork()
	all := addrs("n", 2)
	mod := func(c *Config[string, string]) {
		c.RepairInterval = time.Hour
		c.FirstRepairDelay = 20 * time.Millisecond
	}
	a := build(t, net, all[0], all, mod)
	b := build(t, net, all[1], all, mod)
	start(t, a)
	start(t, b)
	noFanout(a, b)
	_ = a.Set(context.Background(), "x", "1", 0)
	eventually(t, time.Second, "first repair tick", func() bool { _, ok := b.Get("x"); return ok })
}

func TestCloseStopsRepairLoop(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()
	net := transport.NewNetwork()
	all := addrs("n", 2)
	a := build(t, net, all[0], all, repairCfg)
	b := build(t, net, all[1], all, repairCfg)
	startAll(t, a, b)
	time.Sleep(3 * testRepair)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = a.Close(ctx)
	_ = b.Close(ctx)
	eventually(t, 2*time.Second, "goroutines to exit", func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= before
	})
}

// BenchmarkSnapshotEncodeDecode measures encoding every entry of a store into
// a state blob and decoding it back (the cost of one full-state transfer).
func benchSnapshot(b *testing.B, entries int) {
	st := storage.NewStore[string, string](storage.StoreOptions{})
	for i := 0; i < entries; i++ {
		st.Merge(fmt.Sprintf("key-%d", i), storage.Entry[string]{Value: strings.Repeat("v", 100), Version: version.Version{Clock: uint64(i + 1), NodeID: "n"}})
	}
	kc, _ := wire.NewKeyCodec[string]()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var es []wire.Entry
		for _, ke := range st.Snapshot() {
			w, err := wire.FromEntry(ke.Key, ke.Entry, kc, strCodec{})
			if err != nil {
				b.Fatal(err)
			}
			es = append(es, w)
		}
		blob, err := wire.EncodeEntries(es)
		if err != nil {
			b.Fatal(err)
		}
		out, err := wire.DecodeEntries(blob)
		if err != nil || len(out) != entries {
			b.Fatal(err, len(out))
		}
		for _, w := range out {
			if _, _, err := wire.ToEntry(w, kc, strCodec{}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkSnapshotRoundTrip10k(b *testing.B) { benchSnapshot(b, 10_000) }
func BenchmarkSnapshotRoundTrip50k(b *testing.B) { benchSnapshot(b, 50_000) }
