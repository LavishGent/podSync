package node

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LavishGent/podsync/internal/peers"
	"github.com/LavishGent/podsync/internal/transport"
	"github.com/LavishGent/podsync/internal/wire"
)

type strCodec struct{}

func (strCodec) Marshal(s string) ([]byte, error)   { return []byte(s), nil }
func (strCodec) Unmarshal(b []byte) (string, error) { return string(b), nil }

type N = Node[string, string]

// mutResolver is a Resolver whose list tests can change.
type mutResolver struct {
	mu    sync.Mutex
	addrs []string
}

func (r *mutResolver) Resolve(context.Context) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.addrs...), nil
}
func (r *mutResolver) set(a ...string) { r.mu.Lock(); r.addrs = a; r.mu.Unlock() }

// lockedBuf is a goroutine-safe log sink.
type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

// build creates (but does not start) a node at addr whose peer list is all.
func build(t *testing.T, net *transport.Network, addr string, all []string, mod func(*Config[string, string])) *N {
	t.Helper()
	cfg := Config[string, string]{
		Self:                   addr,
		Transport:              net.Endpoint(addr),
		Peers:                  peers.NewSet(peers.Static(all...), addr),
		ValueCodec:             strCodec{},
		HydrationTimeout:       500 * time.Millisecond,
		HydrationRetryInterval: 10 * time.Millisecond,
		RequestTimeout:         time.Second,
		Logger:                 slog.New(slog.NewTextHandler(&lockedBuf{}, nil)),
	}
	if mod != nil {
		mod(&cfg)
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = n.Close(ctx)
	})
	return n
}

func start(t *testing.T, n *N) {
	t.Helper()
	if err := n.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func startAll(t *testing.T, ns ...*N) {
	t.Helper()
	var wg sync.WaitGroup
	for _, n := range ns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := n.Start(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func addrs(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d:7946", prefix, i)
	}
	return out
}

// cluster builds and starts n nodes that all know each other.
func cluster(t *testing.T, n int, mod func(*Config[string, string])) (*transport.Network, []*N) {
	t.Helper()
	net := transport.NewNetwork()
	all := addrs("n", n)
	var ns []*N
	for _, a := range all {
		ns = append(ns, build(t, net, a, all, mod))
	}
	startAll(t, ns...)
	return net, ns
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func state(n *N) map[string]string {
	out := map[string]string{}
	for _, ke := range n.Snapshot() {
		out[ke.Key] = fmt.Sprintf("%+v", ke.Entry)
	}
	return out
}

func equalState(ns ...*N) bool {
	first := state(ns[0])
	for _, n := range ns[1:] {
		o := state(n)
		if len(o) != len(first) {
			return false
		}
		for k, v := range first {
			if o[k] != v {
				return false
			}
		}
	}
	return true
}

func TestThreeNodesConverge(t *testing.T) {
	_, ns := cluster(t, 3, nil)
	var wg sync.WaitGroup
	for i, n := range ns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				_ = n.Set(context.Background(), fmt.Sprintf("k-%d-%d", i, j), "v", 0)
				runtime.Gosched()
			}
		}()
	}
	wg.Wait()
	eventually(t, 5*time.Second, "convergence", func() bool {
		return len(state(ns[0])) == 3000 && equalState(ns...)
	})
}

func TestOverlappingKeysHighestVersionWins(t *testing.T) {
	_, ns := cluster(t, 3, nil)
	var wg sync.WaitGroup
	for i, n := range ns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = n.Set(context.Background(), fmt.Sprintf("k%d", j), fmt.Sprintf("from-%d", i), 0)
				runtime.Gosched()
			}
		}()
	}
	wg.Wait()
	eventually(t, 5*time.Second, "convergence", func() bool { return equalState(ns...) })
	// Everyone holds the same winner, and a later write from any node
	// (whose clock has Observed the winner) outranks it.
	before := state(ns[0])
	for j := 0; j < 50; j++ {
		_ = ns[2].Set(context.Background(), fmt.Sprintf("k%d", j), "final", 0)
	}
	eventually(t, 5*time.Second, "final writes everywhere", func() bool {
		for _, n := range ns {
			for j := 0; j < 50; j++ {
				if v, _ := n.Get(fmt.Sprintf("k%d", j)); v != "final" {
					return false
				}
			}
		}
		return equalState(ns...)
	})
	for _, ke := range ns[0].Snapshot() {
		if strings.HasPrefix(ke.Key, "k") && before[ke.Key] == state(ns[0])[ke.Key] {
			t.Fatalf("%s unchanged", ke.Key)
		}
		if ke.Entry.Version.Clock == 0 {
			t.Fatal("zero version")
		}
	}
}

func TestDeleteAfterSetConverges(t *testing.T) {
	_, ns := cluster(t, 3, nil)
	if err := ns[1].Set(context.Background(), "k", "v", 0); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, "set visible on A", func() bool { _, ok := ns[0].Get("k"); return ok })
	if err := ns[0].Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, "tombstones everywhere", func() bool {
		for _, n := range ns {
			if n.Stats().Tombstones != 1 {
				return false
			}
		}
		return equalState(ns...)
	})
	for _, n := range ns {
		if _, ok := n.Get("k"); ok {
			t.Fatal("key still readable")
		}
	}
}

func TestJoinHydrates(t *testing.T) {
	net := transport.NewNetwork()
	all := addrs("n", 3)
	a := build(t, net, all[0], all, nil)
	b := build(t, net, all[1], all, nil)
	startAll(t, a, b)
	for i := 0; i < 1000; i++ {
		_ = a.Set(context.Background(), fmt.Sprintf("a%d", i), "x", 0)
	}
	_ = b.Delete(context.Background(), "a5")
	eventually(t, 5*time.Second, "a and b equal", func() bool { return len(state(b)) == 1000 && equalState(a, b) })

	c := build(t, net, all[2], all, nil)
	start(t, c)
	if !c.Ready() {
		t.Fatal("not ready after Start")
	}
	if !equalState(a, b, c) {
		t.Fatalf("joiner state differs: %d vs %d", len(state(c)), len(state(a)))
	}
}

func fixedOrder(cfg *Config[string, string]) { cfg.shuffle = func([]string) {} }

func TestHydrationSkipsPartitionedFirstPeer(t *testing.T) {
	net := transport.NewNetwork()
	all := addrs("n", 3)
	a := build(t, net, all[0], all[:2], nil)
	start(t, a)
	for i := 0; i < 100; i++ {
		_ = a.Set(context.Background(), fmt.Sprintf("k%d", i), "x", 0)
	}
	// Joiner's peers sort as n0 (partitioned), n1 (fine, same data via hydration chain).
	b := build(t, net, all[1], all[:2], nil)
	start(t, b)
	c := build(t, net, "n9:7946", all[:2], fixedOrder)
	net.Partition("n9:7946", all[0])
	start(t, c)
	if !c.Ready() || len(state(c)) != 100 {
		t.Fatalf("ready=%v entries=%d", c.Ready(), len(state(c)))
	}
}

func TestAllPeersPartitionedReadyAfterTimeoutWithErrorLog(t *testing.T) {
	net := transport.NewNetwork()
	all := addrs("n", 3)
	a := build(t, net, all[0], all, nil)
	b := build(t, net, all[1], all, nil)
	startAll(t, a, b)
	_ = a.Set(context.Background(), "k", "v", 0)
	logs := &lockedBuf{}
	c := build(t, net, all[2], all, func(cfg *Config[string, string]) {
		cfg.Logger = slog.New(slog.NewTextHandler(logs, nil))
		cfg.HydrationTimeout = 200 * time.Millisecond
	})
	net.Partition(all[2], all[0])
	net.Partition(all[2], all[1])
	begin := time.Now()
	start(t, c)
	if time.Since(begin) < 150*time.Millisecond {
		t.Fatal("returned before HydrationTimeout")
	}
	if !c.Ready() || len(state(c)) != 0 {
		t.Fatalf("ready=%v entries=%d", c.Ready(), len(state(c)))
	}
	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("no error log: %s", logs.String())
	}
}

// blocker listens at addr and blocks every request until released or ctx ends.
type blocker struct {
	got     chan struct{} // closed on first request
	once    sync.Once
	release chan struct{}
	mu      sync.Mutex
	keys    []string
}

func newBlocker(t *testing.T, net *transport.Network, addr string, blocking bool) *blocker {
	b := &blocker{got: make(chan struct{}), release: make(chan struct{})}
	if !blocking {
		close(b.release)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go net.Endpoint(addr).Listen(ctx, addr, func(ctx context.Context, _ string, mt wire.MsgType, p []byte) (wire.MsgType, any, error) {
		if mt != wire.MsgMutation {
			return wire.MsgPong, wire.Pong{}, nil
		}
		b.once.Do(func() { close(b.got) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
		m, _ := wire.DecodeMessage(mt, p)
		b.mu.Lock()
		b.keys = append(b.keys, string(m.(wire.Mutation).Entry.Key))
		b.mu.Unlock()
		return wire.MsgMutationAck, wire.MutationAck{Applied: true}, nil
	})
	eventually(t, time.Second, "blocker listening", func() bool {
		_, _, err := net.Endpoint("probe").Send(ctx, addr, wire.MsgPing, wire.Ping{})
		return err == nil
	})
	return b
}

func (b *blocker) received() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.keys...)
}

func TestQueueOverflowDropsOldest(t *testing.T) {
	net := transport.NewNetwork()
	const C, M = 8, 50
	slow := "slow:7946"
	b := newBlocker(t, net, slow, true)
	a := build(t, net, "a:7946", []string{"a:7946", slow}, func(cfg *Config[string, string]) {
		cfg.QueueSize = C
		cfg.RequestTimeout = time.Minute
	})
	start(t, a)
	ctx := context.Background()
	_ = a.Set(ctx, "first", "v", 0)
	<-b.got // the worker holds "first" in flight
	for i := 0; i < M; i++ {
		if err := a.Set(ctx, fmt.Sprintf("k%02d", i), "v", 0); err != nil {
			t.Fatal(err)
		}
	}
	if d := a.Stats().Dropped; d != M-C {
		t.Fatalf("Dropped = %d, want %d", d, M-C)
	}
	for i := 0; i < M; i++ { // all readable locally
		if _, ok := a.Get(fmt.Sprintf("k%02d", i)); !ok {
			t.Fatalf("k%02d missing locally", i)
		}
	}
	close(b.release)
	eventually(t, 2*time.Second, "queue to flush", func() bool { return len(b.received()) == 1+C })
	got := b.received()[1:]
	for i, k := range got { // the newest C survive, in order
		if want := fmt.Sprintf("k%02d", M-C+i); k != want {
			t.Fatalf("got %v", got)
		}
	}
}

func TestSlowPeerDoesNotDelayHealthyPeer(t *testing.T) {
	net := transport.NewNetwork()
	all := []string{"a:7946", "slow:7946", "fast:7946"}
	slow := build(t, net, all[1], all, nil)
	fast := build(t, net, all[2], all, nil)
	a := build(t, net, all[0], all, func(cfg *Config[string, string]) { cfg.RequestTimeout = 5 * time.Second })
	startAll(t, slow, fast, a)
	net.SetDelay(all[1], 3*time.Second)
	begin := time.Now()
	for i := 0; i < 100; i++ {
		_ = a.Set(context.Background(), fmt.Sprintf("k%d", i), "v", 0)
	}
	eventually(t, time.Second, "fast peer to receive all", func() bool { return len(state(fast)) == 100 })
	if el := time.Since(begin); el > time.Second {
		t.Fatalf("healthy peer delayed: %v", el)
	}
	if len(state(slow)) != 0 {
		t.Fatal("slow peer should not have received yet")
	}
}

func TestPartitionThenHeal(t *testing.T) {
	net := transport.NewNetwork()
	all := addrs("n", 2)
	a := build(t, net, all[0], all, nil)
	b := build(t, net, all[1], all, nil)
	startAll(t, a, b)
	ctx := context.Background()

	net.Partition(all[0], all[1])
	_ = a.Set(ctx, "lost", "v", 0)
	eventually(t, time.Second, "send failure counted", func() bool { return a.Stats().SendFailures >= 1 })
	net.Heal(all[0], all[1])
	_ = a.Set(ctx, "after", "v", 0)
	eventually(t, 2*time.Second, "post-heal write", func() bool { _, ok := b.Get("after"); return ok })
	if _, ok := b.Get("lost"); ok {
		t.Fatal("a failed send must not be retried (T5 repair's job)")
	}
	// Dropped connections: FailNext fails sends once, then recovers.
	net.FailNext(all[1], 2)
	_ = a.Set(ctx, "f1", "v", 0)
	_ = a.Set(ctx, "f2", "v", 0)
	_ = a.Set(ctx, "f3", "v", 0)
	eventually(t, 2*time.Second, "f3 arrives", func() bool { _, ok := b.Get("f3"); return ok })
}

func TestOverflowWhilePartitionedCountsDropped(t *testing.T) {
	net := transport.NewNetwork()
	all := addrs("n", 2)
	a := build(t, net, all[0], all, func(cfg *Config[string, string]) {
		cfg.QueueSize = 4
		cfg.RequestTimeout = 300 * time.Millisecond
	})
	b := build(t, net, all[1], all, nil)
	startAll(t, a, b)
	net.SetDelay(all[1], time.Hour) // black hole: the first send hangs until RequestTimeout
	for i := 0; i < 100; i++ {
		_ = a.Set(context.Background(), fmt.Sprintf("k%d", i), "v", 0)
	}
	if a.Stats().Dropped == 0 {
		t.Fatal("expected drops")
	}
	if a.Stats().LiveKeys != 100 {
		t.Fatalf("local keys = %d", a.Stats().LiveKeys)
	}
	net.SetDelay(all[1], 0)
	net.Partition(all[0], all[1])
	time.Sleep(400 * time.Millisecond)
	net.Heal(all[0], all[1])
	_ = a.Set(context.Background(), "post-heal", "v", 0)
	eventually(t, 2*time.Second, "post-heal write", func() bool { _, ok := b.Get("post-heal"); return ok })
}

func TestCloseDrainsQueues(t *testing.T) {
	net := transport.NewNetwork()
	all := addrs("n", 2)
	a := build(t, net, all[0], all, nil)
	b := build(t, net, all[1], all, nil)
	startAll(t, a, b)
	net.SetDelay(all[1], 20*time.Millisecond)
	for i := 0; i < 100; i++ {
		_ = a.Set(context.Background(), fmt.Sprintf("k%d", i), "v", 0)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(state(b)); n != 100 {
		t.Fatalf("peer received %d/100 by the time Close returned", n)
	}
	if err := a.Set(ctx, "x", "v", 0); err != ErrClosed {
		t.Fatalf("Set after Close = %v", err)
	}
}

func TestCloseReturnsWhenCtxDoneWithBlockedPeer(t *testing.T) {
	net := transport.NewNetwork()
	slow := "slow:7946"
	newBlocker(t, net, slow, true)
	a := build(t, net, "a:7946", []string{"a:7946", slow}, func(cfg *Config[string, string]) { cfg.RequestTimeout = time.Minute })
	start(t, a)
	_ = a.Set(context.Background(), "k", "v", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if err := a.Close(ctx); err == nil {
		t.Fatal("expected ctx error")
	}
	if time.Since(begin) > time.Second {
		t.Fatal("Close did not honor ctx")
	}
}

func TestPeerQueueLifecycleFollowsPeerSet(t *testing.T) {
	net := transport.NewNetwork()
	res := &mutResolver{}
	res.set("a:7946")
	b := build(t, net, "b:7946", []string{"b:7946"}, nil)
	a := build(t, net, "a:7946", nil, func(cfg *Config[string, string]) {
		cfg.Peers = peers.NewSet(res, "a:7946")
		cfg.Peers.RefreshInterval = time.Hour
	})
	start(t, b)
	start(t, a)
	if a.Stats().Peers != 0 {
		t.Fatalf("peers = %d", a.Stats().Peers)
	}
	res.set("a:7946", "b:7946")
	_ = a.peers.Refresh(context.Background())
	if a.Stats().Peers != 1 {
		t.Fatalf("peers = %d, want 1", a.Stats().Peers)
	}
	_ = a.Set(context.Background(), "one", "v", 0)
	eventually(t, time.Second, "delivery", func() bool { _, ok := b.Get("one"); return ok })

	res.set("a:7946")
	_ = a.peers.Refresh(context.Background())
	if a.Stats().Peers != 0 {
		t.Fatalf("peers = %d, want 0", a.Stats().Peers)
	}
	_ = a.Set(context.Background(), "two", "v", 0)
	time.Sleep(50 * time.Millisecond)
	if _, ok := b.Get("two"); ok {
		t.Fatal("removed peer still receives")
	}
	if a.Stats().Dropped != 0 {
		t.Fatal("removal must not count as drops")
	}
	// Departed peer's entries stay: no eviction on departure.
	if _, ok := b.Get("one"); !ok {
		t.Fatal("entry vanished")
	}
	runtime.GC()
}

func TestServesWhileHydrating(t *testing.T) {
	net := transport.NewNetwork()
	all := addrs("n", 3)
	b := build(t, net, all[1], all, nil)
	c := build(t, net, all[2], all, nil)
	startAll(t, b, c)
	a := build(t, net, all[0], all, func(cfg *Config[string, string]) {
		cfg.HydrationTimeout = 600 * time.Millisecond
	})
	net.Partition(all[0], all[1]) // hydration: n1 fails, n2 not partitioned though.
	net.Partition(all[0], all[2])
	done := make(chan struct{})
	go func() { start(t, a); close(done) }()
	eventually(t, time.Second, "hydrating", func() bool { return a.Stats().State == StateHydrating })
	net.Heal(all[0], all[2])
	_ = c.Set(context.Background(), "during", "v", 0)
	eventually(t, 2*time.Second, "mutation applied while hydrating", func() bool {
		_, ok := a.Get("during")
		return ok
	})
	<-done
}

func TestPagedHydrationAcrossShardCounts(t *testing.T) {
	for _, tc := range []struct{ src, dst int }{{8, 16}, {16, 8}, {8, 8}} {
		t.Run(fmt.Sprintf("%dto%d", tc.src, tc.dst), func(t *testing.T) {
			net := transport.NewNetwork()
			all := addrs("n", 2)
			a := build(t, net, all[0], all[:1], func(cfg *Config[string, string]) { cfg.Store.NumShards = tc.src })
			start(t, a)
			for i := 0; i < 500; i++ {
				_ = a.Set(context.Background(), fmt.Sprintf("k%d", i), "v", 0)
			}
			b := build(t, net, all[1], all, func(cfg *Config[string, string]) { cfg.Store.NumShards = tc.dst })
			start(t, b)
			if !equalState(a, b) || len(state(b)) != 500 {
				t.Fatalf("entries = %d", len(state(b)))
			}
		})
	}
}

func TestHydrationResumesOnNextPeerSameShardCount(t *testing.T) {
	net := transport.NewNetwork()
	all := addrs("n", 3)
	a := build(t, net, all[0], all[:1], nil)
	b := build(t, net, all[1], all[:1], nil)
	for _, n := range []*N{a, b} {
		start(t, n)
	}
	// Same data on both via direct sets with identical versions is awkward;
	// write on a, let b hydrate from it, then take a down.
	for i := 0; i < 300; i++ {
		_ = a.Set(context.Background(), fmt.Sprintf("k%d", i), "v", 0)
	}
	bb := build(t, net, "b2:7946", []string{all[0]}, nil)
	start(t, bb)
	// First peer fails on its 5th page request; the next peer serves the rest.
	calls := 0
	net.Drop(func(from, to string, mt wire.MsgType) bool {
		if from != all[2] || mt != wire.MsgStateRequest {
			return false
		}
		calls++
		return to == all[0] && calls > 4
	})
	c := build(t, net, all[2], []string{all[0], "b2:7946"}, fixedOrder)
	start(t, c)
	if len(state(c)) != 300 {
		t.Fatalf("entries = %d, calls = %d", len(state(c)), calls)
	}
}

func TestPingReturnsClock(t *testing.T) {
	net := transport.NewNetwork()
	a := build(t, net, "a:7946", nil, nil)
	start(t, a)
	_ = a.Set(context.Background(), "k", "v", 0)
	mt, p, err := net.Endpoint("x:1").Send(context.Background(), "a:7946", wire.MsgPing, wire.Ping{Origin: "x"})
	if err != nil || mt != wire.MsgPong {
		t.Fatal(mt, err)
	}
	m, _ := wire.DecodeMessage(mt, p)
	if m.(wire.Pong).Clock != 1 {
		t.Fatalf("clock = %d", m.(wire.Pong).Clock)
	}
}

func TestBadMutationCountedAndIgnored(t *testing.T) {
	net := transport.NewNetwork()
	a := build(t, net, "a:7946", nil, nil)
	start(t, a)
	bad := wire.Mutation{Origin: "x", Entry: wire.Entry{Key: []byte("k"), Deleted: true}} // zero version
	if _, _, err := net.Endpoint("x:1").Send(context.Background(), "a:7946", wire.MsgMutation, bad); err == nil {
		t.Fatal("expected error")
	}
	if a.Stats().DecodeErrors != 1 || a.Stats().TotalEntries != 0 {
		t.Fatalf("%+v", a.Stats())
	}
}

func TestConfigValidation(t *testing.T) {
	net := transport.NewNetwork()
	base := Config[string, string]{
		Self: "a:1", Transport: net.Endpoint("a:1"),
		Peers: peers.NewSet(peers.Static(), "a:1"), ValueCodec: strCodec{},
	}
	bad := base
	bad.Store.NumShards = wire.MaxShards + 1
	if _, err := New(bad); err == nil {
		t.Fatal("expected shard cap error")
	}
	bad = base
	bad.Transport = nil
	if _, err := New(bad); err == nil {
		t.Fatal("expected transport error")
	}
}

var _ = sort.Strings

// Close during an in-progress Start must cancel hydration and wait for Start to return.
func TestCloseDuringHydrationStopsStart(t *testing.T) {
	net := transport.NewNetwork()
	all := addrs("n", 2)
	a := build(t, net, all[0], all, nil)
	startAll(t, a)
	c := build(t, net, all[1], all, func(cfg *Config[string, string]) {
		cfg.HydrationTimeout = time.Hour
	})
	net.Partition(all[1], all[0])
	done := make(chan error, 1)
	go func() { done <- c.Start(context.Background()) }()
	time.Sleep(100 * time.Millisecond)
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.Close(closeCtx)
	select {
	case <-done:
	default:
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Start still running after Close")
		}
	}
}
