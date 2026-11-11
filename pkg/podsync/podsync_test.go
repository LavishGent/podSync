package podsync_test

import (
	"context"
	"net"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LavishGent/podsync/pkg/podsync"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func pair(t *testing.T) (a, b *podsync.Cache[string, string]) {
	t.Helper()
	addrA, addrB := freeAddr(t), freeAddr(t)
	mk := func(self string) *podsync.Cache[string, string] {
		c, err := podsync.New[string, string](podsync.Config[string, string]{
			Resolver:   podsync.Static(addrA, addrB),
			ListenAddr: self,
			Self:       self,
			Codec:      podsync.StringCodec{},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a, b = mk(addrA), mk(addrB)
	ctx := context.Background()
	// Start concurrently: each node hydrates from the other, which must be
	// listening, or Start waits out HydrationTimeout.
	errs := make(chan error, 2)
	for _, c := range []*podsync.Cache[string, string]{a, b} {
		go func() { errs <- c.Start(ctx) }()
		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = c.Close(cctx)
		})
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "peers", func() bool { return a.Stats().Peers == 1 && b.Stats().Peers == 1 })
	return a, b
}

func TestNewValidation(t *testing.T) {
	good := func() podsync.Config[string, string] {
		return podsync.Config[string, string]{Resolver: podsync.Static("127.0.0.1:1"), Codec: podsync.StringCodec{}}
	}
	cases := map[string]func(*podsync.Config[string, string]){
		"no resolver":  func(c *podsync.Config[string, string]) { c.Resolver = nil },
		"no codec":     func(c *podsync.Config[string, string]) { c.Codec = nil },
		"neg shards":   func(c *podsync.Config[string, string]) { c.Shards = -1 },
		"huge shards":  func(c *podsync.Config[string, string]) { c.Shards = 1 << 30 },
		"short secret": func(c *podsync.Config[string, string]) { c.Secret = []byte("short") },
		"neg ttl":      func(c *podsync.Config[string, string]) { c.DefaultTTL = -time.Second },
		"bad listen":   func(c *podsync.Config[string, string]) { c.ListenAddr = "nonsense" },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := good()
			mut(&cfg)
			if c, err := podsync.New(cfg); err == nil {
				t.Fatalf("want error, got cache %v", c)
			}
		})
	}
	t.Run("good", func(t *testing.T) {
		if _, err := podsync.New(good()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("good with secret", func(t *testing.T) {
		cfg := good()
		cfg.Secret = []byte(strings.Repeat("s", 32))
		if _, err := podsync.New(cfg); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("bad key type", func(t *testing.T) {
		_, err := podsync.New(podsync.Config[[2]int, string]{Resolver: podsync.Static(), Codec: podsync.StringCodec{}})
		if err == nil {
			t.Fatal("want error for unsupported key type")
		}
	})
}

func TestReplicatesSetAndDelete(t *testing.T) {
	a, b := pair(t)
	ctx := context.Background()
	if !a.Ready() || !b.Ready() {
		t.Fatal("not ready")
	}
	if err := a.Set(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "set on b", func() bool { v, ok := b.Get("k"); return ok && v == "v" })

	if err := b.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "delete on a", func() bool { _, ok := a.Get("k"); return !ok })
}

func TestSetWithTTL(t *testing.T) {
	a, _ := pair(t)
	ctx := context.Background()
	if err := a.Set(ctx, "k", "v", podsync.WithTTL(30*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.Get("k"); !ok {
		t.Fatal("missing before ttl")
	}
	eventually(t, "expiry", func() bool { _, ok := a.Get("k"); return !ok })
	if err := a.Set(ctx, "k", "v", podsync.WithTTL(-1)); err == nil {
		t.Fatal("negative ttl accepted")
	}
}

func TestRangeLiveOnly(t *testing.T) {
	a, _ := pair(t)
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c", "dead", "exp"} {
		if err := a.Set(ctx, k, "v-"+k); err != nil {
			t.Fatal(err)
		}
	}
	_ = a.Delete(ctx, "dead")
	_ = a.Set(ctx, "exp", "x", podsync.WithTTL(time.Millisecond))
	time.Sleep(10 * time.Millisecond)

	var got []string
	a.Range(func(k, v string) bool {
		if v != "v-"+k {
			t.Errorf("value for %s = %s", k, v)
		}
		got = append(got, k)
		return true
	})
	sort.Strings(got)
	if strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("got %v", got)
	}
}

func TestRangeEarlyStopAndReentrant(t *testing.T) {
	a, _ := pair(t)
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c", "d"} {
		_ = a.Set(ctx, k, "v")
	}
	var n int
	a.Range(func(string, string) bool { n++; return false })
	if n != 1 {
		t.Fatalf("early stop: fn called %d times", n)
	}

	// Set (and Get) from inside fn must not deadlock.
	var calls atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Range(func(k, v string) bool {
			calls.Add(1)
			if err := a.Set(ctx, k, "updated"); err != nil {
				t.Error(err)
			}
			a.Get(k)
			return true
		})
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Range deadlocked on Set from fn")
	}
	if calls.Load() != 4 {
		t.Fatalf("calls = %d", calls.Load())
	}
	if v, _ := a.Get("a"); v != "updated" {
		t.Fatalf("a = %q", v)
	}
}

func TestSecretMismatchOK(t *testing.T) {
	// Same secret on both nodes replicates.
	addrA, addrB := freeAddr(t), freeAddr(t)
	secret := []byte(strings.Repeat("k", 32))
	var cs []*podsync.Cache[string, string]
	errs := make(chan error, 2)
	for _, self := range []string{addrA, addrB} {
		c, err := podsync.New(podsync.Config[string, string]{
			Resolver: podsync.Static(addrA, addrB), ListenAddr: self, Self: self,
			Codec: podsync.StringCodec{}, Secret: secret,
		})
		if err != nil {
			t.Fatal(err)
		}
		go func() { errs <- c.Start(context.Background()) }()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = c.Close(ctx)
		})
		cs = append(cs, c)
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "peers", func() bool { return cs[0].Stats().Peers == 1 && cs[1].Stats().Peers == 1 })
	_ = cs[0].Set(context.Background(), "k", "v")
	eventually(t, "replicated with secret", func() bool { v, ok := cs[1].Get("k"); return ok && v == "v" })
}
