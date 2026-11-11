package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LavishGent/podsync/pkg/podsync"
)

var fixedNow = time.Unix(1_700_000_000, 0)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func startPods(t *testing.T, n, limit int) []*Limiter {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = freeAddr(t)
	}
	var ls []*Limiter
	errs := make(chan error, n)
	for _, a := range addrs {
		c, err := podsync.New(podsync.Config[string, counter]{
			Resolver: podsync.Static(addrs...), ListenAddr: a, Self: a, Codec: podsync.JSONCodec[counter]{},
		})
		if err != nil {
			t.Fatal(err)
		}
		go func() { errs <- c.Start(context.Background()) }()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = c.Close(ctx)
		})
		l := NewLimiter(c, limit, time.Hour)
		l.now = func() time.Time { return fixedNow }
		ls = append(ls, l)
	}
	for range n {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	return ls
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSinglePodExactCount(t *testing.T) {
	l := startPods(t, 1, 10)[0]
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, err := l.Hit(context.Background(), "alice")
			if err != nil {
				t.Error(err)
			}
			if ok {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := accepted.Load(); got != 10 {
		t.Fatalf("accepted %d, want exactly 10", got)
	}
	// Another client has its own budget.
	if ok, _, _ := l.Hit(context.Background(), "bob"); !ok {
		t.Fatal("bob denied")
	}
}

func TestWindowRollover(t *testing.T) {
	l := startPods(t, 1, 2)[0]
	ctx := context.Background()
	l.Hit(ctx, "c")
	l.Hit(ctx, "c")
	if ok, _, _ := l.Hit(ctx, "c"); ok {
		t.Fatal("third hit allowed")
	}
	l.now = func() time.Time { return fixedNow.Add(time.Hour) }
	if ok, total, _ := l.Hit(ctx, "c"); !ok || total != 1 {
		t.Fatalf("after rollover ok=%v total=%d", ok, total)
	}
}

func TestMultiPodNeverUndercounts(t *testing.T) {
	ls := startPods(t, 2, 1000)
	waitFor(t, "peers", func() bool { return ls[0].cache.Stats().Peers == 1 && ls[1].cache.Stats().Peers == 1 })
	var wg sync.WaitGroup
	var accepted atomic.Int32
	for i, l := range ls {
		for range 40 + i*10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if ok, _, err := l.Hit(context.Background(), "alice"); err != nil {
					t.Error(err)
				} else if ok {
					accepted.Add(1)
				}
			}()
		}
	}
	wg.Wait()
	want := int(accepted.Load()) // 90: limit is far above the load
	if want != 90 {
		t.Fatalf("accepted %d, want 90", want)
	}
	for i, l := range ls {
		waitFor(t, "convergence", func() bool { return l.Total("alice") >= want })
		if got := l.Total("alice"); got != want {
			t.Fatalf("pod %d total %d, want %d", i, got, want)
		}
	}
}

func TestHandlers(t *testing.T) {
	l := startPods(t, 1, 1)[0]
	srv := httptest.NewServer(newMux(l))
	defer srv.Close()
	get := func(path string) *http.Response {
		r, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		return r
	}
	if r := get("/hit"); r.StatusCode != 400 {
		t.Fatalf("missing client: %d", r.StatusCode)
	}
	r := get("/hit?client=a")
	if r.StatusCode != 200 || r.Header.Get("X-Pod") != l.cache.ID() {
		t.Fatalf("first hit: %d pod=%q", r.StatusCode, r.Header.Get("X-Pod"))
	}
	if r := get("/hit?client=a"); r.StatusCode != 429 {
		t.Fatalf("second hit: %d", r.StatusCode)
	}
	if r := get("/readyz"); r.StatusCode != 200 {
		t.Fatalf("readyz: %d", r.StatusCode)
	}
	if r := get("/stats"); r.StatusCode != 200 {
		t.Fatalf("stats: %d", r.StatusCode)
	}
}
