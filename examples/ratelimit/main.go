// Command ratelimit is a PodSync demo: an HTTP server that allows N hits per
// window per client, with the budget shared across every replica.
//
// Each pod writes only its own counter key, "limit:<client>:<nodeID>", whose
// value is {Window, Count}. A request sums Count over every
// "limit:<client>:*" entry in the current window. One writer per key means
// last-writer-wins replication never discards an increment.
//
// The global sum is eventually consistent: it can overshoot the limit by the
// hits still in flight between pods (replication takes a few milliseconds, and
// a pod that is partitioned keeps counting only its own hits). A CRDT counter
// would fix that; it is a v0.2 item.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/LavishGent/podsync/pkg/podsync"
)

// counter is one pod's hit count for one client in one window.
type counter struct {
	Window int64 // window index: unix time / window length
	Count  int
}

// Limiter enforces Limit hits per Window per client across all pods.
type Limiter struct {
	cache  *podsync.Cache[string, counter]
	limit  int
	window time.Duration
	now    func() time.Time // test hook

	locks sync.Map // client -> *sync.Mutex
}

// NewLimiter returns a Limiter over cache.
func NewLimiter(cache *podsync.Cache[string, counter], limit int, window time.Duration) *Limiter {
	return &Limiter{cache: cache, limit: limit, window: window, now: time.Now}
}

func (l *Limiter) windowIndex() int64 { return l.now().UnixNano() / int64(l.window) }

func prefix(client string) string { return "limit:" + client + ":" }

// Total sums this window's counts for client over every pod's key.
func (l *Limiter) Total(client string) int {
	w, p, total := l.windowIndex(), prefix(client), 0
	// Range is O(entries); fine for a demo. A real limiter with many clients
	// would keep a per-client index.
	l.cache.Range(func(k string, c counter) bool {
		if strings.HasPrefix(k, p) && c.Window == w {
			total += c.Count
		}
		return true
	})
	return total
}

// Hit counts one request for client. It returns whether the request is
// allowed and the summed count including it.
func (l *Limiter) Hit(ctx context.Context, client string) (bool, int, error) {
	m, _ := l.locks.LoadOrStore(client, new(sync.Mutex))
	mu := m.(*sync.Mutex)
	// Serialize check, window rollover, increment and Set for this client on
	// this pod; concurrent handlers would otherwise lose increments.
	mu.Lock()
	defer mu.Unlock()

	key := prefix(client) + l.cache.ID()
	w := l.windowIndex()
	own, _ := l.cache.Get(key)
	if own.Window != w { // window rollover (or first hit): start from zero
		own = counter{Window: w}
	}
	// own is the only writer of its key, so Total already includes it when it
	// belongs to the current window and excludes it when stale.
	total := l.Total(client)
	if total >= l.limit {
		return false, total, nil
	}
	own.Count++
	// TTL of two windows: expired counters disappear, including those of pods
	// that left (PodSync never evicts a departed pod's entries itself).
	if err := l.cache.Set(ctx, key, own, podsync.WithTTL(2*l.window)); err != nil {
		return false, total, err
	}
	return true, total + 1, nil
}

func newMux(l *Limiter) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /hit", func(w http.ResponseWriter, r *http.Request) {
		client := r.URL.Query().Get("client")
		if client == "" {
			http.Error(w, "missing client", http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Pod", l.cache.ID())
		ok, total, err := l.Hit(r.Context(), client)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusTooManyRequests)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"allowed": ok, "total": total, "limit": l.limit, "pod": l.cache.ID()})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if l.cache.Ready() {
			fmt.Fprintln(w, "ok")
			return
		}
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		st := l.cache.Stats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			State string
			podsync.Stats
		}{st.State.String(), st})
	})
	return mux
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("%s: %v", name, err)
		}
		return n
	}
	return def
}

func main() {
	// Configuration, all via environment:
	//   HTTP_ADDR    HTTP listen address (default ":8080")
	//   REPL_ADDR    replication listen address (default ":9090")
	//   SELF         this pod's replication ip:port (default: POD_IP + REPL_ADDR port)
	//   PEERS        comma-separated static peer list (local demo); otherwise DNS
	//   SERVICE, NAMESPACE  headless Service for DNS discovery
	//   LIMIT, WINDOW_SECONDS  hits per window (default 5 per 10s)
	// "-sleep 2s" just sleeps and exits: the preStop hook in a distroless image
	// (no shell) so peers' DNS drops this pod before it stops serving.
	sleep := flag.Duration("sleep", 0, "sleep for this long and exit (preStop hook)")
	flag.Parse()
	if *sleep > 0 {
		time.Sleep(*sleep)
		return
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	var resolver podsync.Resolver
	if peers := env("PEERS", ""); peers != "" {
		resolver = podsync.Static(strings.Split(peers, ",")...)
	} else {
		resolver = podsync.DNS(env("SERVICE", "podsync-headless"), env("NAMESPACE", "default"), 9090)
	}
	cache, err := podsync.New(podsync.Config[string, counter]{
		Resolver: resolver, ListenAddr: env("REPL_ADDR", ":9090"), Self: os.Getenv("SELF"),
		Codec: podsync.JSONCodec[counter]{}, Logger: log,
		RefreshInterval: 2 * time.Second, // demo: notice scaling quickly
	})
	if err != nil {
		log.Error("podsync.New", "err", err)
		os.Exit(1)
	}
	limiter := NewLimiter(cache, envInt("LIMIT", 5), time.Duration(envInt("WINDOW_SECONDS", 10))*time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Serve HTTP right away so /readyz can answer 503 while hydrating.
	srv := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: newMux(limiter), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
			os.Exit(1)
		}
	}()
	log.Info("starting", "node", cache.ID(), "http", srv.Addr)
	if err := cache.Start(ctx); err != nil {
		log.Error("cache.Start", "node", cache.ID(), "err", err)
		os.Exit(1)
	}
	log.Info("ready", "node", cache.ID(), "peers", cache.Stats().Peers)

	<-ctx.Done()
	log.Info("shutting down", "node", cache.ID())
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	if err := cache.Close(sctx); err != nil {
		log.Warn("close", "node", cache.ID(), "err", err)
	}
}
