package podsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/LavishGent/podsync/internal/node"
	"github.com/LavishGent/podsync/internal/peers"
	"github.com/LavishGent/podsync/internal/storage"
	"github.com/LavishGent/podsync/internal/transport"
	"github.com/LavishGent/podsync/internal/wire"
)

// Resolver returns the current "host:port" replication addresses of every pod,
// self included.
type Resolver = peers.Resolver

// Static returns a Resolver that always returns addrs.
func Static(addrs ...string) Resolver { return peers.Static(addrs...) }

// DNS resolves <service>.<namespace>.svc.cluster.local (use a headless
// Service) to one "ip:port" per A/AAAA record. An optional *net.Resolver
// replaces net.DefaultResolver.
func DNS(service, namespace string, port int, r ...*net.Resolver) Resolver {
	return peers.DNS(service, namespace, port, r...)
}

// Stats is a point-in-time view of a Cache: entry counts, peer count, lifecycle
// state, and replication and repair counters.
type Stats = node.Stats

// DefaultListenAddr is the replication listen address when Config.ListenAddr is
// empty.
const DefaultListenAddr = ":9090"

// Config configures a Cache. Resolver and Codec are required.
type Config[K comparable, V any] struct {
	// Resolver finds the peers: DNS(...) in Kubernetes, Static(...) elsewhere.
	// It may include this pod's own address; it is filtered out.
	Resolver Resolver
	// ListenAddr is the replication listen address. Default ":9090".
	ListenAddr string
	// Self is the "ip:port" other pods use to reach this one; the peer set
	// excludes it. Default: POD_IP with ListenAddr's port, else the first
	// non-loopback interface address, else 127.0.0.1.
	Self string
	// NodeID stamps this node's writes and log lines. Default: hostname plus a
	// unique suffix.
	NodeID string
	// Codec encodes values for replication. Required.
	Codec Codec[V]
	// DefaultTTL is applied to Set calls without WithTTL. 0 = no expiry. Keep
	// TTLs at least seconds long: expiry uses each pod's own clock.
	DefaultTTL time.Duration
	// Shards is the number of store shards. Default 64, at most 1024.
	Shards int
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// Secret, when set, enables the HMAC handshake on the replication port. At
	// least 32 bytes, identical on every pod. It authenticates peers; it does
	// not encrypt (see the README security note).
	Secret []byte

	// Tuning knobs. Zero means the default noted.

	// TombstoneTTL: how long deleted and expired entries are kept (1h).
	TombstoneTTL time.Duration
	// SweepInterval: how often the store sweeps (1m).
	SweepInterval time.Duration
	// RefreshInterval: how often the peer set is re-resolved (5s).
	RefreshInterval time.Duration
	// RepairInterval: period between repair pulls from a random peer (30s).
	RepairInterval time.Duration
	// FirstRepairDelay: wait before the first repair after Ready (15s).
	FirstRepairDelay time.Duration
	// HydrationTimeout: bound on startup hydration; after it the node goes
	// Ready with whatever it merged (30s).
	HydrationTimeout time.Duration
	// RequestTimeout: bound on one peer request (1s).
	RequestTimeout time.Duration
	// QueueSize: capacity of each peer's fanout queue (1024). Overflow drops
	// the oldest mutation; repair reconciles it. (The ticket called this
	// OutboxSize.)
	QueueSize int
	// MaxStateEntries: the documented envelope (50k); more only logs a warning.
	MaxStateEntries int

	transport transport.Transport // test hook; default TCP
}

// SetOption customizes a Set.
type SetOption func(*setOptions)

type setOptions struct {
	ttl    time.Duration
	hasTTL bool
}

// WithTTL overrides Config.DefaultTTL for one Set. WithTTL(0) means no expiry.
func WithTTL(d time.Duration) SetOption {
	return func(o *setOptions) { o.ttl, o.hasTTL = d, true }
}

// Cache is a replicated in-memory key/value store. Reads are local; writes are
// applied locally and replicated to peers asynchronously.
type Cache[K comparable, V any] struct {
	n          *node.Node[K, V]
	id         string
	defaultTTL time.Duration
}

// New validates cfg and returns an unstarted Cache. It never panics.
func New[K comparable, V any](cfg Config[K, V]) (*Cache[K, V], error) {
	switch {
	case cfg.Resolver == nil:
		return nil, errors.New("podsync: Resolver is required")
	case cfg.Codec == nil:
		return nil, errors.New("podsync: Codec is required")
	case cfg.Shards < 0:
		return nil, fmt.Errorf("podsync: Shards must not be negative, got %d", cfg.Shards)
	case cfg.Shards > wire.MaxShards:
		return nil, fmt.Errorf("podsync: Shards %d exceeds the maximum %d", cfg.Shards, wire.MaxShards)
	case cfg.DefaultTTL < 0:
		return nil, fmt.Errorf("podsync: DefaultTTL must not be negative, got %v", cfg.DefaultTTL)
	}
	if _, err := wire.NewKeyCodec[K](); err != nil {
		return nil, fmt.Errorf("podsync: %w", err)
	}
	if len(cfg.Secret) > 0 && len(cfg.Secret) < transport.MinSecretLen {
		return nil, transport.ErrSecretTooShort
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = DefaultListenAddr
	}
	_, portStr, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("podsync: ListenAddr %q: %w", cfg.ListenAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return nil, fmt.Errorf("podsync: ListenAddr %q: invalid port", cfg.ListenAddr)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Self == "" {
		self, err := peers.SelfAddr(port)
		if err != nil {
			self = net.JoinHostPort("127.0.0.1", portStr)
			logger.Warn("podsync: could not detect own address; set POD_IP or Config.Self", "err", err, "fallback", self)
		} else {
			logger.Info("podsync: detected own address", "self", self)
		}
		cfg.Self = self
	}

	set := peers.NewSet(cfg.Resolver, cfg.Self)
	set.RefreshInterval = cfg.RefreshInterval
	tr := cfg.transport
	if tr == nil {
		tr = &transport.TCP{Secret: cfg.Secret}
	}
	n, err := node.New(node.Config[K, V]{
		NodeID: cfg.NodeID, Self: cfg.Self, ListenAddr: cfg.ListenAddr,
		Transport: tr, Peers: set, ValueCodec: cfg.Codec, Logger: logger,
		Store: storage.StoreOptions{
			NumShards: cfg.Shards, SweepInterval: orDefault(cfg.SweepInterval, time.Minute),
			TombstoneTTL: cfg.TombstoneTTL,
		},
		QueueSize: cfg.QueueSize, RequestTimeout: cfg.RequestTimeout,
		HydrationTimeout: cfg.HydrationTimeout, MaxStateEntries: cfg.MaxStateEntries,
		RepairInterval: cfg.RepairInterval, FirstRepairDelay: cfg.FirstRepairDelay,
	})
	if err != nil {
		return nil, fmt.Errorf("podsync: %w", err)
	}
	return &Cache[K, V]{n: n, id: n.ID(), defaultTTL: cfg.DefaultTTL}, nil
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// ID returns this node's ID.
func (c *Cache[K, V]) ID() string { return c.id }

// Start listens, joins the peers and hydrates from them. It returns once the
// cache is Ready (or hydration timed out). ctx bounds Start only.
func (c *Cache[K, V]) Start(ctx context.Context) error { return c.n.Start(ctx) }

// Get returns the live value for key from local memory. No network I/O.
func (c *Cache[K, V]) Get(key K) (V, bool) { return c.n.Get(key) }

// Set writes key locally and replicates it to peers without blocking on them.
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, opts ...SetOption) error {
	var o setOptions
	for _, opt := range opts {
		opt(&o)
	}
	ttl := c.defaultTTL
	if o.hasTTL {
		ttl = o.ttl
	}
	if ttl < 0 {
		return fmt.Errorf("podsync: negative TTL %v", ttl)
	}
	var exp int64
	if ttl > 0 {
		exp = time.Now().Add(ttl).UnixNano()
	}
	return c.n.Set(ctx, key, value, exp)
}

// Delete removes key everywhere, via a tombstone that replicates like a Set.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error { return c.n.Delete(ctx, key) }

// Range calls fn for every live entry (no tombstones, no expired entries)
// until fn returns false. Order is unspecified.
//
// Range is O(entries): it copies the store, then calls fn without holding any
// lock, so fn may call back into the Cache (Get, Set, Delete). It sees a
// point-in-time copy; writes made during the walk may not be reflected. Use it
// for small stores and infrequent scans, not on a hot path with many entries.
func (c *Cache[K, V]) Range(fn func(K, V) bool) {
	now := time.Now().UnixNano()
	for _, ke := range c.n.Snapshot() {
		if !ke.Entry.IsAlive(now) {
			continue
		}
		if !fn(ke.Key, ke.Entry.Value) {
			return
		}
	}
}

// Ready reports whether hydration has finished and the cache is not closing.
// Wire it to a Kubernetes readiness probe.
func (c *Cache[K, V]) Ready() bool { return c.n.Ready() }

// Stats returns counters for observability.
func (c *Cache[K, V]) Stats() Stats { return c.n.Stats() }

// Close stops accepting writes, drains queued replication until ctx is done,
// and shuts down. Mutations still queued when ctx ends are left to repair.
func (c *Cache[K, V]) Close(ctx context.Context) error { return c.n.Close(ctx) }
