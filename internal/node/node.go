package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LavishGent/podsync/internal/clock"
	"github.com/LavishGent/podsync/internal/peers"
	"github.com/LavishGent/podsync/internal/storage"
	"github.com/LavishGent/podsync/internal/transport"
	"github.com/LavishGent/podsync/internal/version"
	"github.com/LavishGent/podsync/internal/wire"
)

// ErrClosed is returned by Set and Delete once Close has begun.
var ErrClosed = errors.New("node: closed")

// State is a node's lifecycle state.
type State int32

// Lifecycle states. A node serves StateRequest and Mutation in every state
// except StateClosed.
const (
	StateStarting State = iota
	StateHydrating
	StateReady
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateStarting:
		return "Starting"
	case StateHydrating:
		return "Hydrating"
	case StateReady:
		return "Ready"
	case StateClosed:
		return "Closed"
	}
	return fmt.Sprintf("State(%d)", int32(s))
}

// Defaults.
const (
	DefaultQueueSize              = 1024
	DefaultRequestTimeout         = time.Second
	DefaultResolveTimeout         = 5 * time.Second
	DefaultHydrationTimeout       = 30 * time.Second
	DefaultHydrationRetryInterval = time.Second
	DefaultMaxStateEntries        = 50_000
	DefaultRepairInterval         = 30 * time.Second
	DefaultFirstRepairDelay       = 15 * time.Second
)

// Config configures a Node. Self, Transport, Peers and ValueCodec are
// required.
type Config[K comparable, V any] struct {
	// NodeID stamps this node's versions and requests. Default NewID().
	NodeID string
	// Self is this node's "ip:port", also the default listen address. The
	// peer set should exclude it (peers.NewSet(r, Self)).
	Self string
	// ListenAddr overrides the address passed to Transport.Listen.
	ListenAddr string
	Transport  transport.Transport
	Peers      *peers.Set
	// KeyCodec defaults to wire.NewKeyCodec[K]().
	KeyCodec   wire.KeyCodec[K]
	ValueCodec wire.ValueCodec[V]
	// Store configures the node's store. Its Clock defaults to Clock.
	// NumShards is capped at wire.MaxShards.
	Store  storage.StoreOptions
	Clock  clock.Clock
	Logger *slog.Logger

	// QueueSize is the capacity of each peer's fanout queue. Default 1024.
	QueueSize int
	// RequestTimeout bounds one fanout send or hydration page. Default 1s.
	RequestTimeout time.Duration
	// ResolveTimeout bounds the wait for the first peer resolution. Default 5s.
	ResolveTimeout time.Duration
	// HydrationTimeout bounds hydration; after it the node goes Ready with
	// whatever it merged. Default 30s.
	HydrationTimeout time.Duration
	// HydrationRetryInterval is the pause between failed hydration rounds.
	// Default 1s.
	HydrationRetryInterval time.Duration
	// MaxStateEntries is the documented envelope; a hydration round that
	// receives more logs a warning. Default 50k.
	MaxStateEntries int
	// RepairInterval is the period between repair ticks, jittered +-10%.
	// Default 30s.
	RepairInterval time.Duration
	// FirstRepairDelay is the wait before the first repair tick after the
	// node turns Ready (Addendum A2). Default 15s, capped at RepairInterval.
	FirstRepairDelay time.Duration

	shuffle func([]string) // test hook; default random
}

// Stats is a point-in-time view of a node.
type Stats struct {
	storage.StoreStats
	// Peers is the number of peers with a fanout queue.
	Peers int
	State State
	// Dropped counts mutations evicted from a full per-peer queue (per peer,
	// not per write).
	Dropped uint64
	// SendFailures counts fanout sends that failed.
	SendFailures uint64
	// ReceivedMutations counts mutations received from peers.
	ReceivedMutations uint64
	// ReceivedConflicts counts received mutations the merge rejected.
	ReceivedConflicts uint64
	// DecodeErrors counts received entries that failed to decode.
	DecodeErrors uint64
	// Repaired counts entries applied by repair pulls.
	Repaired uint64
	// RepairFailures counts repair rounds that ended on a failed page.
	RepairFailures uint64
}

// Node replicates a Store to its peers and hydrates from them at startup.
type Node[K comparable, V any] struct {
	cfg     Config[K, V]
	id      string
	log     *slog.Logger
	clock   clock.Clock
	store   *storage.Store[K, V]
	lamport *version.Lamport
	tr      transport.Transport
	peers   *peers.Set
	keys    wire.KeyCodec[K]
	vals    wire.ValueCodec[V]

	state atomic.Int32

	mu      sync.RWMutex // guards queues and closing
	queues  map[string]*peerQueue
	closing bool

	startWG    sync.WaitGroup  // tracks an in-progress Start so Close can wait for it
	rootCtx    context.Context // owns background goroutines; cancelled by Close
	rootCancel context.CancelFunc
	wg         sync.WaitGroup
	started    atomic.Bool
	closeOnce  sync.Once

	pending atomic.Int64 // queued or in-flight fanout mutations, all peers

	dropped, sendFailures, recvMutations, recvConflicts, decodeErrors atomic.Uint64
	repaired, repairFailures                                          atomic.Uint64
	lastEnvelopeWarn                                                  atomic.Int64
}

// New validates cfg and returns an unstarted Node.
func New[K comparable, V any](cfg Config[K, V]) (*Node[K, V], error) {
	switch {
	case cfg.Transport == nil:
		return nil, errors.New("node: Transport is required")
	case cfg.Peers == nil:
		return nil, errors.New("node: Peers is required")
	case cfg.ValueCodec == nil:
		return nil, errors.New("node: ValueCodec is required")
	case cfg.Self == "" && cfg.ListenAddr == "":
		return nil, errors.New("node: Self or ListenAddr is required")
	case cfg.Store.NumShards > wire.MaxShards:
		return nil, fmt.Errorf("node: NumShards %d exceeds wire.MaxShards (%d)", cfg.Store.NumShards, wire.MaxShards)
	}
	if cfg.KeyCodec == nil {
		kc, err := wire.NewKeyCodec[K]()
		if err != nil {
			return nil, err
		}
		cfg.KeyCodec = kc
	}
	if cfg.NodeID == "" {
		cfg.NodeID = NewID()
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = cfg.Self
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.SystemClock{}
	}
	if cfg.Store.Clock == nil {
		cfg.Store.Clock = cfg.Clock
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.ResolveTimeout <= 0 {
		cfg.ResolveTimeout = DefaultResolveTimeout
	}
	if cfg.HydrationTimeout <= 0 {
		cfg.HydrationTimeout = DefaultHydrationTimeout
	}
	if cfg.HydrationRetryInterval <= 0 {
		cfg.HydrationRetryInterval = DefaultHydrationRetryInterval
	}
	if cfg.MaxStateEntries <= 0 {
		cfg.MaxStateEntries = DefaultMaxStateEntries
	}
	if cfg.RepairInterval <= 0 {
		cfg.RepairInterval = DefaultRepairInterval
	}
	if cfg.FirstRepairDelay <= 0 {
		cfg.FirstRepairDelay = DefaultFirstRepairDelay
	}
	cfg.FirstRepairDelay = min(cfg.FirstRepairDelay, cfg.RepairInterval)
	if cfg.shuffle == nil {
		cfg.shuffle = func(a []string) { rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] }) }
	}
	n := &Node[K, V]{
		cfg: cfg, id: cfg.NodeID, log: cfg.Logger.With("node", cfg.NodeID),
		clock: cfg.Clock, store: storage.NewStore[K, V](cfg.Store),
		lamport: version.NewLamport(cfg.NodeID), tr: cfg.Transport, peers: cfg.Peers,
		keys: cfg.KeyCodec, vals: cfg.ValueCodec, queues: map[string]*peerQueue{},
	}
	n.rootCtx, n.rootCancel = context.WithCancel(context.Background())
	return n, nil
}

// ID returns the node's ID.
func (n *Node[K, V]) ID() string { return n.id }

// Ready reports whether the node has finished hydrating and is not closing.
func (n *Node[K, V]) Ready() bool {
	return State(n.state.Load()) == StateReady && !n.isClosing()
}

func (n *Node[K, V]) isClosing() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.closing
}

// Get returns the live value for key.
func (n *Node[K, V]) Get(key K) (V, bool) { return n.store.Get(key) }

// Snapshot returns every entry, tombstones and expired entries included.
func (n *Node[K, V]) Snapshot() []storage.KeyEntry[K, V] { return n.store.Snapshot() }

// Stats returns the node's counters.
func (n *Node[K, V]) Stats() Stats {
	n.mu.RLock()
	np := len(n.queues)
	n.mu.RUnlock()
	return Stats{
		StoreStats: n.store.Stats(), Peers: np, State: State(n.state.Load()),
		Dropped: n.dropped.Load(), SendFailures: n.sendFailures.Load(),
		ReceivedMutations: n.recvMutations.Load(), ReceivedConflicts: n.recvConflicts.Load(),
		DecodeErrors: n.decodeErrors.Load(), Repaired: n.repaired.Load(), RepairFailures: n.repairFailures.Load(),
	}
}

// Set writes key locally and enqueues the mutation for every peer without
// blocking. A full peer queue drops its oldest mutation (counted in
// Stats().Dropped); repair (T5) is the safety net. ctx is unused today and
// kept for API stability.
func (n *Node[K, V]) Set(_ context.Context, key K, value V, expiresAt int64) error {
	return n.write(key, storage.Entry[V]{Value: value, ExpiresAt: expiresAt})
}

// Delete writes a tombstone for key and enqueues it like Set.
func (n *Node[K, V]) Delete(_ context.Context, key K) error {
	return n.write(key, storage.Entry[V]{Deleted: true, DeletedAt: n.clock.Now()})
}

func (n *Node[K, V]) write(key K, e storage.Entry[V]) error {
	n.mu.RLock()
	defer n.mu.RUnlock() // held across the enqueue so Close's drain sees every accepted write
	if n.closing {
		return ErrClosed
	}
	e.Version = n.lamport.Next()
	w, err := wire.FromEntry(key, e, n.keys, n.vals) // encode first: a failure leaves local state untouched
	if err != nil {
		return err
	}
	n.store.Merge(key, e)
	m := wire.Mutation{Origin: n.id, Entry: w}
	for _, q := range n.queues {
		q.enqueue(m)
	}
	return nil
}

// Start listens, resolves peers, hydrates, and starts background work. It
// returns once the node is Ready (or hydration timed out). Background
// goroutines run until Close, not until ctx is done; ctx only bounds Start.
func (n *Node[K, V]) Start(ctx context.Context) error {
	if !n.started.CompareAndSwap(false, true) {
		return errors.New("node: already started")
	}
	n.mu.Lock()
	if n.closing {
		n.mu.Unlock()
		return ErrClosed
	}
	n.startWG.Add(1) // Close waits for Start; the closing check above makes Add safe against Wait
	n.mu.Unlock()
	defer n.startWG.Done()
	listenErr := make(chan error, 1)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		listenErr <- n.tr.Listen(n.rootCtx, n.cfg.ListenAddr, n.handle)
	}()
	if err := n.awaitListening(ctx, listenErr); err != nil {
		return err
	}

	n.peers.OnChange(n.onPeersChanged)
	n.resolveFirst(ctx)
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.peers.Run(n.rootCtx) }()
	n.store.Start(n.rootCtx)

	if len(n.peers.Peers()) == 0 {
		n.log.Info("first node, starting with empty state")
	} else {
		n.state.Store(int32(StateHydrating))
		if err := n.hydrate(ctx); err != nil {
			return err
		}
	}
	n.state.CompareAndSwap(int32(StateHydrating), int32(StateReady))
	n.state.CompareAndSwap(int32(StateStarting), int32(StateReady))
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.repairLoop() }()
	return nil
}

// awaitListening pings Self until the transport answers, so Start does not
// return before the handler is registered. Listen errors surface here.
func (n *Node[K, V]) awaitListening(ctx context.Context, listenErr <-chan error) error {
	target := n.cfg.Self
	if target == "" {
		return nil
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		pctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		_, _, err := n.tr.Send(pctx, target, wire.MsgPing, wire.Ping{Origin: n.id})
		cancel()
		if err == nil {
			return nil
		}
		select {
		case lerr := <-listenErr:
			return fmt.Errorf("node: listen: %w", lerr)
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("node: not listening on %s: %w", target, err)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// resolveFirst refreshes the peer set until it succeeds or ResolveTimeout
// passes.
func (n *Node[K, V]) resolveFirst(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, n.cfg.ResolveTimeout)
	defer cancel()
	for {
		err := n.peers.Refresh(rctx)
		if err == nil {
			return
		}
		select {
		case <-rctx.Done():
			n.log.Warn("peer resolution failed; continuing without peers", "err", err)
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Close stops accepting writes, drains the per-peer queues until they are
// empty (and no send is in flight) or ctx is done, then stops background
// work and closes the transport. Queued mutations left when ctx ends are lost;
// repair is the safety net. It returns ctx's error if the drain was cut short.
func (n *Node[K, V]) Close(ctx context.Context) error {
	var drainErr error
	n.closeOnce.Do(func() {
		n.mu.Lock()
		n.closing = true
		n.mu.Unlock()
		for n.pending.Load() > 0 {
			select {
			case <-ctx.Done():
				drainErr = fmt.Errorf("node: close: %d mutations undelivered: %w", n.pending.Load(), ctx.Err())
			case <-time.After(time.Millisecond):
				continue
			}
			break
		}
		n.rootCancel()
		n.startWG.Wait()
		n.wg.Wait()
		n.state.Store(int32(StateClosed))
		_ = n.tr.Close()
	})
	return drainErr
}

// onPeersChanged creates and stops per-peer workers as the peer set changes.
func (n *Node[K, V]) onPeersChanged(added, removed []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closing {
		return
	}
	n.log.Info("peers changed", "added", added, "removed", removed)
	for _, a := range added {
		if _, ok := n.queues[a]; !ok {
			q := n.newPeerQueue(a)
			n.queues[a] = q
			n.wg.Add(1)
			go func() { defer n.wg.Done(); q.run() }()
		}
	}
	for _, r := range removed {
		if q, ok := n.queues[r]; ok {
			delete(n.queues, r)
			q.stop() // queued mutations are discarded, not counted as drops
		}
	}
}

// Fanout ---------------------------------------------------------------

// peerQueue is one peer's bounded FIFO and the goroutine that drains it.
type peerQueue struct {
	addr   string
	ch     chan wire.Mutation
	ctx    context.Context
	cancel context.CancelFunc
	pend   *atomic.Int64
	drop   *atomic.Uint64
	fail   *atomic.Uint64
	send   func(ctx context.Context, addr string, m wire.Mutation) error
	log    *slog.Logger

	lastDropWarn, lastFailWarn atomic.Int64
}

func (n *Node[K, V]) newPeerQueue(addr string) *peerQueue {
	ctx, cancel := context.WithCancel(n.rootCtx)
	return &peerQueue{
		addr: addr, ch: make(chan wire.Mutation, n.cfg.QueueSize), ctx: ctx, cancel: cancel,
		pend: &n.pending, drop: &n.dropped, fail: &n.sendFailures,
		send: func(ctx context.Context, addr string, m wire.Mutation) error {
			ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
			defer cancel()
			_, _, err := n.tr.Send(ctx, addr, wire.MsgMutation, m)
			return err
		},
		log: n.log.With("peer", addr),
	}
}

// enqueue never blocks. When the queue is full the OLDEST queued mutation is
// dropped: it is the one most likely to be overwritten already or repaired.
func (q *peerQueue) enqueue(m wire.Mutation) {
	q.pend.Add(1)
	for {
		select {
		case q.ch <- m:
			return
		default:
		}
		select {
		case <-q.ch:
			q.pend.Add(-1)
			q.drop.Add(1)
			sampledWarn(&q.lastDropWarn, q.log, "peer queue full; dropped oldest mutation (repair will reconcile)")
		default:
		}
	}
}

func (q *peerQueue) run() {
	defer func() { // discard whatever is left; not a drop
		for {
			select {
			case <-q.ch:
				q.pend.Add(-1)
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-q.ctx.Done():
			return
		case m := <-q.ch:
			if err := q.send(q.ctx, q.addr, m); err != nil && q.ctx.Err() == nil {
				q.fail.Add(1)
				sampledWarn(&q.lastFailWarn, q.log, "fanout send failed", "err", err)
			}
			q.pend.Add(-1)
		}
	}
}

func (q *peerQueue) stop() { q.cancel() }

// sampledWarn logs at most once per second per last.
func sampledWarn(last *atomic.Int64, l *slog.Logger, msg string, args ...any) {
	now := time.Now().UnixNano()
	prev := last.Load()
	if now-prev >= int64(time.Second) && last.CompareAndSwap(prev, now) {
		l.Warn(msg, args...)
	}
}
