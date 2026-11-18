# PodSync

PodSync is an embeddable Go library for synchronizing small, short-lived operational state across Kubernetes pods — without Redis, Memcached, or any external cache service.

It is designed for fast local reads, low operational overhead, and eventual consistency across a fleet of application pods.

> **Status:** Active development. The local storage engine is done; networking is not. The plan was rescoped in 2026-11 to a six-ticket v0.1 with zero non-stdlib dependencies; see [docs/rescope_v0.1.md](docs/rescope_v0.1.md) and the [backlog](docs/backlog/Backlog_Index.md). Parts of this README still describe the original design and will be rewritten in T6.

---

## The Problem

Modern Kubernetes services often need to share small pieces of runtime state across replicas:

- Feature flags
- Runtime configuration
- Permission snapshots
- Temporary coordination metadata
- Short-lived operational cache entries

The default answer is Redis. Redis works — but it also adds infrastructure, network hops, cost, operational complexity, and another dependency to manage. For _ephemeral, low-latency state that can tolerate eventual consistency_, that trade-off is often not worth it.

---

## The Solution

PodSync keeps state inside the application process and replicates it directly between pods.

```go
cache, err := podsync.New[string, FeatureFlag](podsync.Config{
    Namespace:   "default",
    ServiceName: "podsync-headless",
    Port:        9090,
    Shards:      64,
    DefaultTTL:  5 * time.Minute,
    Codec:       podsync.JSONCodec[FeatureFlag]{},
})

// Reads are purely local — zero network I/O
flag, ok := cache.Get("checkout-v2")

// Writes update local memory immediately, replicate asynchronously
cache.Set(ctx, "checkout-v2", FeatureFlag{Enabled: true, RolloutPercent: 25})
```

---

## Design Philosophy

### Reads are always local

No network call is required on the read path. Application code reads directly from process memory.

```
Request → App → PodSync local memory → Response
```

### Writes are local-first

A `Set` updates local state immediately and enqueues replication asynchronously. The caller is never blocked on peer acknowledgment.

### Eventually consistent

PodSync favors availability and partition tolerance over strict consistency. During a network partition, each pod continues serving reads and accepting writes. When the partition heals, peers reconcile using deterministic conflict resolution and anti-entropy repair.

### Kubernetes-native

- Headless Services for initial peer discovery via DNS
- Readiness probe integration for hydration state
- Pod lifecycle awareness and graceful leave behavior

---

## Architecture

PodSync is organized into five internal systems:

```
┌─────────────────────────────────────────┐
│             Host Application            │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│              PodSync API                │
│   Get / Set / Delete / Ready / Close    │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│          Local Storage Engine           │
│  Sharded map · TTL · tombstones ·       │
│  versioned entries                      │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│              Replication                │
│  Outbox · direct fanout to every peer   │
│  · Lamport-clock LWW merge              │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│               Peer Set                  │
│  Headless Service DNS, re-resolved on   │
│  a timer · kubelet owns health          │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│                 Repair                  │
│  Periodic full-state exchange with one  │
│  random peer                            │
└─────────────────────────────────────────┘
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| Direct fanout, no gossip | Target fleets are 3 to 25 pods. Sending each mutation straight from the origin to every peer is cheap at that size and needs no dedup or routing. Gossip is a v0.2 item if a real workload outgrows this. |
| Lamport clock versions, not wall-clock | Avoids clock skew bugs. Every node advances its clock to `max(local, received)` on each message, so versions from different nodes are comparable even after a restart. Higher clock wins; ties broken by NodeID lexicographically. |
| Tombstones for deletes | A hard-delete would allow stale peers to reintroduce a key during anti-entropy repair. Tombstones are retained for a configurable GC period. |
| Absolute TTL on the wire | Replicating a duration instead of an `ExpiresAt` timestamp causes keys to expire at different times on different pods. PodSync replicates the absolute epoch value. |
| Plain TCP + `encoding/gob` | The wire format is internal and never crosses a language boundary, so protobuf and gRPC bought nothing but a toolchain. One persistent TCP connection per peer carries `[4B length][1B type][gob body]` frames. No non-stdlib dependencies. |
| Queue overflow drops mutations | Blocking the caller during peer degradation defeats local-first writes. Periodic repair is the correctness guarantee; the queue is a best-effort fast path. Drop counts are visible in `Stats()`. |

---

## Public API

### Initialization

```go
cache, err := podsync.New[string, FeatureFlag](podsync.Config{
    Namespace:   "default",
    ServiceName: "podsync-headless",
    Port:        9090,
    Shards:      64,
    DefaultTTL:  5 * time.Minute,
    Codec:       podsync.JSONCodec[FeatureFlag]{},
})
```

### Core operations

```go
// Write — local-first, async replication
cache.Set(ctx, "checkout-v2", FeatureFlag{Enabled: true}, podsync.WithTTL(10*time.Minute))

// Read — local memory only, no network I/O
flag, ok := cache.Get("checkout-v2")

// Delete — stores tombstone, replicates asynchronously
cache.Delete(ctx, "checkout-v2")
```

### Readiness probe

```go
http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
    if cache.Ready() {
        w.WriteHeader(http.StatusOK)
        return
    }
    w.WriteHeader(http.StatusServiceUnavailable)
})
```

### Graceful shutdown

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := cache.Close(ctx); err != nil {
    log.Printf("podsync shutdown error: %v", err)
}
```

---

## Security

The replication port has no authentication by default: anything that can reach it can write any key. Two mitigations:

- **NetworkPolicy (recommended).** Admit the replication port only from your own pods. See [`deployments/kubernetes/networkpolicy.yaml`](deployments/kubernetes/networkpolicy.yaml).
- **Optional shared-secret handshake (off by default).** Set a secret of at least 32 bytes, identical on every node, and each connection must prove it knows the secret (HMAC-SHA256 challenge/response) before any frame is processed. A node with a secret and a node without one refuse each other immediately (`handshake mode mismatch`).

The handshake proves the peer knows the secret. It does **not** encrypt traffic, authenticate frames after the handshake, or bind the handshake to an address, so an on-path attacker, or one who can make a real peer dial it, can relay a handshake. For confidentiality and stronger identity, use a service mesh (mTLS).

## Failure Modes

- **Repair is the safety net.** Fanout is best effort: a full queue drops its oldest mutation, a send can fail, a partition can last. Every `RepairInterval` (default 30s, jittered ±10%) each node pulls one random peer's state (and pushes its own when it holds fewer than `MaxStateEntries / 4` entries), so dropped fanout converges within a few intervals. The first tick after a node turns Ready fires after `FirstRepairDelay` (default 15s).
- **A partition longer than `TombstoneTTL` can resurrect deleted keys.** Once a tombstone is swept, a peer that missed the delete still holds the old value and repair copies it back. Raise `TombstoneTTL` if that matters for your workload.
- **A departed pod's entries live until their TTL.** There is no eviction on departure; entries written by a pod that left remain on the others until they expire or are overwritten.
- **Operating envelope.** Repair and hydration move full state, one shard page at a time. Above `MaxStateEntries` (default 50k) the node logs a warning each round; see the T5 ticket for encode/decode timings.

---

## What PodSync Is NOT For

PodSync is explicit about its scope. Do not use it for:

- **Durable sessions** — data is in-process memory; pod restarts lose local state
- **Job queues** — no ordering, delivery, or durability guarantees
- **Database replacement** — no persistence layer
- **Large object storage** — designed for small, hot operational state
- **Strictly consistent state** — financial, accounting, or transactional workloads require strong consistency

---

## Project Structure

```
podsync/
├── internal/
│   ├── clock/          # Clock interface, SystemClock, CoarseClock, FakeClock
│   └── storage/        # Sharded map, TTL, tombstones, versioned entries
├── pkg/
│   └── podsync/        # Public API surface (T6)
└── docs/
    ├── rescope_v0.1.md # Why the plan changed and what v0.1 is
    ├── backlog/        # T1 to T6 tickets, plus archive/ of the original plan
    ├── kanban_board.md # Obsidian Kanban tracking board
    └── roadmap_v1.md   # Original design document (historical)
```

Packages planned by ticket: `internal/version` and `internal/node` (T1), `internal/wire` (T2), `internal/transport` and `internal/peers` (T3), `examples/` and `deployments/` (T6).

---

## Development

Go 1.26 or newer and [`just`](https://github.com/casey/just). There is no code generation step.

```bash
# Run tests
just test

# Run tests with race detector
just race

# Run linter
just lint
```


## Documentation

- **[Rescope v0.1](docs/rescope_v0.1.md)**: what v0.1 is, the design decisions that changed, and the operating envelope.
- **[Backlog Index](docs/backlog/Backlog_Index.md)**: the six tickets that deliver v0.1, each with its definition of done.
- **[Kanban Board](docs/kanban_board.md)**: ticket status, for the Obsidian Kanban plugin.
- **[Roadmap v1](docs/roadmap_v1.md)**: the original design document, kept for its reasoning. Historical.

---

## License

[MIT](LICENSE)
