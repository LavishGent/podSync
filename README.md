# PodSync

PodSync is an embeddable Go library for synchronizing small, short-lived operational state across Kubernetes pods — without Redis, Memcached, or any external cache service.

It is designed for fast local reads, low operational overhead, and eventual consistency across a fleet of application pods.

> **Status:** Active development — Phase 0 (repo foundation) is complete. See [docs/roadmap_v1.md](docs/roadmap_v1.md) for the phased build plan.

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
│          Replication Engine             │
│  Mutation log · fanout · dedup ·        │
│  retries                                │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│          Membership Engine              │
│  DNS seed discovery · gossip/SWIM-style │
│  health                                 │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│          Anti-Entropy Engine            │
│  Shard digests · peer comparison ·      │
│  repair                                 │
└─────────────────────────────────────────┘
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| Gossip-assisted replication | Full-mesh fanout doesn't scale past ~25 pods (600+ directed peer relationships). Gossip keeps fanout sub-linear. |
| Logical versions, not wall-clock | Avoids clock skew bugs. Conflict resolution uses `(counter, nodeID)` — higher counter wins; ties broken by NodeID lexicographically. |
| Tombstones for deletes | A hard-delete would allow stale peers to reintroduce a key during anti-entropy repair. Tombstones are retained for a configurable GC period. |
| Absolute TTL on the wire | Replicating a duration instead of an `ExpiresAt` timestamp causes keys to expire at different times on different pods. PodSync replicates the absolute epoch value. |
| Custom TCP + Protobuf framing | gRPC was rejected due to HTTP/2 overhead on high-frequency small gossip messages. PodSync uses one persistent TCP connection per peer with a lightweight `[4B length][1B type][proto body]` frame. |
| Queue overflow drops mutations | Blocking the caller during peer degradation defeats local-first writes. Anti-entropy is the correctness guarantee; the queue is a best-effort fast path. Drop rates are observable via `podsync_mutations_dropped_total`. |

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
│   ├── storage/        # Sharded map, TTL, tombstones, versioned entries
│   ├── version/        # Logical version counter, conflict resolution
│   ├── node/           # NodeID generation, NodeInfo
│   ├── replication/    # Mutation log, fanout, deduplication
│   ├── membership/     # Peer discovery, SWIM-style health
│   ├── transport/      # TCP connection manager, protobuf framing
│   └── entropy/        # Anti-entropy digest exchange and repair
├── pkg/
│   └── podsync/        # Public API surface
├── proto/              # Protobuf message definitions
├── examples/           # local-two-node, k8s-kind
├── deployments/
│   └── kubernetes/     # Headless Service manifests
└── docs/
    ├── backlog/        # Milestone tickets (MS1-A through MS5-C) and Index
    ├── kanban_board.md # Project kanban tracking board
    ├── roadmap_v1.md   # Full design doc and phased build plan
    └── steps_breakdown_v1.md # 223 granular checklist steps
```

---

## Development

### Prerequisites

To generate protobuf code, you will need the Protocol Buffers compiler (`protoc`) and the Go protobuf plugin installed:

#### macOS
```bash
# Install protoc compiler
brew install protobuf

# Install Go compiler plugin
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

#### Windows
1. Download the pre-compiled `protoc` binary from the [protobuf releases page](https://github.com/protocolbuffers/protobuf/releases) (e.g. `protoc-*-win64.zip`).
2. Extract the archive and add the `bin/` directory to your system's `PATH`.
3. Install the Go compiler plugin:
```powershell
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

Ensure your `$GOPATH/bin` (or `%USERPROFILE%\go\bin` on Windows) is in your system's `PATH` so `protoc` can locate `protoc-gen-go`.

### Commands

Before running tests, ensure you compile the protobuf messages:

```bash
# Generate protobuf code
just proto

# Run tests
just test

# Run tests with race detector
just race

# Run linter
just lint
```


## Documentation

- **[Roadmap & Architecture Design](docs/roadmap_v1.md)**: Explains the project vision, core design decisions, conflict resolution models, and the high-level architecture.
- **[Granular Build Steps](docs/steps_breakdown_v1.md)**: The 223 step-by-step checklist tracking execution details of every phase.
- **[Backlog Index](docs/backlog/Backlog_Index.md)**: The master map of the 15 implementation tickets divided across 5 milestones and 3 tracks.
- **[Kanban Board](docs/kanban_board.md)**: Visual dashboard powered by the Obsidian Kanban plugin tracking ticket status.

---

## License

[MIT](LICENSE)
