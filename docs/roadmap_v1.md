# PodSync Roadmap

> **PodSync** is an embeddable Go library for synchronizing small, short-lived operational state across Kubernetes pods without requiring an external cache service like Redis.

PodSync is designed for fast local reads, low operational overhead, and eventual consistency across a fleet of application pods.

---

## 1. Project Vision

Modern Kubernetes services often need to share small pieces of runtime state across replicas:

- Feature flags
- Runtime configuration
- Permission snapshots
- Temporary coordination metadata
- Presence-like state
- Short-lived operational cache entries

The default solution is usually to introduce Redis, Memcached, or another external system. That works, but it also adds infrastructure, network hops, cost, operational complexity, and another dependency to manage.

PodSync explores a different model:

> Keep the state inside the application process and replicate it directly between pods.

PodSync is **not** trying to replace Redis for durable caching, queues, sessions, or large datasets. Instead, it focuses on ephemeral, low-latency, eventually consistent state that can safely live inside process memory.

---

## 2. Core Design Philosophy

PodSync should be:

### Embeddable

PodSync should be imported as a Go library and run inside the host application process.

```go
cache, err := podsync.New[string, FeatureFlag](podsync.Config{
    Namespace:   "default",
    ServiceName: "podsync-headless",
    Port:        9090,
    Shards:      64,
})
```

### Fast on the Read Path

Reads should hit local memory only.

```go
flag, ok := cache.Get("checkout-v2")
```

No network call should be required for normal reads.

### Eventually Consistent

PodSync favors availability and partition tolerance over strict consistency.

During a network partition, each pod continues serving reads and accepting writes. When the partition heals, peers reconcile state using deterministic conflict resolution and anti-entropy repair.

### Kubernetes-Native

PodSync should integrate naturally with Kubernetes:

- Headless Services for initial peer discovery
- Readiness probes for hydration state
- Pod lifecycle awareness
- Graceful peer leave behavior when possible

### Operationally Honest

PodSync should clearly document what it is and is not for.

Good use cases:

- Feature flags
- Runtime config
- Small permission snapshots
- Ephemeral metadata
- Short-lived local state

Bad use cases:

- Durable sessions
- Job queues
- Database replacement
- Large object storage
- Strictly consistent financial/accounting state

---

## 3. High-Level Architecture

PodSync is split into five major internal systems:

```text
┌─────────────────────────────────────────────────────────────┐
│                        Host Application                     │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                         PodSync API                         │
│                                                             │
│  Get / Set / Delete / Ready / Stats / Close                 │
└───────────────┬─────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────┐
│                     Local Storage Engine                    │
│                                                             │
│  Sharded map + TTL + tombstones + versioned entries          │
└───────────────┬─────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────┐
│                    Replication Engine                       │
│                                                             │
│  Mutation log + fanout + deduplication + retries             │
└───────────────┬─────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────┐
│                    Membership Engine                        │
│                                                             │
│  Kubernetes DNS seed discovery + gossip/SWIM-style health    │
└───────────────┬─────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────┐
│                    Anti-Entropy Engine                      │
│                                                             │
│  Shard digests + peer comparison + repair                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 4. Key Architectural Decisions

### 4.1 Use Gossip-Assisted Replication

PodSync should not rely on full-mesh replication as the long-term default.

A full mesh becomes expensive as the cluster grows:

```text
5 pods   = 20 directed peer relationships
25 pods  = 600 directed peer relationships
100 pods = 9,900 directed peer relationships
```

Instead, PodSync should use a hybrid model:

```text
Kubernetes DNS       -> initial peer discovery
Gossip/SWIM-style    -> membership and failure detection
Direct RPC           -> hydration, snapshots, and important data exchange
Gossip fanout        -> scalable mutation spread
Anti-entropy         -> background repair
```

### 4.2 Reads Are Always Local

PodSync is useful because application reads do not need to leave the process.

```text
Request -> App -> PodSync local memory -> Response
```

This makes PodSync valuable for hot operational state.

### 4.3 Writes Are Local-First

A write should update local memory immediately and then replicate asynchronously.

```text
Set(key, value)
    1. Validate input
    2. Assign version
    3. Update local shard
    4. Append mutation to replication queue
    5. Return to caller
```

Network replication must not block the application request path by default.

### 4.4 Use Logical Versions, Not Wall-Clock Timestamps

PodSync should not depend on perfectly synchronized system clocks.

Each mutation should receive a deterministic logical version:

```go
type Version struct {
    Counter uint64
    NodeID  string
}
```

Conflict resolution:

1. Higher counter wins.
2. If counters tie, higher NodeID wins deterministically.

This gives PodSync a simple Last-Write-Wins model without relying on wall-clock precision.

### 4.5 Deletes Require Tombstones

A deleted key cannot simply disappear locally. Otherwise, a stale peer could later reintroduce it during anti-entropy repair.

PodSync should store tombstones:

```go
type Entry[V any] struct {
    Value     V
    Version   Version
    ExpiresAt int64
    Deleted   bool
}
```

Tombstones should be retained for a configurable period before garbage collection.

### 4.6 TTL Should Replicate as an Absolute Expiration

Do not replicate only a duration.

Avoid:

```go
TTL: 5 * time.Minute
```

Prefer:

```go
ExpiresAt: 1792877400
```

All peers should receive the same absolute expiration value so keys expire consistently across the cluster.

### 4.7 Transport Protocol: Custom TCP + Protobuf Framing

PodSync uses a custom TCP transport with protobuf-encoded messages rather than gRPC.

Each frame is structured as:

```text
[4-byte length][1-byte message type][protobuf body]
```

One persistent TCP connection is maintained per peer and reused for all message types. This approach:

- Adds zero external runtime dependencies.
- Minimizes per-message overhead on the hot mutation fanout path.
- Gives full control over connection management, batching, and framing.
- Remains compatible with protobuf codegen for type safety.

gRPC was considered and rejected due to HTTP/2 overhead on high-frequency small gossip messages and the desire to keep PodSync as lightweight as possible for an embeddable library.

### 4.8 NodeID Generation

Each node generates a random UUID at process startup as its NodeID.

```go
nodeID := NodeID(uuid.New().String())
```

Pod IPs and hostnames were rejected:

- Pod IPs are not stable across restarts (explicitly noted in section 6.1).
- Hostnames are stable for StatefulSets but not for Deployments, creating an asymmetric identity model.

A UUID-per-startup means a restarted pod always gets a new identity. The old NodeID is eventually marked Dead by the membership engine. The `Incarnation` field in `NodeInfo` is reserved for future use if same-logical-node tracking becomes necessary.

### 4.9 Replication Queue Overflow Behavior

When the replication queue is full, PodSync **drops the mutation and increments `podsync_mutations_dropped_total`**.

The local write always succeeds. Only replication fanout is affected.

Drops are tolerated by design: anti-entropy repair is the correctness guarantee for eventual convergence, not the replication queue. The queue is a best-effort fast path. Operators can observe drop rates via metrics and tune queue depth accordingly.

Blocking the caller was rejected because it would stall application code during peer degradation, defeating the purpose of local-first writes.

Returning an error was rejected because the local write already succeeded — a partial-success error would create a confusing contract.

A future `WithSync()` option may allow opt-in quorum acknowledgment for callers who require it.

### 4.10 Generic Values Need a Codec

Go generics make the local API clean, but network replication still needs serialization.

PodSync should use a pluggable codec interface:

```go
type Codec[V any] interface {
    Marshal(V) ([]byte, error)
    Unmarshal([]byte) (V, error)
}
```

Initial codecs:

- JSON
- Protobuf-compatible bytes
- Raw bytes

---

## 5. Proposed Public API

### 5.1 Basic Usage

```go
type FeatureFlag struct {
    Enabled        bool
    RolloutPercent int
}

cache, err := podsync.New[string, FeatureFlag](podsync.Config{
    Namespace:   "default",
    ServiceName: "podsync-headless",
    Port:        9090,
    Shards:      64,
    DefaultTTL:  5 * time.Minute,
    Codec:       podsync.JSONCodec[FeatureFlag]{},
})
if err != nil {
    log.Fatal(err)
}

cache.Set(ctx, "checkout-v2", FeatureFlag{
    Enabled:        true,
    RolloutPercent: 25,
}, podsync.WithTTL(10*time.Minute))

flag, ok := cache.Get("checkout-v2")
```

### 5.2 Readiness Probe Integration

```go
http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
    if cache.Ready() {
        w.WriteHeader(http.StatusOK)
        return
    }

    w.WriteHeader(http.StatusServiceUnavailable)
})
```

### 5.3 Graceful Shutdown

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := cache.Close(ctx); err != nil {
    log.Printf("podsync shutdown error: %v", err)
}
```

---

## 6. Internal Data Model

### 6.1 Node Identity

Pod IPs are not stable enough to serve as long-term identity.

Each node generates a random UUID at startup as its NodeID (see section 4.8). A restarted pod always receives a new identity. The old identity is eventually evicted by the membership engine.

```go
type NodeID string

type NodeInfo struct {
    ID          NodeID
    Addr        string
    StartedAt   int64
    Incarnation uint64  // reserved for future same-logical-node tracking
}
```

### 6.2 Mutation Envelope

```go
type MutationOp string

const (
    MutationSet    MutationOp = "SET"
    MutationDelete MutationOp = "DELETE"
    MutationEvict  MutationOp = "EVICT"
)

type Mutation struct {
    ID        string
    Origin    NodeID
    Key       []byte
    Value     []byte
    Op        MutationOp
    Version   Version
    ExpiresAt int64
}
```

### 6.3 Conflict Resolution

Every replicated entry should carry a version.

```go
func ShouldApplyIncoming(local Version, incoming Version) bool {
    if incoming.Counter > local.Counter {
        return true
    }

    if incoming.Counter < local.Counter {
        return false
    }

    return incoming.NodeID > local.NodeID
}
```

### 6.4 Deduplication

Gossip-style fanout can cause duplicate mutation delivery.

Each node should maintain a bounded deduplication cache:

```go
type SeenMutationCache interface {
    Seen(id string) bool
    MarkSeen(id string)
}
```

The cache should evict old mutation IDs after a configurable retention period.

---

## 7. Roadmap Phases

## Phase 0: Repository Foundation

Goal: Set up the repo so development stays organized from the beginning.

### Tasks

- [ ] Initialize Go module.
- [ ] Add project README.
- [ ] Add ROADMAP.md.
- [ ] Add LICENSE.
- [ ] Add Makefile.
- [ ] Add GitHub Actions or GitLab CI.
- [ ] Add basic lint/test workflow.
- [ ] Add package structure.
- [ ] Add example application folder.

### Suggested Structure

```text
podsync/
├── cmd/
│   └── examples/
├── internal/
│   ├── storage/
│   ├── membership/
│   ├── replication/
│   ├── entropy/
│   └── transport/
├── pkg/
│   └── podsync/
├── proto/
├── examples/
│   ├── local-two-node/
│   └── k8s-kind/
├── deployments/
│   └── kubernetes/
├── docs/
├── go.mod
├── Makefile
├── README.md
└── ROADMAP.md
```

### Acceptance Criteria

- [ ] `go test ./...` runs successfully.
- [ ] CI runs tests on pull requests.
- [ ] README explains the project in one paragraph.
- [ ] ROADMAP.md describes the phased build plan.

---

## Phase 1: Local Storage Engine

Goal: Build the local in-memory cache before adding networking.

### Features

- [ ] Generic typed cache: `Cache[K comparable, V any]`
- [ ] Sharded map storage
- [ ] Configurable shard count
- [ ] `Get`
- [ ] `Set`
- [ ] `Delete`
- [ ] TTL expiration
- [ ] Tombstones
- [ ] Basic stats
- [ ] Benchmarks

### Design Notes

The storage engine should avoid a single global lock.

Instead of this:

```go
type Store struct {
    mu   sync.RWMutex
    data map[string]Entry
}
```

Use sharding:

```go
type Store struct {
    shards []Shard
}

type Shard struct {
    mu   sync.RWMutex
    data map[string]Entry
}
```

### Acceptance Criteria

- [ ] Concurrent reads and writes are race-free.
- [ ] `go test -race ./...` passes.
- [ ] Benchmarks exist for `Get`, `Set`, and `Delete`.
- [ ] Expired keys are not returned.
- [ ] Deleted keys cannot be resurrected by stale versions.
- [ ] Shard selection is deterministic.

---

## Phase 2: Versioning and Conflict Resolution

Goal: Make local state safe for distributed reconciliation.

### Features

- [ ] NodeID generation
- [ ] Logical version counter
- [ ] Version comparison
- [ ] Last-Write-Wins register semantics
- [ ] Tombstone-aware conflict resolution
- [ ] Tests for stale updates
- [ ] Tests for conflicting writes

### Acceptance Criteria

- [ ] Newer versions replace older versions.
- [ ] Older versions are ignored.
- [ ] Tie-breaking is deterministic.
- [ ] Deletes win over older sets.
- [ ] Newer sets can replace older tombstones.
- [ ] Conflict resolution behavior is documented.

---

## Phase 3: Serialization and Codecs

Goal: Support generic values over the network.

### Features

- [ ] Define `Codec[V any]`.
- [ ] Implement JSON codec.
- [ ] Implement raw bytes codec.
- [ ] Add codec errors.
- [ ] Add tests for serialization/deserialization.
- [ ] Make codec required in distributed mode.

### Acceptance Criteria

- [ ] User-defined types can be serialized and replicated.
- [ ] Codec failures do not corrupt local state.
- [ ] Codec behavior is documented.
- [ ] JSON codec works in example app.

---

## Phase 4: Transport Layer

Goal: Create the network foundation for peer-to-peer communication using custom TCP + protobuf framing (see section 4.7).

### Features

- [ ] Define protobuf message types.
- [ ] Implement TCP frame encoder/decoder (4-byte length prefix + 1-byte message type + proto body).
- [ ] Implement per-peer connection manager (persistent, reused connections).
- [ ] Implement injectable transport interface for testability.
- [ ] Add mutation send handler (`ApplyMutation`).
- [ ] Add snapshot request handler (`RequestSnapshot`).
- [ ] Add digest exchange handler (`ExchangeDigest`).
- [ ] Add delta request handler (`RequestDelta`).
- [ ] Add ping/health handler (`Ping`).
- [ ] Add local two-node example.
- [ ] Add retry/backoff behavior.
- [ ] Add request timeouts.

### Initial RPC Concepts

```text
ApplyMutation(Mutation) -> Ack
RequestSnapshot(SnapshotRequest) -> SnapshotResponse
ExchangeDigest(DigestRequest) -> DigestResponse
RequestDelta(DeltaRequest) -> DeltaResponse
Ping(PingRequest) -> PingResponse
```

### Acceptance Criteria

- [ ] Two local processes can exchange a `SET` mutation.
- [ ] Two local processes can exchange a `DELETE` mutation.
- [ ] RPC calls have timeouts.
- [ ] Failed RPCs do not block local cache operations.
- [ ] Transport code is isolated from storage code.
- [ ] Transport can be swapped via interface for in-process testing.

---

## Phase 5: Replication Engine

Goal: Replicate local mutations to peers asynchronously.

### Features

- [ ] Buffered mutation queue
- [ ] Worker pool
- [ ] Peer selection
- [ ] Fanout replication
- [ ] Deduplication cache
- [ ] Retry with backoff
- [ ] Replication metrics
- [ ] Queue overflow: drop mutation + increment `podsync_mutations_dropped_total` (see section 4.9)

### Write Path

```text
cache.Set()
    -> apply locally
    -> create mutation
    -> enqueue mutation
    -> return to caller

replication worker
    -> read mutation from queue
    -> select peers
    -> send mutation
    -> mark success/failure
```

### Acceptance Criteria

- [ ] Local writes do not require network success.
- [ ] Mutations are replicated to selected peers.
- [ ] Duplicate mutations are ignored.
- [ ] Failed peer sends are retried or surfaced in metrics.
- [ ] Queue overflow behavior is explicit and documented.

---

## Phase 6: Membership Engine

Goal: Discover peers and track cluster health.

### Features

- [ ] Kubernetes headless service DNS lookup
- [ ] Static peer list for local development
- [ ] Gossip/SWIM-style membership loop
- [ ] Peer health states
- [ ] Join detection
- [ ] Leave/failure detection
- [ ] Peer manager API
- [ ] Reconnect behavior

### Peer States

```text
Alive
Suspect
Dead
Left
```

### Acceptance Criteria

- [ ] Nodes can discover peers from a static config.
- [ ] Nodes can discover peers from Kubernetes DNS.
- [ ] New peers are added to the peer manager.
- [ ] Dead peers are eventually removed or marked unavailable.
- [ ] Membership changes are logged.
- [ ] Membership does not require full mesh connections.

---

## Phase 7: Hydration and Readiness

Goal: Prevent empty new pods from serving traffic before they have state.

### Features

- [ ] Bootstrapping state
- [ ] Snapshot request
- [ ] Snapshot ingestion
- [ ] Mutation buffering during hydration
- [ ] Ready state
- [ ] Readiness API
- [ ] Kubernetes readiness example

### Lifecycle

```text
Starting
    -> DiscoveringPeers
        -> [no peers found] -> Ready (first node; empty state is valid)
        -> [peers found]    -> Bootstrapping
                                -> ApplyingSnapshot
                                -> CatchingUp
                                -> Ready
```

### Hydration Race to Handle

A peer may continue receiving writes while sending a snapshot.

The hydration protocol should support:

```text
1. Snapshot request starts.
2. Source peer records current version watermark.
3. Source peer sends snapshot.
4. New node applies snapshot.
5. New node requests mutations after watermark.
6. New node becomes Ready.
```

### Acceptance Criteria

- [ ] New node starts as not ready.
- [ ] If no peers are found, node transitions directly to Ready with empty state.
- [ ] New node can request a snapshot from a peer.
- [ ] New node applies snapshot successfully.
- [ ] New node becomes ready after hydration.
- [ ] Readiness endpoint reports correct state.
- [ ] Failed hydration retries or fails clearly.

---

## Phase 8: Anti-Entropy Repair

Goal: Make the system self-healing after missed mutations, restarts, or network issues.

### Features

- [ ] Shard digests
- [ ] Periodic peer comparison
- [ ] Delta request
- [ ] Stale key repair
- [ ] Missing key repair
- [ ] Tombstone repair
- [ ] Repair metrics

### V1 Strategy: Shard Digests

Instead of implementing Merkle trees immediately, start with shard-level digests.

```go
type ShardDigest struct {
    ShardID int
    Hash    uint64
    Count   int
}
```

Repair flow:

```text
1. Pick random peer.
2. Exchange shard digests.
3. Identify mismatched shards.
4. Exchange key/version metadata for mismatched shards.
5. Pull missing or stale entries.
6. Apply using normal conflict resolution.
```

### Acceptance Criteria

- [ ] Nodes detect mismatched shards.
- [ ] Nodes can repair missing keys.
- [ ] Nodes can repair stale keys.
- [ ] Tombstones prevent deleted keys from being resurrected.
- [ ] Repair loop does not block application reads/writes.
- [ ] Repair behavior is covered by integration tests.

---

## Phase 9: Kubernetes Deployment Example

Goal: Prove PodSync works in a real local Kubernetes environment.

### Features

- [ ] Kind or Minikube deployment
- [ ] Headless Service manifest
- [ ] Example app Deployment
- [ ] Readiness probe config
- [ ] Horizontal scaling demo
- [ ] Logs showing discovery and replication
- [ ] Basic load test script

### Example Kubernetes Pieces

```yaml
apiVersion: v1
kind: Service
metadata:
  name: podsync-headless
spec:
  clusterIP: None
  selector:
    app: podsync-example
  ports:
    - name: podsync
      port: 9090
      targetPort: 9090
```

### Acceptance Criteria

- [ ] Example runs in Kind or Minikube.
- [ ] Pods discover each other.
- [ ] New pods hydrate before becoming ready.
- [ ] Mutations replicate across pods.
- [ ] Scaling replicas up/down works.
- [ ] Documentation explains how to run the demo.

---

## Phase 10: Observability

Goal: Make PodSync understandable in production-like environments.

### Features

- [ ] Structured logs
- [ ] Metrics interface
- [ ] Replication queue depth
- [ ] Peer count
- [ ] Failed sends
- [ ] Successful sends
- [ ] Hydration duration
- [ ] Repair duration
- [ ] Conflict count
- [ ] Expired key count
- [ ] Tombstone count

### Suggested Metrics

```text
podsync_peers_alive
podsync_peers_suspect
podsync_mutations_enqueued_total
podsync_mutations_replicated_total
podsync_mutations_dropped_total
podsync_replication_queue_depth
podsync_hydration_duration_seconds
podsync_anti_entropy_repairs_total
podsync_conflicts_total
podsync_tombstones_total
podsync_keys_total
```

### Acceptance Criteria

- [ ] Metrics can be exported.
- [ ] Logs include node ID and peer ID where relevant.
- [ ] Replication failures are visible.
- [ ] Hydration failures are visible.
- [ ] Anti-entropy repairs are visible.

---

## Phase 11: Performance and Reliability Testing

Goal: Validate PodSync under realistic concurrent workloads.

### Tests

- [ ] Local benchmark tests
- [ ] Multi-node integration tests
- [ ] Race tests
- [ ] Network failure simulation
- [ ] Peer restart simulation
- [ ] Scale-up hydration test
- [ ] Scale-down peer removal test
- [ ] High write-rate test
- [ ] High read-rate test
- [ ] Queue overflow test
- [ ] TTL expiration test
- [ ] Tombstone retention test

### Benchmark Targets

Initial targets can be rough and adjusted later.

```text
Local Get p99:        under 1ms
Local Set p99:        under 2ms
Hydration:            documented by dataset size
Replication latency:  documented by cluster size
Repair latency:       documented by mismatched shard count
```

### Acceptance Criteria

- [ ] Performance benchmarks are repeatable.
- [ ] Race detector passes.
- [ ] Tests cover concurrent access.
- [ ] Tests cover stale mutation handling.
- [ ] Tests cover peer failures.
- [ ] README includes benchmark results.

---

## Phase 12: Developer Experience

Goal: Make PodSync easy to understand, install, and demo.

### Features

- [ ] Quickstart guide
- [ ] Local two-node demo
- [ ] Kubernetes demo
- [ ] API docs
- [ ] Architecture docs
- [ ] Failure-mode docs
- [ ] Examples for common use cases

### Example Docs

- `docs/architecture.md`
- `docs/conflict-resolution.md`
- `docs/kubernetes.md`
- `docs/hydration.md`
- `docs/anti-entropy.md`
- `docs/failure-modes.md`
- `docs/benchmarks.md`

### Acceptance Criteria

- [ ] A new developer can run the local demo in under 5 minutes.
- [ ] A new developer can run the Kubernetes demo with documented commands.
- [ ] Public API is documented.
- [ ] Failure modes are documented honestly.

---

## 8. Future Ideas

These are intentionally not part of the first stable version.

### 8.1 CRDT Data Types

Add specialized replicated types beyond LWW registers:

- Grow-only counter
- PN-counter
- Observed-remove set
- Distributed token bucket
- Bounded counters

### 8.2 Pluggable Membership Backends

Support alternate membership providers:

- Built-in DNS + gossip
- HashiCorp memberlist adapter
- Kubernetes API watcher
- Static config
- Consul

### 8.3 Merkle Tree Repair

Replace or extend shard digest repair with Merkle tree-based reconciliation for larger datasets.

### 8.4 Persistent Warm Start

Optionally write local snapshots to disk so restarted pods can warm-start before cluster hydration.

### 8.5 Security

Add optional:

- mTLS
- Shared secret authentication
- Message signing
- Peer allowlists

### 8.6 Admin UI

A small dashboard showing:

- Current peers
- Key count
- Replication stats
- Conflict stats
- Repair stats
- Hydration status

---

## 9. Non-Goals

PodSync should not attempt to be everything.

The following are non-goals for the core project:

- Replacing Redis entirely
- Durable storage
- Strong consistency
- SQL-like querying
- Large object replication
- Distributed transactions
- Job queues
- Ordered event streaming
- Exactly-once delivery
- Cross-region database replication

Being explicit about non-goals makes the project easier to trust.

---

## 10. Open Questions

These should be answered as the implementation evolves.

### Replication

- ~~What should happen when the mutation queue is full?~~ **Resolved: Drop mutation + increment metric (section 4.9).**
- ~~Should PodSync drop mutations, block, or return an error?~~ **Resolved: Drop + metric (section 4.9).**
- Should users be able to choose sync vs async write behavior? *(Future: opt-in `WithSync()` option.)*
- What is the default fanout count?

### Membership

- How aggressive should failure detection be?
- How should suspected peers be handled during replication?
- How long should dead peers remain in the peer table?

### Hydration

- How should a new pod choose the best peer to hydrate from?
- Should hydration use one peer or multiple peers?
- What is the maximum snapshot size PodSync should support?

### Storage

- What is the default tombstone retention period? *(Recommendation: 24h conservative default to reduce the risk of resurrecting deleted keys after long partitions. Document in `docs/failure-modes.md`.)*
- Should max memory be enforced by entry count, byte size, or both?
- Should eviction be LRU, random, TTL-only, or configurable?

### API

- ~~Should `Set` return after local write only or after replication quorum?~~ **Resolved: Local write only by default (section 4.9). Quorum opt-in is a future concern.**
- Should PodSync expose strongly typed replicated collections?
- How much of the internal state should users be able to inspect?

---

## 11. Initial Milestone Plan

### Milestone 1: Local Cache

Deliverable:

- Typed sharded cache
- TTL
- Delete
- Tombstones
- Benchmarks

Status:

- [ ] Not started

### Milestone 2: Two-Node Local Replication

Deliverable:

- Two local processes
- Basic mutation exchange
- Conflict resolution
- Deduplication

Status:

- [ ] Not started

### Milestone 3: Kubernetes Discovery

Deliverable:

- Headless service DNS discovery
- Peer manager
- Local mocked DNS tests

Status:

- [ ] Not started

### Milestone 4: Hydration

Deliverable:

- Snapshot transfer
- Bootstrapping lifecycle
- Readiness state

Status:

- [ ] Not started

### Milestone 5: Anti-Entropy

Deliverable:

- Shard digests
- Random peer repair
- Missing/stale key reconciliation

Status:

- [ ] Not started

### Milestone 6: Kind Demo

Deliverable:

- Deploy sample app to Kind
- Scale replicas
- Show state convergence
- Document setup

Status:

- [ ] Not started

---

## 12. Definition of Done for v0.1

PodSync v0.1 should be considered complete when:

- [ ] A Go application can embed PodSync.
- [ ] The local cache is generic and type-safe.
- [ ] Reads are served from local memory.
- [ ] Writes apply locally and replicate asynchronously.
- [ ] Multiple local processes can converge on shared state.
- [ ] Kubernetes pods can discover each other through a headless service.
- [ ] A new pod can hydrate state before becoming ready.
- [ ] Missed updates are eventually repaired.
- [ ] Basic metrics and logs exist.
- [ ] The README clearly explains what PodSync is and is not.

---

## 13. Positioning Statement

PodSync is best described as:

> **Peer-to-peer ephemeral state sync for Go services running in Kubernetes.**

Longer version:

> **PodSync is an embeddable Go library that lets Kubernetes pods share small, short-lived operational state directly with each other, giving applications fast local reads and eventual consistency without requiring a separate cache service.**

This positioning should guide every design decision.


