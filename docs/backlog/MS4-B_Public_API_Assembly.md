# MS4-B: Public API Assembly & Demos

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]]

## Overview
Assemble all internal packages (Storage, Versioning, Node ID, Transport, Replication, Membership, Hydration, Entropy) into a clean public-facing API (`Cache[K, V]`). Create a configuration system with defaults and build a local two-node in-process demo simulating peer cluster convergence.

- **Milestone**: 4 (Consistency & Deploy)
- **Track**: B (Developer B)
- **Status**: Ready
- **Dependencies**: [[MS1-A_Repo_Storage|MS1-A]], [[MS1-B_Versioning_Identity|MS1-B]], [[MS1-C_Codecs_Protobuf|MS1-C]], [[MS2-A_TCP_Framing_Server|MS2-A]], [[MS2-B_Membership_Discovery|MS2-B]], [[MS2-C_Replication_Buffers|MS2-C]], [[MS3-A_Replication_Worker|MS3-A]], [[MS3-B_SWIM_Probing|MS3-B]], [[MS3-C_Hydration_Lifecycle|MS3-C]]

## Phase Reference
This ticket implements:
- [[steps_breakdown_v1#Phase 12 — Public API Assembly|Phase 12 — Public API Assembly]] (Steps 177–189)

## Detailed Steps

### Configuration & Public Cache Client
1. Create `pkg/podsync/config.go`:
   - Define a public `Config` struct containing tuning fields: `NodeAddr`, `Peers` (static list), `Fanout`, `ProbeInterval`, `RepairInterval`, `TombstoneRetention`, and logical default values for each.
2. Create `pkg/podsync/cache.go`:
   - Define the main wrapper `Cache[K comparable, V any]` struct.
   - Implement `New[K comparable, V any](cfg Config) (*Cache[K, V], error)`:
     - Generate a Node ID.
     - Instantiate internal components: `Store`, `MutationQueue`, `Worker`, `MembershipManager`, `LifecycleManager`, and `RepairLoop`.
     - Spin up background goroutines for workers, probing, and repair loops.
     - Initiate peer discovery and start the hydration protocol.
   - Implement client API methods:
     - `Get(key K) (V, bool)`: delegates directly to the underlying `Store` (no top-level locking).
     - `Set(ctx context.Context, key K, value V, opts ...Option) error`: writes locally (generating version and mutation envelope) and enqueues to the replication queue.
     - `Delete(ctx context.Context, key K) error`: marks local tombstone and enqueues to the replication queue.
     - `Ready() bool`: calls `LifecycleManager.IsReady()`.
     - `Stats() CacheStats`: aggregate store, queue, and peer registry stats.
     - `Close(ctx context.Context) error`: broadcast `Left` state to peers, drain replication queue up to context deadline, shut down all background routines, and close transport sockets.

### Convergence Demo
3. Create `examples/local-two-node/main.go`:
   - Set up two `Cache` instances inside a single binary using the in-memory `MockTransport` to simulate a 2-node cluster.
   - Write values to Node 1, wait for replication, and assert Node 2 converges to the same state.
   - Run: `go run ./examples/local-two-node`.

## Verification & Testing
1. Run the local-two-node binary and check console output for convergence success logs.
2. Run `go test ./...` and `go test -race ./...` to verify all components compile and cooperate successfully.
