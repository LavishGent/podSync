# MS1-A: Repository Foundation & Local Storage Engine

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]]

## Overview
Initialize the project structure, repository scaffolding, continuous integration config, and implement the local in-memory storage engine. This engine will support sharding, thread-safety, basic Get/Set/Delete operations, and TTL sweeping.

- **Milestone**: 1 (Foundations & Core Engines)
- **Track**: A (Developer A)
- **Status**: Done
- **Dependencies**: None

## Implementation Notes

Completed 2026-06-25. Design spec: `docs/superpowers/specs/2026-06-25-storage-engine-design.md`.

### What Was Built
- `internal/storage/entry.go` — `Entry[V any]` with `Value`, `ExpiresAt`, `Deleted`, `Version` (placeholder); `IsExpired` / `IsAlive` methods
- `internal/storage/shard.go` — Unexported `shard[K,V]` with `sync.RWMutex` + map; `get` (RLock), `set`, `delete` (tombstone), `sweep` (removes expired non-tombstones)
- `internal/storage/store.go` — `Store[K,V]` with `StoreOptions` (NumShards default 64, SweepInterval 0=disable), `StoreStats`, FNV-1a shard routing with type-switch, `Get`/`Set`/`Delete`/`Stats`/`Start`
- `internal/storage/store_test.go` — 7 tests: `TestSetGet`, `TestDeleteGet`, `TestExpiredKey`, `TestResurrectionStaleWrite`, `TestShardDeterministic`, `TestStats`, `TestSweeper`; all pass with `-race`
- `internal/storage/bench_test.go` — `BenchmarkSet` (~99 ns/op), `BenchmarkGet` (~139 ns/op), `BenchmarkConcurrentGet` (~38 ns/op), `BenchmarkConcurrentMixed` (~64 ns/op)

### Key Design Decisions
- Tombstones accumulate (not swept); tombstone GC deferred to MS1-B
- `Delete` on a non-existent key is a no-op; write-before-delete ordering assumed for MS1-A (replication ordering addressed in MS2+)
- `Start(ctx)` sweeper cannot restart after context cancellation; create a new `Store` to resume sweeping

## Phase Reference
This ticket implements:
- [[steps_breakdown_v1#Phase 0 — Repo Foundation|Phase 0 — Repo Foundation]] (Steps 1–12)
- [[steps_breakdown_v1#Phase 1 — Local Storage Engine|Phase 1 — Local Storage Engine]] (Steps 10–41, except versioning integration)

## Detailed Steps

### Repo Scaffolding
1. Create project directory structure: `internal/{storage,version,node,replication,membership,transport,entropy}`, `pkg/podsync`, `proto`, `examples/local-two-node`, `deployments/kubernetes`, `docs`.
2. Initialize Go module: `go mod init github.com/yourname/podsync`.
3. Create `pkg/podsync/doc.go` with `package podsync` as a placeholder.
4. Create `Makefile` with targets: `test`, `race`, `lint`, `proto`, `build`.
5. Create `.github/workflows/ci.yml` running `go test ./...` and `go vet ./...` on push.
6. Write initial `README.md` (brief description of PodSync) and `LICENSE` (Apache 2.0 or MIT).

### In-Memory Sharded Store
7. Create `internal/storage/entry.go`:
   - Define `Entry[V any]` struct: `Value V`, `ExpiresAt int64`, `Deleted bool`.
   - Implement `IsExpired(now int64) bool` and `IsAlive(now int64) bool` (not deleted and not expired).
   - *Note: Version field will be added by [[MS1-B_Versioning_Identity|MS1-B]]. Define it simply for now.*
8. Create `internal/storage/shard.go`:
   - Define `Shard[K comparable, V any]` with `mu sync.RWMutex` and `data map[K]Entry[V]`.
   - Implement `get(key K, now int64) (V, bool)`, `set(key K, entry Entry[V])`, and `delete(key K, now int64)` (mark Deleted = true, tombstone).
9. Create `internal/storage/store.go`:
   - Define `Store[K comparable, V any]` with `shards []Shard[K, V]` and `numShards int`.
   - Implement `shardFor(key K) *Shard` using FNV-1a hash of the key's string representation.
   - Implement `Get(key K) (V, bool)`, `Set(key K, value V, expiresAt int64)`, and `Delete(key K)`.
   - Implement `Stats() StoreStats` summarizing live keys, expired keys, and tombstones.
10. Add `startSweeper(ctx context.Context, interval time.Duration)` on `Store`:
    - Loop and iterate through all shards under write lock to clean up expired entries. Do not collect active tombstones yet.

## Verification & Testing
1. Create `internal/storage/store_test.go` and implement:
   - `TestSetGet`: Verify setting and getting values works.
   - `TestDeleteGet`: Verify deleted key is missing.
   - `TestExpiredKey`: Mock time or wait to verify expired key is missing.
   - `TestResurrectionStaleWrite`: Test that a second set works (skip/mark pending version checks, since [[MS1-B_Versioning_Identity|MS1-B]] resolves actual resurrection protection).
   - `TestShardDeterministic`: Verify key-to-shard mapping is consistent.
   - Run concurrency test: `go test -race ./internal/storage/...` to verify safety.
2. Create `internal/storage/bench_test.go` and implement:
   - `BenchmarkGet`, `BenchmarkSet`, `BenchmarkConcurrentGet`, `BenchmarkConcurrentMixed`.
   - Run `go test -bench=. -benchmem ./internal/storage/...` and record results in `docs/benchmarks.md` if available.
