# MS1-B: Logical Versioning & Node Identity

## Overview
Implement the logical vector-like Versioning system to support Last-Write-Wins (LWW) conflict resolution across distributed nodes. Also, build the unique node identity generation and version increment counter.

- **Milestone**: 1 (Foundations & Core Engines)
- **Track**: B (Developer B)
- **Status**: Ready
- **Dependencies**: MS1-A (specifically the storage structures to wire in versioning)

## Phase Reference
This ticket implements:
- [Phase 2 — Versioning and Conflict Resolution](file:///c:/Obsidian-Vaults/podSync/PodSync%20-%20Granular%20Build%20Steps.md#L71-L100) (Steps 42–58)
- [Phase 3 — NodeID and Version Generation](file:///c:/Obsidian-Vaults/podSync/PodSync%20-%20Granular%20Build%20Steps.md#L103-L117) (Steps 59–67)

## Detailed Steps

### Version Types & Logic
1. Create `internal/version/version.go`:
   - Define `Version struct { Counter uint64; NodeID string }`.
   - Implement `IsZero() bool`.
   - Implement `Compare(a, b Version) int` returning -1 (a < b), 0 (a == b), or 1 (a > b).
   - Implement `ShouldApplyIncoming(local, incoming Version) bool`:
     - Returns true if `incoming.Counter > local.Counter`.
     - Returns false if `incoming.Counter < local.Counter`.
     - Tie-breaker: returns `incoming.NodeID > local.NodeID` (lexicographic comparison).

### Wiring Versions into Storage Engine
2. Update `Entry[V]` in `internal/storage/entry.go`:
   - Add `Version version.Version` field.
3. Update `Shard.set(key K, entry Entry[V])` in `internal/storage/shard.go`:
   - Compare the incoming entry's version against the existing entry using `ShouldApplyIncoming`. Only apply the write if the incoming version wins.
4. Update `Shard.delete(key K, tombstone Entry[V])`:
   - Tombstones must carry a version. Only apply deletion if the tombstone's version wins against the existing entry.

### Node Identity & Version Generators
5. Fetch UUID library: `go get github.com/google/uuid`.
6. Create `internal/node/identity.go`:
   - Implement `GenerateNodeID() string` using `uuid.New().String()`.
   - Define `NodeInfo struct { ID string; Addr string; StartedAt int64; Incarnation uint64 }`.
7. Create `internal/node/counter.go`:
   - Define `Counter` wrapping `atomic.Uint64`.
   - Implement `Counter.Next() uint64`.
   - Implement `NextVersion(nodeID string, counter *Counter) version.Version`.

## Verification & Testing
1. Create `internal/version/version_test.go`:
   - Test: higher counter wins.
   - Test: lower counter is rejected.
   - Test: equal counters, higher NodeID wins.
   - Test: equal counters, equal NodeID -> no-op (idempotency).
   - Test: tombstone with higher version beats live entry with lower version, and vice-versa.
   - Unskip and verify the resurrection stale write test from MS1-A.
2. Create tests in `internal/node/node_test.go` (or `identity_test.go`):
   - Test: `GenerateNodeID()` returns unique values.
   - Test: `Counter.Next()` is monotonic and safe under concurrent load (`go test -race`).
