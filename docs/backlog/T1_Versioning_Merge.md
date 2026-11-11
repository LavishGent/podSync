# T1: Versioning & Merge Rule

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]] · [[../rescope_v0.1|Rescope]]

> **Addendum:** the [[../rescope_v0.1#Addendum (2026-11, before T1)|rescope addendum]] changes this ticket: A4 (`ExpiresAt` skew is documented, not corrected).

## Overview
Give every entry a Lamport-clock version and route every write (local or remote) through a single `merge` that applies Last-Write-Wins. Replaces [[archive/MS1-B_Versioning_Identity|MS1-B]].

- **Status**: Ready
- **Dependencies**: [[archive/MS1-A_Repo_Storage|MS1-A]] (done)

## Detailed Steps

### Version type
1. Create `internal/version/version.go`:
   - `type Version struct { Clock uint64; NodeID string }`.
   - `Compare(a, b Version) int`: by `Clock`, then by `NodeID` lexicographically. Document that the NodeID tie-break is arbitrary but deterministic.
   - `IsZero() bool`.
2. Create `internal/version/lamport.go`:
   - `type Lamport struct { nodeID string; clock atomic.Uint64 }`.
   - `Next() Version`: increment, return `{clock, nodeID}`.
   - `Observe(v Version)`: CAS loop setting `clock = max(clock, v.Clock)`. Called on every received message before merge.

### Node identity
3. Create `internal/node/identity.go`:
   - `NewID() string`: `os.Hostname()` + `-` + startup unix nanos. If hostname is empty or errors, use 8 bytes from `crypto/rand` as hex. No uuid dependency.

### Storage changes
4. `internal/storage/entry.go`: replace the placeholder `Version uint64` with `version.Version`. Add `DeletedAt int64` (unix nanos, set when the tombstone is written) for tombstone GC.
5. `internal/storage/shard.go`: replace `set` and `delete` with `merge(key K, incoming Entry[V]) bool`. Apply only if `Compare(incoming.Version, existing.Version) > 0` (or the key is absent). Return whether the write was applied. Tombstones are entries with `Deleted=true` and go through the same path.
6. `internal/storage/store.go`:
   - `Set(key, value, expiresAt, v Version) bool` and `Delete(key, v Version) bool` build the entry and call `merge`. `Delete` on an absent key still writes a tombstone (a peer may hold the live value).
   - Add `Merge(key K, e Entry[V]) bool` for the replication and repair paths.
   - `StoreOptions.TombstoneTTL` (default 1h). The sweeper removes tombstones whose `DeletedAt + TombstoneTTL <= now`. Document the trade-off: a partition longer than `TombstoneTTL` can resurrect a deleted key via repair.
   - `StoreStats.Conflicts` counter incremented when `merge` returns false (atomic; read in `Stats()`).
   - Add `Snapshot() []KeyEntry[K, V]` returning all entries including tombstones, for hydration and repair in T4/T5.

## Verification & Testing
1. `internal/version/version_test.go`:
   - Higher clock wins; lower clock rejected; equal clock, higher NodeID wins; identical version is a no-op.
   - `Observe` is monotonic under concurrent load (`-race`).
   - Restarted node (clock 0) that observes one message with clock N produces `N+1` on its next write.
2. `internal/storage/store_test.go`:
   - Unskip `TestResurrectionStaleWrite` from MS1-A: a Set with an older version than the tombstone is rejected.
   - Newer Set replaces an older tombstone.
   - Convergence property test: generate 200 mutations across 3 fake node IDs, apply them to two stores in different shuffled orders, assert `Snapshot()` is identical.
   - Tombstone sweep removes tombstones past `TombstoneTTL` and keeps younger ones (use `clock.FakeClock`).
   - `Stats().Conflicts` counts rejected merges.
3. `internal/node/identity_test.go`: two calls return distinct IDs; fallback path works with empty hostname (inject via a package-level var or option).
