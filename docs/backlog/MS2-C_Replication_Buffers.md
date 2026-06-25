# MS2-C: Replication Buffer & Deduplication

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]]

## Overview
Implement the lock-free or buffered channel-based `MutationQueue` for queuing outgoing mutations to be replicated. Build the `SeenCache` deduplication index to track incoming mutation IDs and drop duplicates, with automatic time-based eviction.

- **Milestone**: 2 (P2P Transport & Buffers)
- **Track**: C (Developer C)
- **Status**: Ready
- **Dependencies**: [[MS1-C_Codecs_Protobuf|MS1-C]], [[MS1-A_Repo_Storage|MS1-A]]

## Phase Reference
This ticket implements:
- [[steps_breakdown_v1#Phase 8 — Replication Engine|Phase 8 — Replication Engine]] (Steps 103–106: Mutation Queue; Steps 111–115: Deduplication Cache)

## Detailed Steps

### Replication Buffer Queue
1. Create `internal/replication/queue.go`:
   - Define `MutationQueue` wrapping a channel of protobuf `Mutation` objects. Use a configurable capacity buffer (e.g. 10,000 entries).
   - Implement `Enqueue(m Mutation) bool`:
     - Must be completely non-blocking. If the channel is full, immediately return `false` and increment a dropped mutations counter metric.
   - Implement `Drain() <-chan Mutation`:
     - Returns the read-only channel for consumer workers to read from.

### Deduplication seen Cache
2. Create `internal/replication/dedup.go`:
   - Define `SeenCache` struct holding a map of `string` to `int64` (mutation ID -> epoch timestamp received) protected by a `sync.RWMutex`.
   - Implement `Seen(id string) bool` and `MarkSeen(id string)`.
   - Implement background eviction: run a loop that cleans up keys older than a configurable retention threshold (e.g. 1 hour).

## Verification & Testing
1. Create tests in `internal/replication/queue_test.go` and `internal/replication/dedup_test.go`:
   - Test: `Enqueue` returns true under normal operation, and false immediately when the buffer limit is exceeded (does not block).
   - Test: `Seen` and `MarkSeen` work correctly.
   - Test eviction: Mock time or decrease interval to verify that old mutation keys are garbage collected from the deduplication map.
   - Verify concurrent safety with `go test -race ./internal/replication/...`.
