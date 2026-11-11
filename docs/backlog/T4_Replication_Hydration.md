# T4: Replication & Hydration

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]] · [[../rescope_v0.1|Rescope]]

> **Addendum:** the [[../rescope_v0.1#Addendum (2026-11, before T1)|rescope addendum]] changes this ticket: A2 (hydration DNS gap is accepted) and A5 (bounded per-peer queues replace the single outbox and fanout wait).

## Overview
Wire the store, versioning, transport, and peer set together into a node that replicates writes to every peer and hydrates from a peer at startup. Replaces [[archive/MS2-C_Replication_Buffers|MS2-C]], [[archive/MS3-A_Replication_Worker|MS3-A]], and [[archive/MS3-C_Hydration_Lifecycle|MS3-C]]. No worker pool, no dedup cache, no mutation buffering during hydration; see [[../rescope_v0.1|Rescope]] for why each was dropped.

- **Status**: Ready
- **Dependencies**: [[T3_Transport_Peers|T3]]

## Detailed Steps

### Node
1. Create `internal/node/node.go` with `Node[K, V]` owning: `*storage.Store`, `*version.Lamport`, `transport.Transport`, `*peers.Set`, codecs, `*slog.Logger`, and a `state atomic.Int32` (`Starting`, `Hydrating`, `Ready`, `Closed`).

### Write path
2. `Set(ctx, key, value, expiresAt)`:
   1. `v := lamport.Next()`
   2. `store.Set(key, value, expiresAt, v)`
   3. Encode to `wire.Mutation`; non-blocking send on `outbox chan wire.Mutation` (default capacity 4096). If full: drop, increment `Stats().Dropped`, log at warn with sampling (once per second).
   4. Return.
   `Delete` is the same with a tombstone.
3. Replication loop (one goroutine): drain `outbox`; for each mutation, send to every address in `peers.Peers()` with `RequestTimeout`. Failures are counted (`Stats().SendFailures`) and logged with sampling; nothing is retried. Sends to different peers for the same mutation run concurrently (bounded by `len(peers)`); the loop waits for all before taking the next mutation so ordering per origin is preserved on the wire.

### Receive path
4. Handler for `MsgMutation`: `lamport.Observe(m.Entry.Version)`, decode key/value, `store.Merge`, reply `MutationAck{Applied}`. Decode failure returns an error (and is counted) but never affects local state.
5. Handler for `MsgStateRequest`: `store.Snapshot()` → `StateResponse`. Cap at `MaxStateEntries` (default 50k); if exceeded, send the first N and log a warning naming the operating envelope.
6. Handler for `MsgPing`: reply `Pong` with the current clock.

### Lifecycle
7. `Start(ctx)`:
   1. `transport.Listen` on the configured port.
   2. `peers.Set.Run` and wait for the first resolution (bounded by `ResolveTimeout`, default 5s).
   3. If no peers: `Ready`. Log "first node, starting with empty state".
   4. Else `Hydrating`: shuffle peers; for each, send `StateRequest`; on success `Observe` + `Merge` every entry and transition to `Ready`. On failure, try the next peer. If all fail, retry the round after 1s until `HydrationTimeout` (default 30s), then go `Ready` with whatever was merged and log at error.
   5. Start the replication loop, the store sweeper, and (T5) the repair loop.
   Mutations that arrive while `Hydrating` are merged immediately; LWW makes order irrelevant.
8. `Ready() bool`.
9. `Close(ctx)`: set `Closed`, stop accepting new `Set`s (return `ErrClosed`), drain `outbox` until empty or `ctx` done, then `transport.Close()`.

### Stats
10. `Stats()` returns `storage.StoreStats` plus `Peers`, `State`, `Dropped`, `SendFailures`, `ReceivedMutations`, `ReceivedConflicts`.

## Verification & Testing
All tests use `transport.Memory` and `peers.Static`; no sockets.

1. `internal/node/node_test.go`:
   - Three nodes, each writes 1000 keys with interleaving (`runtime.Gosched` between writes); poll until all three `Snapshot()`s are identical, within 5s.
   - Overlapping keys: three nodes write the same 50 keys concurrently; assert convergence and that the winning value on every node has the highest `Version`.
   - Delete on A after Set on B converges to a tombstone everywhere.
   - New node joins after 1000 writes; after `Start` returns `Ready` its snapshot equals the others.
   - Hydration peer order: first peer partitioned, second answers; node becomes `Ready` with full state.
   - All peers partitioned: node becomes `Ready` after `HydrationTimeout` with empty state and logs an error (capture with a `slog.Handler` test double).
   - Outbox overflow: `Drop` all sends, write 5000 keys fast, assert `Stats().Dropped > 0` and all 5000 are readable locally.
   - `Close` drains: write 100 keys, `Close`, assert a peer received all 100.
   - Everything under `-race`.
2. One socket-level smoke test in `internal/node/tcp_smoke_test.go`: two nodes on `127.0.0.1:0` with `TCP` transport, one Set, assert the other reads it.
