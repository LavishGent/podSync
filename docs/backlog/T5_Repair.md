# T5: Repair

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]] · [[../rescope_v0.1|Rescope]]

> **Addendum:** the [[../rescope_v0.1#Addendum (2026-11, before T1)|rescope addendum]] changes this ticket: A2 (early first repair tick), A5 (repair recovers evicted keys), A8 (paged state transfer and push), and A10 (tombstone test ordering).

## Overview
Periodic full-state exchange with one random peer so that dropped sends, partitions, and overflowed outboxes converge anyway. Replaces [[archive/MS4-A_Active_Anti_Entropy|MS4-A]]. No digests in v0.1; the state request from T4 is reused as-is.

- **Status**: In review
- **Dependencies**: [[T4_Replication_Hydration|T4]]

## Detailed Steps

1. Add a repair loop to `Node.Start`:
   - Every `RepairInterval` (default 30s, jittered ±10% so a fleet that restarted together does not synchronize), pick one peer uniformly at random from `peers.Peers()`.
   - Walk the peer's shard pages with the same request machinery as hydration (`pullFrom`, A8); `Observe` and `Merge` every entry via `applyEntry`. Count applied entries in `Stats().Repaired`.
   - On a failed page, count it in `Stats().RepairFailures` and end the round; the next tick picks again.
   - The first tick after Start returns fires after `FirstRepairDelay` (A2, default 15s, capped at `RepairInterval`).
   - Skip the tick if the node is not `Ready` or has no peers.
2. Push side: the `StateRequest` handler already exists from T4. Make the exchange symmetric cheaply: the requester includes its own entries for shard `i` in page `i`'s `StateRequest.EntryBlob` (the T2 message already carries it) when its whole store is under `MaxStateEntries / 4` entries, and the responder merges them after building its response. Above that size the request is pull-only. No new message type.
3. Tombstone interaction: repair must never resurrect a key whose tombstone is still within `TombstoneTTL`. This falls out of `merge` as long as tombstones are included in `Snapshot()` (T1 requires this). Add an explicit test anyway.
4. Document in the README failure-modes section: a partition longer than `TombstoneTTL` can resurrect deleted keys; raise `TombstoneTTL` if that matters for your workload. The README also notes that a departed pod's entries live until TTL and that repair is the safety net for dropped fanout.
5. Over `MaxStateEntries` entries received in a tick logs the envelope warning once for that tick.

## Verification & Testing
All tests use `transport.Memory` with `Partition`/`Heal`.

1. `internal/node/repair_test.go`:
   - Partition A from B for the duration of 200 writes on each side, keep writing on both, `Heal`, assert convergence within two `RepairInterval`s (set the interval to 50ms in tests).
   - Three nodes, `Drop` every third mutation between all pairs, write 300 keys per node, assert convergence through repair alone.
   - Tombstone survives repair: Set on A, replicate, Delete on A, partition, B still has the live value, heal, assert B holds the tombstone after repair and A does not resurrect the value.
   - Tombstone past TTL: same setup with `FakeClock` advanced past `TombstoneTTL` before heal; document the observed resurrection in the test name so the behavior is explicit, not accidental.
   - Symmetric path: node with 10 entries repairs against a node with 10 different entries; one tick converges both.
   - Over-envelope: node with `MaxStateEntries + 1` entries logs the envelope warning once per tick.
2. Benchmark `Snapshot()` encode + decode at 10k and 50k entries and record the numbers in this ticket. They set the README's operating envelope.

## Results

Tests: `internal/node/repair_test.go` (partition/heal, dropped-every-third mutation, tombstone within TTL, resurrection past TTL, symmetric one-tick, over-envelope warning, first-tick delay, Close leaves no goroutines). Tests call `repairTick` directly where they need one deterministic round.

Benchmark (`go test -bench SnapshotRoundTrip ./internal/node`, Apple M5 Pro, 100-byte values, string keys; Snapshot → `FromEntry` → `EncodeEntries` → `DecodeEntries` → `ToEntry`, paging not included):

| Entries | Time/op | Alloc/op | Allocs/op |
|---|---|---|---|
| 10k | ~91 ms | ~134 MB | ~2.4M |
| 50k | ~438 ms | ~694 MB | ~12.2M |

Cost is linear and allocation heavy (per-entry gob), which is why 50k is the documented envelope and why a full transfer every 30s from each node is acceptable but not free.
