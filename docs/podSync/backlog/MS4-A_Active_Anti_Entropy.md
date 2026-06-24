# MS4-A: Active Anti-Entropy

## Overview
Implement background active anti-entropy using XOR-based shard digests (similar to Merkle Trees but lightweight and partitionable by shard). Nodes will periodically compare shard digests with random peers, identify mismatched shards, request delta updates for those shards, and converge on identical state.

- **Milestone**: 4 (Consistency & Deploy)
- **Track**: A (Developer A)
- **Status**: Ready
- **Dependencies**: MS2-A (Transport), MS2-B (Membership manager), MS3-A (Replication)

## Phase Reference
This ticket implements:
- [Phase 11 — Anti-Entropy](file:///c:/Obsidian-Vaults/podSync/PodSync%20-%20Granular%20Build%20Steps.md#L273-L300) (Steps 163–176)

## Detailed Steps

### Shard XOR Digest Computation
1. Create `internal/entropy/digest.go`:
   - Implement `ComputeShardDigest(shard) ShardDigest`:
     - Iterate through all live (non-expired, non-deleted) entries in a single shard.
     - Generate a hash of `key + version` (e.g. using FNV-1a).
     - XOR all hashes together to produce a single `uint64` hash representation of the shard state.
     - Record the total count of live keys in the shard.
   - Implement `ComputeAllDigests(store) []ShardDigest` returning digests for all shards.

### Digest Exchange & Repair Protocol
2. Create `internal/entropy/repair.go`:
   - Implement `ExchangeDigests(ctx, peerAddr, localDigests, transport) ([]int, error)`:
     - Send `DigestRequest` RPC containing local digests to the target peer.
     - Peer handler compares the received list with its local digests and returns a list of shard IDs where the hashes or counts differ.
   - Implement `RequestDelta(ctx, peerAddr, mismatchedShardIDs, transport) ([]Entry, error)`:
     - Send `DeltaRequest` for the specific mismatched shards.
     - Peer handler returns all database entries belonging to those shards.
     - Caller applies each entry locally using LWW version rules.
   - Implement background `RepairLoop(ctx)`:
     - Every `RepairInterval` (e.g. 30 seconds): pick a random alive peer, run the digest exchange, request deltas for mismatched shards, and apply updates. Increment `podsync_anti_entropy_repairs_total` metric.
3. Write `docs/failure-modes.md` outlining the tombstone GC risk (if network partition duration exceeds tombstone retention period, deleted keys can be resurrected during anti-entropy repair).

## Verification & Testing
1. Create `internal/entropy/entropy_test.go`:
   - Test: Two stores with identical data produce identical XOR digests.
   - Test repair: Store A has an extra key. Store B runs digest exchange and repair, asserting that Store B receives and applies the missing key.
   - Test stale update: Store A has a newer version of an existing key than Store B. Assert B repairs its local key to match A.
   - Test tombstones: Assert that a deleted key (tombstone) on one node is not resurrected on the other during anti-entropy repair.
   - Run `go test -race ./internal/entropy/...` to verify safety under load.
