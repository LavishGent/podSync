# MS3-A: Replication Worker & Store Integration

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]]

## Overview
Implement the background replication worker loop that drains the outgoing mutation queue and broadcasts writes to a random subset of healthy peers (gossip fanout). Connect the local storage engine so that operations trigger replication events, and write handler logic to apply incoming replication events.

- **Milestone**: 3 (Distributed Replication)
- **Track**: A (Developer A)
- **Status**: Ready
- **Dependencies**: [[MS2-A_TCP_Framing_Server|MS2-A]], [[MS2-B_Membership_Discovery|MS2-B]], [[MS2-C_Replication_Buffers|MS2-C]]

## Phase Reference
This ticket implements:
- [[steps_breakdown_v1#Phase 8 — Replication Engine|Phase 8 — Replication Engine]] (Steps 107–110: Worker Loop; Steps 116–122: Wiring & Receiver)

## Detailed Steps

### Gossip Workers
1. Create `internal/replication/worker.go`:
   - Define `Worker` struct referencing the `MutationQueue`, membership `Manager`, and `Transport`.
   - Implement `Worker.Start(ctx context.Context)`:
     - Run background loops reading from `MutationQueue.Drain()`.
     - For each mutation, retrieve peer list and call `selectPeers` to choose targets. Send replication frames asynchronously.
   - Implement `selectPeers(all []PeerInfo, fanout int) []PeerInfo`:
     - Select a random sample of size `fanout`.
     - Exclude the local node's own address and any peers marked as `Dead` or `Left`.

### Replication Integration
2. Connect local storage calls in `internal/storage/store.go` to the queue:
   - On successful `Store.Set` or `Store.Delete`, construct a `Mutation` protobuf envelope (with current node ID and an incremented version counter) and push it to the `MutationQueue`.
3. Implement incoming replication handler:
   - When the transport receives an `ApplyMutation` command:
     - Check `SeenCache` to ignore duplicate IDs.
     - Call `Store.Set` (using LWW logical version checks from [[MS1-B_Versioning_Identity|MS1-B]]) to apply the write or tombstone.
     - Return an `Ack` payload.

## Verification & Testing
1. Create `internal/replication/replication_test.go`:
   - Test: A local write succeeds and executes even if the replication queue is full.
   - Test: A duplicate mutation ID is discarded on the receiving side without mutating the store.
   - Test end-to-end: Spin up two nodes with mock transports. Write a value to node A and verify it propagates to node B's local storage.
   - Check for race conditions with `go test -race ./internal/replication/...`.
