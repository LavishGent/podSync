# MS3-C: Lifecycle Manager & Hydration Protocol

## Overview
Implement the Node Lifecycle state machine to handle clean startup. New nodes entering the cluster must discover peers, request a full snapshot, apply it, catch up on delta mutations missed during snapshotting (using a watermark version), and buffer incoming writes until they transition to the "Ready" state.

- **Milestone**: 3 (Distributed Replication)
- **Track**: C (Developer C)
- **Status**: Ready
- **Dependencies**: MS2-A (Transport), MS2-B (Membership discovery), MS2-C (SeenCache), MS3-A (Replication handler)

## Phase Reference
This ticket implements:
- [Phase 10 — Hydration and Readiness](file:///c:/Obsidian-Vaults/podSync/PodSync%20-%20Granular%20Build%20Steps.md#L245-L270) (Steps 144–162)

## Detailed Steps

### Lifecycle State Machine
1. Create `internal/hydration/lifecycle.go`:
   - Define `LifecycleState` enum: `Starting`, `DiscoveringPeers`, `Bootstrapping`, `ApplyingSnapshot`, `CatchingUp`, `Ready`.
   - Define `LifecycleManager` with `currentState` and `mu sync.Mutex`.
   - Implement `TransitionTo(state LifecycleState) error` enforcing valid state transitions (e.g. cannot jump from `Starting` directly to `Ready` unless no peers are discovered).
   - Implement `IsReady() bool` returning true only in state `Ready`.

### Hydration Protocol (Snapshot & Catch-up)
2. Create `internal/hydration/snapshot.go`:
   - Implement `RequestSnapshot(ctx, peerAddr, transport) ([]Entry, watermark Version, error)`:
     - Request node sends `SnapshotRequest` RPC.
     - Peer handler: records its current max logical version (watermark), serializes all live entries, and returns them.
   - Implement `ApplySnapshot(entries []Entry)`:
     - Iterates and applies snapshot entries using normal `ShouldApplyIncoming` logic.
   - Implement `CatchUp(ctx, peerAddr, watermark Version, transport)`:
     - Send a `DeltaRequest` with `sinceVersion = watermark` to fetch mutations that happened during snapshot application. Apply returned deltas.

### Replication Buffering during Hydration
3. Create `internal/hydration/buffer.go`:
   - Implement an incoming replication buffer. While in `Bootstrapping` or `CatchingUp` states, incoming replication `ApplyMutation` RPCs must be temporarily stored in a buffer rather than applied directly.
   - Upon transitioning to `Ready`, flush the buffer through normal conflict resolution and stop buffering.
4. Implement first-node handling:
   - If the node is in `DiscoveringPeers` and discovers 0 online peers, skip snapshotting and transition directly to `Ready`.

## Verification & Testing
1. Create `internal/hydration/hydration_test.go`:
   - Test: A node starting with zero discovered peers transitions cleanly `Starting -> DiscoveringPeers -> Ready`.
   - Test: A new node successfully requests and applies a snapshot from a peer, catches up on deltas, and transitions to `Ready`.
   - Test buffering: Verify that incoming replication mutations received while hydrating are buffered and only applied after the node reaches `Ready`.
   - Run `go test -race ./internal/hydration/...`.
