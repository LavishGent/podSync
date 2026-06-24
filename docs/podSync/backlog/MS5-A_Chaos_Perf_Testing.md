# MS5-A: Chaos & Performance Testing

## Overview
Implement extensive end-to-end integration, performance, and chaos tests. Build a test suite simulating real TCP connections, node restarts, network partitions, concurrent write collisions, and replication queue overflow situations. Run benchmarks to verify p99 latency targets.

- **Milestone**: 5 (Production Readiness)
- **Track**: A (Developer A)
- **Status**: Ready
- **Dependencies**: MS4-B (Public API assembly), MS2-A (TCP transport)

## Phase Reference
This ticket implements:
- [Phase 15 — Performance and Chaos Testing](file:///c:/Obsidian-Vaults/podSync/PodSync%20-%20Granular%20Build%20Steps.md#L363-L377) (Steps 206–216)

## Detailed Steps

### Integration & Chaos Tests
1. Create `test/integration/two_node_test.go`:
   - Initialize two `Cache` instances communicating over loopback TCP (`127.0.0.1`).
   - Implement `TestBasicReplication`: Write to Node A, wait, and verify it appears on Node B.
   - Implement `TestNodeRestartAndHydration`: Start A and B. Stop B, write 10 new keys to A. Restart B, verify B requests snapshot, catches up, and becomes Ready.
   - Implement `TestNetworkPartition`:
     - Disconnect transport link using mock layers or iptables wrappers (or a mock transport partition toggle).
     - Verify both partitions continue serving read requests.
     - Heal partition, verify that anti-entropy or replication catches up and both converge to the same state.
   - Implement `TestConcurrentWriters`: Run 100 goroutines writing keys to verify no data races occur (`go test -race`).
   - Implement `TestQueueOverflow`: Flood the cluster with writes so the replication queue overflows, asserting that local writes continue to succeed.
   - Implement `TestTombstoneAntiEntropy`: Assert that deleted keys are not resurrected by repair loops.

### Performance Benchmarking
2. Run micro-benchmarks and verify targets:
   - Target p99 read latency < 1ms.
   - Target p99 write latency < 2ms.
3. Record results in `docs/benchmarks.md`.
4. Add benchmark summary badge/section to the main `README.md`.

## Verification & Testing
1. Execute the entire integration test suite: `go test -v ./test/integration/...`.
2. Run tests with race detector: `go test -race ./test/integration/...`.
