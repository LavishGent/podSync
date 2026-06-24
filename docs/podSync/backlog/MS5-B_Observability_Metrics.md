# MS5-B: Observability & Metrics

## Overview
Implement pluggable observability mechanisms. Define standard cluster telemetry interfaces, implement a zero-cost default `NoopMetrics` engine and a `Prometheus` metrics exporter, instrument all systems (Queue, Replication, SWIM, Entropy, Hydration), and add structured `log/slog` logging.

- **Milestone**: 5 (Production Readiness)
- **Track**: B (Developer B)
- **Status**: Ready
- **Dependencies**: MS4-B (Public API assembly), MS3-A (Replication), MS3-B (SWIM)

## Phase Reference
This ticket implements:
- [Phase 14 — Observability](file:///c:/Obsidian-Vaults/podSync/PodSync%20-%20Granular%20Build%20Steps.md#L344-L361) (Steps 200–205)

## Detailed Steps

### Telemetry Interface & Instrumentations
1. Create `internal/metrics/metrics.go`:
   - Define a pluggable metrics interface containing:
     - `MutationsEnqueued()`, `MutationsDropped()`, `MutationsReplicated()`.
     - `ReplicationFailures()`, `AntiEntropyRepairs()`, `ConflictsEncountered()`.
     - `ObserveHydrationDuration(seconds float64)`.
     - `UpdatePeersCount(alive, suspect int)`.
   - Implement `NoopMetrics` providing empty methods for zero runtime overhead if telemetry is disabled.
2. Instrument codebase:
   - Add increments inside `Queue.Enqueue` (and count drops when full).
   - Track conflicts inside the version comparison logic.
   - Track duration of hydration bootstrap cycles.
   - Update gauge values for alive/suspect peers inside the SWIM loop.
3. Configure structured logging:
   - Wire standard library `log/slog`.
   - Attach context attributes (`node_id`, `peer_id`, `state`, `key`) to every replication, membership state change, and anti-entropy event.

### Prometheus Integration
4. Create `examples/metrics/main.go`:
   - Implement a Prometheus exporter wrapper mapping the telemetry interface to standard Prometheus client collector libraries.
   - Set up `/metrics` endpoint.
5. Verify `/metrics` works correctly during the Kubernetes Kind cluster demo.

## Verification & Testing
1. Compile and run the `examples/metrics` server. Curl the `/metrics` endpoint and verify Prometheus formats are valid.
2. Confirm logs are formatted in structured JSON or standard slog log configurations.
