# MS3-B: SWIM-style Active Probing

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]]

## Overview
Implement the SWIM-style active probing loop to monitor peer health. The system will periodically ping random peers, mark non-responsive peers as "Suspect", and transition them to "Dead" if they fail to recover before a configurable timeout expires.

- **Milestone**: 3 (Distributed Replication)
- **Track**: B (Developer B)
- **Status**: Ready
- **Dependencies**: [[MS2-A_TCP_Framing_Server|MS2-A]], [[MS2-B_Membership_Discovery|MS2-B]]

## Phase Reference
This ticket implements:
- [[steps_breakdown_v1#Phase 9 — Membership Engine|Phase 9 — Membership Engine]] (Steps 136–143: SWIM-style probe implementation)

## Detailed Steps

### SWIM Probe Loop
1. Create `internal/membership/probe.go`:
   - Implement background probing routine on `Manager`:
     - Every `ProbeInterval` (e.g. 1 second): pick a random peer marked as `Alive` or `Suspect`.
     - Send a `Ping` request via `Transport` with a timeout.
     - If ping succeeds and the peer was `Suspect`, transition state back to `Alive` (update `LastSeen`).
     - If ping fails:
       - If the peer was `Alive`, transition it to `Suspect` and record `SuspectSince = now`.
       - If the peer was already `Suspect` and `now - SuspectSince > SuspectTimeout` (e.g. 15 seconds), transition the peer state to `Dead`.
       - Keep `Dead` peers in the registry for a `DeadRetention` duration (e.g. 24 hours) for auditing/recovery tracking, then remove them completely from the registry.

## Verification & Testing
1. Add tests to `internal/membership/membership_test.go`:
   - Test: A static discovery run sets up initial state.
   - Test probe failure: Mock transport to fail requests, run probe loop, assert peer transitions to `Suspect`.
   - Test suspect expiry: Fast-forward mock time (or reduce timeout values) and assert that a suspect peer transitions to `Dead` and is eventually garbage collected.
   - Test peer recovery: Mock transport to succeed after a brief failure window and assert a `Suspect` peer reverts to `Alive`.
   - Run `go test -race ./internal/membership/...`.
