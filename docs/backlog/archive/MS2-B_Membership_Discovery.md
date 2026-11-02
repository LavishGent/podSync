# MS2-B: Membership Registry & Static Discovery

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]]

## Overview
Implement the database and manager for tracking cluster peer states (Alive, Suspect, Dead, Left) and build mechanisms to discover peers statically via config file lists, or dynamically via Kubernetes headless service DNS lookups.

- **Milestone**: 2 (P2P Transport & Buffers)
- **Track**: B (Developer B)
- **Status**: Ready
- **Dependencies**: [[MS1-B_Versioning_Identity|MS1-B]] (needs Node identity structures)

## Phase Reference
This ticket implements:
- [[steps_breakdown_v1#Phase 9 — Membership Engine|Phase 9 — Membership Engine]] (Steps 123–135: Registry & Discovery blocks)

## Detailed Steps

### Membership State & Registry
1. Create `internal/membership/peer.go`:
   - Define `PeerState` enum: `Alive`, `Suspect`, `Dead`, `Left`.
   - Define `PeerInfo struct { ID string; Addr string; State PeerState; LastSeen int64; SuspectSince int64 }`.
2. Create `internal/membership/manager.go`:
   - Define `Manager` struct holding `peers map[string]*PeerInfo` and a `sync.RWMutex`.
   - Implement `AddPeer(info PeerInfo)`, `RemovePeer(id string)`, `AlivePeers() []PeerInfo`, and `UpdateState(id string, state PeerState)`. Ensure all methods are thread-safe.

### Peer Discovery Engines
3. Create `internal/membership/static.go`:
   - Implement `StaticDiscovery` which parses a list of strings (`addr:port`) from config and adds them to the membership `Manager` as initial peers.
4. Create `internal/membership/dns.go`:
   - Implement `KubernetesDNSDiscovery` which runs `net.LookupHost(headlessServiceFQDN)` and adds the resolved IP addresses to the membership `Manager`.

## Verification & Testing
1. Create `internal/membership/membership_test.go`:
   - Implement `TestStaticDiscovery`: Config a list of addresses and verify they are correctly populated into the membership registry.
   - Implement `TestAddRemovePeers`: Verify thread-safe additions and removals under concurrent stress.
   - Mock DNS resolution and verify `KubernetesDNSDiscovery` populates the registry with target IPs.
   - Run `go test -race ./internal/membership/...` to verify race-free execution.
