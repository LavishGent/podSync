# v0.1 Rescope

> 📍 [[roadmap_v1|Roadmap]] · [[backlog/Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]]

Decided 2026-11. This document records why the original 15-ticket plan was replaced by the six tickets in [[backlog/Backlog_Index|Backlog Index]], and which design decisions changed. [[roadmap_v1|roadmap_v1.md]] is kept as the long-form design history; where the two disagree, this document wins.

## Goal

A Go application embeds `podsync`, pods find each other through a headless Service, writes replicate directly to every peer, a new pod hydrates before reporting ready, and missed writes are repaired by periodic state exchange. **Zero non-stdlib dependencies.**

## Positioning

> Share pod-originated ephemeral state between replicas without Redis.

The original README led with feature flags and runtime config. Those have a single source of truth, so a ConfigMap and an informer beat peer-to-peer replication on both simplicity and consistency. PodSync earns its complexity only for state that **originates inside the pods**: rate-limit windows, circuit-breaker trips, presence, "which pod holds which connection". The v0.1 demo is a sliding-window rate limiter whose counters converge across replicas.

## What changed and why

### Lighter than memberlist means leaning on Kubernetes

The original plan reimplemented SWIM membership, gossip fanout, a dedup cache, and shard-digest anti-entropy. That is roughly what HashiCorp memberlist already provides in ~6k lines, so finishing the plan would have produced a less tested memberlist with a similar dependency footprint. memberlist and Olric are environment-agnostic and must carry their own failure detection and discovery. PodSync is Kubernetes-only, which lets it delete that layer:

| Concern | Original plan | v0.1 |
|---|---|---|
| Discovery | DNS seed + gossip membership | Headless Service DNS, re-resolved on a timer. DNS *is* the peer list. |
| Failure detection | SWIM probing, Alive/Suspect/Dead states | kubelet readiness probes. A failed pod leaves DNS. A failed TCP write is the only local signal. |
| Mutation spread | Gossip fanout + dedup cache | Direct fanout from the origin to every peer. No duplicates, so no dedup. |
| Repair | Shard digests + delta exchange | Full-state exchange with one random peer per interval. |
| Graceful leave | Gossip "Left" message | preStop hook. |

Full mesh was rejected in the roadmap because "25 pods = 600 directed relationships". The services this targets run 3 to 20 replicas; 25 pods × 24 peers × a few hundred bytes per mutation is nothing. Gossip routing solved a problem v0.1 does not have.

### Versioning: Lamport clock, not a per-node counter

The original `Version{Counter, NodeID}` gave each node its own `atomic.Uint64` and resolved conflicts by higher counter. Counters from different nodes are not comparable: a node up for a week sits at millions while a freshly restarted node starts at 1, so the new node loses every conflict for days. v0.1 uses a Lamport clock: every node runs `Observe(remote)` on receipt, setting its clock to `max(local, remote)` before the next increment. A restarted node is caught up after its first received message.

### Wire format: gob envelope, user-pluggable value codec

Protobuf and snappy are removed. The envelope between peers is `encoding/gob` behind a 4-byte length prefix and a 1-byte type. The envelope is internal and never crosses a language boundary, so protobuf bought nothing but a toolchain step. Snappy on sub-100-byte mutation messages was a likely net loss. The user-facing `Codec[V]` interface stays, so values are still serialized however the application wants.

### Node identity: pod name, no uuid dependency

`<HOSTNAME>-<startup-unix-nanos>`, falling back to `crypto/rand` hex outside Kubernetes. Pod name in the ID makes logs readable.

### Hydration: no mutation buffering

The roadmap described a watermark protocol to handle writes arriving during a snapshot transfer. With LWW merge, a mutation that arrives during hydration can be applied immediately; if the snapshot later carries an older version of the same key, merge rejects it. The watermark machinery is unnecessary.

### `merge` returns whether it applied

Every write path (local Set, received mutation, hydration, repair) goes through one `merge(key, Entry) bool`. The return value feeds a conflict counter in `Stats()` at no cost.

## Operating envelope

v0.1 is designed for **under ~50k entries and under ~25 pods**. Full-state repair is O(entries) per interval and direct fanout is O(pods) per write. Both limits are documented in the README; shard-digest repair is the first v0.2 item if a real workload hits them.

## Deferred to v0.2+

- Metrics exporter interface (expose `Stats()` first; let the host app export it)
- Shard-digest or Merkle repair
- memberlist-backed `Resolver` for non-Kubernetes environments
- `WithSync()` quorum-acknowledged writes
- CRDT counters (PN-counter). The rate-limiter demo will make this tempting; LWW ships first.
- Chaos tests beyond the partition test in T5
