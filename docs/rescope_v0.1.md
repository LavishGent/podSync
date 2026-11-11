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

## Addendum (2026-11, before T1)

A review of the six tickets found seven gaps. Each decision below overrides the ticket text it names; the tickets carry a one-line pointer back here.

### A1. Rate-limiter demo uses one writer per key (T6)

LWW loses concurrent increments when two pods write the same counter key. The demo therefore gives each pod its own key, `limit:<client>:<nodeID>`, holding that pod's count for the current window. A request sums every `limit:<client>:*` entry whose window matches the current one. Each key has exactly one writer, so LWW never discards an increment; the sum is eventually consistent and can overshoot the limit by the hits still in flight between pods.

Summing needs to enumerate keys, so `Cache` gains `Range(fn func(K, V) bool)` over live entries. It is O(entries); the demo's key count is tiny, and the doc comment says so.

### A2. Hydration and DNS readiness: accept the gap, repair early (T4, T5)

A headless Service publishes only ready pods by default. A hydrating pod is not ready, so peers do not send it writes until it turns ready and their next DNS refresh sees it. Writes in that window (up to `RefreshInterval` plus the readiness probe period) are missed by the new pod.

v0.1 accepts the gap instead of setting `publishNotReadyAddresses: true`. Publishing not-ready pods would let a hydrating peer answer a `StateRequest` with empty state, which needs a readiness flag on the wire and a rule for "every peer is starting". T5 closes the gap instead: **the first repair tick after a node turns `Ready` fires after `2 × RefreshInterval`**, not after a full `RepairInterval`. By then every peer has re-resolved DNS and includes the new pod. The manifests keep the default and document why.

### A3. Optional shared-secret handshake on the replication port (T3)

Anything that can reach the replication port can write any key. `TCP` gains an optional `Secret []byte`. When set, every new connection runs a mutual challenge-response before the first frame:

1. The listener sends a 32-byte random nonce `Ns`.
2. The dialer replies with its own 32-byte nonce `Nc` and `HMAC-SHA256(secret, "podsync/v1 client" || Ns || Nc)`.
3. The listener checks the MAC with `hmac.Equal` and replies `HMAC-SHA256(secret, "podsync/v1 server" || Nc || Ns)`.
4. The dialer checks that MAC. Any failure or a timeout (bounded by `DialTimeout`) closes the connection.

Both sides must use the same setting; a mismatch fails the handshake and closes the connection. This proves the peer knows the secret. It does not encrypt traffic or authenticate individual frames after the handshake. T3 adds a README security note saying so, and `deployments/kubernetes/networkpolicy.yaml`, which admits replication-port traffic only from pods with the app label. T6 exposes the option as `Config.Secret`.

### A4. `ExpiresAt` stays an absolute time; skew is documented (T1, T2)

Entries keep an absolute `ExpiresAt` in unix nanoseconds. Replicating a relative TTL would make the same version carry a different `ExpiresAt` on every node, which breaks the snapshot-equality convergence tests and the "same version means same entry" rule. The cost is that an entry expires early or late on a peer by that peer's clock skew. Kubernetes nodes run NTP and are normally within tens of milliseconds, so v0.1 documents the tolerance: **TTLs should be at least seconds long, well above expected skew**. The `Entry.ExpiresAt` doc comment (T1) and the README failure-modes section (T6) state this. `DeletedAt` is only read by the local tombstone sweeper, so skew there shifts GC timing and nothing else.

### A5. Fanout uses bounded per-peer queues (T4)

T4's single replication loop waits for every peer before taking the next mutation, so one slow peer slows writes to all of them. T4 instead keeps **one queue per peer** (default capacity 1024, `OutboxSize`), each drained in order by its own goroutine with `RequestTimeout` per send. `Set` enqueues the mutation on every peer queue without blocking. When a queue is full, the **oldest** queued mutation is dropped to make room: under LWW a newer mutation is worth more than an older one, and per-key the newest value is the one that matters. Each drop increments `Stats().Dropped`. Queues are created and torn down as the peer set changes. `Close` drains every queue until empty or `ctx` is done. T5 repair delivers anything dropped. The T4 overflow test uses a peer whose handler blocks, since a peer that fails fast never fills its queue.

### A6. A departed pod's entries live until their TTL

PodSync has no per-node ownership of entries. When a pod leaves, the entries it wrote stay on every other pod until their TTL expires; entries written without a TTL stay until deleted. Applications that store per-pod state (A1, presence) should always set a TTL. The README failure-modes section (T6) states this.

### A7. Housekeeping (this PR)

CI reads the Go version from `go.mod` (`go-version-file`), so the two can no longer drift. `roadmap_v1.md` and `steps_breakdown_v1.md` already carry superseded banners. [[go_1.27_upgrade|go_1.27_upgrade.md]] is rewritten to match the protobuf-free repo; the Go bump itself is not part of v0.1.
