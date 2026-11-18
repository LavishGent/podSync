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

A review of the six tickets found the gaps below. Each decision overrides the ticket text it names; the tickets carry a one-line pointer back here.

### A1. Rate-limiter demo uses one writer per key (T6)

LWW keeps only one of two concurrent writes to the same key. The demo therefore gives each pod its own key, `limit:<client>:<nodeID>`, whose value is `{Window, Count}` for that pod. A request sums `Count` over every `limit:<client>:*` entry whose `Window` is the current one. Each key has exactly one writer pod, so replication never discards an increment. Concurrent HTTP handlers on the same pod would still race on read, increment, and `Set`, so the demo **serializes the check, window rollover, increment, and `Set` under a per-client mutex** on each pod. Counter keys carry a TTL of two windows (A6). The global sum is eventually consistent: it can overshoot the limit by the hits still in flight between pods.

Summing needs to enumerate keys, so `Cache` gains `Range(fn func(K, V) bool)` over live entries. `Range` copies each shard under its read lock and calls `fn` after unlocking, so `fn` may call back into the `Cache`. It is O(entries), which the doc comment states. T6 tests cover live-only filtering (no tombstones, no expired entries), early stop when `fn` returns false, a `Set` from inside `fn`, and a concurrent single-pod demo test that asserts the exact accepted count.

### A2. Hydration and DNS readiness: accept the gap, repair early (T4, T5)

A headless Service publishes only ready pods by default. A hydrating pod is not ready, so peers do not send it writes until kubelet sees it ready, the endpoints and DNS update, and each peer's next refresh picks it up. The new pod misses writes made in that window.

v0.1 accepts the gap instead of setting `publishNotReadyAddresses: true`. Publishing not-ready pods would let a hydrating peer answer a `StateRequest` with empty state, which needs a readiness flag on the wire and a rule for "every peer is starting". Repair narrows the gap instead, best effort: **after a node turns `Ready`, its first repair tick fires after `FirstRepairDelay`** (default 15s, capped at `RepairInterval`) instead of a full `RepairInterval`. 15s covers a 2s readiness probe period, DNS caching, and a 5s `RefreshInterval`. If a peer has still not seen the new pod by then, the normal repair ticks deliver the missed writes. The demo manifest sets `readinessProbe.periodSeconds: 2` and documents why the default `publishNotReadyAddresses: false` is kept. T5 tests that the first tick fires at `FirstRepairDelay`, not at `RepairInterval`. T6 adds `FirstRepairDelay` to `Config`.

**Superseded in T4 (decision):** the headless Service sets `publishNotReadyAddresses: true` (`deployments/kubernetes/headless-service.yaml`), so a hydrating pod receives fanout while it hydrates. Every node serves `StateRequest` and `Mutation` in every state except `Closed`, so a hydrating peer may answer a `StateRequest` with partial or empty state; hydration merges whatever it gets, a node whose peers are all still starting goes `Ready` after `HydrationTimeout`, and LWW plus repair converge the rest. No readiness flag goes on the wire. The gap that remains (a peer whose DNS refresh has not yet seen the new pod, up to `RefreshInterval`) is closed by T5 repair and the early `FirstRepairDelay` tick below, which stay as designed.

### A3. Optional shared-secret handshake on the replication port (T3)

Anything that can reach the replication port can write any key. `TCP` gains an optional `Secret []byte`, which must be at least 32 bytes when set. Every new connection starts with a hello, so a configuration mismatch fails at once with a clear error instead of a timeout:

1. Both sides send a 5-byte hello: the magic `PSY1` and a mode byte (`0` = no secret, `1` = secret). If the modes differ, both sides close the connection and log `handshake mode mismatch`.
2. In mode `1`, the listener sends a 32-byte random nonce `Ns`.
3. The dialer replies with its own 32-byte nonce `Nc` and `HMAC-SHA256(secret, "podsync/v1 client" || Ns || Nc)`.
4. The listener checks the MAC with `hmac.Equal` and replies `HMAC-SHA256(secret, "podsync/v1 server" || Nc || Ns)`. The dialer checks that MAC.

Both sides bound the whole handshake with a deadline (`DialTimeout` on the dialer, the same value on the listener) and clear it with `SetDeadline(time.Time{})` once the handshake succeeds, so an idle persistent connection is not closed by a stale deadline. Any failure closes the connection before a frame handler runs. The connection map holds one slot per address, each with its own mutex: `Send` takes the map lock only to find or create the slot, then dials and runs the handshake under the slot's mutex. One unreachable peer therefore cannot block sends to other peers (A5), and concurrent first sends to one peer open exactly one connection.

This proves the peer knows the secret. It does not encrypt traffic, authenticate frames after the handshake, or bind the handshake to an address, so an attacker who is on-path or who can make a real peer dial it can relay a handshake. The README security note states these limits and recommends a NetworkPolicy, and a service mesh for encryption. T3 adds `deployments/kubernetes/networkpolicy.yaml`, which admits replication-port traffic only from pods with the app label. T6 exposes the option as `Config.Secret`.

T3 tests: matching secrets round-trip; a wrong secret is rejected and the handler never runs; a hand-written listener that skips the client check and sends a bad server MAC makes the dialer return an auth error without sending a frame; a `Send` on a connection idle longer than `DialTimeout` succeeds without redialing; 10 concurrent first `Send`s to a new peer produce exactly one accepted connection; mode mismatch in both directions fails fast with the mismatch error; a dialer that connects and sends nothing is closed at the listener's deadline without leaking a goroutine; a replayed `Nc` and MAC from an earlier connection is rejected.

### A4. `ExpiresAt` stays an absolute time; skew is documented (T1, T2)

Entries keep an absolute `ExpiresAt` in unix nanoseconds. Replicating a relative TTL would make the same version carry a different `ExpiresAt` on every node, which breaks the snapshot-equality convergence tests and the rule that one version means one entry. The cost is that an entry expires early or late on a peer by that peer's clock skew. Kubernetes nodes run NTP and are normally within tens of milliseconds of each other, so v0.1 documents the tolerance: **TTLs should be at least seconds long, well above expected skew**. The `Entry.ExpiresAt` doc comment (T1) and the README failure-modes section (T6) state this. `DeletedAt` is only read by the local sweeper, so skew there shifts cleanup timing and nothing else.

### A5. Fanout uses bounded per-peer queues (T4)

T4's single replication loop waits for every peer before taking the next mutation, so one slow peer slows writes to all of them. T4 instead keeps **one queue per peer**: a mutex-guarded ring buffer (capacity `OutboxSize`, default 1024) plus a one-slot signal channel, drained in order by one goroutine per peer with `RequestTimeout` per send. `Set` enqueues on every peer queue without blocking, holding the read lock of the peer-queue map; adding and removing peers takes the write lock.

When a queue is full, the **oldest** queued mutation is dropped. The oldest mutation is the one most likely to have been overwritten by a later write or already delivered by repair. Each drop increments `Stats().Dropped`, which therefore counts per peer, not per write. When a peer leaves the peer set, its sender goroutine is cancelled and its queued mutations are discarded without counting as drops. `Close` waits until every queue is empty **and** no send is in flight, or until `ctx` is done, and only then closes the transport. T5 repair delivers anything dropped.

These T4 tests, all with `transport.Memory`, replace the ticket's "Outbox overflow" test: a peer whose handler blocks until its `ctx` is done does not delay delivery to a second peer; with capacity `C` and `RequestTimeout` longer than the test, a peer whose handler blocks on the first mutation and `M` more writes made after the handler has received it, `Dropped` is exactly `max(0, M - C)` and the queue holds the newest `C`; `Close` delivers all 100 keys to every peer when each handler sleeps 20ms, so the last send is still running when its queue empties; `Close` returns once `ctx` is done when one peer is blocked. T5 adds a test that repair delivers a key evicted from a full queue.

### A6. A departed pod's entries live until their TTL

PodSync has no per-node ownership of entries. When a pod leaves, the entries it wrote stay on every other pod until their TTL expires; entries written without a TTL stay until deleted. Applications that store per-pod state (A1, presence) should always set a TTL. The README failure-modes section (T6) states this.

### A7. Expired entries are kept as long as tombstones (T1)

The existing sweeper deletes an expired entry outright. Its version goes with it, so a peer that missed the newer write can later repair an older, non-expiring version back in. T1 therefore treats an expired entry like a tombstone: **the sweeper removes deleted or expired entries only once `DeletedAt` or `ExpiresAt` is older than `TombstoneTTL`**. Reads already treat expired entries as absent, and `Snapshot()` includes them. T1 adds a fake-clock test: a newer entry expires and is swept past the window, while inside the window an older version from repair is rejected.

### A8. State transfer is paged by shard (T1, T2, T4, T5)

One `StateResponse` carrying the whole store does not fit in a 16 MiB frame once values average more than about 300 bytes at 50k entries, and then hydration and repair fail every time. State transfer is therefore **one request per shard**:

- T1 adds `Store.NumShards()` and `Store.SnapshotShard(i)`.
- T2 defines `StateRequest{Origin, Shard, NumShards, EntryBlob}` and `StateResponse{Origin, Shard, NumShards, EntryBlob}`. A request for a shard index the responder does not have gets an empty page. **The `Entries` field of this addendum is `EntryBlob []byte`** (length-prefixed, individually gob-encoded entries): senders build messages with `wire.NewStateRequest`/`wire.NewStateResponse`; receivers call `wire.DecodeMessage`, then `DecodeEntries`, then `ToEntry` per entry. `NumShards` is the sender's own count and `Shard` may exceed it (`Validate` bounds them independently: `1 <= NumShards <= MaxShards`, `0 <= Shard < MaxShards`). `MaxEntriesPerMessage` is derived from `MaxFrameSize` (`MaxFrameSize/144`, about 116k), so the entry cap never refuses a page that fits in a frame.
- The requester walks shard indexes from 0 up to the larger of its own and the responder's `NumShards`, which it learns from the first response. Every page is merged as it arrives; LWW makes partial or out-of-order pages safe.
- Hydration: if a page fails, the requester moves to the next peer. It resumes at the failed shard only when the new peer reports the same `NumShards`; otherwise it starts again at shard 0, because shard indexes mean different key sets under different shard counts. A node becomes `Ready` only after it has merged every page, or when `HydrationTimeout` expires.
- `MaxStateEntries` stays the documented envelope. The requester sums the entries it received in a round and logs the envelope warning once per round if the total is over. Responders always serve every page.
- A single page that cannot be encoded under `MaxFrameSize` fails that request with an error naming the shard. With 64 shards and FNV hashing that happens only above roughly 1 GiB of total state; fewer shards lower the ceiling proportionally, which the README states. Paging inside a shard is a v0.2 item.
- T5's push side sends the requester's own entries for shard `i` in `StateRequest.EntryBlob` (built with `NewStateRequest`) when the requester's whole store is under `MaxStateEntries / 4` entries. Merging pushed entries is safe whatever the responder's shard layout, so there is no shard-count check.

T4's TCP smoke test also hydrates a node whose total state is larger than `MaxFrameSize` (for example 20k entries with 1 KiB values).

### A9. Keys are `string` or integer types; no `[]byte` keys (T2)

Store keys must be `comparable`, and `[]byte` is not, so T2's `KeyCodec` has built-ins for `string` and the integer types only. Applications with byte-slice keys convert them to `string`.

### A10. Test-plan corrections (T5, T6)

- T5 over-envelope test: the requester pulls from a peer that holds `MaxStateEntries + 1` entries, and the requester logs the warning exactly once for that round.
- T5 tombstone tests: wait for the first `Set` to reach B, then partition **before** the `Delete` on A, and check that B still holds the old value before healing. In the past-TTL case, check that A's tombstone has been swept before healing.
- T6 `demo-hit`: `kubectl port-forward` to a Service pins one pod, so the demo sends its requests from a pod inside the cluster through the Service and shows several distinct `X-Pod` values.
- T6 dependency check: `go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./...` must print only packages under `github.com/LavishGent/podsync`.

### A11. Housekeeping (this PR)

CI reads the Go version from `go.mod` (`go-version-file`), so the two can no longer drift. `roadmap_v1.md` and `steps_breakdown_v1.md` already carry superseded banners. [[go_1.27_upgrade|go_1.27_upgrade.md]] is rewritten to match the protobuf-free repo; the Go bump itself is not part of v0.1.
