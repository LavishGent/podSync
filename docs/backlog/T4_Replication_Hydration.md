# T4: Replication & Hydration

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]] · [[../rescope_v0.1|Rescope]]

> **Addendum:** the [[../rescope_v0.1#Addendum (2026-11, before T1)|rescope addendum]] changes this ticket: A2 (hydrating pods receive fanout; `publishNotReadyAddresses: true`), A5 (bounded per-peer queues replace the single outbox), A6 (no eviction on peer departure) and A8 (paged state transfer). This text reflects the code as built.

## Overview
Wire the store, versioning, transport, and peer set together into a node (`internal/node`) that replicates writes to every peer and hydrates from a peer at startup. Replaces [[archive/MS2-C_Replication_Buffers|MS2-C]], [[archive/MS3-A_Replication_Worker|MS3-A]], and [[archive/MS3-C_Hydration_Lifecycle|MS3-C]]. No worker pool, no dedup cache, no mutation buffering during hydration; see [[../rescope_v0.1|Rescope]] for why each was dropped.

- **Status**: In review
- **Dependencies**: [[T3_Transport_Peers|T3]]

## What was built

### Node
`Node[K, V]` (`New(Config)`, then `Start`, `Close`) owns the `*storage.Store` (built from `Config.Store`, sharing the node clock), a `*version.Lamport`, a `transport.Transport`, a `*peers.Set`, key and value codecs, a `*slog.Logger`, and a `state atomic.Int32` (`Starting`, `Hydrating`, `Ready`, `Closed`). `Get`, `Snapshot`, `Stats`, `Ready` are the read side. `version.Lamport` gained `Current()` (read without advancing) for `Pong`.

`Config` validation rejects a missing transport, peer set or value codec and a `Store.NumShards` above `wire.MaxShards`. `KeyCodec` defaults to `wire.NewKeyCodec[K]()`.

### Write path
`Set(ctx, key, value, expiresAt)` and `Delete(ctx, key)`:
1. `v := lamport.Next()`; encode with `wire.FromEntry` first, so a codec failure leaves local state untouched.
2. `store.Merge` the entry locally.
3. Enqueue the `wire.Mutation` on **every current peer's queue** and return. Neither call ever blocks on the network.

**Per-peer bounded queues (A5).** Each peer gets its own goroutine and a bounded channel (`QueueSize`, default 1024). The goroutine sends one mutation at a time with `RequestTimeout` (default 1s). A slow or partitioned peer therefore delays only its own queue.
- **Drop policy:** when a peer's queue is full, the **oldest** queued mutation for that peer is dropped (it is the one most likely already overwritten or repaired) and the new one is queued. Each drop increments `Stats().Dropped` (per peer, not per write) and logs a warning, sampled to once per second per peer.
- **Failures:** a failed send increments `Stats().SendFailures`, logs a sampled warning, and is **not retried**. Dropped and failed mutations are the job of **T5 repair**, which is the safety net for both.
- **Lifecycle follows `peers.Set`:** a worker is created when a peer appears and stopped when it disappears. Mutations still queued for a departed peer are discarded and are not counted as drops.
- **A departed pod's entries live until their TTL (A6).** There is no peer-departure eviction: entries written by a pod that left stay on every other pod until they expire, or until deleted if they carry no TTL. Applications that store per-pod state must set a TTL.

### Receive path
`handle` serves every message type in every state except `Closed`:
- `MsgMutation`: `lamport.Observe(version)`, decode, `store.Merge`, reply `MutationAck{Applied}`. A decode failure returns an error, increments `Stats().DecodeErrors`, and never touches local state. Rejected merges increment `ReceivedConflicts`.
- `MsgStateRequest`: replies with **one shard page** (`SnapshotShard(req.Shard)`, built with `wire.NewStateResponse`, `NumShards` = this store's count). A shard the node does not have yields an empty page. A page that cannot be encoded under `wire.MaxFrameSize` (or exceeds `wire.MaxEntriesPerMessage`) fails with an error naming the shard. Responders always serve every page; `MaxStateEntries` is a requester-side envelope only (A8). Pushed entries in the request blob are ignored in T4; T5 adds the push side.
- `MsgPing`: replies `Pong` with `lamport.Current()`.

### Lifecycle
`Start(ctx)`:
1. `Listen` on `ListenAddr` (default `Self`) in a goroutine, then ping `Self` until the transport answers, so `Start` never returns before the handler is registered. Listen errors surface here.
2. Register the peer-change callback, then refresh the peer set until it succeeds or `ResolveTimeout` (default 5s) passes (a warning is logged, the node proceeds with no peers), then run `peers.Set.Run` in the background.
3. No peers: `Ready`, log "first node, starting with empty state".
4. Otherwise `Hydrating` (**paged, A8**): shuffle peers; for each, request shard 0, 1, 2, … up to the larger of this node's and the peer's `NumShards`, merging each page (`Observe` + `Merge`) as it arrives. If a page fails, move to the next peer, resuming at the failed shard only if that peer reports the same `NumShards`; otherwise restart at shard 0. When every page is merged the node goes `Ready`. If a whole round fails, retry after `HydrationRetryInterval` (1s) until `HydrationTimeout` (30s), then go `Ready` with whatever was merged and log at **error**. If a round receives more than `MaxStateEntries` (50k) entries, log the envelope warning.
5. The store sweeper starts at step 2. The T5 repair loop will start here.
Mutations that arrive while `Hydrating` are merged immediately; LWW makes order irrelevant.

Background goroutines run until `Close`, not until `Start`'s ctx ends (ctx only bounds `Start`).

`Close(ctx)`: stop accepting `Set`/`Delete` (`ErrClosed`), **drain the per-peer queues** until every queue is empty and no send is in flight, or `ctx` is done (then `Close` returns an error counting the undelivered mutations), then stop workers and the listener, set `Closed`, and call `transport.Close()`. `Ready()` is false as soon as `Close` begins.

### Readiness and DNS (A2 decision)
The headless Service sets **`publishNotReadyAddresses: true`** (`deployments/kubernetes/headless-service.yaml`). A hydrating pod therefore appears in DNS and receives fanout while it hydrates. A hydrating peer may answer a `StateRequest` with partial state; the joiner merges what it gets, and LWW plus T5 repair converge the rest. No readiness flag is added to the wire. The residual gap (a peer whose next DNS refresh has not yet seen the pod) is closed by T5 repair and its early `FirstRepairDelay` tick.

### Stats
`Stats()` returns `storage.StoreStats` plus `Peers` (peers with a queue), `State`, `Dropped`, `SendFailures`, `ReceivedMutations`, `ReceivedConflicts`, `DecodeErrors`.

## Verification & Testing
`internal/node/node_test.go` uses `transport.Memory` and `peers.Static` (a mutable resolver for the lifecycle test); no sockets. Everything runs under `-race`.

- Three nodes each write 1000 keys with `runtime.Gosched`; snapshots become identical within 5s.
- Overlapping keys: three nodes write the same 50 keys concurrently; all converge, and a later write (whose clock has observed the winner) outranks it everywhere.
- Delete on A after Set on B converges to a tombstone everywhere.
- A node joining after 1000 writes is `Ready` after `Start` with an identical snapshot.
- Paged hydration across shard counts: 8→16, 16→8, 8→8; and resume on the next peer after a mid-walk page failure.
- Hydration skips a partitioned first peer (fixed peer order via a test hook); all peers partitioned: `Ready` after `HydrationTimeout`, empty state, error logged.
- A node serves mutations while `Hydrating`.
- Queue overflow: a peer whose handler blocks on the first mutation, then `M` more writes with capacity `C`: `Dropped == M - C`, the newest `C` survive in order, every key is readable locally.
- Fault injection: a slow peer (`SetDelay`) does not delay a healthy peer; partition then heal (new writes arrive, the write made while partitioned is not retried, `FailNext` recovers); overflow while a peer is a black hole is counted in `Dropped` and delivery resumes after heal.
- `Close` drains (100 keys to a 20ms-per-send peer, all present when `Close` returns); `Close` returns when `ctx` is done with a blocked peer; `Set` after `Close` returns `ErrClosed`.
- Peer lifecycle: worker created on add, stopped on removal, removal is not counted as drops, the departed peer's entries stay.
- `Ping` returns the clock; a bad mutation is counted and ignored; config validation.
- `internal/node/tcp_smoke_test.go`: three nodes on `127.0.0.1` with the `TCP` transport (ports picked up front). One `Set` replicates; then 20k entries with 1 KiB values (over `MaxFrameSize`) are written and a joiner hydrates all of it page by page.

## Known limits (POC)
- No retry: dropped and failed mutations rely on T5 repair.
- No hostile-peer hardening beyond what T2/T3 provide.
- `Start` pings `Self`, so `Self` must be reachable by the transport (on Kubernetes, the pod IP and port).
