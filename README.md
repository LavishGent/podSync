# PodSync

**Embeddable Go library that replicates small, short-lived state between the pods of a Kubernetes Deployment, from memory to memory, with no Redis.** Reads are local; writes replicate asynchronously; replicas converge.

```go
cache, err := podsync.New(podsync.Config[string, Flag]{
    Resolver: podsync.DNS("podsync-headless", "default", 9090),
    Codec:    podsync.JSONCodec[Flag]{},
})
if err != nil { log.Fatal(err) }
if err := cache.Start(ctx); err != nil { log.Fatal(err) } // returns once hydrated from peers
defer cache.Close(shutdownCtx)

cache.Set(ctx, "checkout-v2", Flag{On: true}, podsync.WithTTL(10*time.Minute))
flag, ok := cache.Get("checkout-v2") // local memory, no network
```

> **Status:** v0.1 proof of concept, in review as a stack of PRs ([backlog](docs/backlog/Backlog_Index.md), [rescope](docs/rescope_v0.1.md)). No non-stdlib dependencies. The local demo is tested; the **Kind demo is untested** (see [Running the demo](#running-the-demo)).

## When to use this

State that a **pod originates** and every pod wants to read quickly, where a few hundred milliseconds of staleness is fine:

- Per-pod counters summed across pods (the rate-limiter demo)
- Presence and heartbeat data ("which pods are serving which tenant")
- Locally computed hot results other pods can reuse
- Short-lived flags or overrides written by the application itself

### When not to

| Need | Use instead |
|---|---|
| Durable data, sessions that survive a restart of all pods | Redis, a database |
| Shared state larger than ~50k small entries, or large values | Redis, [Olric](https://github.com/olric-data/olric) |
| Config that operators edit, read by every pod | ConfigMap plus an informer; it is already replicated and durable |
| Strong consistency, exact counting, transactions, queues | A database or broker |
| Many fleets, many nodes outside Kubernetes | A gossip system (memberlist) or Olric |

## How it works

Each pod keeps a sharded in-memory [store](internal/storage) in which every entry has a TTL, or a tombstone once deleted. Every write gets a Lamport-clock [version](internal/version) (ties broken by node ID), and the highest version wins a merge on every pod. A `Set` is applied locally, then queued to each peer over persistent [TCP connections](internal/transport) with gob-encoded frames ([node](internal/node), [wire](internal/wire)); a full queue drops its oldest mutation. Peers are found by re-resolving a headless Service in [DNS](internal/peers) every few seconds, and a new pod hydrates from its peers before it reports Ready. Every `RepairInterval` each pod pulls a random peer's state and merges it, which is what makes dropped or missed writes converge ([repair](internal/node/repair.go)).

## Operating envelope and knobs

Designed for **3 to 25 pods**, about **50k entries** of small values, TTLs of **seconds or longer** (expiry uses each pod's clock). Keys are strings or integers. Hydration and repair move full state one shard page at a time; with the default 64 shards a single shard page fails to encode above roughly 1 GiB of total state, and fewer shards lower that ceiling.

`podsync.Config` (defaults in parentheses; see `go doc ./pkg/podsync Config`):

| Knob | Meaning |
|---|---|
| `Resolver`, `Codec` | Required. `DNS(service, namespace, port)` or `Static(addrs...)`; `JSONCodec`, `StringCodec`, `BytesCodec` or your own |
| `ListenAddr` (`:9090`), `Self` (POD_IP + listen port) | Replication port and the address peers use to reach this pod |
| `DefaultTTL` (none), `Shards` (64) | Default expiry; store shard count (max 1024) |
| `TombstoneTTL` (1h), `SweepInterval` (1m) | How long deleted and expired entries are kept, and how often they are swept |
| `RefreshInterval` (5s) | DNS re-resolve period |
| `RepairInterval` (30s), `FirstRepairDelay` (15s) | Repair period (jittered 10%) and the delay before the first repair after Ready |
| `HydrationTimeout` (30s), `RequestTimeout` (1s) | Startup hydration bound; per-request bound |
| `QueueSize` (1024), `MaxStateEntries` (50k) | Per-peer fanout queue; envelope above which a warning is logged |
| `Secret` (none) | Optional shared secret for the handshake, at least 32 bytes |

`Stats()` is the observability surface: entry counts, peers, state, and counters for dropped mutations, send failures, received mutations, conflicts, repaired entries and repair failures. `Range(fn)` walks live entries; it is O(entries), so use it for small stores.

Deploy with a headless Service with `publishNotReadyAddresses: true` ([example](deployments/kubernetes/headless-service.yaml)), `POD_IP` from the downward API, a readiness probe on `cache.Ready()`, and a short `preStop` sleep ([example](deployments/kubernetes/deployment.yaml)).

## Failure modes

- **Repair is the safety net.** Fanout is best effort: a full queue drops its oldest mutation (counted in `Stats().Dropped`), a send can fail, a partition can last. Every `RepairInterval` (default 30s, jittered ±10%) each node pulls one random peer's state (and pushes its own when it holds fewer than `MaxStateEntries / 4` entries), so dropped fanout converges within a few intervals. The first tick after a node turns Ready fires after `FirstRepairDelay` (default 15s), which covers a peer whose DNS has not yet seen the new pod.
- **A departed pod's entries live until their TTL.** There is no per-pod ownership or eviction on departure; entries a pod wrote stay on the others until they expire or are overwritten. Always set a TTL on per-pod state.
- **A partition longer than `TombstoneTTL` can resurrect deleted keys.** Once a tombstone is swept, a peer that missed the delete still holds the old value and repair copies it back. Raise `TombstoneTTL` if that matters.
- **If every pod restarts at once, state is lost.** It lives only in memory; there is no one left to hydrate from. That is the contract.
- **A restarted pod's clock starts over.** Versions are Lamport clocks, and a new process begins low. It hydrates before serving, which advances its clock past what it saw, but a write made during a window in which it had not yet seen a key's latest version can lose to the older write. Hydration and repair bound this.
- **Concurrent writes to one key are last-writer-wins.** One of two simultaneous writes from different pods is discarded. Give each pod its own key if increments must not be lost (the demo does), and treat summed values as eventually consistent: they can overshoot the limit.
- **Clock skew moves expiry.** `ExpiresAt` is an absolute time compared with each pod's clock, so entries expire early or late by the skew. Keep TTLs at seconds or more.
- **Over the envelope.** Above `MaxStateEntries` the node logs a warning each hydration or repair round; it still works until a shard page no longer fits in a frame.

## Security

The replication port has no authentication by default: anything that can reach it can write any key. Two mitigations:

- **NetworkPolicy (recommended).** Admit the replication port only from your own pods. See [`deployments/kubernetes/networkpolicy.yaml`](deployments/kubernetes/networkpolicy.yaml).
- **Optional shared-secret handshake (off by default).** Set `Config.Secret` to at least 32 bytes, identical on every node, and each connection must prove it knows the secret (HMAC-SHA256 challenge/response) before any frame is processed. A node with a secret and a node without one refuse each other immediately (`handshake mode mismatch`).

The handshake proves the peer knows the secret. It does **not** encrypt traffic, authenticate frames after the handshake, or bind the handshake to an address, so an on-path attacker, or one who can make a real peer dial it, can relay a handshake. For confidentiality and stronger identity, use a service mesh (mTLS).

## Running the demo

The demo is a rate limiter: 5 hits per 10 s per client, shared across pods, one counter key per pod. Full steps and expected output are in [docs/demo_manual.md](docs/demo_manual.md).

- **Local, no Kubernetes (tested):** `just demo-local` or `scripts/demo-local.sh` runs three instances on 127.0.0.1 and hits them with `curl`.
- **Kind (untested):** `just demo-up`, `just demo-hit`, `just demo-scale 5`, `just demo-down`. These were written on a machine without `kind` or Docker and have never been run; treat them as a draft.

## Development

Go 1.26 or newer and [`just`](https://github.com/casey/just). No code generation, no dependencies beyond the standard library.

```sh
just test    # go test ./...
just race    # go test -race ./...
just lint    # golangci-lint run ./...
just deps    # list non-stdlib dependencies (must be only this module)
```

The file tree and `go doc ./pkg/podsync` are the source of truth for structure. More: [Rescope v0.1](docs/rescope_v0.1.md), [Backlog](docs/backlog/Backlog_Index.md), [Roadmap v1](docs/roadmap_v1.md) (historical).

## License

[MIT](LICENSE)
