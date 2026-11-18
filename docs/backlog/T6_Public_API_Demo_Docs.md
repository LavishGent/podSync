# T6: Public API, Kind Demo & Docs

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]] · [[../rescope_v0.1|Rescope]]

> **Addendum:** the [[../rescope_v0.1#Addendum (2026-11, before T1)|rescope addendum]] changes this ticket: A1 (per-pod rate-limit keys and `Cache.Range`), A2 and A3 (`Config.FirstRepairDelay`, `Config.Secret`), A4 and A6 (README failure modes), and A10 (in-cluster `demo-hit`, dependency check).

## Overview
Expose `Node` through `pkg/podsync`, prove it in Kind with a rate-limiter demo, and rewrite the README around pod-originated state. Replaces [[archive/MS4-B_Public_API_Assembly|MS4-B]], [[archive/MS4-C_Kubernetes_Demo|MS4-C]], and [[archive/MS5-C_Technical_Docs_DX|MS5-C]]. Metrics export ([[archive/MS5-B_Observability_Metrics|MS5-B]]) stays deferred; `Stats()` is the v0.1 observability surface.

- **Status**: In review
- **Kind demo is untested.** It was written on a machine without `kind`, `docker` or `just`. The Deployment, Dockerfile and `demo-*` just targets have never been run. A tested, no-Kubernetes alternative runs the same example as three local processes: `scripts/demo-local.sh` (`just demo-local`); the manual is [[../demo_manual|demo_manual.md]]. Verification item 4 is therefore open until someone runs it on Kind.
- **Dependencies**: [[T5_Repair|T5]]

## Detailed Steps

### Public API
1. `pkg/podsync/podsync.go`:
   ```go
   type Config[K comparable, V any] struct {
       Resolver   Resolver          // required; podsync.DNS(...) or podsync.Static(...)
       ListenAddr string            // default ":9090"
       Codec      Codec[V]          // required
       DefaultTTL time.Duration     // 0 = no expiry
       Shards     int               // default 64
       Logger     *slog.Logger      // default slog.Default()
       // Tuning, all with defaults from T1-T5:
       TombstoneTTL, SweepInterval, RefreshInterval, RepairInterval,
       HydrationTimeout, RequestTimeout time.Duration
       QueueSize, MaxStateEntries int // the node calls it QueueSize; the ticket said OutboxSize
       RefreshInterval, SweepInterval, FirstRepairDelay time.Duration
       Self, NodeID string // Self defaults to POD_IP + listen port; excluded from the peer set
       Secret []byte      // optional handshake secret, >= 32 bytes (A3)
   }

   func New[K comparable, V any](cfg Config[K, V]) (*Cache[K, V], error)
   func (c *Cache[K, V]) Start(ctx context.Context) error
   func (c *Cache[K, V]) Get(key K) (V, bool)
   func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, opts ...SetOption) error
   func (c *Cache[K, V]) Delete(ctx context.Context, key K) error
   func (c *Cache[K, V]) Range(fn func(K, V) bool) // live entries, O(entries) (A1)
   func (c *Cache[K, V]) ID() string
   func (c *Cache[K, V]) Ready() bool
   func (c *Cache[K, V]) Stats() Stats
   func (c *Cache[K, V]) Close(ctx context.Context) error
   ```
   - `WithTTL(d)` as the only `SetOption` in v0.1.
   - `New` validates: resolver and codec present, key type supported, shards > 0. Returns errors, never panics.
   - Re-export `Resolver`, `DNS`, `Static`, `Codec`, `JSONCodec`, `BytesCodec`, `StringCodec` so users import one package.
2. `pkg/podsync/doc.go`: package doc with the positioning line, a 20-line example, and the operating envelope.

### Demo
3. `examples/ratelimit/main.go`: HTTP server with `GET /hit?client=<id>` enforcing N requests per 10s window per client. Per addendum A1, each pod writes its own key `limit:<client>:<nodeID>` with value `{Window, Count}` and TTL of two windows; a request sums the current-window entries via `Range`, under a per-client mutex. The global sum is eventually consistent and can overshoot; a comment says so and names the PN-counter as the v0.2 fix. Implemented in `limiter_test.go`: exact single-pod count, rollover, and a two-pod no-undercount test.
   - `GET /readyz` wired to `cache.Ready()`.
   - `GET /stats` returning `cache.Stats()` as JSON.
   - Logs every hydration, peer change, and repair at info with node ID.
4. `deployments/kubernetes/`:
   - `headless-service.yaml` (`clusterIP: None`, `publishNotReadyAddresses: true`, port 9090; T3/T4 had 7946, changed to match the default listen port).
   - `deployment.yaml`: 3 replicas, `POD_IP` from `fieldRef`, readiness probe on `/readyz`, `preStop` sleep 2s (the distroless image has no shell, so the binary runs `ratelimit -sleep 2s`), a ClusterIP `Service` on the HTTP port for the demo script.
5. `justfile` targets: `demo-up` (kind create, build image, load, apply), `demo-hit` (a `kubectl run` curl pod loops `/hit` through the Service, A10, showing the limit holding across pods via the `X-Pod` header), `demo-scale N` (scale and tail logs for hydration), `demo-down`.

### Docs
6. README rewrite, in this order:
   1. Positioning line and the ten-line example.
   2. "When to use this" with the pod-originated examples; "When not to" with a table against Redis, ConfigMap+informer, and Olric.
   3. How it works: five sentences, one per layer (store, version, transport, peers, repair), each linking to the code.
   4. Operating envelope and tuning knobs.
   5. Failure modes: partition, simultaneous restart of all pods (state is lost; that is the contract), partition longer than `TombstoneTTL`, queue overflow, clock reset on restart.
   6. Running the demo.
   7. Development (`just test`, `just race`, `just lint`), no protoc section.
7. Delete the stale project-structure tree from the README; `go doc` and the file tree are the source of truth.
8. Mark [[../roadmap_v1|roadmap_v1.md]] as historical at the top (already done in the rescope PR) and leave it.

## Verification & Testing
1. `pkg/podsync/podsync_test.go`: `New` validation cases; end-to-end two-cache test on `transport.TCP` with `Static` resolver; `Get` after `Set` on the other cache.
2. `go test -race ./...`, `go vet ./...`, `golangci-lint run` green in CI.
3. Done: `TestOnlyThisModule` in `pkg/podsync/deps_test.go` and `just deps`. `go.mod` has no `require` block; `go list -deps ./... | grep -v '^std'` lists only this module.
4. **Not run** (no Kind here; the local demo covers the same behaviour on 127.0.0.1). From a clean checkout with Kind installed: `just demo-up && just demo-hit` works in under five minutes; `just demo-scale 5` shows two new pods log `Hydrating → Ready` and serve correct limits.
5. Unverified by a stranger; the README was rewritten in the ticket order. A reader who has never seen the project can explain what it is for from the first screen of the README.
