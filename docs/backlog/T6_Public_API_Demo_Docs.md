# T6: Public API, Kind Demo & Docs

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]] · [[../rescope_v0.1|Rescope]]

> **Addendum:** the [[../rescope_v0.1#Addendum (2026-11, before T1)|rescope addendum]] changes this ticket: A1 (per-pod rate-limit keys and `Cache.Range`), A2 and A3 (`Config.FirstRepairDelay`, `Config.Secret`), A4 and A6 (README failure modes), and A10 (in-cluster `demo-hit`, dependency check).

## Overview
Expose `Node` through `pkg/podsync`, prove it in Kind with a rate-limiter demo, and rewrite the README around pod-originated state. Replaces [[archive/MS4-B_Public_API_Assembly|MS4-B]], [[archive/MS4-C_Kubernetes_Demo|MS4-C]], and [[archive/MS5-C_Technical_Docs_DX|MS5-C]]. Metrics export ([[archive/MS5-B_Observability_Metrics|MS5-B]]) stays deferred; `Stats()` is the v0.1 observability surface.

- **Status**: Ready
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
       OutboxSize, MaxStateEntries int
   }

   func New[K comparable, V any](cfg Config[K, V]) (*Cache[K, V], error)
   func (c *Cache[K, V]) Start(ctx context.Context) error
   func (c *Cache[K, V]) Get(key K) (V, bool)
   func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, opts ...SetOption) error
   func (c *Cache[K, V]) Delete(ctx context.Context, key K) error
   func (c *Cache[K, V]) Ready() bool
   func (c *Cache[K, V]) Stats() Stats
   func (c *Cache[K, V]) Close(ctx context.Context) error
   ```
   - `WithTTL(d)` as the only `SetOption` in v0.1.
   - `New` validates: resolver and codec present, key type supported, shards > 0. Returns errors, never panics.
   - Re-export `Resolver`, `DNS`, `Static`, `Codec`, `JSONCodec`, `BytesCodec`, `StringCodec` so users import one package.
2. `pkg/podsync/doc.go`: package doc with the positioning line, a 20-line example, and the operating envelope.

### Demo
3. `examples/ratelimit/main.go`: HTTP server with `GET /hit?client=<id>` enforcing N requests per 10s window per client. Window counters live in podsync keyed by `client`; the handler reads locally, increments, and `Set`s. Note in a comment that LWW makes concurrent increments on different pods lossy; the demo shows convergence, not exact counting, and names the PN-counter as the v0.2 fix.
   - `GET /readyz` wired to `cache.Ready()`.
   - `GET /stats` returning `cache.Stats()` as JSON.
   - Logs every hydration, peer change, and repair at info with node ID.
4. `deployments/kubernetes/`:
   - `headless-service.yaml` (`clusterIP: None`, port 9090).
   - `deployment.yaml`: 3 replicas, `POD_IP` from `fieldRef`, readiness probe on `/readyz`, `preStop` sleep 2s, a ClusterIP `Service` on the HTTP port for the demo script.
5. `justfile` targets: `demo-up` (kind create, build image, load, apply), `demo-hit` (port-forward and loop `curl /hit` showing the limit holding across pods via the `X-Pod` header), `demo-scale N` (scale and tail logs for hydration), `demo-down`.

### Docs
6. README rewrite, in this order:
   1. Positioning line and the ten-line example.
   2. "When to use this" with the pod-originated examples; "When not to" with a table against Redis, ConfigMap+informer, and Olric.
   3. How it works: five sentences, one per layer (store, version, transport, peers, repair), each linking to the code.
   4. Operating envelope and tuning knobs.
   5. Failure modes: partition, simultaneous restart of all pods (state is lost; that is the contract), partition longer than `TombstoneTTL`, outbox overflow, clock reset on restart.
   6. Running the demo.
   7. Development (`just test`, `just race`, `just lint`), no protoc section.
7. Delete the stale project-structure tree from the README; `go doc` and the file tree are the source of truth.
8. Mark [[../roadmap_v1|roadmap_v1.md]] as historical at the top (already done in the rescope PR) and leave it.

## Verification & Testing
1. `pkg/podsync/podsync_test.go`: `New` validation cases; end-to-end two-cache test on `transport.TCP` with `Static` resolver; `Get` after `Set` on the other cache.
2. `go test -race ./...`, `go vet ./...`, `golangci-lint run` green in CI.
3. `go.mod` has no `require` block; `go list -deps ./... | grep -v '^std'` lists only this module.
4. From a clean checkout with Kind installed: `just demo-up && just demo-hit` works in under five minutes; `just demo-scale 5` shows two new pods log `Hydrating → Ready` and serve correct limits.
5. A reader who has never seen the project can explain what it is for from the first screen of the README.
