# T3: Transport & Peer Resolution

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]] · [[../rescope_v0.1|Rescope]]

> **Addendum:** the [[../rescope_v0.1#Addendum (2026-11, before T1)|rescope addendum]] changes this ticket: A3 (optional shared-secret handshake, its tests, README security note, NetworkPolicy example).

## Overview
A minimal request/response TCP transport with one persistent connection per peer, and a peer set driven by DNS instead of gossip membership. Replaces [[archive/MS2-A_TCP_Framing_Server|MS2-A]], [[archive/MS2-B_Membership_Discovery|MS2-B]], and [[archive/MS3-B_SWIM_Probing|MS3-B]]. There is no SWIM, no peer health state, and no retry loop: kubelet owns failure detection, and T5 repair owns recovery from dropped sends.

- **Status**: In review ([PR #10](https://github.com/LavishGent/podSync/pull/10))
- **Dependencies**: [[T2_Wire_Encoding|T2]]

## Detailed Steps

### Transport
1. Create `internal/transport/transport.go`:
   ```go
   type Handler func(ctx context.Context, from string, t wire.MsgType, payload []byte) (wire.MsgType, any, error)

   type Transport interface {
       Listen(ctx context.Context, addr string, h Handler) error
       Send(ctx context.Context, addr string, t wire.MsgType, msg any) (wire.MsgType, []byte, error)
       Close() error
   }
   ```
2. Create `internal/transport/tcp.go` implementing `TCP`:
   - `Listen`: `net.Listen`, accept loop, one goroutine per connection reading frames in a loop and writing one response frame per request. Connection is closed on any read/write/handler error. Returns when `ctx` is cancelled.
   - `Send`: look up `conns[addr]` under a mutex; dial lazily with `net.Dialer{Timeout}` if absent. Set read/write deadlines from `ctx`. On any error, close and delete the cached conn so the next `Send` redials. One in-flight request per connection (guard with a per-conn mutex); concurrent callers to the same peer serialize. This is adequate at v0.1 write rates; pipelining is a v0.2 item.
   - Defaults: `DialTimeout` 2s, `RequestTimeout` 1s.
3. Create `internal/transport/mem.go` implementing `Memory`:
   - A process-local registry mapping addr → `Handler`. `Send` calls the handler directly.
   - `Partition(a, b string)` and `Heal(a, b string)` so T4/T5 tests can simulate network splits. `Drop(fn func(from, to string, t wire.MsgType) bool)` for selective loss.

### Peer resolution
4. Create `internal/peers/resolver.go`:
   ```go
   type Resolver interface {
       Resolve(ctx context.Context) ([]string, error) // "host:port" list
   }
   ```
   - `Static(addrs ...string) Resolver`.
   - `DNS(service, namespace string, port int) Resolver`: `LookupIPAddr` on `<service>.<namespace>.svc.cluster.local`, returns `ip:port` for each A/AAAA record. Accepts an optional `*net.Resolver` for tests.
5. Create `internal/peers/set.go`:
   - `Set` holds the current peer list minus self. `Run(ctx)` re-resolves every `RefreshInterval` (default 5s) and swaps the list atomically. `Peers() []string` returns a copy. `OnChange(func(added, removed []string))` for logging.
   - Self detection: `SelfAddr(port)` reads `POD_IP`, falling back to the first non-loopback unicast interface address. Logged at startup so misconfiguration is visible.

## Verification & Testing
1. `internal/transport/tcp_test.go`:
   - Two `TCP` instances on `127.0.0.1:0`; Ping → Pong roundtrip.
   - Second `Send` to the same addr reuses the connection (assert dial count via an injected dialer or by counting accepts on the listener).
   - Stop the listener; next `Send` fails within `RequestTimeout`; restart on the same port; the following `Send` succeeds (redial).
   - Handler error closes the connection and surfaces an error to the sender.
   - Concurrent `Send`s from 10 goroutines to one peer all complete (`-race`).
2. `internal/transport/mem_test.go`: Ping roundtrip; `Partition` makes `Send` fail; `Heal` restores it; `Drop` filter is honored.
3. `internal/peers/resolver_test.go`: `DNS` against a fake `net.Resolver` (use `Dial` hook to a local DNS stub, or wrap `LookupIPAddr` behind a small interface) returns the expected addrs and excludes self. `Set.Run` observes additions and removals across two resolutions.
