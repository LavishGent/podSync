PodSync: Granular Build Steps

----------------------------------------------------------------------------------------------------

Phase 0 — Repo Foundation

 1. Create project directory: mkdir podsync && cd podsync
 2. Initialize Go module: go mod init github.com/yourname/podsync
 3. Create package skeleton:
 mkdir -p internal/{storage,version,node,replication,membership,transport,entropy} pkg/podsync proto examples/local-two-node deployments/kubernetes docs
 4. Add a go.sum-friendly placeholder: create pkg/podsync/doc.go with just package podsync
 5. Create Makefile with targets: test, race, lint, proto, build
 6. Add .github/workflows/ci.yml — run go test ./... and go vet ./... on push
 7. Add README.md — one paragraph describing what PodSync is and is not
 8. Add LICENSE (MIT or Apache 2.0)
 9. Verify: go test ./... passes (no tests yet, just confirm the module compiles)

----------------------------------------------------------------------------------------------------

Phase 1 — Local Storage Engine

Entry model:

 10. Create internal/storage/entry.go
 11. Define Entry[V any] struct with fields: Value V, ExpiresAt int64, Deleted bool
 12. Write IsExpired(now int64) bool method on Entry
 13. Write IsAlive(now int64) bool — not deleted and not expired

Shard:

 14. Create internal/storage/shard.go
 15. Define Shard[K comparable, V any] with mu sync.RWMutex and data map[K]Entry[V]
 16. Implement shard.get(key K, now int64) (V, bool) — read lock, return zero + false if missing/expired/deleted
 17. Implement shard.set(key K, entry Entry[V]) — write lock, store entry
 18. Implement shard.delete(key K, now int64) — write lock, mark Deleted = true (tombstone, don't remove)

Store:

 19. Create internal/storage/store.go
 20. Define Store[K comparable, V any] with shards []Shard[K, V] and numShards int
 21. Implement shardFor(key K)
  *Shard using FNV-1a hash of the key's string representation
 22. Implement Get(key K) (V, bool) — delegates to shard
 23. Implement Set(key K, value V, expiresAt int64) — delegates to shard
 24. Implement Delete(key K) — delegates to shard
 25. Implement Stats() StoreStats — iterate all shards, count live keys, expired keys, tombstones

TTL sweeper:

 26. Add startSweeper(ctx context.Context, interval time.Duration) goroutine on Store
 27. Sweeper iterates all shards under write lock, removes entries where ExpiresAt > 0 && IsExpired(now) AND tombstone retention has passed
 28. Note: tombstones should NOT be GC'd yet — leave that for Phase 8 (retention period TBD)

Tests:

 29. Create internal/storage/store_test.go
 30. Test: Set then Get returns value
 31. Test: Delete then Get returns false
 32. Test: expired key returns false (mock time)
 33. Test: deleted key cannot be resurrected by a second Set (stale write — this will fail until Phase 2, mark it pending)
 34. Test: shard selection is deterministic (same key → same shard every time)
 35. Test: go test -race ./internal/storage/... passes with concurrent goroutines

Benchmarks:

 36. Create internal/storage/bench_test.go
 37. BenchmarkGet — 1M sequential reads
 38. BenchmarkSet — 1M sequential writes
 39. BenchmarkConcurrentGet — runtime.NumCPU() goroutines, reads only
 40. BenchmarkConcurrentMixed — 80% reads / 20% writes
 41. Run: go test -bench=. -benchmem ./internal/storage/... — record baseline

----------------------------------------------------------------------------------------------------

Phase 2 — Versioning and Conflict Resolution

Version type:

 42. Create internal/version/version.go
 43. Define Version struct { Counter uint64; NodeID string }
 44. Implement IsZero() bool
 45. Implement Compare(a, b Version) int — returns -1, 0, 1
 46. Implement ShouldApplyIncoming(local, incoming Version) bool: - incoming.Counter > local.Counter → true
 - incoming.Counter < local.Counter → false
 - tie → incoming.NodeID > local.NodeID (lexicographic)

Wire versions into Entry:

 47. Update Entry[V] in storage/entry.go — add Version version.Version field
 48. Update shard.set — only apply if ShouldApplyIncoming(existing.Version, incoming.Version) is true
 49. Update shard.delete — tombstone gets a version; only apply if incoming version wins

Tests:

 50. Create internal/version/version_test.go
 51. Test: higher counter wins
 52. Test: lower counter is rejected
 53. Test: equal counters, higher NodeID wins
 54. Test: equal counters, equal NodeID → no-op (idempotent)
 55. Test: tombstone with higher version beats a live entry with lower version
 56. Test: live entry with higher version beats a tombstone with lower version
 57. Go back and un-skip the resurrection test from step 33 — it should now pass
 58. go test -race ./... — confirm still clean

----------------------------------------------------------------------------------------------------

Phase 3 — NodeID and Version Generation

 59. go get github.com/google/uuid
 60. Create internal/node/identity.go
 61. Implement GenerateNodeID() string — uuid.New().String()
 62. Define NodeInfo struct { ID string; Addr string; StartedAt int64; Incarnation uint64 }
 63. Create internal/node/counter.go
 64. Implement Counter — wraps atomic.Uint64
 65. Implement Counter.Next() uint64 — atomic increment, returns new value
 66. Implement NextVersion(nodeID string, counter
  *Counter) version.Version
 67. Tests: - GenerateNodeID() returns unique values across 1000 calls
 - Counter.Next() is monotonically increasing
 - Concurrent calls to Counter.Next() produce no duplicates (go test -race)

----------------------------------------------------------------------------------------------------

Phase 4 — Serialization (Codecs)

 68. Create internal/codec/codec.go
 69. Define Codec[V any] interface { Marshal(V) ([]byte, error); Unmarshal([]byte) (V, error) }
 70. Implement JSONCodec[V any] struct{} — uses encoding/json
 71. Implement BytesCodec struct{} — passthrough for []byte values
 72. Create internal/codec/codec_test.go
 73. Test: JSONCodec roundtrip preserves struct values
 74. Test: JSONCodec unmarshal error on bad input returns error, not panic
 75. Test: BytesCodec roundtrip is identity
 76. Export JSONCodec and BytesCodec from pkg/podsync package

----------------------------------------------------------------------------------------------------

Phase 5 — Transport: Protobuf Messages

 77. go get google.golang.org/protobuf
 78. Create proto/podsync.proto
 79. Define messages: - Mutation (id, origin, key, value, op, version counter+nodeID, expires_at)
 - Ack (success, error_message)
 - SnapshotRequest / SnapshotResponse (entries list)
 - DigestRequest / DigestResponse (shard digests list)
 - DeltaRequest / DeltaResponse
 - PingRequest / PingResponse (node_id, addr)
 - MessageEnvelope (type enum + bytes payload — the outer frame body)
 80. Add protoc invocation to justfile: just proto
 81. Run just proto — verify generated Go files appear in proto/

----------------------------------------------------------------------------------------------------

Phase 6 — Transport: TCP Framing

 82. Create internal/transport/frame.go
 83. Define MessageType uint8 constants (one per message type from proto)
 84. Implement WriteFrame(w io.Writer, msgType MessageType, payload []byte) error: - Write 4-byte big-endian length of payload
 - Write 1-byte message type
 - Write payload bytes
 85. Implement ReadFrame(r io.Reader) (MessageType, []byte, error): - Read 4 bytes → decode length
 - Read 1 byte → message type
 - Read length bytes → payload
 86. Create internal/transport/frame_test.go
 87. Test: WriteFrame + ReadFrame roundtrip using net.Pipe()
 88. Test: truncated read returns error (use limited reader)
 89. Test: zero-length payload works
 90. Test: large payload (1MB) roundtrips correctly

----------------------------------------------------------------------------------------------------

Phase 7 — Transport: Connection Manager and Server

 91. Create internal/transport/transport.go — define Transport interface:
 type Transport interface {
     Send(ctx context.Context, addr string, msgType MessageType, payload []byte) ([]byte, error)
     Listen(addr string, handler MessageHandler) error
     Close() error
 }
 92. Create internal/transport/tcp.go — TCPTransport struct
 93. Implement TCPTransport.dial(addr string) (net.Conn, error) with exponential backoff (3 attempts)
 94. Implement connection cache: map[string]net.Conn with mutex — reuse existing connections
 95. Implement TCPTransport.Send(...) — get/create connection, WriteFrame, ReadFrame response
 96. Implement TCPTransport.Listen(addr, handler) — net.Listen("tcp", addr), accept loop, dispatch to handler
 97. Create internal/transport/mock.go — MockTransport that routes calls to in-process handler functions
 98. Create internal/transport/tcp_test.go
 99. Test: two TCPTransport instances exchange a Ping message
 100. Test: connection is reused on second send to same addr
 101. Test: failed connection retries up to 3 times then returns error
 102. Test: MockTransport works as drop-in for unit tests without real sockets

----------------------------------------------------------------------------------------------------

Phase 8 — Replication Engine

 103. Create internal/replication/queue.go
 104. Define MutationQueue wrapping a chan Mutation (buffered, configurable capacity)
 105. Implement Enqueue(m Mutation) bool — non-blocking, returns false + increments drop metric if full
 106. Implement Drain() <-chan Mutation — returns the channel for workers to read from
 107. Create internal/replication/worker.go
 108. Define Worker struct with queue reference, peer manager reference, transport reference
 109. Implement Worker.Start(ctx context.Context) — goroutine loop: read mutation, select peers, send
 110. Implement selectPeers(all []PeerInfo, fanout int) []PeerInfo — random sample, skip self and Dead peers
 111. Create internal/replication/dedup.go
 112. Define SeenCache struct — map[string]int64 (mutation ID → seen-at timestamp) with mutex
 113. Implement Seen(id string) bool
 114. Implement MarkSeen(id string)
 115. Implement background eviction loop (configurable retention duration)
 116. Wire storage.Store.Set and storage.Store.Delete to enqueue a Mutation after local write
 117. Wire Worker to call storage.Store.set on received ApplyMutation RPC after dedup check
 118. Create internal/replication/replication_test.go
 119. Test: local Set succeeds even when queue is full (queue.Enqueue returns false, write still succeeds)
 120. Test: duplicate mutation ID is ignored on receiving side
 121. Test: mutation received by peer applies to peer's local store
 122. Test: go test -race ./internal/replication/...

----------------------------------------------------------------------------------------------------

Phase 9 — Membership Engine

 123. Create internal/membership/peer.go
 124. Define PeerState enum: Alive, Suspect, Dead, Left
 125. Define PeerInfo struct { ID string; Addr string; State PeerState; LastSeen int64; SuspectSince int64 }
 126. Create internal/membership/manager.go
 127. Define Manager struct — peers map[string]*PeerInfo with mutex
 128. Implement AddPeer(info PeerInfo)
 129. Implement RemovePeer(id string)
 130. Implement AlivePeers() []PeerInfo
 131. Implement UpdateState(id string, state PeerState)
 132. Create internal/membership/static.go
 133. Implement StaticDiscovery — reads []string of addr:port from config, adds each to manager
 134. Create internal/membership/dns.go
 135. Implement KubernetesDNSDiscovery — net.LookupHost(headlessServiceFQDN), adds resolved IPs to manager
 136. Create internal/membership/probe.go
 137. Implement SWIM-style probe loop:
 - Every ProbeInterval: pick random Alive peer, send Ping
 - No response → mark Suspect, record SuspectSince
 - Still no response after SuspectTimeout → mark Dead
 - Dead peers held for DeadRetention then removed
 138. Create internal/membership/membership_test.go
 139. Test: static discovery adds all configured peers
 140. Test: peer is marked Suspect after probe failure (use MockTransport)
 141. Test: peer is marked Dead after SuspectTimeout passes
 142. Test: recovered peer (Ping succeeds again) transitions back to Alive
 143. Test: go test -race ./internal/membership/...

----------------------------------------------------------------------------------------------------

Phase 10 — Hydration and Readiness

 144. Create internal/hydration/lifecycle.go
 145. Define LifecycleState enum: Starting, DiscoveringPeers, Bootstrapping, ApplyingSnapshot, CatchingUp, Ready
 146. Implement LifecycleManager struct with current state + mutex
 147. Implement TransitionTo(state LifecycleState) error — validate legal transitions
 148. Implement IsReady() bool
 149. Create internal/hydration/snapshot.go
 150. Implement RequestSnapshot(ctx, peerAddr, transport) ([]Entry, watermark Version, error):
 - Send SnapshotRequest RPC
 - Peer handler: record current max version, serialize all live entries, send back
 151. Implement ApplySnapshot(entries []Entry) — apply each via normal ShouldApplyIncoming logic
 152. Implement CatchUp(ctx, peerAddr, afterVersion Version, transport):
 - Send DeltaRequest with sinceVersion = watermark
 - Apply returned mutations
 153. Create internal/hydration/buffer.go
 154. Implement mutation buffer: during hydration, incoming ApplyMutation RPCs are buffered, not applied
 155. After CatchingUp → Ready: flush buffer through normal conflict resolution
 156. Handle first-node case: if DiscoveringPeers finds zero peers → transition directly to Ready
 157. Create internal/hydration/hydration_test.go
 158. Test: first node transitions Starting → DiscoveringPeers → Ready (no peers found)
 159. Test: new node requests snapshot, applies it, becomes Ready
 160. Test: mutations received during hydration are buffered then applied
 161. Test: watermark catch-up fills the gap between snapshot and buffered mutations
 162. Test: go test -race ./internal/hydration/...

----------------------------------------------------------------------------------------------------

Phase 11 — Anti-Entropy

 163. Create internal/entropy/digest.go
 164. Implement ComputeShardDigest(shard) ShardDigest:
 - Iterate all live (non-deleted, non-expired) entries in shard
 - XOR FNV hashes of (key + version) for each entry → produces Hash uint64
 - Count live entries → Count int
 165. Implement ComputeAllDigests(store) []ShardDigest — one per shard
 166. Create internal/entropy/repair.go
 167. Implement ExchangeDigests(ctx, peerAddr, localDigests, transport) ([]int, error):
 - Send DigestRequest with local digests
 - Peer compares to its own digests, returns shard IDs where hashes differ
 168. Implement RequestDelta(ctx, peerAddr, mismatchedShardIDs, transport) ([]Entry, error):
 - Peer returns all entries for those shards
 - Caller applies each via normal ShouldApplyIncoming
 169. Implement RepairLoop(ctx context.Context) goroutine:
 - Every RepairInterval: pick random Alive peer
 - Exchange digests → identify mismatched shards
 - Request delta → apply entries
 - Record podsync_anti_entropy_repairs_total
 170. Document tombstone GC risk in docs/failure-modes.md: if partition > tombstone retention, deleted keys can resurrect
 171. Create internal/entropy/entropy_test.go
 172. Test: two stores with identical content produce identical digests
 173. Test: one extra key on peer → detected as mismatch → repaired
 174. Test: stale key on local → repaired to peer's newer version
 175. Test: deleted key (tombstone) is not resurrected during repair
 176. Test: repair loop does not block concurrent reads/writes (go test -race)

----------------------------------------------------------------------------------------------------

Phase 12 — Public API Assembly

 177. Create pkg/podsync/config.go — Config struct with all tunable fields + defaults
 178. Create pkg/podsync/cache.go — Cache[K comparable, V any] struct
 179. Implement New[K, V](cfg Config) (*Cache[K, V], error):
 - Generate NodeID
 - Initialize Store, MutationQueue, Worker, MembershipManager, LifecycleManager, RepairLoop
 - Start background goroutines
 - Begin discovery and hydration
 180. Implement Get(key K) (V, bool) — delegates to store (no lock on caller side)
 181. Implement Set(ctx context.Context, key K, value V, opts ...Option) error — local write + enqueue
 182. Implement Delete(ctx context.Context, key K) error
 183. Implement Ready() bool — delegates to lifecycle manager
 184. Implement Stats() CacheStats
 185. Implement Close(ctx context.Context) error:
 - Signal Left to peers
 - Drain mutation queue (up to ctx deadline)
 - Stop all goroutines
 - Close transport
 186. Create examples/local-two-node/main.go — two Cache instances in one binary using MockTransport
 187. Verify: go run ./examples/local-two-node — both instances converge on same state
 188. go test ./... — full suite passes
 189. go test -race ./... — race detector clean

----------------------------------------------------------------------------------------------------

Phase 13 — Kubernetes Demo

 190. Create deployments/kubernetes/headless-service.yaml
 191. Create deployments/kubernetes/deployment.yaml with readiness probe on /readyz
 192. Create examples/k8s-kind/main.go — HTTP server with /set, /get, /readyz endpoints
 193. Create examples/k8s-kind/Dockerfile
 194. Deploy to Kind: kind create cluster && kubectl apply -f deployments/kubernetes/
 195. Verify pods discover each other (check logs for membership join events)
 196. Scale up: kubectl scale deployment podsync-example --replicas=3 — verify new pods hydrate
 197. Set a value on pod-1, verify it appears on pod-2 and pod-3
 198. Scale down: verify dead peer is eventually removed from membership
 199. Write docs/kubernetes.md — step-by-step setup guide

----------------------------------------------------------------------------------------------------

Phase 14 — Observability

 200. Create internal/metrics/metrics.go — define all counters/gauges as interface (pluggable backend)
 201. Implement NoopMetrics (default — zero cost if user doesn't wire metrics)
 202. Add metric increments at every significant event:
 - podsync_mutations_enqueued_total
 - podsync_mutations_dropped_total
 - podsync_mutations_replicated_total
 - podsync_replication_failures_total
 - podsync_anti_entropy_repairs_total
 - podsync_conflicts_total
 - podsync_hydration_duration_seconds
 - podsync_peers_alive / podsync_peers_suspect
 203. Add structured logging (log/slog) with node_id and peer_id fields on every replication/membership event
 204. Create examples/metrics/main.go — wires a Prometheus-compatible metrics backend
 205. Verify metrics appear in /metrics endpoint in K8s demo

----------------------------------------------------------------------------------------------------

Phase 15 — Performance and Chaos Testing

 206. Create test/integration/two_node_test.go — two real Cache instances over loopback TCP
 207. Test: write to node A, read from node B after replication delay
 208. Test: node B restarts — hydrates from node A — reaches Ready
 209. Test: network partition simulation (drop transport on MockTransport) — both nodes serve reads — healed partition converges
 210. Test: 100 concurrent writers, verify no data races (go test -race)
 211. Test: mutation queue overflow under write flood — local writes never fail
 212. Test: tombstone not resurrected after anti-entropy round
 213. Benchmark: Get p99 target < 1ms
 214. Benchmark: Set p99 target < 2ms
 215. Benchmark: record results in docs/benchmarks.md
 216. Add results to README.md

----------------------------------------------------------------------------------------------------

Phase 16 — Developer Experience

 217. Write docs/architecture.md — five internal systems + how they connect
 218. Write docs/conflict-resolution.md — LWW, logical versions, tie-breaking, tombstones
 219. Write docs/hydration.md — lifecycle diagram, watermark protocol, first-node case
 220. Write docs/anti-entropy.md — shard digests, repair flow, tombstone retention warning
 221. Write docs/failure-modes.md — what happens during partition, what "eventually consistent" means in practice, tombstone GC risk
 222. Update README.md — quickstart, install, local demo, K8s demo, link to all docs
 223. Final check: go test ./..., go vet ./..., go test -race ./... all pass

