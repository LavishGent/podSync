# MS1-C: Serialization & Message Definitions

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]]

## Overview
Implement the value serialization codecs (JSON and Bytes) and define the Google Protobuf message schemas for peer-to-peer communication. Establish Makefile commands for compiling the `.proto` schemas into Go code. Also implements the wire codec layer (`internal/codec`) which serializes and compresses entries for the replication transport path using protobuf + snappy.

- **Milestone**: 1 (Foundations & Core Engines)
- **Track**: C (Developer C)
- **Status**: In Progress (wire codec layer complete; JSONCodec/BytesCodec/full proto suite pending)
- **Dependencies**: None

## Implementation Progress

### ✅ Wire Codec Layer (complete)
- `proto/wire.proto` — `WireEntry` message definition
- `proto/wire.pb.go` — generated Go bindings
- `internal/codec/codec.go` — `WireEntry` Go struct, `Codec` interface, `snappyProtoCodec` impl (proto marshal → snappy compress), decompression bomb guard (`maxDecompressedSize = 4 MiB`)
- `internal/codec/codec_test.go` — 6 tests passing, race-clean
- `internal/codec/bench_test.go` — `BenchmarkEncode` (~254 ns/op) + `BenchmarkDecode` (~208 ns/op)
- Spec: `docs/superpowers/specs/2026-06-25-wire-codec-design.md`

### ⏳ Remaining
- `JSONCodec[V any]` and `BytesCodec` in `internal/codec`
- Full proto message suite (`Mutation`, `Ack`, `SnapshotRequest/Response`, `DigestRequest/Response`, `DeltaRequest/Response`, `PingRequest/Response`, `MessageEnvelope`) in `proto/podsync.proto`
- Export codecs from `pkg/podsync`

## Wire Codec Design

A wire codec spec was produced during MS1-A: `docs/superpowers/specs/2026-06-25-wire-codec-design.md`. Key decisions recorded here:

- **In-memory storage is NOT compressed.** `Entry[V any]` stores native Go values. This preserves the sub-microsecond read path.
- **Compression lives on the wire only.** Entries are serialized + compressed when sent to peers (replication, MS2+) and decompressed + deserialized on receipt.
- **Serialization:** protobuf. A `WireEntry` message is defined in `proto/wire.proto` (in addition to the other messages below).
- **Compression:** snappy (`github.com/golang/snappy`). Fast (300+ MB/s), low CPU, pure Go. Chosen over zstd for simplicity; swappable via the `Codec` interface.
- **`internal/codec`** exposes a `Codec` interface with `Encode(WireEntry) ([]byte, error)` and `Decode([]byte) (WireEntry, error)`. `WireEntry` is a non-generic Go struct with `Key []byte`, `Value []byte`, `ExpiresAt int64`, `Deleted bool`, `Version uint64`. Value serialization of `V` is the caller's responsibility.

### `proto/wire.proto` — WireEntry message
```protobuf
message WireEntry {
  bytes  key        = 1;
  bytes  value      = 2;
  int64  expires_at = 3;
  bool   deleted    = 4;
  uint64 version    = 5;
}
```

## Phase Reference
This ticket implements:
- [[steps_breakdown_v1#Phase 4 — Serialization (Codecs)|Phase 4 — Serialization (Codecs)]] (Steps 68–76)
- [[steps_breakdown_v1#Phase 5 — Transport: Protobuf Messages|Phase 5 — Transport: Protobuf Messages]] (Steps 77–81)

## Detailed Steps

### Codecs / Serialization
1. Create `internal/codec/codec.go`:
   - Define `Codec[V any]` interface:
     - `Marshal(V) ([]byte, error)`
     - `Unmarshal([]byte) (V, error)`
   - Implement `JSONCodec[V any]` struct utilizing `encoding/json`.
   - Implement `BytesCodec` struct (passthrough for `[]byte` values).
2. Export `JSONCodec` and `BytesCodec` from the main `pkg/podsync` package.

### Protobuf Message Definitions
3. Install Go protobuf library: `go get google.golang.org/protobuf`.
4. Create `proto/podsync.proto` defining:
   - `Mutation`: fields for `id` (string), `origin` (string), `key` (string), `value` (bytes), `op` (enum/int), `version_counter` (uint64), `version_node_id` (string), and `expires_at` (int64).
   - `Ack`: fields for `success` (bool) and `error_message` (string).
   - `SnapshotRequest` & `SnapshotResponse` (contains a list of entries/mutations).
   - `DigestRequest` & `DigestResponse` (contains lists of shard digests/hashes).
   - `DeltaRequest` & `DeltaResponse` (contains mutations).
   - `PingRequest` & `PingResponse` (node metadata).
   - `MessageEnvelope`: fields for `type` (enum of message types) and `payload` (bytes). This serves as the unified frame wrapper.
5. Add `proto` compilation target to the `Makefile` (e.g. using `protoc --go_out=. --go_opt=paths=source_relative proto/podsync.proto`).
6. Run `make proto` to generate the Go bindings in `proto/`.

## Verification & Testing
1. Create `internal/codec/codec_test.go`:
   - Test that `JSONCodec` correctly marshals and unmarshals structs, preserving their fields.
   - Test that invalid input to `JSONCodec` returns a descriptive error rather than causing a panic.
   - Test that `BytesCodec` behaves as an identity function for byte slices.
2. Compile and inspect generated protobuf code in `proto/` folder. Verify imports are correct and types are exposed as expected.
