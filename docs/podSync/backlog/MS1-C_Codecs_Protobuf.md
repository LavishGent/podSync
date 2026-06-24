# MS1-C: Serialization & Message Definitions

## Overview
Implement the value serialization codecs (JSON and Bytes) and define the Google Protobuf message schemas for peer-to-peer communication. Establish Makefile commands for compiling the `.proto` schemas into Go code.

- **Milestone**: 1 (Foundations & Core Engines)
- **Track**: C (Developer C)
- **Status**: Ready
- **Dependencies**: None

## Phase Reference
This ticket implements:
- [Phase 4 — Serialization (Codecs)](file:///c:/Obsidian-Vaults/podSync/PodSync%20-%20Granular%20Build%20Steps.md#L120-L130) (Steps 68–76)
- [Phase 5 — Transport: Protobuf Messages](file:///c:/Obsidian-Vaults/podSync/PodSync%20-%20Granular%20Build%20Steps.md#L134-L147) (Steps 77–81)

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
