# T2: Wire Encoding

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]] · [[../rescope_v0.1|Rescope]]

> **Addendum:** the [[../rescope_v0.1#Addendum (2026-11, before T1)|rescope addendum]] changes this ticket: A4 (`ExpiresAt` stays absolute), A8 (state messages are paged by shard), and A9 (no `[]byte` keys).

## Overview
Define the messages peers exchange and the framing that carries them. `encoding/gob` for the envelope, a user-pluggable `Codec[V]` for values. Replaces [[archive/MS1-C_Codecs_Protobuf|MS1-C]]; protobuf and snappy have already been removed from the repo.

- **Status**: In Progress
- **Dependencies**: [[T1_Versioning_Merge|T1]] (needs `version.Version`)

## Detailed Steps

### Messages
1. Create `internal/wire/messages.go`:
   ```go
   type MsgType uint8
   const (
       MsgMutation MsgType = iota + 1
       MsgMutationAck
       MsgStateRequest
       MsgStateResponse
       MsgPing
       MsgPong
   )

   type Entry struct {
       Key       []byte
       Value     []byte
       ExpiresAt int64
       DeletedAt int64
       Deleted   bool
       Version   version.Version
   }
   type Mutation struct { Origin string; Entry Entry }
   type MutationAck struct { Applied bool }
   type StateRequest struct { Origin string; Shard, NumShards int; EntryBlob []byte }   // A8
   type StateResponse struct { Origin string; Shard, NumShards int; EntryBlob []byte } // A8; was Entries []Entry
   type Ping struct { Origin string }
   type Pong struct { Origin string; Clock uint64 }
   ```
   Every message carries `Origin` so the receiver can `Observe` the sender's clock and log the peer by name.

### Framing
2. Create `internal/wire/frame.go`:
   - Frame layout: `[4-byte big-endian payload length][1-byte MsgType][gob payload]`.
   - `WriteFrame(w io.Writer, t MsgType, msg any) error`: gob-encode into a buffer, then write header + payload in one `Write` call.
   - `ReadFrame(r io.Reader) (MsgType, []byte, error)`: read 5 header bytes with `io.ReadFull`; reject length `> MaxFrameSize` (16 MiB) **before** allocating; `io.ReadFull` the payload.
   - `Decode(payload []byte, into any) error` wrapping `gob.NewDecoder`.
   - Note on gob: a fresh encoder per frame re-sends type info each time. That is acceptable at v0.1 message sizes; do not share one encoder/decoder pair per connection, because type-definition pinning needs a fresh encoder per frame.

### Value codec
3. Create `pkg/podsync/codec.go` (public, since users implement it):
   ```go
   type Codec[V any] interface {
       Marshal(V) ([]byte, error)
       Unmarshal([]byte) (V, error)
   }
   ```
   - `JSONCodec[V]` using `encoding/json`.
   - `BytesCodec` for `V = []byte` (identity).
   - `StringCodec` for `V = string`.
4. Create `internal/wire/convert.go`: `FromEntry[K, V](key K, e storage.Entry[V], keyEnc, valEnc) (Entry, error)` and the reverse. Keys are encoded with a small `KeyCodec[K]` with built-ins for `string`, `[]byte`, and integer types; anything else returns a clear error at `New` time rather than at first write.

## Verification & Testing
1. `internal/wire/frame_test.go`:
   - Roundtrip every message type through `net.Pipe()`.
   - Truncated header and truncated payload each return an error (not a hang; use a deadline).
   - Length `> MaxFrameSize` is rejected without allocation (assert the error fires before reading the payload).
   - Empty payload frame works.
   - Unknown `MsgType` is surfaced as an error by the caller-facing decode helper.
2. `pkg/podsync/codec_test.go`: JSON roundtrip for a struct, bytes identity, string identity, unsupported key type rejected.
3. `internal/wire/bench_test.go`: encode/decode a 100-byte mutation, record ns/op and allocs in the ticket on completion.
4. Confirm `go.mod` still has no `require` block.

## Benchmark results

100-byte value mutation, fresh gob encoder/decoder per frame (Apple M5 Pro, Go 1.26.3, `go test -bench . -benchmem ./internal/wire`). Decode now includes the type-definition check (round 2):

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `BenchmarkEncodeMutation` (`WriteFrame`) | 1721 | 3232 | 27 |
| `BenchmarkDecodeMutation` (`Decode`) | 7772 | 10648 | 246 |

Before the check, decode was 6568 ns/op, 9648 B/op, 220 allocs/op. Decode dominates because each frame re-sends gob type info. Fine at v0.1 volumes; a shared per-connection encoder is not compatible with type-definition pinning, so this cost is deferred past v0.1.

## Review fixes
Round 1:
- State messages carry entries as `EntryBlob` (length-prefixed, individually gob-encoded entries) instead of a gob `[]Entry`, so a sparse payload cannot amplify into large allocations. Limits are checked by a structural walk before any entry is decoded. Build with `NewStateRequest`/`NewStateResponse`, read with `DecodeEntries`.
- `StateRequest`/`StateResponse.Validate()` runs inside `DecodeMessage`.
- `WriteFrame` returns `io.ErrShortWrite` on a short write; `ReadFrame` grows its buffer with bytes received; `Decode` rejects trailing bytes; `ToEntry` rejects a zero Version.

Round 2:
- **Shard bounds are independent**: `1 <= NumShards <= MaxShards` and `0 <= Shard < MaxShards`. `NumShards` is the sender's own count and `Shard` may exceed it, because under A8 the requester walks shards up to the larger of both counts and a responder answers an unknown shard with an empty page. `NewStateRequest`/`NewStateResponse` apply the same check and return `ErrBadShard`.
- **Entry cap derived from the frame**: `MaxEntriesPerMessage = MaxFrameSize / 144` (116508). An encoded entry is at least 140 bytes plus a 4-byte prefix, so no frame holds more; a 50k-entry page round-trips (test). The old cap of 4096 broke "responders always serve every page". `MaxEntrySize` (4 MiB) and the structural walk before decoding stay.
- **Gob type definitions are pinned** (`ErrUnexpectedTypeDef`). Gob decodes unknown fields it ignores, and an unknown recursive type (`struct{Next *rec}`) costs decoder stack linear in payload size: 200 KB grew the stack by 33 MB, and a 16 MiB frame could reach Go's fatal 1 GiB limit, which `recover` cannot catch. `Decode` (used by `DecodeMessage` and for each entry slot) now walks the type-definition messages at the start of the stream, parses them with bounds checks, and requires them to equal what our own encoder emits for the target type (`typedefs.go`). Type ids are process-global and differ between peers, so ids are renumbered by first use before comparing. Consequence: a sender whose struct differs from ours in any field is rejected, so wire-format changes are breaking (A3's hello magic is the version gate). `Decode` fails closed for target types other than the message types and `Entry`.
- `DecodeEntries` walks the whole blob before materialising any entry. `walkBlob` rejects zero-length slots (so `Validate` fails early). `ToEntry` rejects `Clock == 0`. `Decode`'s trailing-byte check covers bytes after the gob stream, not bytes inside a gob record after its value.
- Sentinels added: `ErrUnexpectedTypeDef`. Constants: `MaxEntriesPerMessage` (derived), `MaxEntrySize`, `MaxShards`.

## Known costs and deferred items
- Per-entry gob re-sends type info: about 266 B on the wire per small entry (8 B key, 100 B value) against 125 B for a plain gob `[]Entry`, and about 9 KB of transient allocation per decoded entry (4096 small entries: 36.9 MB TotalAlloc, 34 ms). Transient, not retained. Accepted for v0.1; a shared encoder per blob (one type definition) or a hand-written `Entry` encoding would remove it.
- Deferred past v0.1: `ReadFrame` peaks at up to 2x the payload capacity (`bytes.Buffer` doubling); a streaming `RangeEntries(func(Entry) error)` helper so T4 can merge without materialising a page; one shared gob encoder per blob.
