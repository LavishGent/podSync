# T2: Wire Encoding

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]] · [[../rescope_v0.1|Rescope]]

> **Addendum:** the [[../rescope_v0.1#Addendum (2026-11, before T1)|rescope addendum]] changes this ticket: A4 (`ExpiresAt` stays absolute), A8 (state messages are paged by shard), and A9 (no `[]byte` keys).

## Overview
Define the messages peers exchange and the framing that carries them. `encoding/gob` for the envelope, a user-pluggable `Codec[V]` for values. Replaces [[archive/MS1-C_Codecs_Protobuf|MS1-C]]; protobuf and snappy have already been removed from the repo.

- **Status**: Ready
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
   type StateRequest struct { Origin string }
   type StateResponse struct { Origin string; Entries []Entry }
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
   - Note on gob: a fresh encoder per frame re-sends type info each time. That is acceptable at v0.1 message sizes; if it shows in profiles, switch to one encoder/decoder pair per connection in T3.

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
