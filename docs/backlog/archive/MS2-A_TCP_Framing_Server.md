# MS2-A: TCP Framing & Transport Server

> 📍 [[Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]]

## Overview
Implement the custom length-prefixed TCP framing protocol for streaming protobuf envelopes, and construct the connection manager/cache for reusing sockets. Build both the `TCPTransport` and a `MockTransport` for local unit-testing.

- **Milestone**: 2 (P2P Transport & Buffers)
- **Track**: A (Developer A)
- **Status**: Ready
- **Dependencies**: [[MS1-C_Codecs_Protobuf|MS1-C]] (needs message definitions & Makefile dependencies)

## Phase Reference
This ticket implements:
- [[steps_breakdown_v1#Phase 6 — Transport: TCP Framing|Phase 6 — Transport: TCP Framing]] (Steps 82–90)
- [[steps_breakdown_v1#Phase 7 — Transport: Connection Manager and Server|Phase 7 — Transport: Connection Manager and Server]] (Steps 91–102)

## Detailed Steps

### TCP Framing Protocol
1. Create `internal/transport/frame.go`:
   - Define `MessageType uint8` constants (mapping to each message type defined in protobuf).
   - Implement `WriteFrame(w io.Writer, msgType MessageType, payload []byte) error`:
     - Write a 4-byte big-endian length prefix.
     - Write a 1-byte message type field.
     - Write the raw payload bytes.
   - Implement `ReadFrame(r io.Reader) (MessageType, []byte, error)`:
     - Read 4 bytes for length -> decode.
     - Read 1 byte for message type.
     - Read `length` bytes for the payload. Ensure safety limits on maximum frame size (e.g., limit payload to 16MB to avoid out-of-memory attacks).

### Transport Engine
2. Create `internal/transport/transport.go` defining the `Transport` interface:
   ```go
   type MessageHandler func(msgType MessageType, payload []byte) ([]byte, error)

   type Transport interface {
       Send(ctx context.Context, addr string, msgType MessageType, payload []byte) ([]byte, error)
       Listen(addr string, handler MessageHandler) error
       Close() error
   }
   ```
3. Create `internal/transport/tcp.go`:
   - Implement `TCPTransport` struct.
   - Implement a connection cache: `map[string]net.Conn` protected by a `sync.Mutex` to pool and reuse connections to peers.
   - Implement dial retry logic with exponential backoff (attempt 3 times before failing).
   - Implement `Send(...)` (retrieve or dial connection, write frame, read response frame).
   - Implement `Listen(...)` (start `net.Listen`, run accept loop, handle framing per connection, dispatch to handler in concurrent goroutines).
4. Create `internal/transport/mock.go`:
   - Implement `MockTransport` which bypasses TCP sockets entirely, routing payload exchanges between in-memory message handler maps. This is crucial for local testing.

## Verification & Testing
1. Create `internal/transport/frame_test.go`:
   - Test `WriteFrame` and `ReadFrame` roundtrips using `net.Pipe()`.
   - Test truncated or broken frame reads (verify they fail gracefully and return a clear error).
   - Test empty payloads.
   - Test large payloads (1MB+).
2. Create `internal/transport/tcp_test.go`:
   - Spin up two `TCPTransport` instances, configure one to listen and one to send. Exchange a `Ping` message and assert success.
   - Assert that subsequent sends to the same target address reuse the cached socket connection.
   - Simulate a network failure (invalid address/offline listener) and assert that connection retries happen 3 times before failing.
   - Verify `MockTransport` successfully routes messages in unit tests.
