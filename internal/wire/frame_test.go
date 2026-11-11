package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LavishGent/podsync/internal/version"
)

const testDeadline = 2 * time.Second

func pipe(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	dl := time.Now().Add(testDeadline)
	_ = a.SetDeadline(dl)
	_ = b.SetDeadline(dl)
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

func sampleEntry() Entry {
	return Entry{
		Key: []byte("k"), Value: []byte("v"), ExpiresAt: 99, DeletedAt: 7, Deleted: true,
		Version: version.Version{Clock: 5, NodeID: "n1"},
	}
}

func TestRoundtripEveryMessageType(t *testing.T) {
	e := sampleEntry()
	cases := []struct {
		typ MsgType
		msg any
	}{
		{MsgMutation, Mutation{Origin: "a", Entry: e}},
		{MsgMutationAck, MutationAck{Applied: true}},
		{MsgStateRequest, StateRequest{Origin: "a", Shard: 3, NumShards: 64, Entries: []Entry{e}}},
		{MsgStateResponse, StateResponse{Origin: "a", Shard: 3, NumShards: 64, Entries: []Entry{e, e}}},
		{MsgPing, Ping{Origin: "a"}},
		{MsgPong, Pong{Origin: "a", Clock: 42}},
	}
	for _, c := range cases {
		a, b := pipe(t)
		errc := make(chan error, 1)
		go func() { errc <- WriteFrame(a, c.typ, c.msg) }()
		typ, payload, err := ReadFrame(b)
		if err != nil {
			t.Fatalf("type %d: ReadFrame: %v", c.typ, err)
		}
		if err := <-errc; err != nil {
			t.Fatalf("type %d: WriteFrame: %v", c.typ, err)
		}
		if typ != c.typ {
			t.Fatalf("type = %d, want %d", typ, c.typ)
		}
		got, err := DecodeMessage(typ, payload)
		if err != nil {
			t.Fatalf("type %d: DecodeMessage: %v", c.typ, err)
		}
		if !reflect.DeepEqual(got, c.msg) {
			t.Fatalf("type %d: got %+v, want %+v", c.typ, got, c.msg)
		}
	}
}

func TestTruncatedHeader(t *testing.T) {
	a, b := pipe(t)
	go func() { a.Write([]byte{0, 0, 0}); a.Close() }()
	if _, _, err := ReadFrame(b); err == nil {
		t.Fatal("expected error for truncated header")
	}
}

func TestTruncatedPayload(t *testing.T) {
	a, b := pipe(t)
	go func() {
		hdr := []byte{0, 0, 0, 10, byte(MsgPing)}
		a.Write(append(hdr, 1, 2, 3))
		a.Close()
	}()
	_, _, err := ReadFrame(b)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}
}

// failAfterHeader serves a header and fails the test if read past it.
type failAfterHeader struct {
	t      *testing.T
	header []byte
	off    int
}

func (r *failAfterHeader) Read(p []byte) (int, error) {
	if r.off >= len(r.header) {
		r.t.Error("ReadFrame read past the header of an oversized frame")
		return 0, io.EOF
	}
	n := copy(p, r.header[r.off:])
	r.off += n
	return n, nil
}

func TestOversizedFrameRejectedBeforePayload(t *testing.T) {
	hdr := make([]byte, 5)
	binary.BigEndian.PutUint32(hdr, MaxFrameSize+1)
	hdr[4] = byte(MsgPing)
	_, _, err := ReadFrame(&failAfterHeader{t: t, header: hdr})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestWriteFrameRejectsOversized(t *testing.T) {
	big := Mutation{Origin: "a", Entry: Entry{Value: make([]byte, MaxFrameSize)}}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgMutation, big); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Fatal("oversized frame must not be written")
	}
}

type countingWriter struct {
	writes int
	buf    bytes.Buffer
}

func (w *countingWriter) Write(p []byte) (int, error) { w.writes++; return w.buf.Write(p) }

func TestWriteFrameSingleWrite(t *testing.T) {
	w := &countingWriter{}
	if err := WriteFrame(w, MsgPing, Ping{Origin: "a"}); err != nil {
		t.Fatal(err)
	}
	if w.writes != 1 {
		t.Fatalf("writes = %d, want 1", w.writes)
	}
}

func TestEmptyPayloadFrame(t *testing.T) {
	a, b := pipe(t)
	go func() { a.Write([]byte{0, 0, 0, 0, byte(MsgPing)}) }()
	typ, payload, err := ReadFrame(b)
	if err != nil {
		t.Fatal(err)
	}
	if typ != MsgPing || len(payload) != 0 {
		t.Fatalf("got type %d len %d", typ, len(payload))
	}
}

func TestUnknownMsgType(t *testing.T) {
	for _, typ := range []MsgType{0, 7, 255} {
		if _, err := DecodeMessage(typ, nil); !errors.Is(err, ErrUnknownMsgType) {
			t.Fatalf("type %d: err = %v, want ErrUnknownMsgType", typ, err)
		}
	}
}

func TestDecodeGarbage(t *testing.T) {
	if _, err := DecodeMessage(MsgPing, []byte("not gob")); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestDecodeSizedForMessage(t *testing.T) {
	var p Ping
	if err := Decode([]byte{}, &p); err == nil {
		t.Fatal("empty payload must not decode into a message")
	}
	if !strings.Contains(MsgType(9).String(), "9") {
		t.Fatal("String should include unknown numeric value")
	}
}
