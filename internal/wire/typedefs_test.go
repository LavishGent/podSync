package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"math/rand"
	"runtime"
	"testing"
)

type recursive struct{ Next *recursive }

func gobBytes(t testing.TB, v any) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(v); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func slot(b []byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(b))), b...)
}

// deepRecursive encodes a sender struct whose unknown field is a linked list
// n deep. It runs on its own goroutine so its stack is released afterwards.
func deepRecursive(t testing.TB, n int, withShards bool) []byte {
	t.Helper()
	var raw []byte
	done := make(chan struct{})
	go func() {
		defer close(done)
		x := &recursive{}
		for i := 0; i < n; i++ {
			x = &recursive{Next: x}
		}
		if withShards {
			raw = gobBytes(t, struct {
				NumShards int
				Unknown   *recursive
			}{1, x})
		} else {
			raw = gobBytes(t, struct {
				Key     []byte
				Unknown *recursive
			}{[]byte("k"), x})
		}
	}()
	<-done
	return raw
}

// measureOnFreshGoroutine runs f and returns the stack and heap growth.
func measureOnFreshGoroutine(f func()) (stack int64, alloc uint64) {
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
		runtime.ReadMemStats(&b)
	}()
	<-done
	return int64(b.StackInuse) - int64(a.StackInuse), b.TotalAlloc - a.TotalAlloc
}

func TestRecursiveUnknownFieldInEnvelopeRejected(t *testing.T) {
	raw := deepRecursive(t, 100_000, true)
	var err error
	stack, alloc := measureOnFreshGoroutine(func() {
		_, err = DecodeMessage(MsgStateResponse, raw)
	})
	if !errors.Is(err, ErrUnexpectedTypeDef) {
		t.Fatalf("err = %v, want ErrUnexpectedTypeDef", err)
	}
	if stack > 1<<20 || alloc > 1<<20 {
		t.Fatalf("decode grew stack by %d and allocated %d bytes", stack, alloc)
	}
}

func TestRecursiveUnknownFieldInEntryRejected(t *testing.T) {
	blob := slot(deepRecursive(t, 100_000, false))
	var err error
	stack, alloc := measureOnFreshGoroutine(func() {
		_, err = DecodeEntries(blob)
	})
	if !errors.Is(err, ErrUnexpectedTypeDef) {
		t.Fatalf("err = %v, want ErrUnexpectedTypeDef", err)
	}
	if stack > 1<<20 || alloc > 1<<20 {
		t.Fatalf("decode grew stack by %d and allocated %d bytes", stack, alloc)
	}
	// And through a message envelope that is itself valid.
	resp := StateResponse{NumShards: 1, EntryBlob: blob}
	if _, err := DecodeMessage(MsgStateResponse, gobBytes(t, resp)); err != nil {
		t.Fatalf("envelope: %v", err)
	}
	if _, err := resp.DecodeEntries(); !errors.Is(err, ErrUnexpectedTypeDef) {
		t.Fatalf("DecodeEntries err = %v", err)
	}
}

func TestOwnEncodingPassesTypeDefCheck(t *testing.T) {
	for _, tc := range []struct {
		t   MsgType
		msg any
	}{
		{MsgMutation, Mutation{Origin: "a", Entry: sampleEntry()}},
		{MsgMutationAck, MutationAck{Applied: true}},
		{MsgStateRequest, StateRequest{NumShards: 1}},
		{MsgStateResponse, StateResponse{NumShards: 1}},
		{MsgPing, Ping{Origin: "a"}},
		{MsgPong, Pong{Origin: "a", Clock: 3}},
	} {
		if _, err := DecodeMessage(tc.t, gobBytes(t, tc.msg)); err != nil {
			t.Fatalf("%v: %v", tc.t, err)
		}
	}
}

// Gob type ids are process-global and depend on registration order; a peer
// that registered types in another order must still be accepted.
func TestTypeDefCheckIgnoresIDAssignment(t *testing.T) {
	type other struct{ A int }
	gobBytes(t, other{}) // shift the global id counter
	if _, err := DecodeMessage(MsgMutation, gobBytes(t, Mutation{Origin: "a", Entry: sampleEntry()})); err != nil {
		t.Fatal(err)
	}
}

func TestMalformedTypeDefsNeverPanic(t *testing.T) {
	good := gobBytes(t, Mutation{Origin: "a", Entry: sampleEntry()})
	cases := map[string][]byte{
		"empty":             {},
		"nil":               nil,
		"truncated length":  {0xff},
		"truncated length2": {0xfe, 0x01},
		"huge length":       {0xf8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		"length past end":   {0x10, 0x01},
		"zero length msg":   {0x00},
		"bad byte count":    {0x80, 0x01},
		"def id only":       {0x01, 0x01},
		"def truncated":     good[:len(good)/3],
		"def trailing":      append(append([]byte{}, good[:5]...), 0xff),
		"value only":        {0x03, 0x04, 0x00, 0x00},
		"many empty defs":   bytes.Repeat([]byte{0x02, 0x01, 0x00}, 5000),
	}
	for name, p := range cases {
		for _, call := range []struct {
			n string
			f func() error
		}{
			{"message", func() error { _, err := DecodeMessage(MsgMutation, p); return err }},
			{"entry", func() error { _, err := DecodeEntries(slot(p)); return err }},
		} {
			if err := call.f(); err == nil {
				t.Fatalf("%s/%s: accepted", name, call.n)
			}
		}
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		b := append([]byte{}, good...)
		for j := 0; j < 1+i%3; j++ {
			b[rng.Intn(len(b))] = byte(rng.Intn(256))
		}
		_, _ = DecodeMessage(MsgMutation, b)
		_, _ = DecodeEntries(slot(b))
		_, _ = DecodeMessage(MsgMutation, b[:rng.Intn(len(b))])
	}
}

func TestDecodeUnregisteredTypeFailsClosed(t *testing.T) {
	var x struct{ A int }
	if err := Decode(gobBytes(t, x), &x); err == nil {
		t.Fatal("Decode into an unregistered type must fail")
	}
}
