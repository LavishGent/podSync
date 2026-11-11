package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"reflect"
	"runtime"
	"testing"

	"github.com/LavishGent/podsync/internal/version"
)

func entries(n int) []Entry {
	out := make([]Entry, n)
	for i := range out {
		out[i] = sampleEntry()
	}
	return out
}

func TestEntriesLimits(t *testing.T) {
	tiny := func(n int) []Entry {
		out := make([]Entry, n)
		for i := range out {
			out[i] = Entry{Version: version.Version{Clock: 1, NodeID: "n"}}
		}
		return out
	}
	for _, n := range []int{0, 1, 100} {
		in := tiny(n)
		req, err := NewStateRequest("a", 0, 1, in)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if err := req.Validate(); err != nil {
			t.Fatalf("n=%d: Validate: %v", n, err)
		}
		got, err := req.DecodeEntries()
		if err != nil || len(got) != n {
			t.Fatalf("n=%d: got %d entries, err %v", n, len(got), err)
		}
		if n > 0 && !reflect.DeepEqual(got[n-1], in[n-1]) {
			t.Fatalf("entry mismatch: %+v", got[n-1])
		}
	}
	if _, err := NewStateResponse("a", 0, 1, tiny(MaxEntriesPerMessage+1)); !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("limit+1 encode: err = %v", err)
	}
	// limit+1 on the decode side, built by hand.
	blob, err := EncodeEntries(tiny(MaxEntriesPerMessage))
	if err != nil {
		t.Fatal(err)
	}
	if err := (StateResponse{NumShards: 1, EntryBlob: blob}).Validate(); err != nil {
		t.Fatalf("exactly at the cap: %v", err)
	}
	one, _ := EncodeEntries(tiny(1))
	resp := StateResponse{NumShards: 1, EntryBlob: append(blob, one...)}
	if err := resp.Validate(); !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("limit+1 Validate: err = %v", err)
	}
	if _, err := resp.DecodeEntries(); !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("limit+1 DecodeEntries: err = %v", err)
	}
}

func TestEntryTooLarge(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], MaxEntrySize+1)
	resp := StateResponse{NumShards: 1, EntryBlob: hdr[:]}
	if err := resp.Validate(); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("err = %v", err)
	}
	if _, err := EncodeEntries([]Entry{{Value: make([]byte, MaxEntrySize)}}); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("encode err = %v", err)
	}
}

func TestMalformedBlob(t *testing.T) {
	for _, blob := range [][]byte{{0, 0}, {0, 0, 0, 9, 1}, {0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 0}} {
		resp := StateResponse{NumShards: 1, EntryBlob: blob}
		if err := resp.Validate(); !errors.Is(err, ErrBadEntryBlob) {
			t.Fatalf("blob %v: Validate err = %v, want ErrBadEntryBlob", blob, err)
		}
		if _, err := resp.DecodeEntries(); !errors.Is(err, ErrBadEntryBlob) {
			t.Fatalf("blob %v: DecodeEntries err = %v, want ErrBadEntryBlob", blob, err)
		}
	}
}

func allocDelta(f func()) uint64 {
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	f()
	runtime.ReadMemStats(&b)
	return b.TotalAlloc - a.TotalAlloc
}

// DecodeEntries called directly walks the whole blob before decoding the
// first entry, so a bad tail costs no per-entry allocation.
func TestDecodeEntriesPreflight(t *testing.T) {
	good, err := EncodeEntries(entries(1000))
	if err != nil {
		t.Fatal(err)
	}
	// Decoding 1000 entries really does allocate far more than the bound.
	if a := allocDelta(func() { _, _ = DecodeEntries(good) }); a < 2<<20 {
		t.Fatalf("control decode allocated only %d bytes; bound is meaningless", a)
	}
	truncated := append(append([]byte{}, good...), 0, 0, 0, 9, 1)
	small, _ := EncodeEntries([]Entry{{Version: version.Version{Clock: 1, NodeID: "n"}}})
	overCap := bytes.Repeat(small, MaxEntriesPerMessage+1)
	for name, blob := range map[string][]byte{"malformed final slot": truncated, "over cap": overCap} {
		var err error
		a := allocDelta(func() { _, err = DecodeEntries(blob) })
		if err == nil {
			t.Fatalf("%s accepted", name)
		}
		if a > 256<<10 {
			t.Fatalf("%s: allocated %d bytes before rejecting", name, a)
		}
	}
	if _, err := DecodeEntries(overCap); !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("over cap err = %v", err)
	}
}

// A8: responders always serve every page. 50k entries in one shard must fit.
func TestLargePageRoundtrip(t *testing.T) {
	const n = 50_000
	in := make([]Entry, n)
	for i := range in {
		in[i] = Entry{Key: []byte{byte(i), byte(i >> 8)}, Version: version.Version{Clock: uint64(i) + 1, NodeID: "n"}}
	}
	resp, err := NewStateResponse("a", 0, 1, in)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgStateResponse, resp); err != nil {
		t.Fatal(err)
	}
	typ, pl, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	m, err := DecodeMessage(typ, pl)
	if err != nil {
		t.Fatal(err)
	}
	got, err := m.(StateResponse).DecodeEntries()
	if err != nil || len(got) != n || !reflect.DeepEqual(got[n-1], in[n-1]) {
		t.Fatalf("got %d entries, err %v", len(got), err)
	}
}

// MaxEntriesPerMessage assumes no encoded entry is smaller than
// minEntryWire; fail loudly if gob ever emits less.
func TestMinEntryWireSize(t *testing.T) {
	blob, err := EncodeEntries([]Entry{{}})
	if err != nil {
		t.Fatal(err)
	}
	if len(blob) < minEntryWire {
		t.Fatalf("smallest entry is %d bytes on the wire, below minEntryWire %d", len(blob), minEntryWire)
	}
}

// sparseGobPayload is the Codex finding: 100k empty elements in ~100 KB of
// gob, encoded by an old-style sender with a valid NumShards.
func sparseGobPayload(t testing.TB, n int) []byte {
	type emptyEntry struct{ Key []byte }
	msg := struct {
		Origin    string
		Shard     int
		NumShards int
		Entries   []emptyEntry
	}{NumShards: 1, Entries: make([]emptyEntry, n)}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(msg); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestSparseAmplificationRejected(t *testing.T) {
	payload := sparseGobPayload(t, 100_000)
	if len(payload) > 200_000 {
		t.Fatalf("payload unexpectedly large: %d", len(payload))
	}
	// Old-style sender: Entries as a gob slice. Its type definitions differ
	// from ours, so it is refused up front (before, gob ignored the unknown
	// field; either way nothing is materialised).
	var err error
	if a := allocDelta(func() { _, err = DecodeMessage(MsgStateResponse, payload) }); a > 2<<20 {
		t.Fatalf("decode allocated %d bytes", a)
	}
	if !errors.Is(err, ErrUnexpectedTypeDef) {
		t.Fatalf("err = %v, want ErrUnexpectedTypeDef", err)
	}

	// Same sparse data with our own definitions and a valid shard: the only
	// place to hide it is the blob, where 100k zero-length slots are
	// malformed. The walk rejects the first.
	blob := make([]byte, 4*100_000)
	resp := StateResponse{NumShards: 1, EntryBlob: blob}
	var p bytes.Buffer
	if err := WriteFrame(&p, MsgStateResponse, resp); err != nil {
		t.Fatal(err)
	}
	typ, pl, err := ReadFrame(&p)
	if err != nil {
		t.Fatal(err)
	}
	if a := allocDelta(func() { _, err = DecodeMessage(typ, pl) }); a > 2<<20 {
		t.Fatalf("decode allocated %d bytes", a)
	}
	if !errors.Is(err, ErrBadEntryBlob) {
		t.Fatalf("err = %v, want ErrBadEntryBlob", err)
	}
}

func roundtripMsg(t *testing.T, typ MsgType, msg any) any {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteFrame(&buf, typ, msg); err != nil {
		t.Fatal(err)
	}
	gt, pl, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMessage(gt, pl)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	return got
}

// A8: shard counts may differ between peers, so Shard can exceed the
// sender's NumShards in both directions.
func TestShardBeyondSenderNumShards(t *testing.T) {
	req := mustReq(t, "a", 40, 32, nil)
	if got := roundtripMsg(t, MsgStateRequest, req).(StateRequest); got.Shard != 40 || got.NumShards != 32 {
		t.Fatalf("request: %+v", got)
	}
	resp := mustResp(t, "b", 40, 32, nil)
	got := roundtripMsg(t, MsgStateResponse, resp).(StateResponse)
	if got.Shard != 40 || got.NumShards != 32 {
		t.Fatalf("response: %+v", got)
	}
	if es, err := got.DecodeEntries(); err != nil || len(es) != 0 {
		t.Fatalf("empty page: %v %v", es, err)
	}
}

func TestValidateShards(t *testing.T) {
	bad := []struct{ shard, n int }{{-1, 4}, {0, 0}, {0, -1}, {0, MaxShards + 1}, {MaxShards, 4}, {MaxShards, MaxShards}}
	for _, c := range bad {
		if err := (StateRequest{Shard: c.shard, NumShards: c.n}).Validate(); !errors.Is(err, ErrBadShard) {
			t.Fatalf("%+v: err = %v", c, err)
		}
		var buf bytes.Buffer
		_ = WriteFrame(&buf, MsgStateResponse, StateResponse{Shard: c.shard, NumShards: c.n})
		typ, pl, _ := ReadFrame(&buf)
		if _, err := DecodeMessage(typ, pl); !errors.Is(err, ErrBadShard) {
			t.Fatalf("DecodeMessage %+v: err = %v", c, err)
		}
		if _, err := NewStateRequest("a", c.shard, c.n, nil); !errors.Is(err, ErrBadShard) {
			t.Fatalf("NewStateRequest %+v: err = %v", c, err)
		}
		if _, err := NewStateResponse("a", c.shard, c.n, nil); !errors.Is(err, ErrBadShard) {
			t.Fatalf("NewStateResponse %+v: err = %v", c, err)
		}
	}
	for _, c := range []struct{ shard, n int }{{MaxShards - 1, MaxShards}, {MaxShards - 1, 1}, {0, 1}, {40, 32}} {
		if err := (StateRequest{Shard: c.shard, NumShards: c.n}).Validate(); err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
	}
}

var _ = version.Version{}
