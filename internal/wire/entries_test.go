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
	for _, n := range []int{0, 1, MaxEntriesPerMessage} {
		in := entries(n)
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
	if _, err := NewStateResponse("a", 0, 1, entries(MaxEntriesPerMessage+1)); !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("limit+1 encode: err = %v", err)
	}
	// limit+1 on the decode side, built by hand.
	blob, _ := EncodeEntries(entries(MaxEntriesPerMessage))
	one, _ := EncodeEntries(entries(1))
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
	for _, blob := range [][]byte{{0, 0}, {0, 0, 0, 9, 1}, {0, 0, 0, 0}} {
		resp := StateResponse{NumShards: 1, EntryBlob: blob}
		if err := resp.Validate(); err == nil {
			// zero-length entry passes the structural walk but must fail decode
			if _, err := resp.DecodeEntries(); err == nil {
				t.Fatalf("blob %v accepted", blob)
			}
		}
	}
}

// sparseGobPayload is the Codex finding: 100k empty elements in ~100 KB of
// gob, encoded with a structurally compatible sender type.
func sparseGobPayload(t testing.TB, n int) []byte {
	type emptyEntry struct{ Key []byte }
	msg := struct {
		Origin  string
		Entries []emptyEntry
	}{Entries: make([]emptyEntry, n)}
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
	// Old-style sender: Entries as a gob slice. The receiver has no such
	// field, so gob ignores it; nothing is materialised.
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := DecodeMessage(MsgStateResponse, payload)
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Fatal("sparse payload accepted")
	}
	if d := after.TotalAlloc - before.TotalAlloc; d > 2<<20 {
		t.Fatalf("decode allocated %d bytes", d)
	}

	// Blob form: 100k zero-length entries (4 bytes each) is rejected by the
	// structural walk without decoding any entry.
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
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = DecodeMessage(typ, pl)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrTooManyEntries) {
		t.Fatalf("err = %v, want ErrTooManyEntries", err)
	}
	if d := after.TotalAlloc - before.TotalAlloc; d > 2<<20 {
		t.Fatalf("decode allocated %d bytes", d)
	}
}

func TestValidateShards(t *testing.T) {
	bad := []struct{ shard, n int }{{-1, 4}, {4, 4}, {0, 0}, {0, -1}, {0, MaxShards + 1}}
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
	}
	if err := (StateRequest{Shard: MaxShards - 1, NumShards: MaxShards}).Validate(); err != nil {
		t.Fatal(err)
	}
}

var _ = version.Version{}
