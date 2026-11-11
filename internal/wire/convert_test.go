package wire

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/LavishGent/podsync/internal/storage"
	"github.com/LavishGent/podsync/internal/version"
)

type strCodec struct{}

func (strCodec) Marshal(s string) ([]byte, error)   { return []byte(s), nil }
func (strCodec) Unmarshal(b []byte) (string, error) { return string(b), nil }

type failCodec struct{}

func (failCodec) Marshal(string) ([]byte, error)   { return nil, errors.New("boom") }
func (failCodec) Unmarshal([]byte) (string, error) { return "", errors.New("boom") }

type userID int32
type name string

func TestKeyCodecRoundtrip(t *testing.T) {
	sc, err := NewKeyCodec[string]()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := sc.Encode("hello")
	if k, err := sc.Decode(b); err != nil || k != "hello" {
		t.Fatalf("got %q %v", k, err)
	}

	ic, err := NewKeyCodec[int64]()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []int64{0, 1, -1, 1 << 62, -1 << 63} {
		b, _ := ic.Encode(v)
		if k, err := ic.Decode(b); err != nil || k != v {
			t.Fatalf("got %d %v want %d", k, err, v)
		}
	}

	uc, _ := NewKeyCodec[uint8]()
	b, _ = uc.Encode(200)
	if k, err := uc.Decode(b); err != nil || k != 200 {
		t.Fatalf("got %d %v", k, err)
	}

	nc, err := NewKeyCodec[userID]()
	if err != nil {
		t.Fatal(err)
	}
	b, _ = nc.Encode(-5)
	if k, err := nc.Decode(b); err != nil || k != -5 {
		t.Fatalf("named int: got %d %v", k, err)
	}
	sn, err := NewKeyCodec[name]()
	if err != nil {
		t.Fatal(err)
	}
	b, _ = sn.Encode("x")
	if k, err := sn.Decode(b); err != nil || k != "x" {
		t.Fatalf("named string: got %q %v", k, err)
	}
}

func TestKeyCodecDecodeErrors(t *testing.T) {
	ic, _ := NewKeyCodec[int8]()
	if _, err := ic.Decode([]byte{1, 2}); err == nil {
		t.Fatal("wrong length must error")
	}
	wide, _ := NewKeyCodec[int64]()
	b, _ := wide.Encode(1000)
	if _, err := ic.Decode(b); err == nil {
		t.Fatal("overflow must error")
	}
}

func TestKeyCodecUnsupported(t *testing.T) {
	type pair struct{ a, b int }
	if _, err := NewKeyCodec[pair](); err == nil || !strings.Contains(err.Error(), "unsupported key type") {
		t.Fatalf("struct key: err = %v", err)
	}
	if _, err := NewKeyCodec[float64](); err == nil {
		t.Fatal("float key must be rejected")
	}
	if _, err := NewKeyCodec[[2]byte](); err == nil {
		t.Fatal("array key must be rejected")
	}
	if _, err := NewKeyCodec[any](); err == nil {
		t.Fatal("interface key must be rejected")
	}
}

func TestEntryConvertRoundtrip(t *testing.T) {
	kc, _ := NewKeyCodec[string]()
	in := storage.Entry[string]{Value: "v", ExpiresAt: 50, Version: version.Version{Clock: 3, NodeID: "n"}}
	w, err := FromEntry("k", in, kc, strCodec{})
	if err != nil {
		t.Fatal(err)
	}
	k, out, err := ToEntry(w, kc, strCodec{})
	if err != nil || k != "k" || out != in {
		t.Fatalf("got %q %+v %v", k, out, err)
	}
}

func TestTombstoneConvert(t *testing.T) {
	kc, _ := NewKeyCodec[string]()
	in := storage.Entry[string]{Deleted: true, DeletedAt: 10, Version: version.Version{Clock: 3, NodeID: "n"}}
	// A failing value codec must not matter for a tombstone: no value is carried.
	w, err := FromEntry("k", in, kc, failCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if len(w.Value) != 0 {
		t.Fatal("tombstone must not carry a value")
	}
	_, out, err := ToEntry(w, kc, failCodec{})
	if err != nil || out != in {
		t.Fatalf("got %+v %v", out, err)
	}
}

func TestToEntryRejectsTombstoneWithoutDeletedAt(t *testing.T) {
	kc, _ := NewKeyCodec[string]()
	w := Entry{Key: []byte("k"), Deleted: true, Version: version.Version{Clock: 1, NodeID: "n"}}
	if _, _, err := ToEntry(w, kc, strCodec{}); !errors.Is(err, ErrTombstoneNoDeletedAt) {
		t.Fatalf("err = %v, want ErrTombstoneNoDeletedAt", err)
	}
}

func TestFromEntryRejectsTombstoneWithoutDeletedAt(t *testing.T) {
	kc, _ := NewKeyCodec[string]()
	in := storage.Entry[string]{Deleted: true}
	if _, err := FromEntry("k", in, kc, strCodec{}); !errors.Is(err, ErrTombstoneNoDeletedAt) {
		t.Fatalf("err = %v", err)
	}
}

func TestConvertCodecErrors(t *testing.T) {
	kc, _ := NewKeyCodec[string]()
	if _, err := FromEntry("k", storage.Entry[string]{Value: "v"}, kc, failCodec{}); err == nil {
		t.Fatal("marshal error must surface")
	}
	if _, _, err := ToEntry(Entry{Key: []byte("k"), Value: []byte("v")}, kc, failCodec{}); err == nil {
		t.Fatal("unmarshal error must surface")
	}
}

func TestToEntryRejectsZeroVersion(t *testing.T) {
	_, _, err := ToEntry[string, string](Entry{Key: []byte("k"), Value: []byte("v")}, mustKC(t), strCodec{})
	if !errors.Is(err, ErrZeroVersion) {
		t.Fatalf("err = %v, want ErrZeroVersion", err)
	}
}

func mustKC(t *testing.T) KeyCodec[string] {
	kc, err := NewKeyCodec[string]()
	if err != nil {
		t.Fatal(err)
	}
	return kc
}

func TestFullPathRoundtrip(t *testing.T) {
	kc := mustKC(t)
	in := storage.Entry[string]{Value: "v", ExpiresAt: 123, Version: version.Version{Clock: 9, NodeID: "n"}}
	w, err := FromEntry("key", in, kc, strCodec{})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgMutation, Mutation{Origin: "n", Entry: w}); err != nil {
		t.Fatal(err)
	}
	typ, pl, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := DecodeMessage(typ, pl)
	if err != nil {
		t.Fatal(err)
	}
	k, out, err := ToEntry[string, string](msg.(Mutation).Entry, kc, strCodec{})
	if err != nil || k != "key" || out != in {
		t.Fatalf("got %q %+v %v", k, out, err)
	}
}
