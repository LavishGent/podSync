package podsync

import (
	"reflect"
	"testing"

	"github.com/LavishGent/podsync/internal/wire"
)

type payload struct {
	Name  string
	Count int
	Tags  []string
}

// Compile-time proof that the public Codec satisfies the internal interface.
var _ wire.ValueCodec[payload] = JSONCodec[payload]{}
var _ Codec[payload] = JSONCodec[payload]{}
var _ wire.ValueCodec[[]byte] = BytesCodec{}
var _ wire.ValueCodec[string] = StringCodec{}

func TestJSONCodecRoundtrip(t *testing.T) {
	c := JSONCodec[payload]{}
	in := payload{Name: "a", Count: 3, Tags: []string{"x", "y"}}
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Unmarshal(b)
	if err != nil || !reflect.DeepEqual(in, out) {
		t.Fatalf("got %+v %v", out, err)
	}
	if _, err := c.Unmarshal([]byte("{")); err == nil {
		t.Fatal("invalid JSON must error")
	}
}

func TestBytesCodecIdentity(t *testing.T) {
	c := BytesCodec{}
	in := []byte{1, 2, 3}
	b, err := c.Marshal(in)
	if err != nil || !reflect.DeepEqual(b, in) {
		t.Fatalf("marshal: %v %v", b, err)
	}
	out, err := c.Unmarshal(b)
	if err != nil || !reflect.DeepEqual(out, in) {
		t.Fatalf("unmarshal: %v %v", out, err)
	}
}

func TestStringCodecIdentity(t *testing.T) {
	c := StringCodec{}
	b, err := c.Marshal("héllo")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := c.Unmarshal(b); err != nil || out != "héllo" {
		t.Fatalf("got %q %v", out, err)
	}
}

func TestUnsupportedKeyTypeRejected(t *testing.T) {
	if _, err := wire.NewKeyCodec[struct{ A int }](); err == nil {
		t.Fatal("struct key must be rejected")
	}
	if _, err := wire.NewKeyCodec[[2]byte](); err == nil {
		t.Fatal("array key must be rejected")
	}
}
