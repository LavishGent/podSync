package wire

import (
	"bytes"
	"testing"

	"github.com/LavishGent/podsync/internal/version"
)

func benchMutation() Mutation {
	return Mutation{Origin: "node-1", Entry: Entry{
		Key: []byte("key-1"), Value: bytes.Repeat([]byte("x"), 100),
		ExpiresAt: 1700000000000000000, Version: version.Version{Clock: 12345, NodeID: "node-1"},
	}}
}

func BenchmarkEncodeMutation(b *testing.B) {
	m := benchMutation()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := WriteFrame(&buf, MsgMutation, m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeMutation(b *testing.B) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgMutation, benchMutation()); err != nil {
		b.Fatal(err)
	}
	_, payload, err := ReadFrame(&buf)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var m Mutation
		if err := Decode(payload, &m); err != nil {
			b.Fatal(err)
		}
	}
}
