package codec

import (
	"testing"
)

func BenchmarkEncode(b *testing.B) {
	c := New()

	// Create a realistic WireEntry:
	// - Key: ~16 bytes
	// - Value: realistic semi-structured JSON (~100 bytes)
	// - Non-zero ExpiresAt
	// - Version: 1
	value := []byte(`{"enabled":true,"rollout":25,"experiment_id":"exp-abc-123","tags":["canary","us-east-1"]}`)
	entry := WireEntry{
		Key:       []byte("cache-key-12345"),
		Value:     value,
		ExpiresAt: 1792877400000000000,
		Deleted:   false,
		Version:   1,
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(value)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c.Encode(entry)
		if err != nil {
			b.Fatalf("Encode failed: %v", err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	c := New()

	// Create the same realistic WireEntry and pre-encode it
	value := []byte(`{"enabled":true,"rollout":25,"experiment_id":"exp-abc-123","tags":["canary","us-east-1"]}`)
	entry := WireEntry{
		Key:       []byte("cache-key-12345"),
		Value:     value,
		ExpiresAt: 1792877400000000000,
		Deleted:   false,
		Version:   1,
	}

	encoded, err := c.Encode(entry)
	if err != nil {
		b.Fatalf("setup: Encode failed: %v", err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(value)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c.Decode(encoded)
		if err != nil {
			b.Fatalf("Decode failed: %v", err)
		}
	}
}
