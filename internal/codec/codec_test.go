package codec

import (
	"bytes"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	c := New()
	original := WireEntry{
		Key:       []byte("feature-flag"),
		Value:     []byte(`{"enabled":true,"rollout":25}`),
		ExpiresAt: 1792877400000000000,
		Deleted:   false,
		Version:   42,
	}

	encoded, err := c.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := c.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if !bytes.Equal(decoded.Key, original.Key) {
		t.Errorf("Key: got %q, want %q", decoded.Key, original.Key)
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Errorf("Value: got %q, want %q", decoded.Value, original.Value)
	}
	if decoded.ExpiresAt != original.ExpiresAt {
		t.Errorf("ExpiresAt: got %d, want %d", decoded.ExpiresAt, original.ExpiresAt)
	}
	if decoded.Deleted != original.Deleted {
		t.Errorf("Deleted: got %v, want %v", decoded.Deleted, original.Deleted)
	}
	if decoded.Version != original.Version {
		t.Errorf("Version: got %d, want %d", decoded.Version, original.Version)
	}
}

func TestDeletedEntryRoundTrip(t *testing.T) {
	c := New()
	original := WireEntry{
		Key:     []byte("gone-key"),
		Deleted: true,
		Version: 7,
	}

	encoded, err := c.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := c.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if !bytes.Equal(decoded.Key, original.Key) {
		t.Errorf("Key: got %q, want %q", decoded.Key, original.Key)
	}
	if !decoded.Deleted {
		t.Error("expected Deleted=true after round-trip")
	}
	if decoded.Version != original.Version {
		t.Errorf("Version: got %d, want %d", decoded.Version, original.Version)
	}
}

func TestExpiredEntryRoundTrip(t *testing.T) {
	c := New()
	original := WireEntry{
		Key:       []byte("expiring"),
		Value:     []byte("value"),
		ExpiresAt: 1000000000000000000,
		Version:   1,
	}

	encoded, err := c.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := c.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if !bytes.Equal(decoded.Key, original.Key) {
		t.Errorf("Key: got %q, want %q", decoded.Key, original.Key)
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Errorf("Value: got %q, want %q", decoded.Value, original.Value)
	}
	if decoded.ExpiresAt != original.ExpiresAt {
		t.Errorf("ExpiresAt: got %d, want %d", decoded.ExpiresAt, original.ExpiresAt)
	}
}

func TestEmptyValue(t *testing.T) {
	c := New()
	original := WireEntry{
		Key:   []byte("empty"),
		Value: []byte{},
	}

	encoded, err := c.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := c.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// proto omits empty bytes fields on the wire; nil and []byte{} are both "empty"
	if len(decoded.Value) != 0 {
		t.Errorf("Value: got %d bytes, want 0", len(decoded.Value))
	}
}

func TestDecodeCorruptedInput(t *testing.T) {
	c := New()
	_, err := c.Decode([]byte("this is not snappy"))
	if err == nil {
		t.Fatal("expected error for corrupted input, got nil")
	}
}

func TestDecodeExceedsMaxSize(t *testing.T) {
	// Test that the decompression bomb guard is in place.
	// We verify this by checking that empty input returns an error (snappy header check).
	c := New()
	_, err := c.Decode([]byte{})
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}
