package podsync

import "encoding/json"

// Codec converts cache values to and from the bytes sent between peers.
// Implement it to replicate values that JSONCodec does not suit.
type Codec[V any] interface {
	Marshal(V) ([]byte, error)
	Unmarshal([]byte) (V, error)
}

// JSONCodec encodes values with encoding/json.
type JSONCodec[V any] struct{}

// Marshal encodes v as JSON.
func (JSONCodec[V]) Marshal(v V) ([]byte, error) { return json.Marshal(v) }

// Unmarshal decodes JSON into a V.
func (JSONCodec[V]) Unmarshal(b []byte) (V, error) {
	var v V
	err := json.Unmarshal(b, &v)
	return v, err
}

// BytesCodec passes []byte values through unchanged.
type BytesCodec struct{}

// Marshal returns b.
func (BytesCodec) Marshal(b []byte) ([]byte, error) { return b, nil }

// Unmarshal returns b.
func (BytesCodec) Unmarshal(b []byte) ([]byte, error) { return b, nil }

// StringCodec encodes string values as their bytes.
type StringCodec struct{}

// Marshal returns s as bytes.
func (StringCodec) Marshal(s string) ([]byte, error) { return []byte(s), nil }

// Unmarshal returns b as a string.
func (StringCodec) Unmarshal(b []byte) (string, error) { return string(b), nil }
