package codec

import (
	"fmt"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/proto"

	wirePb "github.com/LavishGent/podsync/proto"
)

// maxDecompressedSize is the upper bound on a single decoded wire message.
// Protects against decompression bomb attacks from compromised peers.
const maxDecompressedSize = 4 << 20 // 4 MiB

// WireEntry is the wire representation of a storage entry.
// Key and Value are raw bytes; the caller is responsible for serializing the value type V.
type WireEntry struct {
	Key       []byte
	Value     []byte
	ExpiresAt int64
	Deleted   bool
	Version   uint64
}

// Codec encodes and decodes WireEntry values to/from compressed wire bytes.
type Codec interface {
	Encode(e WireEntry) ([]byte, error)
	Decode(b []byte) (WireEntry, error)
}

// New returns a Codec using protobuf serialization with snappy compression.
func New() Codec {
	return snappyProtoCodec{}
}

type snappyProtoCodec struct{}

// Encode serializes e to protobuf and compresses with snappy.
func (c snappyProtoCodec) Encode(e WireEntry) ([]byte, error) {
	pb := &wirePb.WireEntry{
		Key:       e.Key,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
		Deleted:   e.Deleted,
		Version:   e.Version,
	}
	protoBytes, err := proto.Marshal(pb)
	if err != nil {
		return nil, fmt.Errorf("codec: proto marshal: %w", err)
	}
	return snappy.Encode(nil, protoBytes), nil
}

// Decode decompresses snappy and deserializes from protobuf.
func (c snappyProtoCodec) Decode(b []byte) (WireEntry, error) {
	decompLen, err := snappy.DecodedLen(b)
	if err != nil {
		return WireEntry{}, fmt.Errorf("codec: snappy header: %w", err)
	}
	if int64(decompLen) > maxDecompressedSize {
		return WireEntry{}, fmt.Errorf("codec: decompressed size %d exceeds limit %d", decompLen, maxDecompressedSize)
	}
	protoBytes, err := snappy.Decode(nil, b)
	if err != nil {
		return WireEntry{}, fmt.Errorf("codec: snappy decode: %w", err)
	}
	var pb wirePb.WireEntry
	if err := proto.Unmarshal(protoBytes, &pb); err != nil {
		return WireEntry{}, fmt.Errorf("codec: proto unmarshal: %w", err)
	}
	return WireEntry{
		Key:       pb.Key,
		Value:     pb.Value,
		ExpiresAt: pb.ExpiresAt,
		Deleted:   pb.Deleted,
		Version:   pb.Version,
	}, nil
}
