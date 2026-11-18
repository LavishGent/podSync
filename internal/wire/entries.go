package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
)

// Limits on entry lists. They bound decoded memory independently of
// MaxFrameSize: a gob slice of N empty elements costs about one byte per
// element on the wire but a full Entry in memory, so entry lists travel as
// an opaque blob that is counted before any entry is decoded.
const (
	// MaxEntriesPerMessage is the most entries one StateRequest or
	// StateResponse may carry. It is derived from MaxFrameSize, not chosen:
	// every blob entry is gob-encoded on its own, so even an empty Entry
	// takes at least minEntryWire bytes (a 140-byte gob stream plus its
	// 4-byte prefix) and no frame can hold more than this many. The cap
	// therefore never refuses a page that fits in a frame (A8: responders
	// always serve every page); memory is bounded by the frame size.
	MaxEntriesPerMessage = MaxFrameSize / minEntryWire
	// MaxEntrySize is the largest single gob-encoded Entry in a blob.
	MaxEntrySize = 4 << 20
	// MaxShards is the largest NumShards a peer may announce.
	MaxShards = 1 << 16
)

// minEntryWire is the least space one entry takes in a blob.
const minEntryWire = 144

// Errors for entry lists and shard fields.
var (
	ErrTooManyEntries = errors.New("wire: too many entries in message")
	ErrEntryTooLarge  = errors.New("wire: entry exceeds MaxEntrySize")
	ErrBadEntryBlob   = errors.New("wire: malformed entry blob")
	ErrBadShard       = errors.New("wire: shard out of range")
)

// EncodeEntries packs entries into a blob: each entry is gob-encoded on its
// own and prefixed with its 4-byte big-endian length.
func EncodeEntries(es []Entry) ([]byte, error) {
	if len(es) > MaxEntriesPerMessage {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyEntries, len(es), MaxEntriesPerMessage)
	}
	var out bytes.Buffer
	for i := range es {
		var eb bytes.Buffer
		if err := gob.NewEncoder(&eb).Encode(&es[i]); err != nil {
			return nil, fmt.Errorf("wire: encode entry: %w", err)
		}
		if eb.Len() > MaxEntrySize {
			return nil, fmt.Errorf("%w: %d bytes", ErrEntryTooLarge, eb.Len())
		}
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], uint32(eb.Len()))
		out.Write(l[:])
		out.Write(eb.Bytes())
	}
	if out.Len() == 0 {
		return nil, nil
	}
	return out.Bytes(), nil
}

// walkBlob visits each entry's bytes in order and returns the entry count.
// It checks the count and every length before calling visit (a zero-length
// slot is malformed), and never allocates per entry itself.
func walkBlob(blob []byte, visit func([]byte) error) (int, error) {
	count := 0
	for len(blob) > 0 {
		if count >= MaxEntriesPerMessage {
			return count, fmt.Errorf("%w: more than %d", ErrTooManyEntries, MaxEntriesPerMessage)
		}
		if len(blob) < 4 {
			return count, fmt.Errorf("%w: truncated length", ErrBadEntryBlob)
		}
		n := binary.BigEndian.Uint32(blob)
		if n == 0 {
			return count, fmt.Errorf("%w: empty entry", ErrBadEntryBlob)
		}
		if n > MaxEntrySize {
			return count, fmt.Errorf("%w: %d bytes", ErrEntryTooLarge, n)
		}
		if uint64(len(blob)-4) < uint64(n) {
			return count, fmt.Errorf("%w: truncated entry", ErrBadEntryBlob)
		}
		if visit != nil {
			if err := visit(blob[4 : 4+n]); err != nil {
				return count, err
			}
		}
		blob = blob[4+n:]
		count++
	}
	return count, nil
}

// DecodeEntries unpacks a blob built by EncodeEntries, one entry at a time.
// The whole blob is walked first, so a bad count, length or truncation fails
// before any entry is materialised. Each entry's gob type definitions are
// checked before it is decoded (see Decode).
func DecodeEntries(blob []byte) ([]Entry, error) {
	count, err := walkBlob(blob, nil)
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0, count)
	_, err = walkBlob(blob, func(b []byte) error {
		var e Entry
		if err := Decode(b, &e); err != nil {
			return fmt.Errorf("%w: %w", ErrBadEntryBlob, err)
		}
		out = append(out, e)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// validateShards bounds the two fields independently. Shard is not checked
// against NumShards: NumShards is the sender's own shard count, and under A8
// the requester walks shard indexes up to the larger of both sides' counts,
// so a request can name a shard the responder lacks (answered with an empty
// page) and a response can echo one the responder lacks.
func validateShards(shard, numShards int) error {
	if numShards < 1 || numShards > MaxShards || shard < 0 || shard >= MaxShards {
		return fmt.Errorf("%w: shard %d of %d (max %d)", ErrBadShard, shard, numShards, MaxShards)
	}
	return nil
}

// NewStateRequest builds a StateRequest carrying entries. numShards is the
// sender's own shard count; shard may exceed it (see validateShards). It
// returns an error for out-of-range shard fields or unencodable entries.
func NewStateRequest(origin string, shard, numShards int, es []Entry) (StateRequest, error) {
	if err := validateShards(shard, numShards); err != nil {
		return StateRequest{}, err
	}
	blob, err := EncodeEntries(es)
	if err != nil {
		return StateRequest{}, err
	}
	return StateRequest{Origin: origin, Shard: shard, NumShards: numShards, EntryBlob: blob}, nil
}

// NewStateResponse builds a StateResponse carrying entries; see
// NewStateRequest.
func NewStateResponse(origin string, shard, numShards int, es []Entry) (StateResponse, error) {
	if err := validateShards(shard, numShards); err != nil {
		return StateResponse{}, err
	}
	blob, err := EncodeEntries(es)
	if err != nil {
		return StateResponse{}, err
	}
	return StateResponse{Origin: origin, Shard: shard, NumShards: numShards, EntryBlob: blob}, nil
}

// Validate checks the shard fields (1 <= NumShards <= MaxShards and
// 0 <= Shard < MaxShards; Shard may exceed the sender's NumShards) and the
// entry blob's structure and count, without decoding any entry.
func (m StateRequest) Validate() error {
	if err := validateShards(m.Shard, m.NumShards); err != nil {
		return err
	}
	_, err := walkBlob(m.EntryBlob, nil)
	return err
}

// Validate is StateRequest.Validate for responses.
func (m StateResponse) Validate() error {
	if err := validateShards(m.Shard, m.NumShards); err != nil {
		return err
	}
	_, err := walkBlob(m.EntryBlob, nil)
	return err
}

// DecodeEntries returns the request's entries.
func (m StateRequest) DecodeEntries() ([]Entry, error) { return DecodeEntries(m.EntryBlob) }

// DecodeEntries returns the response's entries.
func (m StateResponse) DecodeEntries() ([]Entry, error) { return DecodeEntries(m.EntryBlob) }
