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
	// StateResponse may carry.
	MaxEntriesPerMessage = 4096
	// MaxEntrySize is the largest single gob-encoded Entry in a blob.
	MaxEntrySize = 4 << 20
	// MaxShards is the largest NumShards a peer may announce.
	MaxShards = 1 << 16
)

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

// walkBlob visits each entry's bytes in order. It checks the count and every
// length before calling visit, and never allocates per entry itself.
func walkBlob(blob []byte, visit func([]byte) error) error {
	count := 0
	for len(blob) > 0 {
		if count >= MaxEntriesPerMessage {
			return fmt.Errorf("%w: more than %d", ErrTooManyEntries, MaxEntriesPerMessage)
		}
		if len(blob) < 4 {
			return fmt.Errorf("%w: truncated length", ErrBadEntryBlob)
		}
		n := binary.BigEndian.Uint32(blob)
		if n > MaxEntrySize {
			return fmt.Errorf("%w: %d bytes", ErrEntryTooLarge, n)
		}
		if uint64(len(blob)-4) < uint64(n) {
			return fmt.Errorf("%w: truncated entry", ErrBadEntryBlob)
		}
		if visit != nil {
			if err := visit(blob[4 : 4+n]); err != nil {
				return err
			}
		}
		blob = blob[4+n:]
		count++
	}
	return nil
}

// DecodeEntries unpacks a blob built by EncodeEntries, one entry at a time.
func DecodeEntries(blob []byte) ([]Entry, error) {
	var out []Entry
	err := walkBlob(blob, func(b []byte) error {
		var e Entry
		if err := Decode(b, &e); err != nil {
			return fmt.Errorf("%w: %v", ErrBadEntryBlob, err)
		}
		out = append(out, e)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func validateShards(shard, numShards int) error {
	if numShards < 1 || numShards > MaxShards || shard < 0 || shard >= numShards {
		return fmt.Errorf("%w: shard %d of %d (max %d)", ErrBadShard, shard, numShards, MaxShards)
	}
	return nil
}

// NewStateRequest builds a StateRequest carrying entries.
func NewStateRequest(origin string, shard, numShards int, es []Entry) (StateRequest, error) {
	blob, err := EncodeEntries(es)
	return StateRequest{Origin: origin, Shard: shard, NumShards: numShards, EntryBlob: blob}, err
}

// NewStateResponse builds a StateResponse carrying entries.
func NewStateResponse(origin string, shard, numShards int, es []Entry) (StateResponse, error) {
	blob, err := EncodeEntries(es)
	return StateResponse{Origin: origin, Shard: shard, NumShards: numShards, EntryBlob: blob}, err
}

// Validate checks the shard fields (0 <= Shard < NumShards <= MaxShards) and
// the entry blob's structure and count, without decoding any entry.
func (m StateRequest) Validate() error {
	if err := validateShards(m.Shard, m.NumShards); err != nil {
		return err
	}
	return walkBlob(m.EntryBlob, nil)
}

// Validate is StateRequest.Validate for responses.
func (m StateResponse) Validate() error {
	if err := validateShards(m.Shard, m.NumShards); err != nil {
		return err
	}
	return walkBlob(m.EntryBlob, nil)
}

// DecodeEntries returns the request's entries.
func (m StateRequest) DecodeEntries() ([]Entry, error) { return DecodeEntries(m.EntryBlob) }

// DecodeEntries returns the response's entries.
func (m StateResponse) DecodeEntries() ([]Entry, error) { return DecodeEntries(m.EntryBlob) }
