package wire

import (
	"errors"
	"fmt"

	"github.com/LavishGent/podsync/internal/storage"
)

// ValueCodec converts values to and from bytes. It is declared here, not
// imported, so internal/wire stays free of pkg/podsync; the public
// podsync.Codec[V] satisfies it structurally.
type ValueCodec[V any] interface {
	Marshal(V) ([]byte, error)
	Unmarshal([]byte) (V, error)
}

// ErrTombstoneNoDeletedAt reports a tombstone whose DeletedAt is zero.
// storage.Store.Merge requires every tombstone to carry a DeletedAt.
var ErrTombstoneNoDeletedAt = errors.New("wire: tombstone has zero DeletedAt")

// FromEntry converts a storage entry to its wire form. A tombstone carries no
// value, so valEnc is not called for it.
func FromEntry[K comparable, V any](key K, e storage.Entry[V], keyEnc KeyCodec[K], valEnc ValueCodec[V]) (Entry, error) {
	if e.Deleted && e.DeletedAt == 0 {
		return Entry{}, ErrTombstoneNoDeletedAt
	}
	kb, err := keyEnc.Encode(key)
	if err != nil {
		return Entry{}, fmt.Errorf("wire: encode key: %w", err)
	}
	w := Entry{
		Key: kb, ExpiresAt: e.ExpiresAt, DeletedAt: e.DeletedAt,
		Deleted: e.Deleted, Version: e.Version,
	}
	if !e.Deleted {
		if w.Value, err = valEnc.Marshal(e.Value); err != nil {
			return Entry{}, fmt.Errorf("wire: encode value: %w", err)
		}
	}
	return w, nil
}

// ToEntry converts a wire entry back to a key and storage entry. It rejects a
// tombstone whose DeletedAt is zero with ErrTombstoneNoDeletedAt.
func ToEntry[K comparable, V any](w Entry, keyDec KeyCodec[K], valDec ValueCodec[V]) (K, storage.Entry[V], error) {
	var zeroK K
	if w.Deleted && w.DeletedAt == 0 {
		return zeroK, storage.Entry[V]{}, ErrTombstoneNoDeletedAt
	}
	key, err := keyDec.Decode(w.Key)
	if err != nil {
		return zeroK, storage.Entry[V]{}, fmt.Errorf("wire: decode key: %w", err)
	}
	e := storage.Entry[V]{
		ExpiresAt: w.ExpiresAt, DeletedAt: w.DeletedAt,
		Deleted: w.Deleted, Version: w.Version,
	}
	if !w.Deleted {
		if e.Value, err = valDec.Unmarshal(w.Value); err != nil {
			return zeroK, storage.Entry[V]{}, fmt.Errorf("wire: decode value: %w", err)
		}
	}
	return key, e, nil
}
