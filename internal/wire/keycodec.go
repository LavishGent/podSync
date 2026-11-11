package wire

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

// KeyCodec converts store keys to and from their wire bytes.
type KeyCodec[K comparable] interface {
	Encode(K) ([]byte, error)
	Decode([]byte) (K, error)
}

// NewKeyCodec returns the built-in KeyCodec for K. K's underlying type must be
// string or an integer type; anything else returns an error, so New can fail
// at startup instead of at the first write. Integers are encoded as 8 bytes,
// big-endian.
func NewKeyCodec[K comparable]() (KeyCodec[K], error) {
	t := reflect.TypeFor[K]()
	switch k := t.Kind(); {
	case k == reflect.String:
		return stringKeys[K]{}, nil
	case k >= reflect.Int && k <= reflect.Int64:
		return intKeys[K]{signed: true}, nil
	case k >= reflect.Uint && k <= reflect.Uintptr:
		return intKeys[K]{}, nil
	}
	return nil, fmt.Errorf("wire: unsupported key type %v: keys must be string or an integer type", t)
}

type stringKeys[K comparable] struct{}

func (stringKeys[K]) Encode(k K) ([]byte, error) {
	return []byte(reflect.ValueOf(k).String()), nil
}

func (stringKeys[K]) Decode(b []byte) (K, error) {
	var k K
	reflect.ValueOf(&k).Elem().SetString(string(b))
	return k, nil
}

type intKeys[K comparable] struct{ signed bool }

func (c intKeys[K]) Encode(k K) ([]byte, error) {
	v := reflect.ValueOf(k)
	var u uint64
	if c.signed {
		u = uint64(v.Int())
	} else {
		u = v.Uint()
	}
	return binary.BigEndian.AppendUint64(nil, u), nil
}

func (c intKeys[K]) Decode(b []byte) (K, error) {
	var k K
	if len(b) != 8 {
		return k, fmt.Errorf("wire: integer key is %d bytes, want 8", len(b))
	}
	u := binary.BigEndian.Uint64(b)
	v := reflect.ValueOf(&k).Elem()
	if c.signed {
		if v.OverflowInt(int64(u)) {
			return k, fmt.Errorf("wire: key %d overflows %v", int64(u), v.Type())
		}
		v.SetInt(int64(u))
	} else {
		if v.OverflowUint(u) {
			return k, fmt.Errorf("wire: key %d overflows %v", u, v.Type())
		}
		v.SetUint(u)
	}
	return k, nil
}
