package wire

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// ErrUnexpectedTypeDef reports a gob stream whose type definitions differ
// from the ones this package's own encoder emits for the expected type.
var ErrUnexpectedTypeDef = errors.New("wire: unexpected gob type definition")

// Why type definitions are pinned: gob skips fields it has no destination
// for, but it still decodes them, and a recursive unknown type (a struct
// with a *self field) makes the decoder recurse once per nesting level. A
// 200 KB payload costs tens of MB of goroutine stack, and a frame-sized one
// can hit Go's fatal stack limit, which recover cannot catch. The
// definitions of every type in a gob stream arrive before the first value,
// so before decoding anything we walk those definitions and require them to
// match what our own encoder produces for the target type. Our types are
// flat structs of builtin kinds and one another, so a match proves the
// value cannot contain an unknown or recursive type.

// firstUserID is the first gob type id handed out to non-builtin types.
// Ids are assigned process-wide in registration order, so two peers
// disagree about them; the comparison renumbers user ids by first use.
const firstUserID = 64

type typeField struct {
	Name string
	ID   int64
}

type typeDef struct {
	ID     int64
	Name   string
	Fields []typeField
}

// gobReader reads gob primitives with bounds checks. After the first
// failure every read returns zero and err stays set.
type gobReader struct {
	b   []byte
	bad bool
}

func (g *gobReader) uint() uint64 {
	if g.bad || len(g.b) == 0 {
		g.bad = true
		return 0
	}
	c := g.b[0]
	g.b = g.b[1:]
	if c < 128 {
		return uint64(c)
	}
	n := int(-int8(c))
	if n < 1 || n > 8 || n > len(g.b) {
		g.bad = true
		return 0
	}
	var v uint64
	for _, x := range g.b[:n] {
		v = v<<8 | uint64(x)
	}
	g.b = g.b[n:]
	return v
}

func (g *gobReader) int() int64 {
	u := g.uint()
	if u&1 != 0 {
		return ^int64(u >> 1)
	}
	return int64(u >> 1)
}

func (g *gobReader) string() string {
	n := g.uint()
	if g.bad || n > uint64(len(g.b)) {
		g.bad = true
		return ""
	}
	s := string(g.b[:n])
	g.b = g.b[n:]
	return s
}

// delta reads the next field delta of a struct and returns the new field
// number, or -1 at the end of the struct.
func (g *gobReader) field(prev int) int {
	d := g.uint()
	if g.bad || d > 16 {
		g.bad = true
		return -1
	}
	if d == 0 {
		return -1
	}
	return prev + int(d)
}

// readNameID reads a {Name string; Id int} struct (gob's CommonType and
// fieldType share this shape).
func (g *gobReader) readNameID() (name string, id int64) {
	f := -1
	for {
		f = g.field(f)
		if f < 0 || g.bad {
			return
		}
		switch f {
		case 0:
			name = g.string()
		case 1:
			id = g.int()
		default:
			g.bad = true
			return
		}
	}
}

// readStructDef reads the body of a type-definition message (after its
// negative id): a wireType holding only a StructT.
func (g *gobReader) readStructDef(id int64) (typeDef, bool) {
	def := typeDef{ID: id}
	f := g.field(-1)
	if g.bad || f != 2 { // wireType.StructT
		return def, false
	}
	var sawCommon bool
	sf := -1
	for {
		sf = g.field(sf)
		if g.bad {
			return def, false
		}
		if sf < 0 {
			break
		}
		switch sf {
		case 0: // CommonType
			var cid int64
			def.Name, cid = g.readNameID()
			if cid != id {
				return def, false
			}
			sawCommon = true
		case 1: // Field []fieldType
			n := g.uint()
			if g.bad || n > uint64(len(g.b)) {
				return def, false
			}
			for i := uint64(0); i < n; i++ {
				var tf typeField
				tf.Name, tf.ID = g.readNameID()
				if g.bad {
					return def, false
				}
				def.Fields = append(def.Fields, tf)
			}
		default:
			return def, false
		}
	}
	if g.field(f) >= 0 || g.bad || len(g.b) != 0 { // end of wireType, nothing after
		return def, false
	}
	return def, sawCommon
}

// parseTypeDefs walks the type-definition messages at the start of a gob
// stream (messages whose type id is negative) and returns them with user
// type ids renumbered by first use. It reads at most max definitions and
// never reads past payload. Reaching a value message, or the end of the
// payload, ends the walk.
func parseTypeDefs(payload []byte, max int) ([]typeDef, error) {
	bad := func(why string) ([]typeDef, error) {
		return nil, fmt.Errorf("%w: %s", ErrUnexpectedTypeDef, why)
	}
	var defs []typeDef
	rest := payload
	for {
		g := gobReader{b: rest}
		n := g.uint()
		if g.bad || n > uint64(len(g.b)) {
			return bad("truncated message")
		}
		body := gobReader{b: g.b[:n]}
		id := body.int()
		if body.bad {
			return bad("truncated message")
		}
		if id >= 0 {
			break
		}
		if len(defs) >= max {
			return bad("too many definitions")
		}
		def, ok := body.readStructDef(-id)
		if !ok {
			return bad("malformed definition")
		}
		defs = append(defs, def)
		rest = g.b[n:]
	}
	renumber(defs)
	return defs, nil
}

func renumber(defs []typeDef) {
	ids := map[int64]int64{}
	re := func(id int64) int64 {
		if id < firstUserID {
			return id
		}
		v, ok := ids[id]
		if !ok {
			v = firstUserID + int64(len(ids))
			ids[id] = v
		}
		return v
	}
	for i := range defs {
		defs[i].ID = re(defs[i].ID)
		for j := range defs[i].Fields {
			defs[i].Fields[j].ID = re(defs[i].Fields[j].ID)
		}
	}
}

var canon = sync.OnceValue(func() map[reflect.Type][]typeDef {
	out := map[reflect.Type][]typeDef{}
	for _, v := range []any{
		Mutation{}, MutationAck{}, StateRequest{}, StateResponse{}, Ping{}, Pong{}, Entry{},
	} {
		defs, err := parseTypeDefs(gobEncode(v), 1<<10)
		if err != nil || len(defs) == 0 {
			panic(fmt.Sprintf("wire: cannot derive type definitions for %T: %v", v, err))
		}
		out[reflect.TypeOf(v)] = defs
	}
	return out
})

// checkTypeDefs returns ErrUnexpectedTypeDef unless payload's type
// definitions are exactly those of an encoded value of type t.
func checkTypeDefs(payload []byte, t reflect.Type) error {
	want, ok := canon()[t]
	if !ok {
		return fmt.Errorf("%w: no definition for %v", ErrUnexpectedTypeDef, t)
	}
	got, err := parseTypeDefs(payload, len(want))
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("%w: definitions differ from %v", ErrUnexpectedTypeDef, t)
	}
	return nil
}
