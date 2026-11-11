package wire

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
)

// MaxFrameSize is the largest payload a frame may carry.
const MaxFrameSize = 16 << 20

const headerSize = 5

// Errors returned by the framing helpers.
var (
	// ErrFrameTooLarge reports a payload over MaxFrameSize.
	ErrFrameTooLarge = errors.New("wire: frame exceeds MaxFrameSize")
	// ErrUnknownMsgType reports a frame whose MsgType is not defined.
	ErrUnknownMsgType = errors.New("wire: unknown message type")
)

// WriteFrame gob-encodes msg and writes it as one frame:
// [4-byte big-endian payload length][1-byte MsgType][payload]. Header and
// payload go out in a single Write call. An encoded payload over
// MaxFrameSize fails before anything is written.
func WriteFrame(w io.Writer, t MsgType, msg any) error {
	var buf bytes.Buffer
	buf.Write(make([]byte, headerSize))
	if err := gob.NewEncoder(&buf).Encode(msg); err != nil {
		return fmt.Errorf("wire: encode %v: %w", t, err)
	}
	b := buf.Bytes()
	n := len(b) - headerSize
	if n > MaxFrameSize {
		return fmt.Errorf("%w: %v payload is %d bytes", ErrFrameTooLarge, t, n)
	}
	binary.BigEndian.PutUint32(b, uint32(n))
	b[4] = byte(t)
	_, err := w.Write(b)
	return err
}

// ReadFrame reads one frame and returns its type and raw payload. A length
// over MaxFrameSize is rejected before the payload is read or allocated.
func ReadFrame(r io.Reader) (MsgType, []byte, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:4])
	if n > MaxFrameSize {
		return 0, nil, fmt.Errorf("%w: header announces %d bytes", ErrFrameTooLarge, n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return MsgType(hdr[4]), payload, nil
}

// Decode gob-decodes payload into the value pointed to by into.
func Decode(payload []byte, into any) error {
	return gob.NewDecoder(bytes.NewReader(payload)).Decode(into)
}

// DecodeMessage decodes payload into the message struct for t and returns it
// by value. An undefined t returns ErrUnknownMsgType.
func DecodeMessage(t MsgType, payload []byte) (any, error) {
	switch t {
	case MsgMutation:
		return decodeAs[Mutation](payload)
	case MsgMutationAck:
		return decodeAs[MutationAck](payload)
	case MsgStateRequest:
		return decodeAs[StateRequest](payload)
	case MsgStateResponse:
		return decodeAs[StateResponse](payload)
	case MsgPing:
		return decodeAs[Ping](payload)
	case MsgPong:
		return decodeAs[Pong](payload)
	}
	return nil, fmt.Errorf("%w: %d", ErrUnknownMsgType, uint8(t))
}

func decodeAs[M any](payload []byte) (any, error) {
	var m M
	if err := Decode(payload, &m); err != nil {
		return nil, fmt.Errorf("wire: decode %T: %w", m, err)
	}
	return m, nil
}
