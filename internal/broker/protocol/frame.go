package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ErrFrameTooLarge is returned when a frame announces a payload bigger
// than MaxPayloadSize. The connection that sent it should be dropped.
var ErrFrameTooLarge = errors.New("protocol: frame payload exceeds 4 MiB limit")

// Frame is one message on the wire.
type Frame struct {
	Opcode        Opcode
	CorrelationID uint64
	Payload       []byte
}

// WriteFrame serializes f and writes it with a single Write call.
// Writes must be serialized by the caller when a connection is shared.
func WriteFrame(w io.Writer, f *Frame) error {
	if len(f.Payload) > MaxPayloadSize {
		return fmt.Errorf("write frame %s: %w", f.Opcode, ErrFrameTooLarge)
	}
	buf := make([]byte, headerSize+len(f.Payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(f.Payload)))
	buf[4] = byte(f.Opcode)
	binary.BigEndian.PutUint64(buf[5:13], f.CorrelationID)
	copy(buf[13:], f.Payload)
	_, err := w.Write(buf)
	return err
}

// ReadFrame reads exactly one frame. It returns ErrFrameTooLarge (after
// consuming nothing but the length word) when the peer announces an
// oversize payload; callers should close the connection in that case
// because the stream is now desynchronized.
func ReadFrame(r io.Reader) (*Frame, error) {
	head := make([]byte, headerSize)
	if _, err := io.ReadFull(r, head[:4]); err != nil {
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint32(head[:4])
	if payloadLen > MaxPayloadSize {
		return nil, ErrFrameTooLarge
	}
	if _, err := io.ReadFull(r, head[4:]); err != nil {
		return nil, err
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return &Frame{
		Opcode:        Opcode(head[4]),
		CorrelationID: binary.BigEndian.Uint64(head[5:13]),
		Payload:       payload,
	}, nil
}

// ErrorFrame builds an OpError response frame for a request.
func ErrorFrame(correlationID uint64, err error) *Frame {
	e, ok := err.(*Error)
	if !ok {
		e = &Error{Code: CodeInternal, Message: "internal error"}
	}
	return &Frame{
		Opcode:        OpError,
		CorrelationID: correlationID,
		Payload:       marshalJSON(e),
	}
}

// DecodeErrorFrame parses the body of an OpError frame.
func DecodeErrorFrame(payload []byte) *Error {
	var e Error
	if err := unmarshalJSON(payload, &e); err != nil || e.Code == "" {
		return &Error{Code: CodeInternal, Message: "unparseable error frame"}
	}
	return &e
}
