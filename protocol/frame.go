package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// ─────────────────────────────────────────────────────────────────────────────
// Frame — how a Message travels over TCP.
//
// Wire format:
//   [TotalLength 4B] [OpCode 1B] [Payload ...]
//
// TotalLength counts the opcode and payload, not itself.
// ─────────────────────────────────────────────────────────────────────────────

const frameHeaderSize = 4

// EncodeFrame returns msg as a complete length-prefixed frame. Useful when the
// same frame is written to many connections (replication broadcast).
func EncodeFrame(msg Message) ([]byte, error) {
	wire, err := EncodeMessage(msg)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, frameHeaderSize+len(wire))
	binary.BigEndian.PutUint32(frame[:frameHeaderSize], uint32(len(wire)))
	copy(frame[frameHeaderSize:], wire)
	return frame, nil
}

// WriteFrame encodes msg and writes it to w as one frame.
func WriteFrame(w io.Writer, msg Message) error {
	frame, err := EncodeFrame(msg)
	if err != nil {
		return err
	}
	_, err = w.Write(frame)
	return err
}

// ReadFrame reads one frame from r and decodes it.
//
// maxLen caps the frame size (0 = no limit); a larger frame returns
// ErrFrameTooLarge without reading its body. A frame that is read fully but
// cannot be decoded returns an error wrapping ErrMalformedMessage — the stream
// is still aligned, so the caller may keep reading. Any other error is an I/O
// error and the connection should be dropped.
func ReadFrame(r io.Reader, maxLen uint32) (Message, error) {
	header := make([]byte, frameHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	msgLen := binary.BigEndian.Uint32(header)
	if maxLen > 0 && msgLen > maxLen {
		return nil, ErrFrameTooLarge
	}

	body := make([]byte, msgLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	msg, err := DecodeMessage(body)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMalformedMessage, err)
	}
	return msg, nil
}
