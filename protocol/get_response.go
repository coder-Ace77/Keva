package protocol

import (
	"encoding/binary"
)

// ─────────────────────────────────────────────────────────────────────────────
// GetResponse — Node sends this back to the client after a GET.
//
// Wire format:
//   [Found 1B] [ValueLen 4B] [Value ...]
//
// Found = 0x01 if the key exists, 0x00 if not.
// When Found == 0x00, ValueLen is 0 and Value is empty.
// ─────────────────────────────────────────────────────────────────────────────

type GetResponse struct {
	Found bool
	Value []byte
}

func (g *GetResponse) OpCode() byte { return OpGetResponse }

func (g *GetResponse) Encode() ([]byte, error) {
	valLen := len(g.Value)
	buf := make([]byte, 1+4+valLen)

	if g.Found {
		buf[0] = 0x01
	} else {
		buf[0] = 0x00
	}

	binary.BigEndian.PutUint32(buf[1:5], uint32(valLen))
	copy(buf[5:], g.Value)

	return buf, nil
}

func (g *GetResponse) Decode(data []byte) error {
	if len(data) < 5 {
		return ErrPayloadTooShort
	}

	g.Found = data[0] == 0x01
	valLen := binary.BigEndian.Uint32(data[1:5])

	if len(data) < 5+int(valLen) {
		return ErrPayloadTooShort
	}

	g.Value = make([]byte, valLen)
	copy(g.Value, data[5:5+valLen])

	return nil
}
