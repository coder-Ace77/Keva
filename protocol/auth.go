package protocol

import "encoding/binary"

// AuthMessage — Client → Node: present a secret token before any command.
//
// Wire format:
//   [TokenLen 2B] [Token ...]
type AuthMessage struct {
	Token []byte
}

func (m *AuthMessage) OpCode() byte { return OpAuth }

func (m *AuthMessage) Encode() ([]byte, error) {
	buf := make([]byte, 2+len(m.Token))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(m.Token)))
	copy(buf[2:], m.Token)
	return buf, nil
}

func (m *AuthMessage) Decode(data []byte) error {
	if len(data) < 2 {
		return ErrPayloadTooShort
	}
	tokenLen := binary.BigEndian.Uint16(data[0:2])
	if len(data) < 2+int(tokenLen) {
		return ErrPayloadTooShort
	}
	m.Token = make([]byte, tokenLen)
	copy(m.Token, data[2:2+tokenLen])
	return nil
}

// AuthResponse — Node → Client: result of authentication attempt.
//
// Wire format:
//   [Success 1B]
type AuthResponse struct {
	Success bool
}

func (m *AuthResponse) OpCode() byte { return OpAuthResponse }

func (m *AuthResponse) Encode() ([]byte, error) {
	if m.Success {
		return []byte{0x01}, nil
	}
	return []byte{0x00}, nil
}

func (m *AuthResponse) Decode(data []byte) error {
	if len(data) < 1 {
		return ErrPayloadTooShort
	}
	m.Success = data[0] == 0x01
	return nil
}
