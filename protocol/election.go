package protocol

import (
	"encoding/binary"
)

// ─────────────────────────────────────────────────────────────────────────────
// VoteRequest — A candidate asks peers to vote for it during leader election.
//
// Wire: [Term 8B] [CandidateIDLen 2B] [CandidateID ...] [LastLogIndex 8B]
// ─────────────────────────────────────────────────────────────────────────────

type VoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
}

func (v *VoteRequest) OpCode() byte { return OpVoteRequest }

func (v *VoteRequest) Encode() ([]byte, error) {
	idBytes := []byte(v.CandidateID)
	buf := make([]byte, 8+2+len(idBytes)+8)
	binary.BigEndian.PutUint64(buf[0:8], v.Term)
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(idBytes)))
	copy(buf[10:], idBytes)
	binary.BigEndian.PutUint64(buf[10+len(idBytes):], v.LastLogIndex)
	return buf, nil
}

func (v *VoteRequest) Decode(data []byte) error {
	if len(data) < 10 {
		return ErrPayloadTooShort
	}
	v.Term = binary.BigEndian.Uint64(data[0:8])
	idLen := int(binary.BigEndian.Uint16(data[8:10]))
	if len(data) < 10+idLen+8 {
		return ErrPayloadTooShort
	}
	v.CandidateID = string(data[10 : 10+idLen])
	v.LastLogIndex = binary.BigEndian.Uint64(data[10+idLen : 18+idLen])
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// VoteResponse — Peer replies to a vote request.
//
// Wire: [Term 8B] [Granted 1B]
// ─────────────────────────────────────────────────────────────────────────────

type VoteResponse struct {
	Term    uint64
	Granted bool
}

func (v *VoteResponse) OpCode() byte { return OpVoteResponse }

func (v *VoteResponse) Encode() ([]byte, error) {
	buf := make([]byte, 9)
	binary.BigEndian.PutUint64(buf[0:8], v.Term)
	if v.Granted {
		buf[8] = 0x01
	} else {
		buf[8] = 0x00
	}
	return buf, nil
}

func (v *VoteResponse) Decode(data []byte) error {
	if len(data) < 9 {
		return ErrPayloadTooShort
	}
	v.Term = binary.BigEndian.Uint64(data[0:8])
	v.Granted = data[8] == 0x01
	return nil
}
