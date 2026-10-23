package wal

import (
	"encoding/binary"
	"io"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Record — one entry in the log. This file is the only place that knows the
// on-disk format; writing, replay and compaction all go through it.
//
// On-disk format (big-endian), records back to back with no separator:
//   [Op 1B] [KeyLen 2B] [ValueLen 4B] [ExpiresAt 8B] [Key ...] [Value ...]
//
// ExpiresAt is absolute Unix nanoseconds. Delete records carry no value and
// a zero ExpiresAt.
// ─────────────────────────────────────────────────────────────────────────────

const (
	OpSet    byte = 1
	OpDelete byte = 2
)

const recordHeaderSize = 1 + 2 + 4 + 8 // op + keyLen + valLen + expiresAt = 15 bytes

type Record struct {
	Op        byte
	Key       string
	Value     []byte    // OpSet only
	ExpiresAt time.Time // OpSet only
}

func (r Record) encode() []byte {
	buf := make([]byte, recordHeaderSize+len(r.Key)+len(r.Value))

	buf[0] = r.Op
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(r.Key)))
	binary.BigEndian.PutUint32(buf[3:7], uint32(len(r.Value)))
	if r.Op == OpSet {
		binary.BigEndian.PutUint64(buf[7:15], uint64(r.ExpiresAt.UnixNano()))
	}

	copy(buf[recordHeaderSize:], r.Key)
	copy(buf[recordHeaderSize+len(r.Key):], r.Value)
	return buf
}

// readRecord reads the next record from r. It returns io.EOF only when r ends
// cleanly between records; a record cut off part-way returns
// io.ErrUnexpectedEOF. header is a reusable recordHeaderSize buffer.
func readRecord(r io.Reader, header []byte) (Record, error) {
	if _, err := io.ReadFull(r, header); err != nil {
		return Record{}, err
	}

	op := header[0]
	keyLen := binary.BigEndian.Uint16(header[1:3])
	valLen := binary.BigEndian.Uint32(header[3:7])
	expiresAt := binary.BigEndian.Uint64(header[7:15])

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return Record{}, unexpectedEOF(err)
	}

	var value []byte
	if op == OpSet {
		value = make([]byte, valLen)
		if _, err := io.ReadFull(r, value); err != nil {
			return Record{}, unexpectedEOF(err)
		}
	}

	return Record{
		Op:        op,
		Key:       string(key),
		Value:     value,
		ExpiresAt: time.Unix(0, int64(expiresAt)),
	}, nil
}

// unexpectedEOF turns a clean EOF inside a record into ErrUnexpectedEOF, so
// callers never mistake a truncated record for the end of the log.
func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
