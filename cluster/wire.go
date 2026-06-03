package cluster

import (
	"encoding/binary"
	"io"
	"key_value_store/protocol"
	"net"
)

func sendFrame(conn net.Conn, msg protocol.Message) error {
	wire, err := protocol.EncodeMessage(msg)
	if err != nil {
		return err
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(wire)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err = conn.Write(wire)
	return err
}

func readFrame(conn net.Conn) (protocol.Message, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(header)
	frame := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return nil, err
	}
	return protocol.DecodeMessage(frame)
}
