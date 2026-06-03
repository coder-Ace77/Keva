package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"key_value_store/cluster"
	"key_value_store/engine"
	"key_value_store/protocol"
	"net"
)

type TCPServer struct {
	node            *cluster.Node
	listener        net.Listener
	maxPayloadBytes uint32
}

func NewTCPServer(node *cluster.Node, maxPayloadBytes uint32) *TCPServer {
	return &TCPServer{node: node, maxPayloadBytes: maxPayloadBytes}
}

func (s *TCPServer) Start(address string) error {
	var err error
	s.listener, err = net.Listen("tcp", address)
	if err != nil {
		return err
	}
	fmt.Printf("node listening on %s\n", address)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Printf("accept error: %v\n", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

// handleConnection reads the first frame and routes based on opcode.
// Each branch owns the conn's lifetime independently.
func (s *TCPServer) handleConnection(conn net.Conn) {
	firstMsg, err := s.readMessage(conn)
	if err != nil {
		conn.Close()
		return
	}

	switch firstMsg.OpCode() {

	// ── Cluster internal messages ─────────────────────────────────────────
	case protocol.OpReplicationJoin:
		// Conn ownership transferred to leader.AddFollower / drainAcks.
		s.handleReplicationPeer(conn, firstMsg)

	case protocol.OpHeartbeat:
		defer conn.Close()
		hb := firstMsg.(*protocol.Heartbeat)
		s.node.HandleHeartbeat(hb.Term, hb.LeaderID)
		_ = s.writeMessage(conn, &protocol.HeartbeatAck{Term: s.node.GetTerm()})

	case protocol.OpVoteRequest:
		defer conn.Close()
		req := firstMsg.(*protocol.VoteRequest)
		granted, term := s.node.HandleVoteRequest(req)
		_ = s.writeMessage(conn, &protocol.VoteResponse{Term: term, Granted: granted})

	case protocol.OpTopologyRequest:
		defer conn.Close()
		_ = s.writeMessage(conn, s.node.GetTopology())

	// ── Client CRUD — this goroutine owns the conn ────────────────────────
	default:
		defer conn.Close()
		s.dispatchToEngine(conn, firstMsg)
		for {
			msg, err := s.readMessage(conn)
			if err != nil {
				return
			}
			s.dispatchToEngine(conn, msg)
		}
	}
}

func (s *TCPServer) handleReplicationPeer(conn net.Conn, firstMsg protocol.Message) {
	if !s.node.IsLeader() {
		s.writeError(conn, protocol.ErrCodeNotLeader, "not the leader")
		conn.Close()
		return
	}
	join := firstMsg.(*protocol.ReplicationJoin)
	s.node.Leader().AddFollower(join.NodeAddr, conn)
}

func (s *TCPServer) dispatchToEngine(conn net.Conn, msg protocol.Message) {
	resp, err := engine.Dispatch(msg, s.node)
	if err != nil {
		s.writeError(conn, protocol.ErrCodeInternal, err.Error())
		return
	}
	if err := s.writeMessage(conn, resp); err != nil {
		fmt.Printf("write error: %v\n", err)
	}
}

func (s *TCPServer) readMessage(conn net.Conn) (protocol.Message, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(header)
	if msgLen > s.maxPayloadBytes {
		s.writeError(conn, protocol.ErrCodeBadRequest, "payload too large")
		return nil, fmt.Errorf("payload too large")
	}
	frame := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return nil, err
	}
	msg, err := protocol.DecodeMessage(frame)
	if err != nil {
		s.writeError(conn, protocol.ErrCodeBadRequest, err.Error())
		return nil, err
	}
	return msg, nil
}

func (s *TCPServer) writeMessage(conn net.Conn, msg protocol.Message) error {
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

func (s *TCPServer) writeError(conn net.Conn, code uint16, message string) {
	_ = s.writeMessage(conn, &protocol.ErrorMessage{Code: code, Message: message})
}
