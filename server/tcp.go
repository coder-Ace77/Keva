package server

import (
	"errors"
	"fmt"
	"key_value_store/cluster"
	"key_value_store/engine"
	"key_value_store/protocol"
	clusterproto "key_value_store/protocol/cluster"
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

// handleConnection reads the first frame and routes based on opcode:
// cluster-internal opcodes (heartbeat, vote, replication) get their own
// handling; anything else starts a client command loop.
func (s *TCPServer) handleConnection(conn net.Conn) {
	firstMsg, err := s.readMessage(conn)
	if err != nil {
		conn.Close()
		return
	}

	switch firstMsg.OpCode() {

	// ── Cluster internal messages ─────────────────────────────────────────
	case protocol.OpReplicationJoin:
		s.handleReplicationPeer(conn, firstMsg)

	case protocol.OpHeartbeat:
		defer conn.Close()
		hb := firstMsg.(*clusterproto.Heartbeat)
		s.node.HandleHeartbeat(hb.Term, hb.LeaderID)
		_ = s.writeMessage(conn, &clusterproto.HeartbeatAck{Term: s.node.GetTerm()})

	case protocol.OpVoteRequest:
		defer conn.Close()
		req := firstMsg.(*clusterproto.VoteRequest)
		granted, term := s.node.HandleVoteRequest(req)
		_ = s.writeMessage(conn, &clusterproto.VoteResponse{Term: term, Granted: granted})

	// ── Client traffic ────────────────────────────────────────────────────
	default:
		defer conn.Close()

		s.dispatchClient(conn, firstMsg)
		for {
			msg, err := s.readMessage(conn)
			if err != nil {
				return
			}
			s.dispatchClient(conn, msg)
		}
	}
}

// dispatchClient routes client messages: topology requests are handled directly,
// everything else goes through the engine.
func (s *TCPServer) dispatchClient(conn net.Conn, msg protocol.Message) {
	if msg.OpCode() == protocol.OpTopologyRequest {
		_ = s.writeMessage(conn, s.node.GetTopology())
		return
	}
	s.dispatchToEngine(conn, msg)
}

func (s *TCPServer) handleReplicationPeer(conn net.Conn, firstMsg protocol.Message) {
	if !s.node.IsLeader() {
		s.writeError(conn, protocol.ErrCodeNotLeader, "not the leader")
		conn.Close()
		return
	}
	join := firstMsg.(*clusterproto.ReplicationJoin)
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
	msg, err := protocol.ReadFrame(conn, s.maxPayloadBytes)
	switch {
	case errors.Is(err, protocol.ErrFrameTooLarge):
		s.writeError(conn, protocol.ErrCodeBadRequest, "payload too large")
	case errors.Is(err, protocol.ErrMalformedMessage):
		s.writeError(conn, protocol.ErrCodeBadRequest, err.Error())
	}
	return msg, err
}

func (s *TCPServer) writeMessage(conn net.Conn, msg protocol.Message) error {
	return protocol.WriteFrame(conn, msg)
}

func (s *TCPServer) writeError(conn net.Conn, code uint16, message string) {
	_ = s.writeMessage(conn, &protocol.ErrorMessage{Code: code, Message: message})
}
