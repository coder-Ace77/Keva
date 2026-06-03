package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"key_value_store/protocol"
	"key_value_store/store"
	"net"
	"sync"
	"sync/atomic"
)

// Leader manages replication to all connected follower peers.
type Leader struct {
	mu        sync.Mutex
	followers map[string]net.Conn // nodeAddr → conn
	logIndex  atomic.Uint64
}

func newLeader() *Leader {
	return &Leader{
		followers: make(map[string]net.Conn),
	}
}

// AddFollower registers a new replication peer and starts reading its acks.
func (l *Leader) AddFollower(nodeAddr string, conn net.Conn) {
	l.mu.Lock()
	l.followers[nodeAddr] = conn
	l.mu.Unlock()
	fmt.Printf("leader: follower joined — %s\n", nodeAddr)
	go l.drainAcks(nodeAddr, conn)
}

// BroadcastSet replicates a SET to all followers.
func (l *Leader) BroadcastSet(key string, value []byte, expiredAt int64) {
	l.broadcast(&protocol.ReplicationSync{
		LogIndex: l.logIndex.Add(1),
		Op:       store.OpSet,
		Key:      []byte(key),
		Value:    value,
		TTL:      uint64(expiredAt), // absolute Unix nanoseconds
	})
}

// BroadcastDelete replicates a DELETE to all followers.
func (l *Leader) BroadcastDelete(key string) {
	l.broadcast(&protocol.ReplicationSync{
		LogIndex: l.logIndex.Add(1),
		Op:       store.OpDelete,
		Key:      []byte(key),
	})
}

func (l *Leader) broadcast(msg protocol.Message) {
	wire, err := protocol.EncodeMessage(msg)
	if err != nil {
		return
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(wire)))
	frame := append(header, wire...)

	l.mu.Lock()
	defer l.mu.Unlock()

	var dead []string
	for addr, conn := range l.followers {
		if _, err := conn.Write(frame); err != nil {
			dead = append(dead, addr)
		}
	}
	for _, addr := range dead {
		l.followers[addr].Close()
		delete(l.followers, addr)
		fmt.Printf("leader: follower %s disconnected during broadcast\n", addr)
	}
}

// drainAcks reads ReplicationAck messages from a follower.
// When the connection drops, it removes the follower from the map.
func (l *Leader) drainAcks(nodeAddr string, conn net.Conn) {
	defer l.removeFollower(nodeAddr)
	header := make([]byte, 4)
	for {
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		msgLen := binary.BigEndian.Uint32(header)
		frame := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, frame); err != nil {
			return
		}
		// Acks are consumed but not acted on yet — future: track replication lag.
	}
}

func (l *Leader) removeFollower(nodeAddr string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if conn, ok := l.followers[nodeAddr]; ok {
		conn.Close()
		delete(l.followers, nodeAddr)
		fmt.Printf("leader: follower %s left\n", nodeAddr)
	}
}
