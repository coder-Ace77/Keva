package cluster

import (
	"fmt"
	"key_value_store/protocol"
	clusterproto "key_value_store/protocol/cluster"
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
	l.broadcast(&clusterproto.ReplicationSync{
		LogIndex:  l.logIndex.Add(1),
		Op:        store.OpSet,
		Key:       []byte(key),
		Value:     value,
		ExpiresAt: uint64(expiredAt),
	})
}

// BroadcastDelete replicates a DELETE to all followers.
func (l *Leader) BroadcastDelete(key string) {
	l.broadcast(&clusterproto.ReplicationSync{
		LogIndex: l.logIndex.Add(1),
		Op:       store.OpDelete,
		Key:      []byte(key),
	})
}

func (l *Leader) broadcast(msg protocol.Message) {
	frame, err := protocol.EncodeFrame(msg)
	if err != nil {
		return
	}

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
	for {
		if _, err := protocol.ReadFrame(conn, 0); err != nil {
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
