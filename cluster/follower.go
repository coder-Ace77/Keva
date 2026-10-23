package cluster

import (
	"errors"
	"fmt"
	"key_value_store/protocol"
	clusterproto "key_value_store/protocol/cluster"
	"key_value_store/store"
	"net"
	"time"
)

// RunFollower continuously syncs with whoever the current leader is.
// When the leader changes (via Raft election), it reconnects automatically.
// Intended to run in a goroutine.
func RunFollower(raft *RaftState, db *store.KVStore, nodeAddr string) {
	for {
		// If this node won the election, stop following.
		if raft.GetRole() == RoleLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		leaderAddr := raft.GetLeaderAddr()
		if leaderAddr == "" || leaderAddr == nodeAddr {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		err := syncWithLeader(leaderAddr, db, nodeAddr)
		if err != nil {
			fmt.Printf("follower: replication from %s ended: %v\n", leaderAddr, err)
		}

		// Brief pause — Raft will update leaderAddr via heartbeats/election.
		time.Sleep(200 * time.Millisecond)
	}
}

func syncWithLeader(leaderAddr string, db *store.KVStore, nodeAddr string) error {
	conn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := protocol.WriteFrame(conn, &clusterproto.ReplicationJoin{NodeAddr: nodeAddr}); err != nil {
		return err
	}
	fmt.Printf("follower: registered with leader at %s\n", leaderAddr)

	for {
		msg, err := protocol.ReadFrame(conn, 0)
		if errors.Is(err, protocol.ErrMalformedMessage) {
			fmt.Printf("follower: decode error: %v\n", err)
			continue
		}
		if err != nil {
			return err
		}

		sync, ok := msg.(*clusterproto.ReplicationSync)
		if !ok {
			continue
		}

		applySync(db, sync)

		_ = protocol.WriteFrame(conn, &clusterproto.ReplicationAck{LastAppliedIndex: sync.LogIndex})
	}
}

func applySync(db *store.KVStore, sync *clusterproto.ReplicationSync) {
	key := string(sync.Key)
	switch sync.Op {
	case store.OpSet:
		expiredAt := time.Unix(0, int64(sync.ExpiresAt))
		if err := db.SetWithExpiry(key, sync.Value, expiredAt); err != nil {
			fmt.Printf("follower: apply SET %q error: %v\n", key, err)
		}
	case store.OpDelete:
		err := db.Delete(key)
		if err != nil && err != store.ErrKeyNotFound {
			fmt.Printf("follower: apply DELETE %q error: %v\n", key, err)
		}
	}
}
