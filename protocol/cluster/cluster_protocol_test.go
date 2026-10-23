package cluster

import (
	"errors"
	"key_value_store/protocol"
	"reflect"
	"testing"
)

// roundTrip encodes msg, decodes it back through the registry, and checks the
// result is identical. It also checks that every truncated prefix of the
// payload is rejected with ErrPayloadTooShort instead of panicking.
func roundTrip(t *testing.T, msg protocol.Message) {
	t.Helper()

	wire, err := protocol.EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode %T: %v", msg, err)
	}
	got, err := protocol.DecodeMessage(wire)
	if err != nil {
		t.Fatalf("decode %T: %v", msg, err)
	}
	if !reflect.DeepEqual(got, msg) {
		t.Fatalf("round trip %T:\n got  %+v\n want %+v", msg, got, msg)
	}

	for n := 1; n < len(wire); n++ {
		if _, err := protocol.DecodeMessage(wire[:n]); !errors.Is(err, protocol.ErrPayloadTooShort) {
			t.Fatalf("%T truncated to %d bytes: want ErrPayloadTooShort, got %v", msg, n, err)
		}
	}
}

func TestClusterMessagesRoundTrip(t *testing.T) {
	for _, msg := range []protocol.Message{
		&Heartbeat{Term: 3, LeaderID: "localhost:7001"},
		&HeartbeatAck{Term: 3, LastAppliedIndex: 9},
		&VoteRequest{Term: 4, CandidateID: "localhost:7002", LastLogIndex: 11},
		&VoteResponse{Term: 4, Granted: true},
		&ReplicationJoin{NodeAddr: "localhost:7003"},
		&ReplicationSync{LogIndex: 7, Op: 1, Key: []byte("k"), Value: []byte("v"), ExpiresAt: 999},
		&ReplicationAck{LastAppliedIndex: 7},
	} {
		roundTrip(t, msg)
	}
}
