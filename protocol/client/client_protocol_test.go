package client

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

func TestClientMessagesRoundTrip(t *testing.T) {
	for _, msg := range []protocol.Message{
		&TopologyResponse{
			LeaderAddr: "localhost:7001",
			Nodes: []NodeInfo{
				{Address: "localhost:7001", Role: TopologyRoleLeader, NodeID: "localhost:7001"},
				{Address: "localhost:7002", Role: TopologyRoleFollower, NodeID: "localhost:7002"},
			},
		},
	} {
		roundTrip(t, msg)
	}
}

// TopologyRequest has an empty payload, so there is nothing to truncate.
func TestTopologyRequestRoundTrip(t *testing.T) {
	wire, _ := protocol.EncodeMessage(&TopologyRequest{})
	got, err := protocol.DecodeMessage(wire)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.(*TopologyRequest); !ok {
		t.Fatalf("got %T, want *TopologyRequest", got)
	}
}
