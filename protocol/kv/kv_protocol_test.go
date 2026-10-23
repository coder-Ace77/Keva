package kv

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

func TestKVMessagesRoundTrip(t *testing.T) {
	for _, msg := range []protocol.Message{
		&GetRequest{Key: []byte("user:1")},
		&GetResponse{Found: true, Value: []byte("alice")},
		&SetRequest{Key: []byte("user:1"), Value: []byte("hello world"), TTL: 12345},
		&SetResponse{Success: true, Message: "OK"},
		&DeleteRequest{Key: []byte("user:1")},
		&DeleteResponse{Success: false, Message: "key not found"},
	} {
		roundTrip(t, msg)
	}
}
