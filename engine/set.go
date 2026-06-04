package engine

import (
	"key_value_store/protocol"
	"time"
)

func handleSet(msg protocol.Message, db Store) (protocol.Message, error) {
	req := msg.(*protocol.SetPayload)
	// TTL is in nanoseconds from the client; 0 means "use server default".
	ttl := time.Duration(req.TTL)
	if err := db.Set(string(req.Key), req.Value, ttl); err != nil {
		return &protocol.SetResponse{Success: false, Message: err.Error()}, nil
	}
	return &protocol.SetResponse{Success: true, Message: "OK"}, nil
}
