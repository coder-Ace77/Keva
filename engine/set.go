package engine

import (
	"key_value_store/protocol"
	"key_value_store/protocol/kv"
	"time"
)

func handleSet(msg protocol.Message, db Store) (protocol.Message, error) {
	req := msg.(*kv.SetRequest)
	// TTL is in nanoseconds from the client; 0 means "use server default".
	ttl := time.Duration(req.TTL)
	if err := db.Set(string(req.Key), req.Value, ttl); err != nil {
		return &kv.SetResponse{Success: false, Message: err.Error()}, nil
	}
	return &kv.SetResponse{Success: true, Message: "OK"}, nil
}
