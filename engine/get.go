package engine

import (
	"key_value_store/protocol"
	"key_value_store/store"
)

func handleGet(msg protocol.Message, db Store) (protocol.Message, error) {
	req := msg.(*protocol.GetPayload)

	val, err := db.Get(string(req.Key))
	if err == store.ErrKeyNotFound {
		return &protocol.GetResponse{Found: false}, nil
	}
	if err != nil {
		return &protocol.ErrorMessage{Code: protocol.ErrCodeInternal, Message: err.Error()}, nil
	}
	return &protocol.GetResponse{Found: true, Value: val}, nil
}
