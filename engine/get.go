package engine

import (
	"key_value_store/protocol"
	"key_value_store/protocol/kv"
	"key_value_store/store"
)

func handleGet(msg protocol.Message, db Store) (protocol.Message, error) {
	req := msg.(*kv.GetRequest)

	val, err := db.Get(string(req.Key))
	if err == store.ErrKeyNotFound {
		return &kv.GetResponse{Found: false}, nil
	}
	if err != nil {
		return &protocol.ErrorMessage{Code: protocol.ErrCodeInternal, Message: err.Error()}, nil
	}
	return &kv.GetResponse{Found: true, Value: val}, nil
}
