package engine

import (
	"key_value_store/protocol"
	"key_value_store/protocol/kv"
	"key_value_store/store"
)

func handleDelete(msg protocol.Message, db Store) (protocol.Message, error) {
	req := msg.(*kv.DeleteRequest)

	if err := db.Delete(string(req.Key)); err != nil {
		if err == store.ErrKeyNotFound {
			return &kv.DeleteResponse{Success: false, Message: "key not found"}, nil
		}
		return &kv.DeleteResponse{Success: false, Message: err.Error()}, nil
	}
	return &kv.DeleteResponse{Success: true, Message: "OK"}, nil
}
