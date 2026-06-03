package engine

import (
	"key_value_store/protocol"
	"key_value_store/store"
)


func handleDelete(msg protocol.Message, db Store) (protocol.Message, error) {
	req := msg.(*protocol.DeletePayload)

	if err := db.Delete(string(req.Key)); err != nil {
		if err == store.ErrKeyNotFound {
			return &protocol.DeleteResponse{Success: false, Message: "key not found"}, nil
		}
		return &protocol.DeleteResponse{Success: false, Message: err.Error()}, nil
	}
	return &protocol.DeleteResponse{Success: true, Message: "OK"}, nil
}
