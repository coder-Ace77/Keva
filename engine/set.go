package engine

import "key_value_store/protocol"

func handleSet(msg protocol.Message, db Store) (protocol.Message, error) {
	req := msg.(*protocol.SetPayload)

	if err := db.Set(string(req.Key), req.Value); err != nil {
		return &protocol.SetResponse{Success: false, Message: err.Error()}, nil
	}
	return &protocol.SetResponse{Success: true, Message: "OK"}, nil
}
