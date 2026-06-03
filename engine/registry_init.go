package engine

import "key_value_store/protocol"

// Adding a new command handler? Add one line here.
func init() {
	Register(protocol.OpGet, handleGet)
	Register(protocol.OpSet, handleSet)
	Register(protocol.OpDelete, handleDelete)
}
