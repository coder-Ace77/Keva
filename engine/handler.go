package engine

import (
	"fmt"
	"key_value_store/protocol"
	"sync"
)

// Store is the interface the engine dispatches against.
// Both *store.KVStore and *cluster.Node satisfy it structurally.
type Store interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}

// Handler processes a decoded protocol message and returns a response message.
type Handler func(msg protocol.Message, db Store) (protocol.Message, error)

var (
	mu       sync.RWMutex
	handlers = make(map[byte]Handler)
)

// Register maps an opcode to a handler. Panics on duplicate registration.
func Register(opcode byte, h Handler) {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := handlers[opcode]; exists {
		panic(fmt.Sprintf("engine: duplicate handler for opcode 0x%02X", opcode))
	}
	handlers[opcode] = h
}

// Dispatch routes a decoded message to its registered handler.
func Dispatch(msg protocol.Message, db Store) (protocol.Message, error) {
	mu.RLock()
	h, ok := handlers[msg.OpCode()]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("engine: no handler for opcode 0x%02X", msg.OpCode())
	}
	return h(msg, db)
}
