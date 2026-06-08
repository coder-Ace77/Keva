# Package: engine

**Files:** `engine/handler.go`, `engine/registry_init.go`, `engine/get.go`, `engine/set.go`, `engine/delete.go`

The engine package is the command dispatcher. It sits between the TCP server and the store, translating decoded protocol messages into store operations.

---

## The Store interface

```go
type Store interface {
    Set(key string, value []byte, ttl time.Duration) error
    Get(key string) ([]byte, error)
    Delete(key string) error
}
```

This interface is crucial. The engine dispatches against this interface, not against `*store.KVStore` or `*cluster.Node` directly. That means:

- `cluster.Node` implements `Store` — when a Set arrives, the node writes to its local store AND replicates to followers.
- `store.KVStore` could also implement `Store` directly if you ever needed to run the engine without the cluster layer.
- Tests can provide a mock `Store` implementation.

This is dependency inversion: the engine depends on an abstraction, not a concrete type.

---

## Handler type and registry

```go
type Handler func(msg protocol.Message, db Store) (protocol.Message, error)

var handlers = make(map[byte]Handler)

func Register(opcode byte, h Handler) {
    // panics on duplicate
    handlers[opcode] = h
}

func Dispatch(msg protocol.Message, db Store) (protocol.Message, error) {
    h, ok := handlers[msg.OpCode()]
    if !ok {
        return nil, fmt.Errorf("engine: no handler for opcode 0x%02X", msg.OpCode())
    }
    return h(msg, db)
}
```

Exactly the same registry pattern as the protocol package but for command handlers. `Dispatch` is called by the TCP server. It's a 2-line function — all complexity lives in the individual handlers.

Registrations happen in `registry_init.go`'s `init()`:
```go
func init() {
    Register(protocol.OpGet, handleGet)
    Register(protocol.OpSet, handleSet)
    Register(protocol.OpDelete, handleDelete)
}
```

**Interview question:** "How would you add a new command like INCR?" — Write `handleIncr` in a new file, add one `Register` call in `registry_init.go`, add the opcode to `protocol/opcodes.go`, add the message struct. Nothing else needs to change.

---

## handleGet

```go
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
```

Note that `ErrKeyNotFound` is a normal (non-error) response — it returns `Found: false`, not an `ErrorMessage`. Only unexpected errors produce an `ErrorMessage`.

The type assertion `msg.(*protocol.GetPayload)` is safe here because the registry guarantees that a `GetPayload` struct is always passed to `handleGet` — the opcode-to-handler mapping is set up at init time and never changes.

---

## handleSet

```go
func handleSet(msg protocol.Message, db Store) (protocol.Message, error) {
    req := msg.(*protocol.SetPayload)
    ttl := time.Duration(req.TTL)  // TTL is nanoseconds from client, matches time.Duration
    if err := db.Set(string(req.Key), req.Value, ttl); err != nil {
        return &protocol.SetResponse{Success: false, Message: err.Error()}, nil
    }
    return &protocol.SetResponse{Success: true, Message: "OK"}, nil
}
```

TTL is passed as nanoseconds in the wire format. `time.Duration` in Go is also nanoseconds, so the cast is direct. A TTL of 0 is passed through to `Node.Set`, which replaces it with the configured `defaultTTL`.

---

## handleDelete

```go
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
```

Delete on a non-existent key returns `Success: false` with a message, not an `ErrorMessage`. This lets clients distinguish "I tried to delete a key that wasn't there" (which is often fine) from "the server had an internal error".

---

## Why separate engine from server?

The server package handles transport: reading bytes, framing, auth, routing by opcode. The engine package handles semantics: what does it mean to execute a SET or GET. This separation means:

- The server can be tested with any `Store` implementation.
- The engine handlers are pure functions — they're easy to unit test.
- Adding a new transport (e.g., HTTP) would only require rewriting the server, not the engine.
