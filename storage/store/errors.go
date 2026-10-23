package store

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrMemoryFull  = errors.New("memory limit reached: write rejected (noevict policy)")
)
