package wal

import "time"

// SyncMode controls durability vs throughput.
type SyncMode string

const (
	SyncAlways   SyncMode = "always"   // write + fsync on every append
	SyncInterval SyncMode = "interval" // batch appends, fsync every FlushInterval or MaxBatchSize
	SyncNone     SyncMode = "none"     // write only; the OS decides when to flush
)

type Options struct {
	Mode          SyncMode
	FlushInterval time.Duration // Only used if Mode == SyncInterval
	MaxBatchSize  int           // Only used if Mode == SyncInterval
}
