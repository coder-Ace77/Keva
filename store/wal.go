package store

import (
	"encoding/binary"
	"os"
	"sync"
	"time"
)

const (
	OpSet    byte = 1
	OpDelete byte = 2
)

type SyncMode string

const (
	SyncAlways   SyncMode = "always"
	SyncInterval SyncMode = "interval"
	SyncNone     SyncMode = "none"
)

type WALOptions struct {
	Mode          SyncMode
	FlushInterval time.Duration // Only used if Mode == SyncInterval
	MaxBatchSize  int           // Only used if Mode == SyncInterval
}

type WAL struct {
	mu      sync.Mutex
	file    *os.File
	options WALOptions

	writeCh chan []byte
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

func NewWal(filePath string, opts WALOptions) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		file:    file,
		options: opts,
	}

	if opts.Mode == SyncInterval {
		w.writeCh = make(chan []byte, opts.MaxBatchSize*2)
		w.stopCh = make(chan struct{})
		w.wg.Add(1)
		// go start fluster
		go w.startFlusher()
	}

	return w, nil
}

func (w *WAL) writeFrame(buf []byte) error {
	// MODE 1: Asynchronous Interval Batching
	if w.options.Mode == SyncInterval {
		w.writeCh <- buf
		return nil // Return immediately, don't wait for disk!
	}

	// MODE 2 & 3: Synchronous Writing
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.file.Write(buf)
	if err != nil {
		return err
	}

	// MODE 2: Strict Durability
	if w.options.Mode == SyncAlways {
		// Force the OS to write from RAM to the physical SSD/HDD
		return w.file.Sync()
	}

	// MODE 3: SyncNone (We wrote to the file, but we don't call Sync().
	// Linux will flush it whenever it feels like it).
	return nil
}

func (w *WAL) AppendSet(key string, value []byte, expiredAt time.Time) error {
	keyLen := uint16(len(key))
	valLen := uint32(len(value))

	size := 1 + 2 + 4 + 8 + len(key) + len(value)
	buf := make([]byte, size)

	buf[0] = OpSet
	binary.BigEndian.PutUint16(buf[1:3], keyLen)
	binary.BigEndian.PutUint32(buf[3:7], valLen)
	binary.BigEndian.PutUint64(buf[7:15], uint64(expiredAt.UnixNano()))

	copy(buf[15:], key)
	copy(buf[15+len(key):], value)

	return w.writeFrame(buf) // Let the router handle it
}

func (w *WAL) AppendDelete(key string) error {
	keyLen := uint16(len(key))

	// Change size to accommodate the standard 15-byte header
	size := 15 + len(key)
	buf := make([]byte, size)

	buf[0] = OpDelete
	binary.BigEndian.PutUint16(buf[1:3], keyLen)

	// Bytes 3:15 (valLen and expiredAt) default to 0, which is perfectly
	// fine since Replay ignores them for OpDelete anyway.

	copy(buf[15:], key)

	return w.writeFrame(buf)
}

func (w *WAL) startFlusher() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.options.FlushInterval)
	defer ticker.Stop()

	var batch [][]byte

	flush := func() {
		if len(batch) == 0 {
			return
		}

		w.mu.Lock()
		for _, b := range batch {
			w.file.Write(b)
		}
		w.file.Sync()
		w.mu.Unlock()

		batch = batch[:0]
	}

	for {
		select {
		case <-w.stopCh:
			flush()
			return
		case data := <-w.writeCh:
			batch = append(batch, data)
			if len(batch) >= w.options.MaxBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// Close gracefully shuts down based on the operating mode
func (w *WAL) Close() error {
	if w.options.Mode == SyncInterval {
		close(w.stopCh)
		w.wg.Wait() // Wait for the final background flush to finish
	} else {
		w.mu.Lock()
		w.file.Sync()
		w.mu.Unlock()
	}

	return w.file.Close()
}
