package wal

import (
	"os"
	"sync"
	"time"
)

// WAL is an append-only log of Set/Delete records. How quickly an append
// reaches the disk depends on Options.Mode.
type WAL struct {
	path    string
	options Options

	mu   sync.Mutex // guards file
	file *os.File

	// SyncInterval only: encoded records waiting for the background flusher.
	writeCh chan []byte
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// Open opens (or creates) the log at path for appending. Replay it first if
// the existing contents are needed.
func Open(path string, opts Options) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		path:    path,
		options: opts,
		file:    file,
	}

	if opts.Mode == SyncInterval {
		w.writeCh = make(chan []byte, opts.MaxBatchSize*2)
		w.stopCh = make(chan struct{})
		w.wg.Add(1)
		go w.runFlusher()
	}

	return w, nil
}

func (w *WAL) AppendSet(key string, value []byte, expiresAt time.Time) error {
	return w.append(Record{Op: OpSet, Key: key, Value: value, ExpiresAt: expiresAt})
}

func (w *WAL) AppendDelete(key string) error {
	return w.append(Record{Op: OpDelete, Key: key})
}

func (w *WAL) append(r Record) error {
	buf := r.encode()

	// SyncInterval: hand off to the flusher and return without waiting for disk.
	if w.options.Mode == SyncInterval {
		w.writeCh <- buf
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Write(buf); err != nil {
		return err
	}

	// SyncAlways: force the OS to write from RAM to the physical disk.
	if w.options.Mode == SyncAlways {
		return w.file.Sync()
	}

	// SyncNone: written to the file, but Linux flushes it whenever it likes.
	return nil
}

// runFlusher batches records from writeCh and writes + fsyncs them every
// FlushInterval, or sooner once MaxBatchSize records are waiting.
func (w *WAL) runFlusher() {
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

// Close flushes anything pending and closes the file.
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
