package wal

import (
	"bufio"
	"os"
)

// Compact replaces the log with a fresh one that holds only the records
// snapshot writes (typically one OpSet per live key), discarding the history
// that led to them.
//
// The new log is built in <path>.tmp while appends continue to the old one,
// then swapped in with an atomic rename while appends are blocked.
func (w *WAL) Compact(snapshot func(write func(Record) error) error) error {
	tmpPath := w.path + ".tmp"

	if err := writeSnapshot(tmpPath, snapshot); err != nil {
		os.Remove(tmpPath)
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := os.Rename(tmpPath, w.path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// The open handle still points at the replaced file; switch to the new one.
	newFile, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	w.file.Close()
	w.file = newFile

	return nil
}

// writeSnapshot writes the records produced by snapshot to a new file at
// path and fsyncs it.
func writeSnapshot(path string, snapshot func(write func(Record) error) error) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	buf := bufio.NewWriter(file)
	err = snapshot(func(r Record) error {
		_, err := buf.Write(r.encode())
		return err
	})
	if err == nil {
		err = buf.Flush()
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}
