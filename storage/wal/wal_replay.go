package wal

import (
	"bufio"
	"io"
	"os"
)

// Replay reads every record in the log at path, oldest first, and hands each
// one to apply. A missing file is treated as an empty log.
func Replay(path string, apply func(Record)) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	r := bufio.NewReader(file)
	header := make([]byte, recordHeaderSize)

	for {
		rec, err := readRecord(r, header)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		apply(rec)
	}
}
