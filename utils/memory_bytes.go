package utils

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseMemoryBytes converts a human-readable size string to bytes.
// Accepts: "512MB", "1GB", "256KB", "1073741824" (raw bytes).
// Suffixes are case-insensitive: KB/K, MB/M, GB/G, TB/T.
func ParseMemoryBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	upper := strings.ToUpper(s)

	suffixes := []struct {
		suffix string
		mult   int64
	}{
		{"TB", 1 << 40},
		{"GB", 1 << 30},
		{"MB", 1 << 20},
		{"KB", 1 << 10},
		{"T", 1 << 40},
		{"G", 1 << 30},
		{"M", 1 << 20},
		{"K", 1 << 10},
	}

	for _, e := range suffixes {
		if strings.HasSuffix(upper, e.suffix) {
			numStr := strings.TrimSpace(s[:len(s)-len(e.suffix)])
			n, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil {
				return 0, err
			}
			return n * e.mult, nil
		}
	}

	return strconv.ParseInt(s, 10, 64)
}

// FormatMemoryBytes returns a human-readable representation of a byte count.
// 0 is shown as "unlimited", matching the meaning of a zero memory cap.
func FormatMemoryBytes(b int64) string {
	switch {
	case b == 0:
		return "unlimited"
	case b >= 1<<40:
		return fmt.Sprintf("%dTB", b>>40)
	case b >= 1<<30:
		return fmt.Sprintf("%dGB", b>>30)
	case b >= 1<<20:
		return fmt.Sprintf("%dMB", b>>20)
	case b >= 1<<10:
		return fmt.Sprintf("%dKB", b>>10)
	default:
		return fmt.Sprintf("%dB", b)
	}
}
