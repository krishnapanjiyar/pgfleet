// Package pg holds small PostgreSQL helpers (LSN arithmetic, pg_controldata parsing).
package pg

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// LSN is a PostgreSQL write-ahead-log position.
type LSN uint64

// ParseLSN parses the "X/Y" hexadecimal form PostgreSQL prints (e.g. "0/3000060").
func ParseLSN(s string) (LSN, error) {
	s = strings.TrimSpace(s)
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("invalid LSN %q", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid LSN %q: %w", s, err)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid LSN %q: %w", s, err)
	}
	return LSN(h<<32 | l), nil
}

// String formats the LSN the way PostgreSQL does.
func (l LSN) String() string { return fmt.Sprintf("%X/%X", uint64(l)>>32, uint64(l)&0xFFFFFFFF) }

// Lag returns how many bytes `replica` is behind `primary` (never negative).
func Lag(primary, replica LSN) uint64 {
	if replica >= primary {
		return 0
	}
	return uint64(primary - replica)
}

var redoRe = regexp.MustCompile(`(?m)^Latest checkpoint's REDO location:\s+(\S+)`)

// ParseControlRedo extracts the checkpoint REDO location from pg_controldata output.
func ParseControlRedo(out string) (LSN, error) {
	m := redoRe.FindStringSubmatch(out)
	if m == nil {
		return 0, fmt.Errorf("REDO location not found in pg_controldata output")
	}
	return ParseLSN(m[1])
}
