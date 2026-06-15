// Package diskguard decides whether the upgrade may keep loading the slot or
// must throttle/abort, based on how much WAL the slot retains vs the configured
// max_slot_wal_keep_size cap.
package diskguard

import (
	"fmt"
	"strconv"
	"strings"
)

type Decision int

const (
	OK Decision = iota
	Throttle
	Abort
)

// throttleFraction is the fraction of the cap at which we pause new load to let
// the slot drain before it invalidates.
const throttleFraction = 0.75

// Decide compares retained WAL bytes to capBytes. A cap of 0 or -1 means
// unbounded (max_slot_wal_keep_size unset/-1) -> never throttle on size.
func Decide(retained, capBytes int64) Decision {
	if capBytes <= 0 {
		return OK
	}
	if retained >= capBytes {
		return Abort
	}
	if float64(retained) >= throttleFraction*float64(capBytes) {
		return Throttle
	}
	return OK
}

// ParseSize parses a PostgreSQL memory/size string (e.g. "1024MB", "2GB", "0",
// "-1") into bytes. "" -> 0 (treated as unbounded). "-1" -> -1 (unbounded).
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if s == "-1" {
		return -1, nil
	}
	mult := int64(1)
	for unit, m := range map[string]int64{"kB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30, "TB": 1 << 40} {
		if strings.HasSuffix(s, unit) {
			mult = m
			s = strings.TrimSpace(strings.TrimSuffix(s, unit))
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("diskguard: parse size %q: %w", s, err)
	}
	return n * mult, nil
}
