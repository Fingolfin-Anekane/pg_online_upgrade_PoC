# Disk-Safety Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the upgrade a primitive to watch how much WAL the logical slot is forcing the prod primary to retain, so the long-pole phases can throttle/abort before the disk fills.

**Architecture:** A pg-client read returns the slot's retained-WAL bytes (`pg_current_wal_lsn() - restart_lsn`). A pure `diskguard.Monitor` compares that to a threshold derived from `max_slot_wal_keep_size`, returning a decision (`OK` / `Throttle` / `Abort`). Phases (Plan 5) consume the decision; this plan only builds and unit-tests the primitive.

**Tech Stack:** Go, pgx/v5, pgxmock, the existing `internal/clients/pg` package + a new `internal/diskguard` package.

Plan 2 of the shadow-cluster upgrade (`docs/superpowers/specs/2026-06-15-shadow-cluster-upgrade-design.md`). Independent and unit-testable.

---

### Task 1: `SlotRetainedBytes` client read

**Files:**
- Modify: `internal/clients/pg/client.go` (interface + internalClient + PoolClient)
- Test: `internal/clients/pg/client_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/clients/pg/client_test.go`:

```go
func TestSlotRetainedBytes(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("pg_wal_lsn_diff").
		WithArgs("slot_upgrade").
		WillReturnRows(pgxmock.NewRows([]string{"retained"}).AddRow(int64(1048576)))

	c := pgclient.NewFromPool(mock)
	n, err := c.SlotRetainedBytes(context.Background(), "slot_upgrade")
	require.NoError(t, err)
	assert.Equal(t, int64(1048576), n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSlotRetainedBytesMissingSlot(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("pg_wal_lsn_diff").
		WithArgs("slot_upgrade").
		WillReturnError(pgx.ErrNoRows)

	c := pgclient.NewFromPool(mock)
	_, err = c.SlotRetainedBytes(context.Background(), "slot_upgrade")
	require.Error(t, err)
}
```

(`pgx` is already imported in `client_test.go`? It is not — add `"github.com/jackc/pgx/v5"` to that file's imports.)

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/clients/pg/ -run TestSlotRetainedBytes -v`
Expected: FAIL — `c.SlotRetainedBytes` undefined.

- [ ] **Step 3: Add the interface method + implementations**

In the `Client` interface (after `OldestTxnAge`), add:

```go
	SlotRetainedBytes(ctx context.Context, slot string) (int64, error)
```

On `internalClient` (near `MaxSlotWALKeepSize`):

```go
// SlotRetainedBytes returns how many bytes of WAL the named slot is keeping the
// primary from recycling: pg_current_wal_lsn() - restart_lsn. This is the slot's
// disk pressure; compared against max_slot_wal_keep_size it drives the throttle.
func (c *internalClient) SlotRetainedBytes(ctx context.Context, slot string) (int64, error) {
	var n int64
	err := c.q.QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
		   FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("pg: slot retained bytes for %q: %w", slot, err)
	}
	return n, nil
}
```

On `PoolClient` (near the other delegations):

```go
func (p *PoolClient) SlotRetainedBytes(ctx context.Context, slot string) (int64, error) {
	return p.ic().SlotRetainedBytes(ctx, slot)
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/clients/pg/ -run TestSlotRetainedBytes -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/clients/pg/client.go internal/clients/pg/client_test.go
git commit -m "feat(diskguard): SlotRetainedBytes read (slot WAL pressure)"
```

---

### Task 2: `diskguard` threshold logic

A pure decision function: given retained bytes and the `max_slot_wal_keep_size` cap (in bytes; 0/-1 ⇒ unbounded), decide OK / Throttle / Abort.

**Files:**
- Create: `internal/diskguard/diskguard.go`
- Test: `internal/diskguard/diskguard_test.go`

- [ ] **Step 1: Write the failing test**

`internal/diskguard/diskguard_test.go`:

```go
package diskguard

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecide(t *testing.T) {
	// cap unbounded -> always OK
	assert.Equal(t, OK, Decide(1<<40, 0))
	assert.Equal(t, OK, Decide(1<<40, -1))
	// below throttle fraction (0.75) -> OK
	assert.Equal(t, OK, Decide(700, 1000))
	// in [0.75, 1.0) -> Throttle
	assert.Equal(t, Throttle, Decide(800, 1000))
	// at/over cap -> Abort
	assert.Equal(t, Abort, Decide(1000, 1000))
	assert.Equal(t, Abort, Decide(1200, 1000))
}

func TestParseSize(t *testing.T) {
	b, err := ParseSize("1024MB")
	assert.NoError(t, err)
	assert.Equal(t, int64(1024)*1024*1024, b)
	b, err = ParseSize("-1")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), b)
	b, err = ParseSize("")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), b)
	_, err = ParseSize("garbage")
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/diskguard/ -v`
Expected: FAIL — package/functions undefined.

- [ ] **Step 3: Implement**

`internal/diskguard/diskguard.go`:

```go
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
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/diskguard/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/diskguard/
git commit -m "feat(diskguard): Decide(retained,cap) + ParseSize"
```

---

### Task 3: `Monitor` that reads live state and decides

Ties the read (Task 1) to the decision (Task 2): one call returns the current decision plus the retained bytes for logging.

**Files:**
- Create: `internal/diskguard/monitor.go`
- Test: `internal/diskguard/monitor_test.go`

- [ ] **Step 1: Write the failing test**

`internal/diskguard/monitor_test.go`:

```go
package diskguard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeReader struct {
	retained int64
	capStr   string
}

func (f fakeReader) SlotRetainedBytes(context.Context, string) (int64, error) { return f.retained, nil }
func (f fakeReader) MaxSlotWALKeepSize(context.Context) (string, error)       { return f.capStr, nil }

func TestMonitorSample(t *testing.T) {
	m := Monitor{Slot: "s", Reader: fakeReader{retained: 800, capStr: "1000"}}
	d, retained, err := m.Sample(context.Background())
	require.NoError(t, err)
	assert.Equal(t, Throttle, d)
	assert.Equal(t, int64(800), retained)
}

func TestMonitorUnbounded(t *testing.T) {
	m := Monitor{Slot: "s", Reader: fakeReader{retained: 1 << 40, capStr: "-1"}}
	d, _, err := m.Sample(context.Background())
	require.NoError(t, err)
	assert.Equal(t, OK, d)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/diskguard/ -run TestMonitor -v`
Expected: FAIL — `Monitor` undefined.

- [ ] **Step 3: Implement**

`internal/diskguard/monitor.go`:

```go
package diskguard

import "context"

// Reader is the subset of the pg client the monitor needs (the prod primary).
type Reader interface {
	SlotRetainedBytes(ctx context.Context, slot string) (int64, error)
	MaxSlotWALKeepSize(ctx context.Context) (string, error)
}

// Monitor samples the slot's disk pressure and turns it into a Decision.
type Monitor struct {
	Slot   string
	Reader Reader
}

// Sample reads the slot's retained bytes and the cap, and returns the current
// Decision plus retained bytes (for logging).
func (m Monitor) Sample(ctx context.Context) (Decision, int64, error) {
	retained, err := m.Reader.SlotRetainedBytes(ctx, m.Slot)
	if err != nil {
		return OK, 0, err
	}
	capStr, err := m.Reader.MaxSlotWALKeepSize(ctx)
	if err != nil {
		return OK, retained, err
	}
	capBytes, err := ParseSize(capStr)
	if err != nil {
		return OK, retained, err
	}
	return Decide(retained, capBytes), retained, nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/diskguard/ -v && go build ./...`
Expected: PASS, no build errors.

- [ ] **Step 5: Commit**

```bash
git add internal/diskguard/monitor.go internal/diskguard/monitor_test.go
git commit -m "feat(diskguard): Monitor.Sample (live decision from slot pressure)"
```

---

## Notes for the implementer

- The monitor's signal is **slot-retained WAL bytes vs the cap**, not host free disk — the cap (`max_slot_wal_keep_size`) is sized to disk headroom by the operator (see the spec's Disk Safety section), so retained-vs-cap is the right SQL-only proxy. No OS calls, no extension needed.
- On the prod **primary**, `pg_current_wal_lsn()` is correct. If this code is ever pointed at a standby, swap to `pg_last_wal_receive_lsn()`; out of scope here (the slot lives on the primary).
- Plan 5 (`rebuild-replicas`) consumes `Monitor.Sample`: `Throttle` ⇒ pause the next `patronictl reinit` and re-sample; `Abort` ⇒ stop with a clear error before the slot invalidates.
