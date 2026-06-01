# loadtool Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `loadtool`, a CLI companion to `pg-upgrade` that drives a verifiable workload against the cluster, simulates an in-flight DSN swap via `SIGHUP`, and renders an authoritative data-consistency verdict from a durable intent-log.

**Architecture:** Three cobra subcommands — `init` (schema), `run` (load + DSN switch + intent-log), `verify` (final reconciliation). `loadgen` owns workers, the swappable active-DSN selector, and the durable JSONL intent-log; it renders no verdict. `oracle` reads the intent-log + DB at rest and is the only place a PASS/FAIL is decided. Workers and reconciliation take a small `Pool`/`Querier` interface so they unit-test against `pgxmock`.

**Tech Stack:** Go 1.25, `pgx/v5` + `pgxpool`, `cobra`, `yaml.v3`, `google/uuid`, `testify`, `pgxmock/v3`. Same module `github.com/dmbabuev/pg-upgrade`.

**Spec:** `docs/superpowers/specs/2026-06-02-loadtool-design.md`

---

## File Structure

```
cmd/loadtool/main.go              # cobra root + init/run/verify subcommands
internal/loadcfg/config.go        # flags + YAML config, defaults, validation
internal/loadgen/intentlog.go     # Record + serialized durable JSONL Writer
internal/loadgen/dsn.go           # Pool interface + atomically-swappable Selector
internal/loadgen/schema.go        # init DDL + account seeding
internal/loadgen/classify.go      # error -> {acked/failed/indoubt} + error class
internal/loadgen/workers.go       # append / ryw / long-txn / transfer iterations
internal/loadgen/metrics.go       # throughput/error counters + unavailability window
internal/loadgen/runner.go        # worker lifecycle, SIGHUP swap, shutdown wiring
internal/oracle/intentlog.go      # JSONL reader, attempt/result pairing
internal/oracle/reconcile.go      # Findings + the membership/sequence/sum checks
internal/oracle/report.go         # render findings + PASS/FAIL
```

**Data model note (unifies append + long-txn):** every `events` row carries a
unique `(writer_id, client_seq)`. An `append`/`ryw` op writes one row
(`rows = 1`). A `long-txn` op writes `rows = batch-size` rows sharing a
`batch_id`, occupying the contiguous `client_seq` range `[start, start+rows)`.
So a single membership map `(writer_id, client_seq) -> count` reconciles all
event-producing workloads; batch wholeness is the all-or-nothing of a
`client_seq` range. `transfer` writes nothing to the log (sum invariant only).

---

## Task 1: Config (`internal/loadcfg`)

**Files:**
- Create: `internal/loadcfg/config.go`
- Test: `internal/loadcfg/config_test.go`

- [ ] **Step 1: Write the failing test**

```go
package loadcfg

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultsApplied(t *testing.T) {
	cfg := Default()
	assert.Equal(t, 100, cfg.Accounts)
	assert.Equal(t, int64(1000), cfg.Balance)
	assert.Equal(t, "loadtool-intent.jsonl", cfg.IntentLog)
	assert.Equal(t, 4, cfg.Workers.Append.Count)
	assert.Equal(t, 1, cfg.Workers.LongTxn.Count)
	assert.Equal(t, 2, cfg.Workers.Transfer.Count)
	assert.Equal(t, 1, cfg.Workers.RYW.Count)
}

func TestLoadYAMLOverridesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	require.NoError(t, os.WriteFile(path, []byte(
		"accounts: 50\nbalance: 500\nduration: 30s\nworkers:\n  append:\n    count: 8\n    rate: 20\n"), 0o644))

	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, 50, cfg.Accounts)
	assert.Equal(t, int64(500), cfg.Balance)
	assert.Equal(t, 30*time.Second, time.Duration(cfg.Duration))
	assert.Equal(t, 8, cfg.Workers.Append.Count)
	assert.Equal(t, 20, cfg.Workers.Append.Rate)
	// untouched fields keep defaults
	assert.Equal(t, 2, cfg.Workers.Transfer.Count)
}

func TestValidateRequiresDSNs(t *testing.T) {
	cfg := Default()
	assert.Error(t, cfg.ValidateRun())
	cfg.DSNA = "postgres://a"
	assert.Error(t, cfg.ValidateRun())
	cfg.DSNB = "postgres://b"
	assert.NoError(t, cfg.ValidateRun())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/loadcfg/`
Expected: FAIL — `undefined: Default` / package has no Go files.

- [ ] **Step 3: Write minimal implementation**

```go
// Package loadcfg holds loadtool's configuration: defaults, YAML loading, and
// validation. Flags are layered on top of these structs by cmd/loadtool.
package loadcfg

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration so YAML can express it as a string ("30s", "5m")
// rather than a raw nanosecond count. Integers are still accepted as ns.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err == nil {
		parsed, perr := time.ParseDuration(s)
		if perr != nil {
			return fmt.Errorf("loadcfg: invalid duration %q: %w", s, perr)
		}
		*d = Duration(parsed)
		return nil
	}
	var n int64
	if err := value.Decode(&n); err != nil {
		return fmt.Errorf(`loadcfg: duration must be a string like "30s" or an integer ns`)
	}
	*d = Duration(n)
	return nil
}

type WorkerSet struct {
	Count int `yaml:"count"`
	Rate  int `yaml:"rate"` // ops/sec/worker; 0 = unthrottled
}

type LongTxnSet struct {
	Count       int      `yaml:"count"`
	TxnDuration Duration `yaml:"txn_duration"`
	BatchSize   int      `yaml:"batch_size"`
}

type Workers struct {
	Append   WorkerSet  `yaml:"append"`
	LongTxn  LongTxnSet `yaml:"long_txn"`
	Transfer WorkerSet  `yaml:"transfer"`
	RYW      WorkerSet  `yaml:"ryw"`
}

type Config struct {
	DSNA      string   `yaml:"dsn_a"`
	DSNB      string   `yaml:"dsn_b"`
	IntentLog string   `yaml:"intent_log"`
	Accounts  int      `yaml:"accounts"`
	Balance   int64    `yaml:"balance"`
	Duration  Duration `yaml:"duration"`
	Workers   Workers  `yaml:"workers"`
}

// Default returns a Config with sensible defaults already applied.
func Default() Config {
	return Config{
		IntentLog: "loadtool-intent.jsonl",
		Accounts:  100,
		Balance:   1000,
		Workers: Workers{
			Append:   WorkerSet{Count: 4, Rate: 0},
			LongTxn:  LongTxnSet{Count: 1, TxnDuration: Duration(5 * time.Second), BatchSize: 10},
			Transfer: WorkerSet{Count: 2, Rate: 0},
			RYW:      WorkerSet{Count: 1, Rate: 0},
		},
	}
}

// Load reads YAML from path on top of Default(); unset YAML fields keep defaults.
func Load(path string) (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("loadcfg read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("loadcfg parse %s: %w", path, err)
	}
	return cfg, nil
}

// ValidateRun checks the fields required by the `run` subcommand.
func (c Config) ValidateRun() error {
	if c.DSNA == "" {
		return fmt.Errorf("loadcfg: dsn_a is required")
	}
	if c.DSNB == "" {
		return fmt.Errorf("loadcfg: dsn_b is required")
	}
	if c.IntentLog == "" {
		return fmt.Errorf("loadcfg: intent_log is required")
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/loadcfg/`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/loadcfg/
git commit -m "feat(loadtool): config with defaults, YAML load, validation"
```

---

## Task 2: Intent-log Writer (`internal/loadgen/intentlog.go`)

**Files:**
- Create: `internal/loadgen/intentlog.go`
- Test: `internal/loadgen/intentlog_test.go`

The Writer is the single serialized owner of the JSONL file + stdout stream.
`WriteAttempt` flushes (fsync) before returning so the attempt is durable
*before* the caller issues COMMIT.

- [ ] **Step 1: Write the failing test**

```go
package loadgen

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readRecords(t *testing.T, path string) []Record {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var recs []Record
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r Record
		require.NoError(t, json.Unmarshal(sc.Bytes(), &r))
		recs = append(recs, r)
	}
	return recs
}

func TestWriterAttemptThenResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.jsonl")
	var stdout bytes.Buffer
	w, err := NewWriter(path, &stdout)
	require.NoError(t, err)

	rec := Record{OpID: "op1", Kind: "append", WriterID: 3, ClientSeq: 41, DSN: "a", Phase: "pre-switch"}
	require.NoError(t, w.WriteAttempt(rec))
	require.NoError(t, w.WriteResult("op1", StatusAcked, ""))
	require.NoError(t, w.Close())

	recs := readRecords(t, path)
	require.Len(t, recs, 2)
	assert.Equal(t, StatusAttempt, recs[0].Status)
	assert.Equal(t, "op1", recs[0].OpID)
	assert.Equal(t, 41, int(recs[0].ClientSeq))
	assert.Equal(t, StatusAcked, recs[1].Status)
	assert.Equal(t, "op1", recs[1].OpID)
	assert.NotEmpty(t, stdout.String()) // human stream also written
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/loadgen/ -run TestWriter`
Expected: FAIL — `undefined: NewWriter`.

- [ ] **Step 3: Write minimal implementation**

```go
package loadgen

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Status values for an intent-log record.
const (
	StatusAttempt = "attempt"
	StatusAcked   = "acked"
	StatusFailed  = "failed"
	StatusIndoubt = "indoubt"
)

// Record is one line of the append-only JSONL intent-log. Attempt records carry
// the full operation identity; result records carry op_id + status (+ error).
type Record struct {
	OpID      string    `json:"op_id"`
	Kind      string    `json:"kind,omitempty"`
	WriterID  int       `json:"writer_id,omitempty"`
	ClientSeq int64     `json:"client_seq,omitempty"`
	Rows      int64     `json:"rows,omitempty"`
	BatchID   string    `json:"batch_id,omitempty"`
	DSN       string    `json:"dsn,omitempty"`
	Phase     string    `json:"phase,omitempty"`
	Status    string    `json:"status"`
	Error     string    `json:"error,omitempty"`
	TS        time.Time `json:"ts"`
}

// Writer is the single serialized owner of the durable intent-log and the
// human-readable stdout stream. All workers share one Writer; the mutex
// guarantees non-interleaved JSONL.
type Writer struct {
	mu  sync.Mutex
	f   *os.File
	out io.Writer
}

// NewWriter opens (creates/appends) the intent-log file and tees a human stream.
func NewWriter(path string, stdout io.Writer) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("intentlog open %s: %w", path, err)
	}
	return &Writer{f: f, out: stdout}, nil
}

// WriteAttempt persists an attempt record and fsyncs before returning, so the
// ground-truth entry is durable before the caller issues COMMIT.
func (w *Writer) WriteAttempt(rec Record) error {
	rec.Status = StatusAttempt
	rec.TS = time.Now().UTC()
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writeLocked(rec); err != nil {
		return err
	}
	return w.f.Sync()
}

// WriteResult persists the terminal status for an op. Not fsynced individually;
// it is flushed by the OS and on Close.
func (w *Writer) WriteResult(opID, status, errMsg string) error {
	rec := Record{OpID: opID, Status: status, Error: errMsg, TS: time.Now().UTC()}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeLocked(rec)
}

func (w *Writer) writeLocked(rec Record) error {
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("intentlog marshal: %w", err)
	}
	line = append(line, '\n')
	if _, err := w.f.Write(line); err != nil {
		return fmt.Errorf("intentlog write: %w", err)
	}
	_, _ = w.out.Write(line) // human stream is best-effort
	return nil
}

// Close fsyncs and closes the underlying file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/loadgen/ -run TestWriter`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/loadgen/intentlog.go internal/loadgen/intentlog_test.go
git commit -m "feat(loadtool): durable serialized intent-log writer"
```

---

## Task 3: DSN Selector (`internal/loadgen/dsn.go`)

**Files:**
- Create: `internal/loadgen/dsn.go`
- Test: `internal/loadgen/dsn_test.go`

- [ ] **Step 1: Write the failing test**

```go
package loadgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectorSwitchFlipsActive(t *testing.T) {
	a := &fakePool{name: "a"}
	b := &fakePool{name: "b"}
	s := NewSelector(a, b)

	pool, label, phase := s.Active()
	assert.Same(t, a, pool)
	assert.Equal(t, "a", label)
	assert.Equal(t, "pre-switch", phase)

	s.Switch()

	pool, label, phase = s.Active()
	assert.Same(t, b, pool)
	assert.Equal(t, "b", label)
	assert.Equal(t, "post-switch", phase)
}
```

Add a tiny in-test fake satisfying `Pool` (only identity matters here):

```go
type fakePool struct {
	Pool
	name string
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/loadgen/ -run TestSelector`
Expected: FAIL — `undefined: NewSelector` / `undefined: Pool`.

- [ ] **Step 3: Write minimal implementation**

```go
package loadgen

import (
	"context"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Pool is the subset of *pgxpool.Pool used by workers and reconciliation. Both
// *pgxpool.Pool and pgxmock pools satisfy it, so callers unit-test on mocks.
type Pool interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Selector holds the two pools and an atomic switch. Workers call Active() on
// every op, so a SIGHUP-driven Switch() takes effect on the next operation.
type Selector struct {
	a, b     Pool
	switched atomic.Bool
}

func NewSelector(a, b Pool) *Selector { return &Selector{a: a, b: b} }

// Switch flips the active pool from A to B. Idempotent.
func (s *Selector) Switch() { s.switched.Store(true) }

// Switched reports whether the swap has happened.
func (s *Selector) Switched() bool { return s.switched.Load() }

// Active returns the live pool, its label ("a"/"b"), and the phase tag.
func (s *Selector) Active() (Pool, string, string) {
	if s.switched.Load() {
		return s.b, "b", "post-switch"
	}
	return s.a, "a", "pre-switch"
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/loadgen/ -run TestSelector`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/loadgen/dsn.go internal/loadgen/dsn_test.go
git commit -m "feat(loadtool): atomically-swappable DSN selector"
```

---

## Task 4: Schema init (`internal/loadgen/schema.go`)

**Files:**
- Create: `internal/loadgen/schema.go`
- Test: `internal/loadgen/schema_test.go`

- [ ] **Step 1: Write the failing test**

```go
package loadgen

import (
	"context"
	"testing"

	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitSchemaSeedsAccounts(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS events").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS accounts").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("INSERT INTO accounts").
		WithArgs(int64(1000), 100).
		WillReturnResult(pgxmock.NewResult("INSERT", 100))

	require.NoError(t, InitSchema(context.Background(), mock, 100, 1000, false))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInitSchemaResetTruncates(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS events").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS accounts").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("TRUNCATE").WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	mock.ExpectExec("INSERT INTO accounts").
		WithArgs(int64(1000), 100).
		WillReturnResult(pgxmock.NewResult("INSERT", 100))

	require.NoError(t, InitSchema(context.Background(), mock, 100, 1000, true))
	assert.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/loadgen/ -run TestInitSchema`
Expected: FAIL — `undefined: InitSchema`.

- [ ] **Step 3: Write minimal implementation**

```go
package loadgen

import (
	"context"
	"fmt"
)

const ddlEvents = `CREATE TABLE IF NOT EXISTS events (
    id         bigserial PRIMARY KEY,
    writer_id  int        NOT NULL,
    client_seq bigint     NOT NULL,
    batch_id   uuid       NULL,
    payload    text       NOT NULL,
    ts         timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (writer_id, client_seq)
)`

const ddlAccounts = `CREATE TABLE IF NOT EXISTS accounts (
    id      int    PRIMARY KEY,
    balance bigint NOT NULL
)`

// seedAccounts inserts K accounts each with balance B; ON CONFLICT keeps it
// idempotent when accounts already exist.
const seedAccounts = `INSERT INTO accounts (id, balance)
SELECT g, $1 FROM generate_series(1, $2) g
ON CONFLICT (id) DO NOTHING`

// InitSchema creates both tables and seeds K accounts with balance B. When
// reset is true it TRUNCATEs first, so the seed re-applies cleanly.
func InitSchema(ctx context.Context, db Pool, accounts int, balance int64, reset bool) error {
	if _, err := db.Exec(ctx, ddlEvents); err != nil {
		return fmt.Errorf("init events: %w", err)
	}
	if _, err := db.Exec(ctx, ddlAccounts); err != nil {
		return fmt.Errorf("init accounts: %w", err)
	}
	if reset {
		if _, err := db.Exec(ctx, "TRUNCATE events, accounts"); err != nil {
			return fmt.Errorf("init truncate: %w", err)
		}
	}
	if _, err := db.Exec(ctx, seedAccounts, balance, accounts); err != nil {
		return fmt.Errorf("init seed accounts: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/loadgen/ -run TestInitSchema`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/loadgen/schema.go internal/loadgen/schema_test.go
git commit -m "feat(loadtool): schema init and account seeding"
```

---

## Task 5: Error classification (`internal/loadgen/classify.go`)

**Files:**
- Create: `internal/loadgen/classify.go`
- Test: `internal/loadgen/classify_test.go`

A server-side `PgError` means the server processed and rejected the statement →
the transaction did not commit → **failed**. A network-level error on the
decisive call (single-statement `Exec`, or `Commit`) leaves the outcome unknown
→ **indoubt**. `nil` → **acked**. Separately, `errorClass` buckets errors for
the live metrics.

- [ ] **Step 1: Write the failing test**

```go
package loadgen

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestClassifyStatus(t *testing.T) {
	assert.Equal(t, StatusAcked, classifyStatus(nil))

	pgErr := &pgconn.PgError{Code: "25006", Message: "cannot execute INSERT in a read-only transaction"}
	assert.Equal(t, StatusFailed, classifyStatus(pgErr))

	assert.Equal(t, StatusIndoubt, classifyStatus(errors.New("write: connection reset by peer")))
	assert.Equal(t, StatusIndoubt, classifyStatus(context.DeadlineExceeded))
}

func TestErrorClass(t *testing.T) {
	assert.Equal(t, "read-only", errorClass(&pgconn.PgError{Code: "25006"}))
	assert.Equal(t, "conn-refused", errorClass(errors.New("dial tcp: connection refused")))
	assert.Equal(t, "timeout", errorClass(context.DeadlineExceeded))
	assert.Equal(t, "other", errorClass(errors.New("boom")))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/loadgen/ -run 'TestClassify|TestErrorClass'`
Expected: FAIL — `undefined: classifyStatus`.

- [ ] **Step 3: Write minimal implementation**

```go
package loadgen

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// classifyStatus maps the error from the decisive call (single-statement Exec
// or tx Commit) to a three-set status. A PgError is a server response, so the
// statement was rejected and did NOT commit (failed). A network error leaves
// the outcome unknown (indoubt). nil is acked.
func classifyStatus(err error) string {
	if err == nil {
		return StatusAcked
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return StatusFailed
	}
	return StatusIndoubt
}

// errorClass buckets an error for the live (non-authoritative) metrics.
func errorClass(err error) string {
	if err == nil {
		return ""
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "25006" { // read_only_sql_transaction
			return "read-only"
		}
		return "other"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"):
		return "conn-refused"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	default:
		return "other"
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/loadgen/ -run 'TestClassify|TestErrorClass'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/loadgen/classify.go internal/loadgen/classify_test.go
git commit -m "feat(loadtool): three-set status + error-class classification"
```

---

## Task 6: Metrics + unavailability window (`internal/loadgen/metrics.go`)

**Files:**
- Create: `internal/loadgen/metrics.go`
- Test: `internal/loadgen/metrics_test.go`

The unavailability window (case 6) is the interval from the first error observed
after `SIGHUP` to the first successful commit on pool B.

- [ ] **Step 1: Write the failing test**

```go
package loadgen

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUnavailabilityWindow(t *testing.T) {
	m := NewMetrics()
	base := time.Unix(1000, 0)

	m.RecordCommit("append", "a", base)            // pre-switch success, ignored for window
	m.RecordSwitch(base.Add(1 * time.Second))
	m.RecordError("read-only", base.Add(2*time.Second)) // first error after switch
	m.RecordError("read-only", base.Add(3*time.Second)) // later error, does not move start
	m.RecordCommit("append", "b", base.Add(5*time.Second)) // first success on B closes window

	start, end, dur, ok := m.Window()
	assert.True(t, ok)
	assert.Equal(t, base.Add(2*time.Second), start)
	assert.Equal(t, base.Add(5*time.Second), end)
	assert.Equal(t, 3*time.Second, dur)
}

func TestUnavailabilityWindowSeamless(t *testing.T) {
	m := NewMetrics()
	base := time.Unix(2000, 0)
	m.RecordSwitch(base)
	m.RecordCommit("append", "b", base.Add(10*time.Millisecond)) // success, no prior error
	_, _, dur, ok := m.Window()
	assert.True(t, ok)
	assert.Equal(t, time.Duration(0), dur)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/loadgen/ -run TestUnavailability`
Expected: FAIL — `undefined: NewMetrics`.

- [ ] **Step 3: Write minimal implementation**

```go
package loadgen

import (
	"sync"
	"time"
)

// Metrics aggregates non-authoritative live observations: per-workload commit
// counts, per-class error counts, and the unavailability window around the swap.
type Metrics struct {
	mu        sync.Mutex
	commits   map[string]int64
	errors    map[string]int64
	switched  bool
	switchAt  time.Time
	firstErr  time.Time // first error after switch
	firstOKB  time.Time // first success on pool B after switch
}

func NewMetrics() *Metrics {
	return &Metrics{commits: map[string]int64{}, errors: map[string]int64{}}
}

func (m *Metrics) RecordSwitch(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.switched = true
	m.switchAt = t
}

func (m *Metrics) RecordCommit(kind, dsn string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commits[kind]++
	if m.switched && dsn == "b" && m.firstOKB.IsZero() {
		m.firstOKB = t
	}
}

func (m *Metrics) RecordError(class string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors[class]++
	if m.switched && m.firstErr.IsZero() {
		m.firstErr = t
	}
}

// Window returns the unavailability interval. ok is false until the first
// post-switch success on B is seen. If no error preceded that success the
// window is zero-length (seamless swap).
func (m *Metrics) Window() (start, end time.Time, dur time.Duration, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.firstOKB.IsZero() {
		return time.Time{}, time.Time{}, 0, false
	}
	end = m.firstOKB
	start = m.firstErr
	if start.IsZero() || start.After(end) {
		start = end // no error window: seamless
	}
	return start, end, end.Sub(start), true
}

// Snapshot returns copies of the counters for rendering.
func (m *Metrics) Snapshot() (commits, errors map[string]int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	commits, errors = map[string]int64{}, map[string]int64{}
	for k, v := range m.commits {
		commits[k] = v
	}
	for k, v := range m.errors {
		errors[k] = v
	}
	return commits, errors
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/loadgen/ -run TestUnavailability`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/loadgen/metrics.go internal/loadgen/metrics_test.go
git commit -m "feat(loadtool): live metrics and unavailability window"
```

---

## Task 7: Workers (`internal/loadgen/workers.go`)

**Files:**
- Create: `internal/loadgen/workers.go`
- Test: `internal/loadgen/workers_test.go`

This task adds the `google/uuid` dependency. Each worker exposes a single-op
function so it unit-tests against `pgxmock`; the run loop (Task 8) calls them
repeatedly. `WriterID`/`client_seq` are owned by the run loop and passed in.

- [ ] **Step 1: Add dependency**

Run: `go get github.com/google/uuid@latest`
Expected: `go.mod` gains `github.com/google/uuid`.

- [ ] **Step 2: Write the failing test**

```go
package loadgen

import (
	"bytes"
	"context"
	"testing"

	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestWriter(t *testing.T) *Writer {
	t.Helper()
	w, err := NewWriter(t.TempDir()+"/log.jsonl", &bytes.Buffer{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestAppendOnceAcked(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectExec("INSERT INTO events").
		WithArgs(7, int64(100), "hello").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	w := newTestWriter(t)
	status, class := appendOnce(context.Background(), mock, w, "a", "pre-switch", 7, 100, "hello")
	assert.Equal(t, StatusAcked, status)
	assert.Empty(t, class)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAppendOnceFailedOnPgError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectExec("INSERT INTO events").
		WillReturnError(&pgconnPgError{Code: "25006"}) // read-only

	w := newTestWriter(t)
	status, class := appendOnce(context.Background(), mock, w, "a", "pre-switch", 7, 100, "x")
	assert.Equal(t, StatusFailed, status)
	assert.Equal(t, "read-only", class)
}

func TestTransferOnceCommits(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE accounts SET balance = balance -").WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec("UPDATE accounts SET balance = balance +").WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	status, _ := transferOnce(context.Background(), mock, 1, 2, 5)
	assert.Equal(t, StatusAcked, status)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLongTxnOnceInsertsBatch(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO events").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("INSERT INTO events").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	w := newTestWriter(t)
	status, _ := longTxnOnce(context.Background(), mock, w, "a", "pre-switch", 3, 200, 2, 0)
	assert.Equal(t, StatusAcked, status)
	assert.NoError(t, mock.ExpectationsWereMet())
}
```

Add the tiny alias used above (a `*pgconn.PgError`) near the top of the test file:

```go
import "github.com/jackc/pgx/v5/pgconn"

type pgconnPgError = pgconn.PgError
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/loadgen/ -run 'TestAppendOnce|TestTransferOnce|TestLongTxnOnce'`
Expected: FAIL — `undefined: appendOnce`.

- [ ] **Step 4: Write minimal implementation**

```go
package loadgen

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const insertEvent = `INSERT INTO events (writer_id, client_seq, batch_id, payload) VALUES ($1, $2, $3, $4)`
const insertEventNoBatch = `INSERT INTO events (writer_id, client_seq, payload) VALUES ($1, $2, $3)`

// appendOnce inserts one event row in autocommit and logs the op. Returns the
// resolved status and the error class ("" when acked) for live metrics.
func appendOnce(ctx context.Context, db Pool, w *Writer, dsn, phase string, writerID int, seq int64, payload string) (string, string) {
	opID := uuid.NewString()
	_ = w.WriteAttempt(Record{
		OpID: opID, Kind: "append", WriterID: writerID, ClientSeq: seq, Rows: 1,
		DSN: dsn, Phase: phase,
	})
	_, err := db.Exec(ctx, insertEventNoBatch, writerID, seq, payload)
	status := classifyStatus(err)
	_ = w.WriteResult(opID, status, errMsg(err))
	return status, errorClass(err)
}

// rywOnce does an append, then reads back this writer's max client_seq from the
// active pool. The read result is observation only (a stale read is lag, not a
// verdict). Returns status, error class, and the observed max client_seq.
func rywOnce(ctx context.Context, db Pool, w *Writer, dsn, phase string, writerID int, seq int64) (string, string, int64) {
	status, class := appendOnce(ctx, db, w, dsn, phase, writerID, seq, "ryw")
	var maxSeq int64 = -1
	_ = db.QueryRow(ctx, `SELECT coalesce(max(client_seq), -1) FROM events WHERE writer_id = $1`, writerID).Scan(&maxSeq)
	return status, class, maxSeq
}

// transferOnce moves `amount` between two accounts in one transaction. Not
// logged — verified by the SUM(balance) invariant. Status is resolved from the
// decisive Commit call; intermediate statement errors mean a rolled-back txn.
func transferOnce(ctx context.Context, db Pool, from, to, amount int) (string, string) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return classifyStatus(err), errorClass(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance - $1 WHERE id = $2`, amount, from); err != nil {
		_ = tx.Rollback(ctx)
		return StatusFailed, errorClass(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance + $1 WHERE id = $2`, amount, to); err != nil {
		_ = tx.Rollback(ctx)
		return StatusFailed, errorClass(err)
	}
	commitErr := tx.Commit(ctx)
	return classifyStatus(commitErr), errorClass(commitErr)
}

// longTxnOnce inserts `rows` event rows sharing a batch_id in one slow
// transaction, occupying the client_seq range [startSeq, startSeq+rows). The
// hold duration makes commits straddle the isolation point.
func longTxnOnce(ctx context.Context, db Pool, w *Writer, dsn, phase string, writerID int, startSeq, rows int64, hold time.Duration) (string, string) {
	opID := uuid.NewString()
	batchID := uuid.NewString()
	_ = w.WriteAttempt(Record{
		OpID: opID, Kind: "long-txn", WriterID: writerID, ClientSeq: startSeq, Rows: rows,
		BatchID: batchID, DSN: dsn, Phase: phase,
	})
	tx, err := db.Begin(ctx)
	if err != nil {
		status := classifyStatus(err)
		_ = w.WriteResult(opID, status, errMsg(err))
		return status, errorClass(err)
	}
	for i := int64(0); i < rows; i++ {
		if _, err := tx.Exec(ctx, insertEvent, writerID, startSeq+i, batchID, "long"); err != nil {
			_ = tx.Rollback(ctx)
			_ = w.WriteResult(opID, StatusFailed, errMsg(err))
			return StatusFailed, errorClass(err)
		}
	}
	if hold > 0 {
		select {
		case <-time.After(hold):
		case <-ctx.Done():
		}
	}
	commitErr := tx.Commit(ctx)
	status := classifyStatus(commitErr)
	_ = w.WriteResult(opID, status, errMsg(commitErr))
	return status, errorClass(commitErr)
}

func errMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/loadgen/ -run 'TestAppendOnce|TestTransferOnce|TestLongTxnOnce'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/loadgen/workers.go internal/loadgen/workers_test.go go.mod go.sum
git commit -m "feat(loadtool): append/ryw/transfer/long-txn worker ops"
```

---

## Task 8: Runner (`internal/loadgen/runner.go`)

**Files:**
- Create: `internal/loadgen/runner.go`
- Test: `internal/loadgen/runner_test.go`

The Runner wires pools → Selector → workers → Writer → Metrics, installs the
`SIGHUP` swap and shutdown, and runs each worker's loop until ctx is done. The
loop logic (seq increment, rate limiting, Active() per op, metric recording) is
the unit under test; we drive it with a mock pool and a cancellable context.

- [ ] **Step 1: Write the failing test**

```go
package loadgen

import (
	"context"
	"testing"
	"time"

	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendLoopRunsAndIncrementsSeq(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	// allow any number of inserts
	for i := 0; i < 100; i++ {
		mock.ExpectExec("INSERT INTO events").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	}

	sel := NewSelector(mock, mock)
	w := newTestWriter(t)
	m := NewMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()

	n := appendLoop(ctx, sel, w, m, 1, 0) // writerID 1, unthrottled
	assert.Positive(t, n)                  // at least one op happened
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/loadgen/ -run TestAppendLoop`
Expected: FAIL — `undefined: appendLoop`.

- [ ] **Step 3: Write minimal implementation**

```go
package loadgen

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/loadcfg"
)

// throttle returns a tick channel for the given rate (ops/sec); 0 = no throttle.
func throttle(rate int) (<-chan time.Time, func()) {
	if rate <= 0 {
		ch := make(chan time.Time)
		close(ch) // always ready
		return ch, func() {}
	}
	tk := time.NewTicker(time.Second / time.Duration(rate))
	return tk.C, tk.Stop
}

// appendLoop runs append ops until ctx is cancelled, returning the op count.
func appendLoop(ctx context.Context, sel *Selector, w *Writer, m *Metrics, writerID, rate int) int64 {
	tick, stop := throttle(rate)
	defer stop()
	var seq, count int64
	for {
		select {
		case <-ctx.Done():
			return count
		default:
		}
		if rate > 0 {
			select {
			case <-ctx.Done():
				return count
			case <-tick:
			}
		}
		pool, dsn, phase := sel.Active()
		seq++
		status, class := appendOnce(ctx, pool, w, dsn, phase, writerID, seq, "append")
		recordOutcome(m, "append", dsn, status, class)
		count++
	}
}

func rywLoop(ctx context.Context, sel *Selector, w *Writer, m *Metrics, writerID, rate int) int64 {
	tick, stop := throttle(rate)
	defer stop()
	var seq, count int64
	for {
		select {
		case <-ctx.Done():
			return count
		default:
		}
		if rate > 0 {
			select {
			case <-ctx.Done():
				return count
			case <-tick:
			}
		}
		pool, dsn, phase := sel.Active()
		seq++
		status, class, _ := rywOnce(ctx, pool, w, dsn, phase, writerID, seq)
		recordOutcome(m, "ryw", dsn, status, class)
		count++
	}
}

func transferLoop(ctx context.Context, sel *Selector, m *Metrics, accounts, rate int) int64 {
	tick, stop := throttle(rate)
	defer stop()
	var count int64
	for {
		select {
		case <-ctx.Done():
			return count
		default:
		}
		if rate > 0 {
			select {
			case <-ctx.Done():
				return count
			case <-tick:
			}
		}
		pool, dsn, _ := sel.Active()
		from := 1 + int(count)%accounts
		to := 1 + int(count+1)%accounts
		if from == to {
			to = 1 + (to % accounts)
		}
		status, class := transferOnce(ctx, pool, from, to, 1)
		recordOutcome(m, "transfer", dsn, status, class)
		count++
	}
}

func longTxnLoop(ctx context.Context, sel *Selector, w *Writer, m *Metrics, writerID int, batchSize int, hold time.Duration) int64 {
	var startSeq, count int64
	for {
		select {
		case <-ctx.Done():
			return count
		default:
		}
		pool, dsn, phase := sel.Active()
		status, class := longTxnOnce(ctx, pool, w, dsn, phase, writerID, startSeq+1, int64(batchSize), hold)
		recordOutcome(m, "long-txn", dsn, status, class)
		startSeq += int64(batchSize)
		count++
	}
}

// recordOutcome feeds a finished op into the live metrics. class is the error
// bucket ("" for acked) computed by the worker via errorClass.
func recordOutcome(m *Metrics, kind, dsn, status, class string) {
	now := time.Now()
	if status == StatusAcked {
		m.RecordCommit(kind, dsn, now)
		return
	}
	m.RecordError(class, now)
}

// reportLoop periodically prints a non-authoritative live snapshot to stdout.
func reportLoop(ctx context.Context, m *Metrics, every time.Duration) {
	tk := time.NewTicker(every)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			commits, errs := m.Snapshot()
			fmt.Fprintf(os.Stdout, "-- live: commits=%v errors=%v --\n", commits, errs)
		}
	}
}

// Run executes the full workload: opens pools, starts all workers, installs the
// SIGHUP swap and shutdown, and blocks until ctx is done or duration elapses.
func Run(ctx context.Context, cfg loadcfg.Config, openPool func(ctx context.Context, dsn string) (Pool, func(), error)) (*Metrics, error) {
	poolA, closeA, err := openPool(ctx, cfg.DSNA)
	if err != nil {
		return nil, fmt.Errorf("run open dsn_a: %w", err)
	}
	defer closeA()
	poolB, closeB, err := openPool(ctx, cfg.DSNB)
	if err != nil {
		return nil, fmt.Errorf("run open dsn_b: %w", err)
	}
	defer closeB()

	sel := NewSelector(poolA, poolB)
	w, err := NewWriter(cfg.IntentLog, os.Stdout)
	if err != nil {
		return nil, err
	}
	defer w.Close()
	m := NewMetrics()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if cfg.Duration > 0 {
		t := time.AfterFunc(time.Duration(cfg.Duration), cancel)
		defer t.Stop()
	}

	// SIGHUP -> swap; SIGINT/SIGTERM -> shutdown.
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		for s := range sigCh {
			if s == syscall.SIGHUP {
				sel.Switch()
				m.RecordSwitch(time.Now())
				fmt.Fprintln(os.Stdout, "== SIGHUP: switched DSN A -> B ==")
			} else {
				cancel()
				return
			}
		}
	}()

	// Live throughput/error snapshot every 5s (observation, not a verdict).
	go reportLoop(runCtx, m, 5*time.Second)

	var wg sync.WaitGroup
	start := func(fn func()) { wg.Add(1); go func() { defer wg.Done(); fn() }() }

	for i := 0; i < cfg.Workers.Append.Count; i++ {
		id := 1000 + i
		start(func() { appendLoop(runCtx, sel, w, m, id, cfg.Workers.Append.Rate) })
	}
	for i := 0; i < cfg.Workers.RYW.Count; i++ {
		id := 2000 + i
		start(func() { rywLoop(runCtx, sel, w, m, id, cfg.Workers.RYW.Rate) })
	}
	for i := 0; i < cfg.Workers.Transfer.Count; i++ {
		start(func() { transferLoop(runCtx, sel, m, cfg.Accounts, cfg.Workers.Transfer.Rate) })
	}
	for i := 0; i < cfg.Workers.LongTxn.Count; i++ {
		id := 3000 + i
		start(func() {
			longTxnLoop(runCtx, sel, w, m, id, cfg.Workers.LongTxn.BatchSize, time.Duration(cfg.Workers.LongTxn.TxnDuration))
		})
	}

	wg.Wait()
	return m, nil
}
```

> Note: long-txn writers each own a disjoint `writer_id` (3000+i) and grow their
> own `client_seq`, so the unique `(writer_id, client_seq)` constraint never
> collides across workers. Append (1000+i), ryw (2000+i), long-txn (3000+i) use
> disjoint writer_id ranges by construction.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/loadgen/ -run TestAppendLoop`
Expected: PASS.

- [ ] **Step 5: Run the whole loadgen package**

Run: `go test ./internal/loadgen/`
Expected: PASS (all loadgen tests).

- [ ] **Step 6: Commit**

```bash
git add internal/loadgen/runner.go internal/loadgen/runner_test.go
git commit -m "feat(loadtool): runner — worker loops, SIGHUP swap, lifecycle"
```

---

## Task 9: Intent-log Reader (`internal/oracle/intentlog.go`)

**Files:**
- Create: `internal/oracle/intentlog.go`
- Test: `internal/oracle/intentlog_test.go`

Pairs `attempt` + result records by `op_id`. An attempt with no result is
treated as **indoubt** (the process may have died around COMMIT).

- [ ] **Step 1: Write the failing test**

```go
package oracle

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeLog(t *testing.T, lines string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "log.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(lines), 0o644))
	return path
}

func TestReadLogPairsRecords(t *testing.T) {
	path := writeLog(t, `{"op_id":"o1","kind":"append","writer_id":7,"client_seq":5,"rows":1,"status":"attempt","ts":"2026-06-02T00:00:00Z"}
{"op_id":"o1","status":"acked","ts":"2026-06-02T00:00:01Z"}
{"op_id":"o2","kind":"long-txn","writer_id":3,"client_seq":10,"rows":2,"status":"attempt","ts":"2026-06-02T00:00:02Z"}
{"op_id":"o2","status":"failed","ts":"2026-06-02T00:00:03Z"}
{"op_id":"o3","kind":"append","writer_id":7,"client_seq":6,"rows":1,"status":"attempt","ts":"2026-06-02T00:00:04Z"}
`)
	ops, err := ReadLog(path)
	require.NoError(t, err)
	require.Len(t, ops, 3)

	byID := map[string]Op{}
	for _, op := range ops {
		byID[op.OpID] = op
	}
	assert.Equal(t, "acked", byID["o1"].Status)
	assert.Equal(t, int64(1), byID["o1"].Rows)
	assert.Equal(t, "failed", byID["o2"].Status)
	assert.Equal(t, int64(2), byID["o2"].Rows)
	assert.Equal(t, "indoubt", byID["o3"].Status) // attempt with no result
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/oracle/ -run TestReadLog`
Expected: FAIL — package has no Go files / `undefined: ReadLog`.

- [ ] **Step 3: Write minimal implementation**

```go
// Package oracle is the authoritative verdict layer: it reads the durable
// intent-log and the database at rest and reconciles them. Nothing here reads
// live during the workload.
package oracle

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// Op is a resolved operation: attempt identity + terminal status.
type Op struct {
	OpID      string
	Kind      string
	WriterID  int
	ClientSeq int64
	Rows      int64
	BatchID   string
	Status    string // acked / failed / indoubt
}

type rawRecord struct {
	OpID      string `json:"op_id"`
	Kind      string `json:"kind"`
	WriterID  int    `json:"writer_id"`
	ClientSeq int64  `json:"client_seq"`
	Rows      int64  `json:"rows"`
	BatchID   string `json:"batch_id"`
	Status    string `json:"status"`
}

// ReadLog parses the JSONL intent-log and pairs attempt/result by op_id. An
// attempt with no terminal result is classified indoubt.
func ReadLog(path string) ([]Op, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("oracle open log: %w", err)
	}
	defer f.Close()

	ops := map[string]*Op{}
	order := []string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var r rawRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("oracle parse line: %w", err)
		}
		if r.Status == "attempt" {
			if _, ok := ops[r.OpID]; !ok {
				order = append(order, r.OpID)
			}
			ops[r.OpID] = &Op{
				OpID: r.OpID, Kind: r.Kind, WriterID: r.WriterID,
				ClientSeq: r.ClientSeq, Rows: max64(r.Rows, 1), BatchID: r.BatchID,
				Status: "indoubt", // until a result arrives
			}
			continue
		}
		if op, ok := ops[r.OpID]; ok {
			op.Status = r.Status
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("oracle scan: %w", err)
	}

	out := make([]Op, 0, len(order))
	for _, id := range order {
		out = append(out, *ops[id])
	}
	return out, nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/oracle/ -run TestReadLog`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/oracle/intentlog.go internal/oracle/intentlog_test.go
git commit -m "feat(loadtool): oracle intent-log reader with attempt/result pairing"
```

---

## Task 10: Reconciliation (`internal/oracle/reconcile.go`)

**Files:**
- Create: `internal/oracle/reconcile.go`
- Test: `internal/oracle/reconcile_test.go`

`Reconcile` loads the `(writer_id, client_seq) -> count` membership map plus the
sequence/sum aggregates, then classifies every op.

- [ ] **Step 1: Write the failing test**

```go
package oracle

import (
	"context"
	"testing"

	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func expectAggregates(mock pgxmock.PgxPoolIface, dupID int64, lastVal, maxID, sum int64, accounts int64) {
	mock.ExpectQuery("FROM .*HAVING count").
		WillReturnRows(pgxmock.NewRows([]string{"c"}).AddRow(dupID))
	mock.ExpectQuery("last_value FROM events_id_seq").
		WillReturnRows(pgxmock.NewRows([]string{"last_value"}).AddRow(lastVal))
	mock.ExpectQuery("coalesce\\(max\\(id\\),0\\)").
		WillReturnRows(pgxmock.NewRows([]string{"m"}).AddRow(maxID))
	mock.ExpectQuery("FROM accounts").
		WillReturnRows(pgxmock.NewRows([]string{"sum", "cnt"}).AddRow(sum, accounts))
}

func TestReconcileDetectsLost(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// membership: writer 7 seq 5 present once; seq 6 absent.
	mock.ExpectQuery("GROUP BY writer_id, client_seq").
		WillReturnRows(pgxmock.NewRows([]string{"writer_id", "client_seq", "c"}).
			AddRow(7, int64(5), int64(1)))
	expectAggregates(mock, 0, 6, 5, 100000, 100)

	ops := []Op{
		{OpID: "o1", WriterID: 7, ClientSeq: 5, Rows: 1, Status: "acked"},
		{OpID: "o2", WriterID: 7, ClientSeq: 6, Rows: 1, Status: "acked"}, // LOST
	}
	f, err := Reconcile(context.Background(), mock, ops, 1000)
	require.NoError(t, err)
	assert.Len(t, f.Lost, 1)
	assert.Equal(t, "o2", f.Lost[0])
	assert.Empty(t, f.Dup)
	assert.Empty(t, f.Phantom)
	assert.True(t, f.Failed()) // a LOST finding must fail the run
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/oracle/ -run TestReconcile`
Expected: FAIL — `undefined: Reconcile`.

- [ ] **Step 3: Write minimal implementation**

```go
package oracle

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Querier is the read subset used at rest. *pgxpool.Pool and pgxmock satisfy it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Findings collects every reconciliation result. Empty == PASS.
type Findings struct {
	Lost         []string // op_id: acked but missing in DB
	Dup          []string // op_id: present more than once
	Phantom      []string // op_id: failed but present in DB
	Partial      []string // op_id: in-doubt batch only partially present
	DupID        int64    // duplicate events.id rows
	SeqBehind    bool     // last_value < max(id)
	LastValue    int64
	MaxID        int64
	SumExpected  int64
	SumActual    int64
}

// Failed reports whether any finding fails the run.
func (f *Findings) Failed() bool {
	return len(f.Lost) > 0 || len(f.Dup) > 0 || len(f.Phantom) > 0 ||
		len(f.Partial) > 0 || f.DupID > 0 || f.SeqBehind ||
		f.SumExpected != f.SumActual
}

type key struct {
	writer int
	seq    int64
}

// Reconcile compares the resolved ops against the database at rest.
func Reconcile(ctx context.Context, db Querier, ops []Op, balance int64) (*Findings, error) {
	present, err := loadMembership(ctx, db)
	if err != nil {
		return nil, err
	}

	f := &Findings{}
	for _, op := range ops {
		var seen, missing int64
		for i := int64(0); i < op.Rows; i++ {
			c := present[key{op.WriterID, op.ClientSeq + i}]
			if c > 1 {
				f.Dup = append(f.Dup, op.OpID)
			}
			if c >= 1 {
				seen++
			} else {
				missing++
			}
		}
		switch op.Status {
		case "acked":
			if missing > 0 {
				f.Lost = append(f.Lost, op.OpID)
			}
		case "failed":
			if seen > 0 {
				f.Phantom = append(f.Phantom, op.OpID)
			}
		case "indoubt":
			if seen > 0 && missing > 0 { // partial batch is never acceptable
				f.Partial = append(f.Partial, op.OpID)
			}
		}
	}

	if err := loadAggregates(ctx, db, f, balance); err != nil {
		return nil, err
	}
	return f, nil
}

func loadMembership(ctx context.Context, db Querier) (map[key]int64, error) {
	rows, err := db.Query(ctx, `SELECT writer_id, client_seq, count(*) FROM events GROUP BY writer_id, client_seq`)
	if err != nil {
		return nil, fmt.Errorf("reconcile membership: %w", err)
	}
	defer rows.Close()
	m := map[key]int64{}
	for rows.Next() {
		var w int
		var seq, c int64
		if err := rows.Scan(&w, &seq, &c); err != nil {
			return nil, err
		}
		m[key{w, seq}] = c
	}
	return m, rows.Err()
}

func loadAggregates(ctx context.Context, db Querier, f *Findings, balance int64) error {
	if err := db.QueryRow(ctx,
		`SELECT count(*) FROM (SELECT id FROM events GROUP BY id HAVING count(*) > 1) d`,
	).Scan(&f.DupID); err != nil {
		return fmt.Errorf("reconcile dup id: %w", err)
	}
	if err := db.QueryRow(ctx, `SELECT last_value FROM events_id_seq`).Scan(&f.LastValue); err != nil {
		return fmt.Errorf("reconcile seq: %w", err)
	}
	if err := db.QueryRow(ctx, `SELECT coalesce(max(id),0) FROM events`).Scan(&f.MaxID); err != nil {
		return fmt.Errorf("reconcile max id: %w", err)
	}
	f.SeqBehind = f.LastValue < f.MaxID

	var accounts int64
	if err := db.QueryRow(ctx,
		`SELECT coalesce(sum(balance),0), count(*) FROM accounts`,
	).Scan(&f.SumActual, &accounts); err != nil {
		return fmt.Errorf("reconcile sum: %w", err)
	}
	f.SumExpected = accounts * balance
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/oracle/ -run TestReconcile`
Expected: PASS.

- [ ] **Step 5: Add a second test — phantom + dup id + seq-behind**

```go
func TestReconcileDetectsPhantomAndSeq(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("GROUP BY writer_id, client_seq").
		WillReturnRows(pgxmock.NewRows([]string{"writer_id", "client_seq", "c"}).
			AddRow(7, int64(5), int64(1)))
	// dup id = 2, last_value 4 < max id 5 -> seq behind, sum mismatch
	expectAggregates(mock, 2, 4, 5, 99000, 100)

	ops := []Op{{OpID: "f1", WriterID: 7, ClientSeq: 5, Rows: 1, Status: "failed"}} // present -> phantom
	f, err := Reconcile(context.Background(), mock, ops, 1000)
	require.NoError(t, err)
	assert.Equal(t, []string{"f1"}, f.Phantom)
	assert.Equal(t, int64(2), f.DupID)
	assert.True(t, f.SeqBehind)
	assert.True(t, f.Failed())
}
```

Run: `go test ./internal/oracle/ -run TestReconcile`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/oracle/reconcile.go internal/oracle/reconcile_test.go
git commit -m "feat(loadtool): oracle reconciliation — membership/sequence/sum"
```

---

## Task 11: Report (`internal/oracle/report.go`)

**Files:**
- Create: `internal/oracle/report.go`
- Test: `internal/oracle/report_test.go`

- [ ] **Step 1: Write the failing test**

```go
package oracle

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderPass(t *testing.T) {
	var b bytes.Buffer
	f := &Findings{SumExpected: 100000, SumActual: 100000, LastValue: 50, MaxID: 50}
	require.NoError(t, Render(&b, f))
	assert.Contains(t, b.String(), "PASS")
}

func TestRenderFailListsFindings(t *testing.T) {
	var b bytes.Buffer
	f := &Findings{Lost: []string{"o2"}, DupID: 1, SumExpected: 100000, SumActual: 99000}
	require.NoError(t, Render(&b, f))
	out := b.String()
	assert.Contains(t, out, "FAIL")
	assert.Contains(t, out, "LOST")
	assert.Contains(t, out, "o2")
	assert.Contains(t, out, "duplicate id")
	assert.Contains(t, out, "sum mismatch")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/oracle/ -run TestRender`
Expected: FAIL — `undefined: Render`.

- [ ] **Step 3: Write minimal implementation**

```go
package oracle

import (
	"fmt"
	"io"
)

// Render writes a human-readable verdict to w and returns nil. The verdict line
// is PASS when f has no findings, FAIL otherwise.
func Render(w io.Writer, f *Findings) error {
	p := func(format string, a ...any) { fmt.Fprintf(w, format+"\n", a...) }

	p("=== loadtool verify ===")
	p("events: %d LOST, %d DUP, %d PHANTOM, %d PARTIAL", len(f.Lost), len(f.Dup), len(f.Phantom), len(f.Partial))
	if len(f.Lost) > 0 {
		p("  LOST ops: %v", f.Lost)
	}
	if len(f.Dup) > 0 {
		p("  DUP ops: %v", f.Dup)
	}
	if len(f.Phantom) > 0 {
		p("  PHANTOM ops: %v", f.Phantom)
	}
	if len(f.Partial) > 0 {
		p("  PARTIAL ops: %v", f.Partial)
	}
	p("sequence: %d duplicate id; last_value=%d max(id)=%d%s",
		f.DupID, f.LastValue, f.MaxID, ifThen(f.SeqBehind, "  <-- SEQUENCE BEHIND", ""))
	p("accounts: sum expected=%d actual=%d%s",
		f.SumExpected, f.SumActual, ifThen(f.SumExpected != f.SumActual, "  <-- sum mismatch", ""))

	if f.Failed() {
		p("VERDICT: FAIL")
	} else {
		p("VERDICT: PASS")
	}
	return nil
}

func ifThen(cond bool, yes, no string) string {
	if cond {
		return yes
	}
	return no
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/oracle/`
Expected: PASS (all oracle tests).

- [ ] **Step 5: Commit**

```bash
git add internal/oracle/report.go internal/oracle/report_test.go
git commit -m "feat(loadtool): oracle report rendering with PASS/FAIL verdict"
```

---

## Task 12: CLI (`cmd/loadtool/main.go`)

**Files:**
- Create: `cmd/loadtool/main.go`

Wires the three subcommands. `openPool` adapts `pgxpool` to the `loadgen.Pool`
interface. No new unit tests (thin glue); verified by build + `--help` smoke.

- [ ] **Step 1: Write the implementation**

```go
// Command loadtool drives a verifiable workload against a Patroni cluster during
// an online upgrade, simulates the in-flight DSN swap (SIGHUP), and renders a
// consistency verdict from a durable intent-log.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/dmbabuev/pg-upgrade/internal/loadcfg"
	"github.com/dmbabuev/pg-upgrade/internal/loadgen"
	"github.com/dmbabuev/pg-upgrade/internal/oracle"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "loadtool",
		Short: "Load + correctness harness for pg-upgrade",
	}
	root.AddCommand(initCmd(), runCmd(), verifyCmd())
	return root
}

// loadConfig layers an optional YAML file under the defaults.
func loadConfig(path string) (loadcfg.Config, error) {
	if path == "" {
		return loadcfg.Default(), nil
	}
	return loadcfg.Load(path)
}

// openPool adapts pgxpool to loadgen.Pool and returns a closer.
func openPool(ctx context.Context, dsn string) (loadgen.Pool, func(), error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	return pool, pool.Close, nil
}

func initCmd() *cobra.Command {
	var cfgPath, dsnA string
	var accounts int
	var balance int64
	var reset bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create schema and seed accounts",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(cfgPath)
			if err != nil {
				return err
			}
			if dsnA != "" {
				cfg.DSNA = dsnA
			}
			if accounts > 0 {
				cfg.Accounts = accounts
			}
			if balance > 0 {
				cfg.Balance = balance
			}
			if cfg.DSNA == "" {
				return fmt.Errorf("init: --dsn-a (or config dsn_a) is required")
			}
			ctx := cmd.Context()
			pool, closeFn, err := openPool(ctx, cfg.DSNA)
			if err != nil {
				return err
			}
			defer closeFn()
			if err := loadgen.InitSchema(ctx, pool, cfg.Accounts, cfg.Balance, reset); err != nil {
				return err
			}
			fmt.Printf("init: %d accounts seeded with balance %d\n", cfg.Accounts, cfg.Balance)
			return nil
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "YAML config path")
	cmd.Flags().StringVar(&dsnA, "dsn-a", "", "old primary DSN")
	cmd.Flags().IntVar(&accounts, "accounts", 0, "number of accounts (K)")
	cmd.Flags().Int64Var(&balance, "balance", 0, "per-account balance (B)")
	cmd.Flags().BoolVar(&reset, "reset", false, "TRUNCATE before seeding")
	return cmd
}

func runCmd() *cobra.Command {
	var cfgPath, dsnA, dsnB, intentLog string
	var duration time.Duration
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Generate load; SIGHUP swaps DSN A->B",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(cfgPath)
			if err != nil {
				return err
			}
			if dsnA != "" {
				cfg.DSNA = dsnA
			}
			if dsnB != "" {
				cfg.DSNB = dsnB
			}
			if intentLog != "" {
				cfg.IntentLog = intentLog
			}
			if duration > 0 {
				cfg.Duration = loadcfg.Duration(duration)
			}
			if err := cfg.ValidateRun(); err != nil {
				return err
			}
			m, err := loadgen.Run(cmd.Context(), cfg, openPool)
			if err != nil {
				return err
			}
			commits, errs := m.Snapshot()
			fmt.Printf("run complete. commits=%v errors=%v\n", commits, errs)
			if _, _, dur, ok := m.Window(); ok {
				fmt.Printf("unavailability window: %s\n", dur)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "YAML config path")
	cmd.Flags().StringVar(&dsnA, "dsn-a", "", "old primary DSN")
	cmd.Flags().StringVar(&dsnB, "dsn-b", "", "new primary DSN")
	cmd.Flags().StringVar(&intentLog, "intent-log", "", "intent-log path")
	cmd.Flags().DurationVar(&duration, "duration", 0, "run duration (0 = until SIGINT)")
	return cmd
}

func verifyCmd() *cobra.Command {
	var cfgPath, dsnB, intentLog string
	var balance int64
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Reconcile intent-log against DB and print verdict",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(cfgPath)
			if err != nil {
				return err
			}
			if dsnB != "" {
				cfg.DSNB = dsnB
			}
			if intentLog != "" {
				cfg.IntentLog = intentLog
			}
			if balance > 0 {
				cfg.Balance = balance
			}
			if cfg.DSNB == "" {
				return fmt.Errorf("verify: --dsn-b (or config dsn_b) is required")
			}
			ops, err := oracle.ReadLog(cfg.IntentLog)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			pool, err := pgxpool.New(ctx, cfg.DSNB)
			if err != nil {
				return err
			}
			defer pool.Close()
			f, err := oracle.Reconcile(ctx, pool, ops, cfg.Balance)
			if err != nil {
				return err
			}
			if err := oracle.Render(os.Stdout, f); err != nil {
				return err
			}
			if f.Failed() {
				os.Exit(2)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "YAML config path")
	cmd.Flags().StringVar(&dsnB, "dsn-b", "", "new primary DSN")
	cmd.Flags().StringVar(&intentLog, "intent-log", "", "intent-log path")
	cmd.Flags().Int64Var(&balance, "balance", 0, "per-account balance (B), must match init")
	return cmd
}
```

- [ ] **Step 2: Build the binary**

Run: `go build ./cmd/loadtool/`
Expected: builds clean, no output.

- [ ] **Step 3: Smoke-test the CLI**

Run: `go run ./cmd/loadtool/ --help`
Expected: shows `init`, `run`, `verify` subcommands.

Run: `go run ./cmd/loadtool/ run` (no DSNs)
Expected: `error: loadcfg: dsn_a is required`.

- [ ] **Step 4: Full build + vet + test sweep**

Run: `go build ./... && go vet ./... && gofmt -l . && go test ./...`
Expected: build clean; vet clean; `gofmt -l .` prints nothing; all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/loadtool/main.go
git commit -m "feat(loadtool): CLI wiring for init/run/verify"
```

---

## Final verification

- [ ] All packages build: `go build ./...`
- [ ] All tests pass: `go test ./...`
- [ ] No vet/format issues: `go vet ./... && gofmt -l .`
- [ ] `loadtool --help` lists init/run/verify
- [ ] Spec requirements covered: three-set classification (Tasks 5, 9, 10), set-membership loss detection (Task 10), sequence dup/advance (Task 10), sum invariant for cases 4+5 (Task 10), long-txn batch wholeness via client_seq range (Tasks 7, 10), durable intent-log with flush-before-commit (Task 2), single serialized writer (Task 2), SIGHUP swap (Tasks 3, 8), unavailability window (Task 6), transfer not logged (Task 7).
