# pg-upgrade Plan 2: FSM Engine + Phases 1-4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `pg-upgrade run` orchestrator through the point of no return — the FSM Runner engine plus phases 1-4 (Prepare, Isolate, Drain, Upgrade) — driven from the existing foundation.

**Architecture:** A `Runner` FSM executes ordered, idempotent `Step`s per `Phase`, persists progress via `state.Manager`, and pauses for operator confirmation at phase boundaries (no-op in Headless mode). Phases live in `internal/phases`. Side effects that need a real cluster (pg_ctl, pg_upgrade, Patroni REST, file writes) sit behind thin wrappers; the FSM, transition logic, idempotency `Check()`s, and SQL steps are unit-tested with fakes/pgxmock/in-memory state. Phase 4 (Upgrade) terminates the run at the point of no return; phases 5-8 follow in Plan 3.

**Tech Stack:** Go 1.25, pgx/v5, pglogrepl, cobra, yaml.v3, testify, pgxmock/v3. Module `github.com/dmbabuev/pg-upgrade`.

**Spec:** `docs/superpowers/specs/2026-06-01-pg-upgrade-design.md`

---

## Existing foundation (already built — do NOT rebuild)

- `internal/runner/interfaces.go`: `Step{ID, Check, Run}`, `Transition{To, Condition func(*state.State) bool}`, `Phase{ID, Steps, Transitions}`.
- `internal/runner/types.go`: `PhaseID = string`, `StepID = string`, `StepStatus`, `RunMode` (`Interactive`, `Headless`).
- `internal/state`: `Manager` with `Get()`, `Advance(phase)`, `CompleteStep/SkipStep/FailStep(phase, step[, msg])`, and artifact setters `SetPrimaryHost`, `SetSlotBaseline`, `SetReceivedLSN`, `SetTargetLSN`, `SetDrainReport`, `SetPgUpgradeCheckPassed`, `SetPgUpgradeDone(sysid)`, `SetSequencesSynced`, `SetDSNSwapNotified`. `State.Current`, `State.Artifacts`. `NewManager(path, clusterName, firstPhase)` / `LoadManager(path)`.
- `internal/clients/pg`: `Client` interface (17 methods incl. `ShowWALLevel`, `IsInRecovery`, `GetLastWALReplayLSN`, `GetWALReceiverReceivedLSN`, `Checkpoint`, `GetReplicationSlot`, `CreateLogicalSlot`, `CreatePublication`, ...). `NewFromPool(pgxmock)` for tests; `PoolClient` + `internalClient`.
- `internal/clients/patroni`: `Client{GetCluster, Pause, Resume}`, `ClusterInfo{Members, Paused}`, `Member{Name, Host, Port, Role, State, ...}`.
- `internal/slotdrain`: `Drain(ctx, Config) (*Report, error)`.
- `internal/config`: `Config{ClusterName, Upgrade{TargetNode, SlotName, PublicationName, NewPGBindir}, PG{SuperuserDSN}}`, `Load(path)`.

---

## File Structure (new in this plan)

```
internal/connect/dsn.go              # DSNForHost: derive a per-host DSN from the superuser_dsn template
internal/clients/pgbin/pgbin.go      # PGTools interface + Exec impl (pg_ctl/pg_upgrade/pg_controldata) + parseControlData
internal/runner/runner.go            # Runner FSM: Run / executePhase / transition
internal/runner/checkpoint.go        # Checkpoint type + InteractiveCheckpoint + phase prompts
internal/phases/deps.go              # Deps struct shared by phase constructors; primary-client provider
internal/phases/prepare.go           # Phase 1 + its 5 steps
internal/phases/isolate.go           # Phase 2 + its 5 steps
internal/phases/drain.go             # Phase 3 + its 2 steps
internal/phases/upgrade.go           # Phase 4 + its 5 steps
internal/phases/registry.go          # Assemble phases 1-4 into a map for the Runner
```

Modified:
```
internal/config/config.go            # add OldPGBindir, DataDir, PatroniConfigPath to UpgradeConfig
internal/clients/pg/client.go        # add DisconnectFromWAL, IsWALReceiverActive
cmd/pg-upgrade/main.go               # add `run` subcommand wiring the Runner
```

**Phase/Step ID constants** (use these exact strings everywhere):
- Phases: `"prepare"`, `"isolate"`, `"drain"`, `"upgrade"`.
- Steps: `DiscoverTopology`, `VerifyPrerequisites`, `CreatePublication`, `CreateLogicalSlot`, `RecordSlotBaseline`, `PausePatroni`, `CaptureReceivedLSN`, `DisconnectN1FromWAL`, `WaitReplayComplete`, `RecordTargetLSN`, `RunSlotDrain`, `VerifySlotDrained`, `PromoteN1`, `ShutdownN1Clean`, `RunPgUpgradeCheck`, `RunPgUpgrade`, `WriteFinalPatroniConfig`.

---

## Task 1: Config additions + connection helper

**Files:**
- Modify: `internal/config/config.go`
- Create: `internal/connect/dsn.go`
- Test: `internal/connect/dsn_test.go`

- [ ] **Step 1: Extend the config struct**

In `internal/config/config.go`, add three fields to `UpgradeConfig` (after `NewPGBindir`):

```go
type UpgradeConfig struct {
	TargetNode        string `yaml:"target_node"`
	SlotName          string `yaml:"slot_name"`
	PublicationName   string `yaml:"publication_name"`
	NewPGBindir       string `yaml:"new_pg_bindir"`
	OldPGBindir       string `yaml:"old_pg_bindir"`
	DataDir           string `yaml:"data_dir"`
	PatroniConfigPath string `yaml:"patroni_config_path"`
}
```

These are not required by `validate()` (drain-slot doesn't need them); the Upgrade phase checks them at run time. Leave `validate()` unchanged.

- [ ] **Step 2: Write the failing test `internal/connect/dsn_test.go`**

```go
package connect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDSNForHostSwapsHost(t *testing.T) {
	out, err := DSNForHost("host=primary.old port=5432 user=postgres dbname=app", "n1.internal")
	require.NoError(t, err)
	// host is replaced; other params preserved
	assert.Contains(t, out, "host=n1.internal")
	assert.Contains(t, out, "user=postgres")
	assert.Contains(t, out, "dbname=app")
	assert.NotContains(t, out, "primary.old")
}

func TestDSNForHostURLForm(t *testing.T) {
	out, err := DSNForHost("postgres://postgres:secret@primary.old:5432/app", "n1.internal")
	require.NoError(t, err)
	assert.Contains(t, out, "host=n1.internal")
	assert.Contains(t, out, "user=postgres")
	assert.Contains(t, out, "dbname=app")
}

func TestDSNForHostInvalid(t *testing.T) {
	_, err := DSNForHost("=not a dsn=", "n1")
	assert.Error(t, err)
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/connect/`
Expected: FAIL — package has no Go files / `undefined: DSNForHost`.

- [ ] **Step 4: Write `internal/connect/dsn.go`**

```go
// Package connect derives per-host connection strings from a single credential
// template, so the orchestrator can reach both the primary (discovered at
// runtime) and the local N1 node using one configured superuser_dsn.
package connect

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// DSNForHost parses template (keyword or URL form) and returns a keyword DSN
// with Host replaced by host and all other connection parameters preserved.
func DSNForHost(template, host string) (string, error) {
	cfg, err := pgconn.ParseConfig(template)
	if err != nil {
		return "", fmt.Errorf("connect: parse dsn: %w", err)
	}

	parts := map[string]string{
		"host":   host,
		"port":   fmt.Sprintf("%d", cfg.Port),
		"user":   cfg.User,
		"dbname": cfg.Database,
	}
	if cfg.Password != "" {
		parts["password"] = cfg.Password
	}
	for k, v := range cfg.RuntimeParams {
		parts[k] = v
	}

	keys := make([]string, 0, len(parts))
	for k := range parts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for i, k := range keys {
		if parts[k] == "" {
			continue
		}
		if i > 0 && b.Len() > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%s", k, parts[k])
	}
	return b.String(), nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/connect/ ./internal/config/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/config/config.go internal/connect/
git commit -m "feat(pg-upgrade): config upgrade fields + per-host DSN derivation"
```

---

## Task 2: pg.Client additions (DisconnectFromWAL, IsWALReceiverActive)

**Files:**
- Modify: `internal/clients/pg/client.go`
- Test: `internal/clients/pg/client_test.go`

- [ ] **Step 1: Write the failing tests** (append to `internal/clients/pg/client_test.go`)

```go
func TestIsWALReceiverActive(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("FROM pg_stat_wal_receiver").
		WillReturnRows(pgxmock.NewRows([]string{"active"}).AddRow(true))

	c := pgclient.NewFromPool(mock)
	active, err := c.IsWALReceiverActive(context.Background())
	require.NoError(t, err)
	assert.True(t, active)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDisconnectFromWAL(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("ALTER SYSTEM SET primary_conninfo").WillReturnResult(pgxmock.NewResult("ALTER", 0))
	mock.ExpectExec("pg_reload_conf").WillReturnResult(pgxmock.NewResult("SELECT", 1))

	c := pgclient.NewFromPool(mock)
	require.NoError(t, c.DisconnectFromWAL(context.Background()))
	assert.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/clients/pg/ -run 'TestIsWALReceiverActive|TestDisconnectFromWAL'`
Expected: FAIL — `c.IsWALReceiverActive undefined`.

- [ ] **Step 3: Add the methods**

In `internal/clients/pg/client.go`, add to the `Client` interface (after `GetWALReceiverReceivedLSN`):

```go
	IsWALReceiverActive(ctx context.Context) (bool, error)
	DisconnectFromWAL(ctx context.Context) error
```

Add the implementations on `internalClient` (mirror the existing method style; `internalClient` wraps a `poolQuerier`):

```go
// IsWALReceiverActive reports whether N1 is still streaming WAL from a primary.
// pg_stat_wal_receiver has one row while a walreceiver is connected, none after.
func (c *internalClient) IsWALReceiverActive(ctx context.Context) (bool, error) {
	var active bool
	err := c.q.QueryRow(ctx, `SELECT count(*) > 0 AS active FROM pg_stat_wal_receiver`).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("pg: query wal_receiver: %w", err)
	}
	return active, nil
}

// DisconnectFromWAL clears primary_conninfo and reloads, so N1 stops receiving
// WAL. Patroni must be paused first or it will revert this.
func (c *internalClient) DisconnectFromWAL(ctx context.Context) error {
	if _, err := c.q.Exec(ctx, `ALTER SYSTEM SET primary_conninfo = ''`); err != nil {
		return fmt.Errorf("pg: clear primary_conninfo: %w", err)
	}
	if _, err := c.q.Exec(ctx, `SELECT pg_reload_conf()`); err != nil {
		return fmt.Errorf("pg: reload conf: %w", err)
	}
	return nil
}
```

Add the delegations on `PoolClient`. It uses receiver `p` and delegates through
`p.ic()` (which builds an `internalClient` from the pool) — match the existing
methods (e.g. `func (p *PoolClient) Checkpoint(ctx context.Context) error { return p.ic().Checkpoint(ctx) }`):

```go
func (p *PoolClient) IsWALReceiverActive(ctx context.Context) (bool, error) {
	return p.ic().IsWALReceiverActive(ctx)
}

func (p *PoolClient) DisconnectFromWAL(ctx context.Context) error {
	return p.ic().DisconnectFromWAL(ctx)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/clients/pg/`
Expected: PASS (all pg tests, including the two new ones).

- [ ] **Step 5: Commit**

```bash
git add internal/clients/pg/client.go internal/clients/pg/client_test.go
git commit -m "feat(pg-upgrade): pg client DisconnectFromWAL + IsWALReceiverActive"
```

---

## Task 3: PGTools wrappers (pg_ctl / pg_upgrade / pg_controldata)

**Files:**
- Create: `internal/clients/pgbin/pgbin.go`
- Test: `internal/clients/pgbin/pgbin_test.go`

Only the `pg_controldata` output parser is unit-tested (pure). The exec methods are thin wrappers over `os/exec`, verified on a real cluster later.

- [ ] **Step 1: Write the failing test `internal/clients/pgbin/pgbin_test.go`**

```go
package pgbin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseControlData(t *testing.T) {
	out := `pg_control version number:            1300
Database cluster state:               in production
Database system identifier:           7361852939023499998
Latest checkpoint location:           0/3FA20000`

	cd := parseControlData(out)
	assert.Equal(t, "in production", cd.State)
	assert.Equal(t, "7361852939023499998", cd.SystemID)
}

func TestParseControlDataShutDown(t *testing.T) {
	out := `Database cluster state:               shut down
Database system identifier:           42`
	cd := parseControlData(out)
	assert.Equal(t, "shut down", cd.State)
	assert.Equal(t, "42", cd.SystemID)
}

func TestParseControlDataMissingFields(t *testing.T) {
	cd := parseControlData("garbage\nno fields here")
	assert.Equal(t, "", cd.State)
	assert.Equal(t, "", cd.SystemID)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/clients/pgbin/`
Expected: FAIL — package has no Go files / `undefined: parseControlData`.

- [ ] **Step 3: Write `internal/clients/pgbin/pgbin.go`**

```go
// Package pgbin wraps the PostgreSQL command-line tools (pg_ctl, pg_upgrade,
// pg_controldata) that the upgrade phase shells out to. The exec wrappers are
// thin; only the pg_controldata parser carries logic worth unit-testing.
package pgbin

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

// ControlData is the subset of pg_controldata output the orchestrator needs.
type ControlData struct {
	State    string // e.g. "in production", "shut down"
	SystemID string // database system identifier
}

// UpgradeOptions are the paths pg_upgrade needs.
type UpgradeOptions struct {
	OldBindir  string
	NewBindir  string
	OldDataDir string
	NewDataDir string
}

// PGTools is the seam the upgrade steps depend on, so their idempotency checks
// (which read pg_controldata) are unit-testable with a fake.
type PGTools interface {
	ControlData(ctx context.Context, dataDir string) (*ControlData, error)
	Promote(ctx context.Context, dataDir string) error
	StopClean(ctx context.Context, dataDir string) error
	UpgradeCheck(ctx context.Context, o UpgradeOptions) error
	Upgrade(ctx context.Context, o UpgradeOptions) error
}

// Exec is the real PGTools, invoking binaries under the given bindirs.
type Exec struct {
	NewBindir string
	OldBindir string
}

func (e Exec) bin(dir, name string) string { return filepath.Join(dir, name) }

func (e Exec) ControlData(ctx context.Context, dataDir string) (*ControlData, error) {
	// pg_controldata of the running/old cluster lives in the new bindir too;
	// use NewBindir's pg_controldata which reads any compatible cluster.
	out, err := exec.CommandContext(ctx, e.bin(e.NewBindir, "pg_controldata"), "-D", dataDir).Output()
	if err != nil {
		return nil, fmt.Errorf("pgbin: pg_controldata: %w", err)
	}
	return parseControlData(string(out)), nil
}

func (e Exec) Promote(ctx context.Context, dataDir string) error {
	return run(exec.CommandContext(ctx, e.bin(e.OldBindir, "pg_ctl"), "promote", "-D", dataDir), "promote")
}

func (e Exec) StopClean(ctx context.Context, dataDir string) error {
	return run(exec.CommandContext(ctx, e.bin(e.OldBindir, "pg_ctl"), "stop", "-m", "smart", "-D", dataDir), "stop")
}

func (e Exec) UpgradeCheck(ctx context.Context, o UpgradeOptions) error {
	return run(e.upgradeCmd(ctx, o, true), "pg_upgrade --check")
}

func (e Exec) Upgrade(ctx context.Context, o UpgradeOptions) error {
	return run(e.upgradeCmd(ctx, o, false), "pg_upgrade --link")
}

func (e Exec) upgradeCmd(ctx context.Context, o UpgradeOptions, check bool) *exec.Cmd {
	args := []string{
		"--old-bindir", o.OldBindir, "--new-bindir", o.NewBindir,
		"--old-datadir", o.OldDataDir, "--new-datadir", o.NewDataDir,
		"--link",
	}
	if check {
		args = append(args, "--check")
	}
	return exec.CommandContext(ctx, e.bin(o.NewBindir, "pg_upgrade"), args...)
}

func run(cmd *exec.Cmd, label string) error {
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pgbin: %s: %w: %s", label, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// parseControlData extracts the fields the orchestrator needs from
// pg_controldata's "Label:   value" lines.
func parseControlData(out string) *ControlData {
	cd := &ControlData{}
	for _, line := range strings.Split(out, "\n") {
		idx := strings.Index(line, ":")
		if idx < 0 {
			continue
		}
		label := strings.TrimSpace(line[:idx])
		value := strings.TrimSpace(line[idx+1:])
		switch label {
		case "Database cluster state":
			cd.State = value
		case "Database system identifier":
			cd.SystemID = value
		}
	}
	return cd
}

// compile-time assertion that Exec satisfies PGTools.
var _ PGTools = Exec{}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/clients/pgbin/`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/clients/pgbin/
git commit -m "feat(pg-upgrade): pgbin wrappers + pg_controldata parser"
```

---

## Task 4: Runner FSM engine

**Files:**
- Create: `internal/runner/runner.go`
- Test: `internal/runner/runner_test.go`

- [ ] **Step 1: Write the failing test `internal/runner/runner_test.go`**

```go
package runner

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStep records calls and returns scripted results.
type fakeStep struct {
	id       StepID
	checked  bool
	ran      bool
	checkRes bool
	checkErr error
	runErr   error
}

func (s *fakeStep) ID() StepID { return s.id }
func (s *fakeStep) Check(context.Context) (bool, error) { s.checked = true; return s.checkRes, s.checkErr }
func (s *fakeStep) Run(context.Context) error          { s.ran = true; return s.runErr }

// fakePhase is a phase with fixed steps and transitions.
type fakePhase struct {
	id    PhaseID
	steps []Step
	trans []Transition
}

func (p *fakePhase) ID() PhaseID            { return p.id }
func (p *fakePhase) Steps() []Step          { return p.steps }
func (p *fakePhase) Transitions() []Transition { return p.trans }

func newMgr(t *testing.T, first PhaseID) *state.Manager {
	t.Helper()
	// NewManager(path, clusterName) starts Current at "prepare"; Advance moves it
	// to the phase the test wants to start from.
	m, err := state.NewManager(filepath.Join(t.TempDir(), "state.json"), "test")
	require.NoError(t, err)
	require.NoError(t, m.Advance(first))
	return m
}

func TestRunnerRunsStepsAndTransitions(t *testing.T) {
	s1 := &fakeStep{id: "s1"}
	s2 := &fakeStep{id: "s2"}
	a := &fakePhase{id: "a", steps: []Step{s1}, trans: []Transition{{To: "b"}}}
	b := &fakePhase{id: "b", steps: []Step{s2}} // no transition = terminal

	mgr := newMgr(t, "a")
	r := New([]Phase{a, b}, mgr, Headless, nil)
	require.NoError(t, r.Run(context.Background()))

	assert.True(t, s1.ran)
	assert.True(t, s2.ran)
	assert.Equal(t, "b", mgr.Get().Current)
}

func TestRunnerSkipsCompletedSteps(t *testing.T) {
	s1 := &fakeStep{id: "s1", checkRes: true} // already done
	a := &fakePhase{id: "a", steps: []Step{s1}}
	mgr := newMgr(t, "a")
	r := New([]Phase{a}, mgr, Headless, nil)
	require.NoError(t, r.Run(context.Background()))
	assert.True(t, s1.checked)
	assert.False(t, s1.ran) // skipped
}

func TestRunnerStopsOnStepError(t *testing.T) {
	s1 := &fakeStep{id: "s1", runErr: errors.New("boom")}
	a := &fakePhase{id: "a", steps: []Step{s1}, trans: []Transition{{To: "b"}}}
	mgr := newMgr(t, "a")
	r := New([]Phase{a}, mgr, Headless, nil)
	err := r.Run(context.Background())
	require.Error(t, err)
	assert.Equal(t, "a", mgr.Get().Current) // did not advance
}

func TestRunnerConditionalTransition(t *testing.T) {
	a := &fakePhase{id: "a", steps: nil, trans: []Transition{
		{To: "x", Condition: func(*state.State) bool { return false }},
		{To: "y", Condition: func(*state.State) bool { return true }},
	}}
	y := &fakePhase{id: "y"}
	mgr := newMgr(t, "a")
	r := New([]Phase{a, y}, mgr, Headless, nil)
	require.NoError(t, r.Run(context.Background()))
	assert.Equal(t, "y", mgr.Get().Current)
}

func TestRunnerInteractiveCheckpointAbort(t *testing.T) {
	a := &fakePhase{id: "a", trans: []Transition{{To: "b"}}}
	b := &fakePhase{id: "b"}
	mgr := newMgr(t, "a")
	cp := func(context.Context, PhaseID, PhaseID) error { return errors.New("operator aborted") }
	r := New([]Phase{a, b}, mgr, Interactive, cp)
	err := r.Run(context.Background())
	require.Error(t, err)
	assert.Equal(t, "a", mgr.Get().Current) // aborted before advancing
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/runner/ -run TestRunner`
Expected: FAIL — `undefined: New`.

- [ ] **Step 3: Write `internal/runner/runner.go`**

```go
package runner

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/state"
)

// Checkpoint is invoked at a phase boundary in Interactive mode. Returning an
// error aborts the run (operator declined). In Headless mode it is never called.
type Checkpoint func(ctx context.Context, from, to PhaseID) error

// Runner is the FSM engine: it executes the current phase's steps, then follows
// the first matching transition, persisting progress through the state Manager.
type Runner struct {
	phases map[PhaseID]Phase
	mgr    *state.Manager
	mode   RunMode
	cp     Checkpoint
}

// New builds a Runner from a phase list (indexed by ID), a state Manager, a run
// mode, and a checkpoint (used only in Interactive mode).
func New(phases []Phase, mgr *state.Manager, mode RunMode, cp Checkpoint) *Runner {
	idx := make(map[PhaseID]Phase, len(phases))
	for _, p := range phases {
		idx[p.ID()] = p
	}
	return &Runner{phases: idx, mgr: mgr, mode: mode, cp: cp}
}

// Run drives the FSM from state.Current until a phase has no matching transition
// (terminal) or a step/checkpoint fails.
func (r *Runner) Run(ctx context.Context) error {
	for {
		cur := r.mgr.Get().Current
		phase, ok := r.phases[cur]
		if !ok {
			return fmt.Errorf("runner: unknown phase %q", cur)
		}
		if err := r.executePhase(ctx, phase); err != nil {
			return err
		}
		next := r.transition(phase)
		if next == "" {
			return nil // terminal phase reached
		}
		if r.mode == Interactive && r.cp != nil {
			if err := r.cp(ctx, phase.ID(), next); err != nil {
				return fmt.Errorf("runner: aborted at %s: %w", phase.ID(), err)
			}
		}
		if err := r.mgr.Advance(next); err != nil {
			return err
		}
	}
}

// executePhase runs each step that Check() reports as not-yet-done, persisting
// per-step status. A failing step stops the phase.
func (r *Runner) executePhase(ctx context.Context, phase Phase) error {
	for _, step := range phase.Steps() {
		done, err := step.Check(ctx)
		if err != nil {
			_ = r.mgr.FailStep(phase.ID(), step.ID(), err.Error())
			return fmt.Errorf("runner: %s/%s check: %w", phase.ID(), step.ID(), err)
		}
		if done {
			if err := r.mgr.SkipStep(phase.ID(), step.ID()); err != nil {
				return err
			}
			continue
		}
		if err := step.Run(ctx); err != nil {
			_ = r.mgr.FailStep(phase.ID(), step.ID(), err.Error())
			return fmt.Errorf("runner: %s/%s run: %w", phase.ID(), step.ID(), err)
		}
		if err := r.mgr.CompleteStep(phase.ID(), step.ID()); err != nil {
			return err
		}
	}
	return nil
}

// transition returns the target of the first transition whose condition matches
// (nil condition always matches); "" if none match (terminal).
func (r *Runner) transition(phase Phase) PhaseID {
	st := r.mgr.Get()
	for _, t := range phase.Transitions() {
		if t.Condition == nil || t.Condition(st) {
			return t.To
		}
	}
	return ""
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/runner/`
Expected: PASS (all runner tests).

- [ ] **Step 5: Commit**

```bash
git add internal/runner/runner.go internal/runner/runner_test.go
git commit -m "feat(pg-upgrade): Runner FSM engine"
```

---

## Task 5: Checkpoint (interactive operator confirmation)

**Files:**
- Create: `internal/runner/checkpoint.go`
- Test: `internal/runner/checkpoint_test.go`

- [ ] **Step 1: Write the failing test `internal/runner/checkpoint_test.go`**

```go
package runner

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInteractiveCheckpointAccepts(t *testing.T) {
	in := strings.NewReader("y\n")
	var out bytes.Buffer
	cp := InteractiveCheckpoint(in, &out, map[PhaseID]string{"prepare": "Proceed to isolate?"})
	require.NoError(t, cp(context.Background(), "prepare", "isolate"))
	assert.Contains(t, out.String(), "Proceed to isolate?")
}

func TestInteractiveCheckpointDeclines(t *testing.T) {
	in := strings.NewReader("n\n")
	var out bytes.Buffer
	cp := InteractiveCheckpoint(in, &out, nil)
	err := cp(context.Background(), "prepare", "isolate")
	assert.Error(t, err)
}

func TestInteractiveCheckpointEmptyDeclines(t *testing.T) {
	in := strings.NewReader("\n") // default is No
	var out bytes.Buffer
	cp := InteractiveCheckpoint(in, &out, nil)
	assert.Error(t, cp(context.Background(), "prepare", "isolate"))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/runner/ -run TestInteractiveCheckpoint`
Expected: FAIL — `undefined: InteractiveCheckpoint`.

- [ ] **Step 3: Write `internal/runner/checkpoint.go`**

```go
package runner

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
)

// PhasePrompts maps the just-completed phase to the question shown before
// advancing. A phase with no entry falls back to a generic prompt.
type PhasePrompts = map[PhaseID]string

// InteractiveCheckpoint returns a Checkpoint that prints the phase's prompt and
// requires the operator to type "y" to proceed; anything else aborts.
func InteractiveCheckpoint(in io.Reader, out io.Writer, prompts PhasePrompts) Checkpoint {
	reader := bufio.NewReader(in)
	return func(_ context.Context, from, to PhaseID) error {
		msg := prompts[from]
		if msg == "" {
			msg = fmt.Sprintf("Phase %q complete. Proceed to %q?", from, to)
		}
		fmt.Fprintf(out, "\n>>> %s [y/N]: ", msg)
		line, _ := reader.ReadString('\n')
		if strings.ToLower(strings.TrimSpace(line)) != "y" {
			return fmt.Errorf("operator declined at phase %q", from)
		}
		return nil
	}
}

// DefaultPrompts are the checkpoint questions from the design spec.
func DefaultPrompts() PhasePrompts {
	return PhasePrompts{
		"prepare": "Logical slot created. Proceed to isolate N1?",
		"isolate": "N1 isolated, target_lsn recorded. Run slot drain?",
		"drain":   "Slot drained. Proceed to pg_upgrade (point of no return)?",
		"upgrade": "pg_upgrade complete. Proceed to catchup (Plan 3)?",
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/runner/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/runner/checkpoint.go internal/runner/checkpoint_test.go
git commit -m "feat(pg-upgrade): interactive checkpoint with phase prompts"
```

---

## Task 6: Phase deps + Phase 1 (Prepare)

**Files:**
- Create: `internal/phases/deps.go`
- Create: `internal/phases/prepare.go`
- Test: `internal/phases/prepare_test.go`

`Deps` carries everything the steps need. The primary client is provided lazily
(it depends on the host discovered in step 1).

- [ ] **Step 1: Write `internal/phases/deps.go`**

```go
// Package phases implements the pg-upgrade FSM phases (Prepare, Isolate, Drain,
// Upgrade) as runner.Phase values built from a shared Deps.
package phases

import (
	"context"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/clients/pgbin"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/slotdrain"
	"github.com/dmbabuev/pg-upgrade/internal/state"
)

// Deps is the dependency set shared by all phase steps.
type Deps struct {
	Cfg     config.Config
	Mgr     *state.Manager
	Patroni patroni.Client
	Tools   pgbin.PGTools

	// N1 is the local node's PG client (PG10 before upgrade).
	N1 pg.Client

	// Primary returns a client to the current primary. It is resolved lazily
	// because the primary host is only known after DiscoverTopology.
	Primary func(ctx context.Context) (pg.Client, error)

	// Drain runs the slot drain (injected for testability).
	Drain func(ctx context.Context, cfg slotdrain.Config) (*slotdrain.Report, error)
}
```

- [ ] **Step 2: Write the failing test `internal/phases/prepare_test.go`**

```go
package phases

import (
	"context"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
)

// fakePG embeds pg.Client; only overridden methods are safe to call.
type fakePG struct {
	pg.Client
	walLevel   string
	inRecovery bool
	slot       *pg.ReplicationSlot
	createdPub string
	createdSlot string
}

func (f *fakePG) ShowWALLevel(context.Context) (string, error)       { return f.walLevel, nil }
func (f *fakePG) IsInRecovery(context.Context) (bool, error)         { return f.inRecovery, nil }
func (f *fakePG) GetReplicationSlot(_ context.Context, name string) (*pg.ReplicationSlot, error) {
	return f.slot, nil
}
func (f *fakePG) CreatePublication(_ context.Context, name string) error { f.createdPub = name; return nil }
func (f *fakePG) CreateLogicalSlot(_ context.Context, name, plugin string) (*pg.ReplicationSlot, error) {
	f.createdSlot = name
	return &pg.ReplicationSlot{Name: name, RestartLSN: "0/10", ConfirmedFlushLSN: "0/10"}, nil
}

// fakePatroni implements patroni.Client.
type fakePatroni struct {
	cluster *patroni.ClusterInfo
	paused  bool
}

func (f *fakePatroni) GetCluster(context.Context) (*patroni.ClusterInfo, error) { return f.cluster, nil }
func (f *fakePatroni) Pause(context.Context) error                              { f.paused = true; return nil }
func (f *fakePatroni) Resume(context.Context) error                             { f.paused = false; return nil }

func testMgr(t *testing.T) *state.Manager {
	t.Helper()
	// NewManager starts Current at "prepare"; isolate/drain/upgrade tests call
	// Advance to move forward.
	m, err := state.NewManager(filepath.Join(t.TempDir(), "s.json"), "test")
	require.NoError(t, err)
	return m
}

func TestPrepareDiscoverAndCreate(t *testing.T) {
	primary := &fakePG{walLevel: "logical", inRecovery: false, slot: nil}
	n1 := &fakePG{inRecovery: true}
	pat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "p", Host: "primary.host", Role: "leader"},
		{Name: "n1", Host: "n1.host", Role: "replica"},
	}}}
	mgr := testMgr(t)
	d := Deps{
		Cfg:     config.Config{Upgrade: config.UpgradeConfig{TargetNode: "n1", SlotName: "slot_up", PublicationName: "pub_up"}},
		Mgr:     mgr, Patroni: pat, N1: n1,
		Primary: func(context.Context) (pg.Client, error) { return primary, nil },
	}

	ph := NewPrepare(d)
	assert.Equal(t, "prepare", ph.ID())
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}

	assert.Equal(t, "primary.host", mgr.Get().Artifacts.PrimaryHost)
	assert.Equal(t, "pub_up", primary.createdPub)
	assert.Equal(t, "slot_up", primary.createdSlot)
	require.NotNil(t, mgr.Get().Artifacts.SlotBaseline)
	assert.Equal(t, "0/10", mgr.Get().Artifacts.SlotBaseline.ConfirmedFlushLSN)
}

func TestPrepareTransitionsToIsolate(t *testing.T) {
	ph := NewPrepare(Deps{})
	tr := ph.Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "isolate", tr[0].To)
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/phases/ -run TestPrepare`
Expected: FAIL — `undefined: NewPrepare`.

- [ ] **Step 4: Write `internal/phases/prepare.go`**

```go
package phases

import (
	"context"
	"fmt"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/dmbabuev/pg-upgrade/internal/state"
)

// NewPrepare builds Phase 1: create the logical replication foundation on the
// primary before N1 is isolated.
func NewPrepare(d Deps) runner.Phase {
	return &simplePhase{
		id: "prepare",
		steps: []runner.Step{
			&discoverTopology{d},
			&verifyPrerequisites{d},
			&createPublication{d},
			&createLogicalSlot{d},
			&recordSlotBaseline{d},
		},
		trans: []runner.Transition{{To: "isolate"}},
	}
}

// simplePhase is a reusable Phase backed by fixed steps and transitions.
type simplePhase struct {
	id    runner.PhaseID
	steps []runner.Step
	trans []runner.Transition
}

func (p *simplePhase) ID() runner.PhaseID              { return p.id }
func (p *simplePhase) Steps() []runner.Step            { return p.steps }
func (p *simplePhase) Transitions() []runner.Transition { return p.trans }

// --- DiscoverTopology ---

type discoverTopology struct{ d Deps }

func (s *discoverTopology) ID() runner.StepID { return "DiscoverTopology" }
func (s *discoverTopology) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.PrimaryHost != "", nil
}
func (s *discoverTopology) Run(ctx context.Context) error {
	cluster, err := s.d.Patroni.GetCluster(ctx)
	if err != nil {
		return err
	}
	for _, m := range cluster.Members {
		if m.Role == "leader" {
			return s.d.Mgr.SetPrimaryHost(m.Host)
		}
	}
	return fmt.Errorf("prepare: no leader in Patroni cluster")
}

// --- VerifyPrerequisites ---

type verifyPrerequisites struct{ d Deps }

func (s *verifyPrerequisites) ID() runner.StepID { return "VerifyPrerequisites" }
func (s *verifyPrerequisites) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifyPrerequisites) Run(ctx context.Context) error {
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	level, err := primary.ShowWALLevel(ctx)
	if err != nil {
		return err
	}
	if level != "logical" {
		return fmt.Errorf("prepare: primary wal_level=%q, need logical", level)
	}
	inRec, err := s.d.N1.IsInRecovery(ctx)
	if err != nil {
		return err
	}
	if !inRec {
		return fmt.Errorf("prepare: target node %s is not a replica (not in recovery)", s.d.Cfg.Upgrade.TargetNode)
	}
	return nil
}

// --- CreatePublication ---

type createPublication struct{ d Deps }

func (s *createPublication) ID() runner.StepID { return "CreatePublication" }
func (s *createPublication) Check(ctx context.Context) (bool, error) {
	// Idempotency handled by CREATE PUBLICATION ... IF NOT EXISTS in the client;
	// re-running is safe, so never skip.
	return false, nil
}
func (s *createPublication) Run(ctx context.Context) error {
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	return primary.CreatePublication(ctx, s.d.Cfg.Upgrade.PublicationName)
}

// --- CreateLogicalSlot ---

type createLogicalSlot struct{ d Deps }

func (s *createLogicalSlot) ID() runner.StepID { return "CreateLogicalSlot" }
func (s *createLogicalSlot) Check(ctx context.Context) (bool, error) {
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return false, err
	}
	slot, err := primary.GetReplicationSlot(ctx, s.d.Cfg.Upgrade.SlotName)
	if err != nil {
		return false, err
	}
	return slot != nil, nil
}
func (s *createLogicalSlot) Run(ctx context.Context) error {
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	_, err = primary.CreateLogicalSlot(ctx, s.d.Cfg.Upgrade.SlotName, "pgoutput")
	return err
}

// --- RecordSlotBaseline ---

type recordSlotBaseline struct{ d Deps }

func (s *recordSlotBaseline) ID() runner.StepID { return "RecordSlotBaseline" }
func (s *recordSlotBaseline) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.SlotBaseline != nil, nil
}
func (s *recordSlotBaseline) Run(ctx context.Context) error {
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	slot, err := primary.GetReplicationSlot(ctx, s.d.Cfg.Upgrade.SlotName)
	if err != nil {
		return err
	}
	if slot == nil {
		return fmt.Errorf("prepare: slot %s missing at baseline", s.d.Cfg.Upgrade.SlotName)
	}
	return s.d.Mgr.SetSlotBaseline(&state.SlotBaseline{
		CapturedAt:        time.Now(),
		RestartLSN:        slot.RestartLSN,
		ConfirmedFlushLSN: slot.ConfirmedFlushLSN,
		PrimaryHost:       s.d.Mgr.Get().Artifacts.PrimaryHost,
	})
}

// (interface assertions)
var (
	_ runner.Step = (*discoverTopology)(nil)
	_ runner.Step = (*verifyPrerequisites)(nil)
	_ runner.Step = (*createPublication)(nil)
	_ runner.Step = (*createLogicalSlot)(nil)
	_ runner.Step = (*recordSlotBaseline)(nil)
)
```

(`state.SetSlotBaseline` stores the struct as-is and does not set `CapturedAt`,
so the step sets it. `prepare.go` does not import the `pg` package — the steps
reach PG only through `Deps`, whose types are declared in `deps.go`.)

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/phases/ -run TestPrepare`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/phases/deps.go internal/phases/prepare.go internal/phases/prepare_test.go
git commit -m "feat(pg-upgrade): phase 1 Prepare (topology, publication, slot, baseline)"
```

---

## Task 7: Phase 2 (Isolate)

**Files:**
- Create: `internal/phases/isolate.go`
- Test: `internal/phases/isolate_test.go`

LSN comparison uses `pglogrepl.ParseLSN`.

- [ ] **Step 1: Write the failing test `internal/phases/isolate_test.go`**

```go
package phases

import (
	"context"
	"testing"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// extend fakePG with isolate-phase methods
func (f *fakePG) GetWALReceiverReceivedLSN(context.Context) (string, error) { return f.receivedLSN, nil }
func (f *fakePG) IsWALReceiverActive(context.Context) (bool, error)         { return f.walRcvActive, nil }
func (f *fakePG) DisconnectFromWAL(context.Context) error                   { f.disconnected = true; return nil }
func (f *fakePG) GetLastWALReplayLSN(context.Context) (string, error)       { return f.replayLSN, nil }

func TestIsolateRecordsTargetLSN(t *testing.T) {
	mgr := testMgr(t)
	mgr.Advance("isolate")
	// pre-seed a slot baseline so the invariant check passes
	require.NoError(t, mgr.SetSlotBaseline(&state.SlotBaseline{ConfirmedFlushLSN: "0/10"}))

	n1 := &fakePG{receivedLSN: "0/3FA20000", walRcvActive: false, replayLSN: "0/3FA20000"}
	pat := &fakePatroni{}
	d := Deps{Mgr: mgr, Patroni: pat, N1: n1}

	ph := NewIsolate(d)
	assert.Equal(t, "isolate", ph.ID())
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.True(t, pat.paused)
	assert.True(t, n1.disconnected)
	assert.Equal(t, "0/3FA20000", mgr.Get().Artifacts.ReceivedLSN)
	assert.Equal(t, "0/3FA20000", mgr.Get().Artifacts.TargetLSN)
}

func TestIsolateInvariantViolation(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	// baseline confirmed_flush AFTER the replay boundary -> fatal invariant
	require.NoError(t, mgr.SetSlotBaseline(&state.SlotBaseline{ConfirmedFlushLSN: "0/FF000000"}))

	n1 := &fakePG{replayLSN: "0/10"}
	step := &recordTargetLSN{Deps{Mgr: mgr, N1: n1}}
	err := step.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invariant")
}
```

Add the extra fields to `fakePG` in `prepare_test.go` (so both test files share one struct). Update the `fakePG` struct definition in `prepare_test.go` to include:

```go
	receivedLSN  string
	walRcvActive bool
	disconnected bool
	replayLSN    string
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/phases/ -run TestIsolate`
Expected: FAIL — `undefined: NewIsolate`.

- [ ] **Step 3: Write `internal/phases/isolate.go`**

```go
package phases

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/jackc/pglogrepl"
)

// NewIsolate builds Phase 2: disconnect N1 from WAL and record the physical
// boundary target_lsn.
func NewIsolate(d Deps) runner.Phase {
	return &simplePhase{
		id: "isolate",
		steps: []runner.Step{
			&pausePatroni{d},
			&captureReceivedLSN{d},
			&disconnectN1{d},
			&waitReplayComplete{d},
			&recordTargetLSN{d},
		},
		trans: []runner.Transition{{To: "drain"}},
	}
}

// --- PausePatroni ---

type pausePatroni struct{ d Deps }

func (s *pausePatroni) ID() runner.StepID { return "PausePatroni" }
func (s *pausePatroni) Check(ctx context.Context) (bool, error) {
	c, err := s.d.Patroni.GetCluster(ctx)
	if err != nil {
		return false, err
	}
	return c.Paused, nil
}
func (s *pausePatroni) Run(ctx context.Context) error { return s.d.Patroni.Pause(ctx) }

// --- CaptureReceivedLSN (must run before disconnect; receiver goes empty after) ---

type captureReceivedLSN struct{ d Deps }

func (s *captureReceivedLSN) ID() runner.StepID { return "CaptureReceivedLSN" }
func (s *captureReceivedLSN) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.ReceivedLSN != "", nil
}
func (s *captureReceivedLSN) Run(ctx context.Context) error {
	lsn, err := s.d.N1.GetWALReceiverReceivedLSN(ctx)
	if err != nil {
		return err
	}
	if lsn == "" {
		return fmt.Errorf("isolate: wal receiver already empty; cannot capture received_lsn")
	}
	return s.d.Mgr.SetReceivedLSN(lsn)
}

// --- DisconnectN1FromWAL ---

type disconnectN1 struct{ d Deps }

func (s *disconnectN1) ID() runner.StepID { return "DisconnectN1FromWAL" }
func (s *disconnectN1) Check(ctx context.Context) (bool, error) {
	active, err := s.d.N1.IsWALReceiverActive(ctx)
	if err != nil {
		return false, err
	}
	return !active, nil // already disconnected = done
}
func (s *disconnectN1) Run(ctx context.Context) error { return s.d.N1.DisconnectFromWAL(ctx) }

// --- WaitReplayComplete: replay >= received ---

type waitReplayComplete struct{ d Deps }

func (s *waitReplayComplete) ID() runner.StepID { return "WaitReplayComplete" }
func (s *waitReplayComplete) Check(ctx context.Context) (bool, error) {
	return s.replayCaughtUp(ctx)
}
func (s *waitReplayComplete) Run(ctx context.Context) error {
	caught, err := s.replayCaughtUp(ctx)
	if err != nil {
		return err
	}
	if !caught {
		return fmt.Errorf("isolate: replay has not reached received_lsn yet (retry run)")
	}
	return nil
}
func (s *waitReplayComplete) replayCaughtUp(ctx context.Context) (bool, error) {
	received := s.d.Mgr.Get().Artifacts.ReceivedLSN
	if received == "" {
		return false, fmt.Errorf("isolate: received_lsn not captured")
	}
	replayStr, err := s.d.N1.GetLastWALReplayLSN(ctx)
	if err != nil {
		return false, err
	}
	recv, err := pglogrepl.ParseLSN(received)
	if err != nil {
		return false, fmt.Errorf("isolate: parse received_lsn: %w", err)
	}
	replay, err := pglogrepl.ParseLSN(replayStr)
	if err != nil {
		return false, fmt.Errorf("isolate: parse replay_lsn: %w", err)
	}
	return replay >= recv, nil
}

// --- RecordTargetLSN + post-phase invariant ---

type recordTargetLSN struct{ d Deps }

func (s *recordTargetLSN) ID() runner.StepID { return "RecordTargetLSN" }
func (s *recordTargetLSN) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.TargetLSN != "", nil
}
func (s *recordTargetLSN) Run(ctx context.Context) error {
	target, err := s.d.N1.GetLastWALReplayLSN(ctx)
	if err != nil {
		return err
	}
	if err := s.d.Mgr.SetTargetLSN(target); err != nil {
		return err
	}
	// Invariant: SlotBaseline.ConfirmedFlushLSN <= target_lsn, else changes
	// between baseline and target would be lost.
	bl := s.d.Mgr.Get().Artifacts.SlotBaseline
	if bl == nil {
		return fmt.Errorf("isolate: slot baseline missing")
	}
	conf, err := pglogrepl.ParseLSN(bl.ConfirmedFlushLSN)
	if err != nil {
		return fmt.Errorf("isolate: parse confirmed_flush_lsn: %w", err)
	}
	tgt, err := pglogrepl.ParseLSN(target)
	if err != nil {
		return fmt.Errorf("isolate: parse target_lsn: %w", err)
	}
	if conf > tgt {
		return fmt.Errorf("isolate: FATAL invariant violated: confirmed_flush_lsn %s > target_lsn %s (slot created after N1 disconnected)", bl.ConfirmedFlushLSN, target)
	}
	return nil
}

var (
	_ runner.Step = (*pausePatroni)(nil)
	_ runner.Step = (*captureReceivedLSN)(nil)
	_ runner.Step = (*disconnectN1)(nil)
	_ runner.Step = (*waitReplayComplete)(nil)
	_ runner.Step = (*recordTargetLSN)(nil)
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/phases/ -run TestIsolate`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/phases/isolate.go internal/phases/isolate_test.go internal/phases/prepare_test.go
git commit -m "feat(pg-upgrade): phase 2 Isolate (pause, capture LSN, disconnect, target invariant)"
```

---

## Task 8: Phase 3 (Drain)

**Files:**
- Create: `internal/phases/drain.go`
- Test: `internal/phases/drain_test.go`

- [ ] **Step 1: Write the failing test `internal/phases/drain_test.go`**

```go
package phases

import (
	"context"
	"testing"
	"time"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/slotdrain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDrainRunsAndVerifies(t *testing.T) {
	mgr := testMgr(t)
	mgr.Advance("isolate")
	mgr.Advance("drain")
	require.NoError(t, mgr.SetTargetLSN("0/3FA20000"))

	var called bool
	drainFn := func(_ context.Context, cfg slotdrain.Config) (*slotdrain.Report, error) {
		called = true
		assert.Equal(t, "0/3FA20000", cfg.TargetLSN)
		return &slotdrain.Report{CompletedAt: time.Now(), FinalFlushLSN: "0/3FA20000", TransactionsDrained: 3}, nil
	}
	// primary slot now confirms at target
	primary := &fakePG{slot: &pg.ReplicationSlot{Name: "slot_up", ConfirmedFlushLSN: "0/3FA20000"}}
	d := Deps{
		Cfg:     config.Config{Upgrade: config.UpgradeConfig{SlotName: "slot_up", PublicationName: "pub_up"}, PG: config.PGConfig{SuperuserDSN: "host=primary"}},
		Mgr:     mgr,
		Primary: func(context.Context) (pg.Client, error) { return primary, nil },
		Drain:   drainFn,
	}

	ph := NewDrain(d)
	assert.Equal(t, "drain", ph.ID())
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.True(t, called)
	require.NotNil(t, mgr.Get().Artifacts.DrainReport)
	assert.Equal(t, 3, mgr.Get().Artifacts.DrainReport.TransactionsDrained)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/phases/ -run TestDrain`
Expected: FAIL — `undefined: NewDrain`.

- [ ] **Step 3: Write `internal/phases/drain.go`**

```go
package phases

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/dmbabuev/pg-upgrade/internal/slotdrain"
	"github.com/dmbabuev/pg-upgrade/internal/state"
)

// NewDrain builds Phase 3: advance the slot's confirmed_flush_lsn to the last
// commit <= target_lsn, leaving the tail for the PG17 subscription.
func NewDrain(d Deps) runner.Phase {
	return &simplePhase{
		id: "drain",
		steps: []runner.Step{
			&runSlotDrain{d},
			&verifySlotDrained{d},
		},
		trans: []runner.Transition{{To: "upgrade"}},
	}
}

// --- RunSlotDrain ---

type runSlotDrain struct{ d Deps }

func (s *runSlotDrain) ID() runner.StepID { return "RunSlotDrain" }
func (s *runSlotDrain) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.DrainReport != nil, nil
}
func (s *runSlotDrain) Run(ctx context.Context) error {
	target := s.d.Mgr.Get().Artifacts.TargetLSN
	if target == "" {
		return fmt.Errorf("drain: target_lsn not set")
	}
	report, err := s.d.Drain(ctx, slotdrain.Config{
		ConnString: s.d.Cfg.PG.SuperuserDSN,
		SlotName:   s.d.Cfg.Upgrade.SlotName,
		PubName:    s.d.Cfg.Upgrade.PublicationName,
		TargetLSN:  target,
	})
	if err != nil {
		return err
	}
	return s.d.Mgr.SetDrainReport(&state.DrainReport{
		CompletedAt:         report.CompletedAt,
		FinalFlushLSN:       report.FinalFlushLSN,
		TransactionsDrained: report.TransactionsDrained,
	})
}

// --- VerifySlotDrained: confirmed_flush_lsn is at/after the drain's final flush ---

type verifySlotDrained struct{ d Deps }

func (s *verifySlotDrained) ID() runner.StepID { return "VerifySlotDrained" }
func (s *verifySlotDrained) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifySlotDrained) Run(ctx context.Context) error {
	report := s.d.Mgr.Get().Artifacts.DrainReport
	if report == nil {
		return fmt.Errorf("drain: no drain report to verify")
	}
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	slot, err := primary.GetReplicationSlot(ctx, s.d.Cfg.Upgrade.SlotName)
	if err != nil {
		return err
	}
	if slot == nil {
		return fmt.Errorf("drain: slot %s missing after drain", s.d.Cfg.Upgrade.SlotName)
	}
	if slot.ConfirmedFlushLSN != report.FinalFlushLSN {
		return fmt.Errorf("drain: confirmed_flush_lsn %s != drained final %s", slot.ConfirmedFlushLSN, report.FinalFlushLSN)
	}
	return nil
}

var (
	_ runner.Step = (*runSlotDrain)(nil)
	_ runner.Step = (*verifySlotDrained)(nil)
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/phases/ -run TestDrain`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/phases/drain.go internal/phases/drain_test.go
git commit -m "feat(pg-upgrade): phase 3 Drain (run slot drain + verify confirmed_flush)"
```

---

## Task 9: Phase 4 (Upgrade) — point of no return

**Files:**
- Create: `internal/phases/upgrade.go`
- Test: `internal/phases/upgrade_test.go`

`NewUpgrade` has NO transitions (terminal for Plan 2). It uses `pgbin.PGTools`
and writes the Patroni config file with the PG17 SYSID.

- [ ] **Step 1: Write the failing test `internal/phases/upgrade_test.go`**

```go
package phases

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/clients/pgbin"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// add Checkpoint to fakePG (used by ShutdownN1Clean)
func (f *fakePG) Checkpoint(context.Context) error { f.checkpoints++; return nil }

// fakeTools implements pgbin.PGTools with scripted control-data states.
type fakeTools struct {
	states   []string // ControlData state to return, popped in order
	promoted bool
	stopped  bool
	checked  bool
	upgraded bool
	sysID    string
}

func (f *fakeTools) ControlData(context.Context, string) (*pgbin.ControlData, error) {
	st := "in production"
	if len(f.states) > 0 {
		st = f.states[0]
		f.states = f.states[1:]
	}
	return &pgbin.ControlData{State: st, SystemID: f.sysID}, nil
}
func (f *fakeTools) Promote(context.Context, string) error                  { f.promoted = true; return nil }
func (f *fakeTools) StopClean(context.Context, string) error                { f.stopped = true; return nil }
func (f *fakeTools) UpgradeCheck(context.Context, pgbin.UpgradeOptions) error { f.checked = true; return nil }
func (f *fakeTools) Upgrade(context.Context, pgbin.UpgradeOptions) error    { f.upgraded = true; return nil }

func TestUpgradeHappyPath(t *testing.T) {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade"} {
		require.NoError(t, mgr.Advance(p))
	}
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "patroni.yml")
	n1 := &fakePG{}
	tools := &fakeTools{
		// PromoteN1.Check -> "in production"; ShutdownN1Clean.Check -> "shut down"; RunPgUpgrade reads sysid
		states: []string{"in production", "shut down"},
		sysID:  "7361852939023499998",
	}
	d := Deps{
		Cfg: config.Config{Upgrade: config.UpgradeConfig{
			NewPGBindir: "/n", OldPGBindir: "/o", DataDir: filepath.Join(dir, "data"),
			PatroniConfigPath: cfgPath,
		}},
		Mgr: mgr, N1: n1, Tools: tools,
	}

	ph := NewUpgrade(d)
	assert.Equal(t, "upgrade", ph.ID())
	assert.Empty(t, ph.Transitions()) // terminal in Plan 2

	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.True(t, tools.promoted)
	assert.True(t, tools.stopped)
	assert.True(t, tools.checked)
	assert.True(t, tools.upgraded)
	assert.True(t, mgr.Get().Artifacts.PgUpgradeDone)
	assert.Equal(t, "7361852939023499998", mgr.Get().Artifacts.PG17SYSID)

	data, err := os.ReadFile(cfgPath)
	require.NoError(t, err)
	assert.Contains(t, string(data), "7361852939023499998")
}
```

Add `checkpoints int` to the `fakePG` struct fields in `prepare_test.go`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/phases/ -run TestUpgrade`
Expected: FAIL — `undefined: NewUpgrade`.

- [ ] **Step 3: Write `internal/phases/upgrade.go`**

```go
package phases

import (
	"context"
	"fmt"
	"os"

	"github.com/dmbabuev/pg-upgrade/internal/clients/pgbin"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewUpgrade builds Phase 4: promote N1, shut it down cleanly, run pg_upgrade
// --link (point of no return), and write the new Patroni config. Terminal in
// Plan 2 (phases 5-8 arrive in Plan 3).
func NewUpgrade(d Deps) runner.Phase {
	return &simplePhase{
		id: "upgrade",
		steps: []runner.Step{
			&promoteN1{d},
			&shutdownN1Clean{d},
			&runPgUpgradeCheck{d},
			&runPgUpgrade{d},
			&writeFinalPatroniConfig{d},
		},
		trans: nil, // terminal: point of no return reached
	}
}

func (d Deps) upgradeOpts() pgbin.UpgradeOptions {
	return pgbin.UpgradeOptions{
		OldBindir:  d.Cfg.Upgrade.OldPGBindir,
		NewBindir:  d.Cfg.Upgrade.NewPGBindir,
		OldDataDir: d.Cfg.Upgrade.DataDir,
		NewDataDir: d.Cfg.Upgrade.DataDir, // --link upgrades in place into the same datadir layout
	}
}

// --- PromoteN1 ---

type promoteN1 struct{ d Deps }

func (s *promoteN1) ID() runner.StepID { return "PromoteN1" }
func (s *promoteN1) Check(ctx context.Context) (bool, error) {
	inRec, err := s.d.N1.IsInRecovery(ctx)
	if err != nil {
		return false, err
	}
	return !inRec, nil // already promoted = done
}
func (s *promoteN1) Run(ctx context.Context) error {
	return s.d.Tools.Promote(ctx, s.d.Cfg.Upgrade.DataDir)
}

// --- ShutdownN1Clean ---

type shutdownN1Clean struct{ d Deps }

func (s *shutdownN1Clean) ID() runner.StepID { return "ShutdownN1Clean" }
func (s *shutdownN1Clean) Check(ctx context.Context) (bool, error) {
	cd, err := s.d.Tools.ControlData(ctx, s.d.Cfg.Upgrade.DataDir)
	if err != nil {
		return false, err
	}
	return cd.State == "shut down", nil
}
func (s *shutdownN1Clean) Run(ctx context.Context) error {
	// Flush dirty pages before stopping (spec: two checkpoints).
	if err := s.d.N1.Checkpoint(ctx); err != nil {
		return err
	}
	if err := s.d.N1.Checkpoint(ctx); err != nil {
		return err
	}
	return s.d.Tools.StopClean(ctx, s.d.Cfg.Upgrade.DataDir)
}

// --- RunPgUpgradeCheck ---

type runPgUpgradeCheck struct{ d Deps }

func (s *runPgUpgradeCheck) ID() runner.StepID { return "RunPgUpgradeCheck" }
func (s *runPgUpgradeCheck) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.PgUpgradeCheckPassed, nil
}
func (s *runPgUpgradeCheck) Run(ctx context.Context) error {
	if err := s.d.Tools.UpgradeCheck(ctx, s.d.upgradeOpts()); err != nil {
		return err
	}
	return s.d.Mgr.SetPgUpgradeCheckPassed()
}

// --- RunPgUpgrade (point of no return) ---

type runPgUpgrade struct{ d Deps }

func (s *runPgUpgrade) ID() runner.StepID { return "RunPgUpgrade" }
func (s *runPgUpgrade) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.PgUpgradeDone, nil
}
func (s *runPgUpgrade) Run(ctx context.Context) error {
	if err := s.d.Tools.Upgrade(ctx, s.d.upgradeOpts()); err != nil {
		return err
	}
	cd, err := s.d.Tools.ControlData(ctx, s.d.Cfg.Upgrade.DataDir)
	if err != nil {
		return err
	}
	if cd.SystemID == "" {
		return fmt.Errorf("upgrade: could not read PG17 system identifier after pg_upgrade")
	}
	return s.d.Mgr.SetPgUpgradeDone(cd.SystemID)
}

// --- WriteFinalPatroniConfig ---

type writeFinalPatroniConfig struct{ d Deps }

func (s *writeFinalPatroniConfig) ID() runner.StepID { return "WriteFinalPatroniConfig" }
func (s *writeFinalPatroniConfig) Check(context.Context) (bool, error) {
	path := s.d.Cfg.Upgrade.PatroniConfigPath
	if path == "" {
		return false, nil
	}
	if _, err := os.Stat(path); err == nil {
		return true, nil
	}
	return false, nil
}
func (s *writeFinalPatroniConfig) Run(context.Context) error {
	sysid := s.d.Mgr.Get().Artifacts.PG17SYSID
	if sysid == "" {
		return fmt.Errorf("upgrade: PG17 sysid unknown; cannot write Patroni config")
	}
	path := s.d.Cfg.Upgrade.PatroniConfigPath
	if path == "" {
		return fmt.Errorf("upgrade: patroni_config_path not configured")
	}
	content := fmt.Sprintf("# generated by pg-upgrade\nscope: %s\n# PG17 system identifier: %s\n",
		s.d.Cfg.ClusterName, sysid)
	return os.WriteFile(path, []byte(content), 0o644)
}

var (
	_ runner.Step = (*promoteN1)(nil)
	_ runner.Step = (*shutdownN1Clean)(nil)
	_ runner.Step = (*runPgUpgradeCheck)(nil)
	_ runner.Step = (*runPgUpgrade)(nil)
	_ runner.Step = (*writeFinalPatroniConfig)(nil)
)
```

(`state.SetPgUpgradeDone(sysid)` sets both `Artifacts.PgUpgradeDone=true` and
`Artifacts.PG17SYSID=sysid`, which is what the test asserts.)

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/phases/ -run TestUpgrade`
Expected: PASS. Then the whole phases package: `go test ./internal/phases/`.

- [ ] **Step 5: Commit**

```bash
git add internal/phases/upgrade.go internal/phases/upgrade_test.go internal/phases/prepare_test.go
git commit -m "feat(pg-upgrade): phase 4 Upgrade (promote, shutdown, pg_upgrade, patroni config)"
```

---

## Task 10: Registry + `pg-upgrade run` CLI wiring

**Files:**
- Create: `internal/phases/registry.go`
- Test: `internal/phases/registry_test.go`
- Modify: `cmd/pg-upgrade/main.go`

- [ ] **Step 1: Write the failing test `internal/phases/registry_test.go`**

```go
package phases

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPhases1to4Registry(t *testing.T) {
	ps := Phases1to4(Deps{})
	require.Len(t, ps, 4)
	ids := []string{}
	for _, p := range ps {
		ids = append(ids, p.ID())
	}
	assert.Equal(t, []string{"prepare", "isolate", "drain", "upgrade"}, ids)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/phases/ -run TestPhases1to4Registry`
Expected: FAIL — `undefined: Phases1to4`.

- [ ] **Step 3: Write `internal/phases/registry.go`**

```go
package phases

import "github.com/dmbabuev/pg-upgrade/internal/runner"

// Phases1to4 returns the ordered phases implemented in Plan 2. The first phase
// ("prepare") is the run's entry point.
func Phases1to4(d Deps) []runner.Phase {
	return []runner.Phase{
		NewPrepare(d),
		NewIsolate(d),
		NewDrain(d),
		NewUpgrade(d),
	}
}

// FirstPhase is the entry-point phase id for a fresh run.
const FirstPhase = "prepare"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/phases/`
Expected: PASS.

- [ ] **Step 5: Add the `run` subcommand to `cmd/pg-upgrade/main.go`**

Add the import block entries and register the command in `rootCmd()` (`root.AddCommand(runCmd(&cfgPath))`). Append this function and helpers:

```go
func runCmd(cfgPath *string) *cobra.Command {
	var statePath string
	var headless bool

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Drive the upgrade through phases 1-4 (Prepare → Upgrade)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(*cfgPath)
			if err != nil {
				return err
			}
			if statePath == "" {
				statePath = "pg-upgrade-state.json"
			}

			ctx := context.Background()

			// State: resume if present, else start fresh at the first phase.
			// NewManager always starts Current at phases.FirstPhase ("prepare");
			// resume an in-progress run by loading the existing state file.
			var mgr *state.Manager
			if _, statErr := os.Stat(statePath); statErr == nil {
				mgr, err = state.LoadManager(statePath)
			} else {
				mgr, err = state.NewManager(statePath, cfg.ClusterName)
			}
			if err != nil {
				return err
			}

			// N1-local PG client.
			n1DSN, err := connect.DSNForHost(cfg.PG.SuperuserDSN, "localhost")
			if err != nil {
				return err
			}
			n1, err := pgclient.NewFromDSN(ctx, n1DSN)
			if err != nil {
				return err
			}
			defer n1.Close()

			// Primary client provider (resolved after DiscoverTopology).
			primaryProvider := newPrimaryProvider(cfg.PG.SuperuserDSN, mgr)

			patClient := patroni.NewHTTPClient("http://localhost:8008")

			d := phases.Deps{
				Cfg:     *cfg,
				Mgr:     mgr,
				Patroni: patClient,
				Tools:   pgbin.Exec{NewBindir: cfg.Upgrade.NewPGBindir, OldBindir: cfg.Upgrade.OldPGBindir},
				N1:      n1,
				Primary: primaryProvider,
				Drain:   slotdrain.Drain,
			}

			mode := runner.Interactive
			var cp runner.Checkpoint
			if headless {
				mode = runner.Headless
			} else {
				cp = runner.InteractiveCheckpoint(os.Stdin, os.Stdout, runner.DefaultPrompts())
			}

			r := runner.New(phases.Phases1to4(d), mgr, mode, cp)
			if err := r.Run(ctx); err != nil {
				return err
			}
			fmt.Fprintln(os.Stdout, "\nReached point of no return (pg_upgrade complete). Phases 5-8 (Catchup→Cleanup) arrive in Plan 3.")
			return nil
		},
	}
	cmd.Flags().StringVar(&statePath, "state", "", "Path to state file (default pg-upgrade-state.json)")
	cmd.Flags().BoolVar(&headless, "headless", false, "Skip operator checkpoints (full automation)")
	return cmd
}

// newPrimaryProvider returns a provider that builds (once) a PG client to the
// primary host recorded in state by DiscoverTopology.
func newPrimaryProvider(template string, mgr *state.Manager) func(context.Context) (pgclient.Client, error) {
	var cached pgclient.Client
	return func(ctx context.Context) (pgclient.Client, error) {
		if cached != nil {
			return cached, nil
		}
		host := mgr.Get().Artifacts.PrimaryHost
		if host == "" {
			return nil, fmt.Errorf("run: primary host not yet discovered")
		}
		dsn, err := connect.DSNForHost(template, host)
		if err != nil {
			return nil, err
		}
		c, err := pgclient.NewFromDSN(ctx, dsn)
		if err != nil {
			return nil, err
		}
		cached = c
		return cached, nil
	}
}
```

Update the import block of `cmd/pg-upgrade/main.go` to include:

```go
	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	pgclient "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/clients/pgbin"
	"github.com/dmbabuev/pg-upgrade/internal/connect"
	"github.com/dmbabuev/pg-upgrade/internal/phases"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/dmbabuev/pg-upgrade/internal/slotdrain"
	"github.com/dmbabuev/pg-upgrade/internal/state"
```

Confirmed foundation constructors (use exactly these): `pgclient.NewFromDSN(ctx, dsn) (*PoolClient, error)` (satisfies `pgclient.Client`); `patroni.NewHTTPClient(baseURL) *HTTPClient` (satisfies `patroni.Client`); `state.NewManager(path, clusterName)` / `state.LoadManager(path)`. `phases.Deps.Primary` and `Deps.N1` are typed `pgclient.Client`; `*PoolClient` satisfies it, so the provider's `pgclient.Client` return type lines up directly.

- [ ] **Step 6: Build and smoke-test**

Run: `go build ./...`
Expected: builds clean.

Run: `go vet ./... && gofmt -l . && go test ./...`
Expected: vet clean; gofmt prints nothing; all tests PASS.

Run: `go run ./cmd/pg-upgrade/ run --help`
Expected: shows the `run` command with `--state` and `--headless` flags.

- [ ] **Step 7: Commit**

```bash
git add internal/phases/registry.go internal/phases/registry_test.go cmd/pg-upgrade/main.go
git commit -m "feat(pg-upgrade): phase registry + run command wiring (phases 1-4)"
```

---

## Final verification

- [ ] All packages build: `go build ./...`
- [ ] All tests pass: `go test ./...`
- [ ] No vet/format issues: `go vet ./... && gofmt -l .`
- [ ] `pg-upgrade run --help` lists `--state` and `--headless`
- [ ] Spec coverage: FSM Runner loop (Task 4), checkpoint no-op in Headless (Tasks 4-5), Phase 1 Prepare 5 steps (Task 6), Phase 2 Isolate 5 steps + `confirmed_flush ≤ target` invariant (Task 7), Phase 3 Drain 2 steps (Task 8), Phase 4 Upgrade 5 steps + point-of-no-return terminal (Task 9), dual primary/N1 connection model (Tasks 1, 10), idempotent `Check()` per step. Phases 5-8 intentionally deferred to Plan 3.

## Known follow-ups for Plan 3
- Phases 5-8 (Catchup, Switchover, Finalize, Cleanup), including sequence sync, DSN-swap notification, reverse logical replication, and cluster rename.
- Wire `pg-upgrade status` to render the real state file (currently a placeholder).
- Add Upgrade→Catchup transition to `NewUpgrade` once Catchup exists.
