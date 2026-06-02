# pg-upgrade Plan 3: Phases 5-6 (Catchup + Switchover) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `pg-upgrade run` through phases 5 (Catchup) and 6 (Switchover) — start PG17, subscribe and catch up, freeze the old primary, sync sequences, set up the reverse-replication rollback window, and signal the external DSN swap — leaving the run paused at the rollback window.

**Architecture:** New phases `catchup` and `switchover` are added to `internal/phases`, reusing the Plan 2 `simplePhase`/`Deps`/`runner` machinery. The binary performs the PG-level and SQL work itself (subscription, lag wait, DML-trigger freeze, sequence sync, reverse replication, forward-subscription disable) and **delegates genuinely-external/multi-node actions** (forming the new Patroni cluster, the DSN swap) to the operator via a signal file + interactive checkpoint, then verifies them over SQL/REST. Side effects that need a live cluster stay behind thin wrappers; FSM/SQL/idempotency logic is unit-tested with fakes/pgxmock/in-memory state.

**Tech Stack:** Go 1.25, pgx/v5, pglogrepl, cobra, yaml.v3, testify, pgxmock/v3. Module `github.com/dmbabuev/pg-upgrade`.

**Spec:** `docs/superpowers/specs/2026-06-01-pg-upgrade-design.md` (Phases 5-6). Phases 7-8 (Finalize/Cleanup) are deferred to Plan 4.

**Scope decisions (agreed):**
- Plan covers phases 5-6 only. Phase 6 (switchover) is terminal in this plan; the run pauses at the rollback window. Phase 7 adds the `switchover → finalize` transition.
- **Delegation boundary:** forming the new Patroni cluster (and adding replicas) and the DSN swap are operator/external-tooling actions. The binary pauses at a checkpoint and **verifies** the result (new cluster healthy via its Patroni REST; traffic arriving on the new primary). The binary does NOT run etcdctl or bootstrap Patroni in this plan.

---

## Existing foundation reused

- `internal/runner`: `Runner`, `New`, `simplePhase` lives in `internal/phases/prepare.go`, `Step`/`Phase`/`Transition`, `Checkpoint`, `DefaultPrompts`.
- `internal/phases`: `Deps`, `NewPrepare/NewIsolate/NewDrain/NewUpgrade`, `Phases1to4`, `FirstPhase`.
- `internal/state`: `Manager` with `SetSequencesSynced()`, `SetDSNSwapNotified()`, artifacts `SequencesSynced bool`, `DSNSwapNotified bool`, `PG17SYSID string`.
- `internal/clients/pg`: `CreateSubscription(ctx, name, connStr, pubName, slotName)` (forces `create_slot=false`), `GetSubscriptionLag(ctx, name) (*SubscriptionLag{WriteLagMs,FlushLagMs,ReplayLagMs}, error)` (nil if no row), `GetAllSequences(ctx) ([]SequenceInfo{Schema,Name,LastValue}, error)` (PoolClient only), `SetSequenceValue(ctx, schema, name, value)`, `FreezeForUpgrade(ctx, dbname)`, `CreatePublication(ctx, name)`, `CreateLogicalSlot(ctx, name, plugin)`.
- `internal/clients/patroni`: `Client{GetCluster, Pause, Resume}`, `ClusterInfo.Leader()`, `Member{Role}`. `NewHTTPClient(baseURL)`.
- `internal/clients/pgbin`: `PGTools{ OldControlData, NewControlData, Promote, StopClean, Restart, UpgradeCheck, Upgrade }`, `Exec`.
- `internal/connect`: `DSNForHost(template, host)`.

---

## File Structure (new/modified in this plan)

```
internal/config/config.go            # add catchup/switchover fields (subscription, reverse names, dbname, pg17_dsn, new_patroni_url, signal path, sequence_buffer); extend ValidateForRun
internal/clients/pg/client.go        # add DisableSubscription, CreateSubscriptionCreatingSlot
internal/clients/pgbin/pgbin.go      # add Start (pg_ctl start)
internal/phases/deps.go              # add PG17 provider, NewPatroni client, Signal writer
internal/phases/signal.go            # WriteDSNSwapSignal helper (+ payload type)
internal/phases/catchup.go           # Phase 5 (StartPG17, CreateForwardSubscription, WaitLagZero, VerifyNewClusterHealthy)
internal/phases/switchover.go        # Phase 6 (Freeze, WaitFinalLagZero, SyncSequences, SetupReverseReplication, NotifyDSNSwap, VerifyTrafficOnNew, DisableForwardSubscription)
internal/phases/registry.go          # Phases1to6; add upgrade->catchup transition
cmd/pg-upgrade/main.go               # wire PG17 provider, NewPatroni, signal writer into Deps; use Phases1to6
internal/runner/checkpoint.go        # add catchup/switchover prompts to DefaultPrompts
```

**Phase/Step IDs (exact strings):** phases `"catchup"`, `"switchover"`. Steps: `StartPG17OnN1`, `CreateForwardSubscription`, `WaitLagZero`, `VerifyNewClusterHealthy`, `FreezeOldPrimary`, `WaitFinalLagZero`, `SyncSequences`, `SetupReverseReplication`, `NotifyDSNSwap`, `VerifyTrafficOnNew`, `DisableForwardSubscription`.

---

## Task 1: Config + pg client methods + pgbin.Start

**Files:**
- Modify: `internal/config/config.go`, `internal/clients/pg/client.go`, `internal/clients/pgbin/pgbin.go`
- Test: `internal/clients/pg/client_test.go`

- [ ] **Step 1: Extend `UpgradeConfig` (config.go)**

Add these fields to `UpgradeConfig` (after `NewDataDir`):
```go
	SubscriptionName  string `yaml:"subscription_name"`
	ReversePubName    string `yaml:"reverse_pub_name"`
	ReverseSubName    string `yaml:"reverse_sub_name"`
	DBName            string `yaml:"dbname"`
	PG17DSN           string `yaml:"pg17_dsn"`
	NewPatroniURL     string `yaml:"new_patroni_url"`
	DSNSwapSignalPath string `yaml:"dsn_swap_signal_path"`
	SequenceBuffer    int64  `yaml:"sequence_buffer"`
```

- [ ] **Step 2: Extend `ValidateForRun` (config.go)** — append these required checks inside the existing `ValidateForRun`, before the final `return nil`:
```go
	if u.SubscriptionName == "" {
		missing = append(missing, "subscription_name")
	}
	if u.DBName == "" {
		missing = append(missing, "dbname")
	}
	if u.PG17DSN == "" {
		missing = append(missing, "pg17_dsn")
	}
	if u.NewPatroniURL == "" {
		missing = append(missing, "new_patroni_url")
	}
	if u.DSNSwapSignalPath == "" {
		missing = append(missing, "dsn_swap_signal_path")
	}
	if u.ReversePubName == "" {
		missing = append(missing, "reverse_pub_name")
	}
	if u.ReverseSubName == "" {
		missing = append(missing, "reverse_sub_name")
	}
```
(`missing` is the existing slice already declared in `ValidateForRun`.) Run `go test ./internal/config/` — the existing `TestValidateForRun` will now fail because its valid config lacks the new fields; update that test's first `Config` literal to include `SubscriptionName: "sub_upgrade", ReversePubName: "pub_rb", ReverseSubName: "sub_rb", DBName: "app", PG17DSN: "host=localhost port=5433", NewPatroniURL: "http://localhost:8009", DSNSwapSignalPath: "/run/sig.json"` so it stays valid.

- [ ] **Step 3: Write the failing pg client tests** (append to `internal/clients/pg/client_test.go`)
```go
func TestDisableSubscription(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("ALTER SUBSCRIPTION .* DISABLE").WillReturnResult(pgxmock.NewResult("ALTER", 0))

	c := pgclient.NewFromPool(mock)
	require.NoError(t, c.DisableSubscription(context.Background(), "sub_upgrade"))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateSubscriptionCreatingSlot(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CREATE SUBSCRIPTION .* create_slot = true").
		WillReturnResult(pgxmock.NewResult("CREATE SUBSCRIPTION", 0))

	c := pgclient.NewFromPool(mock)
	require.NoError(t, c.CreateSubscriptionCreatingSlot(context.Background(), "sub_rollback", "host=n1", "pub_rollback"))
	assert.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 4: Run to verify failure** — `go test ./internal/clients/pg/ -run 'TestDisableSubscription|TestCreateSubscriptionCreatingSlot'` → FAIL (undefined).

- [ ] **Step 5: Add the pg client methods (client.go)**

Add to the `Client` interface (after `CreateSubscription`):
```go
	CreateSubscriptionCreatingSlot(ctx context.Context, name, connStr, pubName string) error
	DisableSubscription(ctx context.Context, name string) error
```
Add `internalClient` implementations (reuse the existing `quoteString` helper for the DSN literal and `pgx.Identifier{}.Sanitize()` for identifiers, matching `CreateSubscription`):
```go
// CreateSubscriptionCreatingSlot creates a subscription that CREATES its own
// replication slot on the publisher (create_slot=true). Used for the reverse
// rollback subscription, whose slot does not pre-exist.
func (c *internalClient) CreateSubscriptionCreatingSlot(ctx context.Context, name, connStr, pubName string) error {
	sql := fmt.Sprintf(
		"CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s "+
			"WITH (copy_data = false, create_slot = true, enabled = true)",
		pgx.Identifier{name}.Sanitize(), quoteString(connStr), pgx.Identifier{pubName}.Sanitize())
	_, err := c.q.Exec(ctx, sql)
	return err
}

// DisableSubscription stops a subscription's apply worker without dropping it.
func (c *internalClient) DisableSubscription(ctx context.Context, name string) error {
	_, err := c.q.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s DISABLE", pgx.Identifier{name}.Sanitize()))
	return err
}
```
Add the `PoolClient` delegations (receiver `p`, via `p.ic()`):
```go
func (p *PoolClient) CreateSubscriptionCreatingSlot(ctx context.Context, name, connStr, pubName string) error {
	return p.ic().CreateSubscriptionCreatingSlot(ctx, name, connStr, pubName)
}

func (p *PoolClient) DisableSubscription(ctx context.Context, name string) error {
	return p.ic().DisableSubscription(ctx, name)
}
```

- [ ] **Step 6: Add `pgbin.Start` (pgbin.go)**

Add to the `PGTools` interface (after `Restart`):
```go
	Start(ctx context.Context, bindir, dataDir string) error
```
Implement on `Exec`:
```go
// Start launches a stopped cluster with the given bindir's pg_ctl, waiting for
// readiness. Used to bring PG17 up after pg_upgrade for the catchup subscription.
func (e Exec) Start(ctx context.Context, bindir, dataDir string) error {
	return run(exec.CommandContext(ctx, e.bin(bindir, "pg_ctl"), "start", "-w", "-D", dataDir), "start")
}
```
Update the `fakeTools` in `internal/phases/upgrade_test.go`: add `started bool` and `func (f *fakeTools) Start(context.Context, string, string) error { f.started = true; return nil }`.

- [ ] **Step 7: Run + commit**
```bash
go build ./... && go test ./internal/clients/pg/ ./internal/config/ ./internal/phases/ && gofmt -l . && go vet ./...
git add internal/config/config.go internal/clients/pg/client.go internal/clients/pg/client_test.go internal/clients/pgbin/pgbin.go internal/phases/upgrade_test.go
git commit -m "feat(pg-upgrade): config + pg DisableSubscription/CreateSubscriptionCreatingSlot + pgbin.Start"
```

---

## Task 2: Deps additions + DSN-swap signal writer

**Files:**
- Modify: `internal/phases/deps.go`
- Create: `internal/phases/signal.go`
- Test: `internal/phases/signal_test.go`

- [ ] **Step 1: Extend `Deps` (deps.go)** — add fields (after `Drain`):
```go
	// PG17 returns a client to the upgraded PG17 on N1 (new cluster primary).
	PG17 func(ctx context.Context) (pg.Client, error)

	// NewPatroni is the new cluster's Patroni REST client (separate from Patroni,
	// which manages the old, paused cluster).
	NewPatroni patroni.Client

	// WriteSignal persists the DSN-swap signal file (injected for testability).
	WriteSignal func(path string, data []byte) error
```

- [ ] **Step 2: Write the failing test `internal/phases/signal_test.go`**
```go
package phases

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDSNSwapSignalPayload(t *testing.T) {
	var captured []byte
	write := func(_ string, data []byte) error { captured = data; return nil }
	err := WriteDSNSwapSignal(write, "/run/sig.json", "host=n1 port=5433 dbname=app", "prod")
	require.NoError(t, err)

	var p DSNSwapSignal
	require.NoError(t, json.Unmarshal(captured, &p))
	assert.Equal(t, "host=n1 port=5433 dbname=app", p.NewPrimaryDSN)
	assert.Equal(t, "prod", p.ClusterName)
	assert.Equal(t, "swap-dsn", p.Action)
}
```

- [ ] **Step 3: Run to verify failure** — `go test ./internal/phases/ -run TestDSNSwapSignal` → FAIL (undefined).

- [ ] **Step 4: Write `internal/phases/signal.go`**
```go
package phases

import (
	"encoding/json"
	"fmt"
	"time"
)

// DSNSwapSignal is the payload the binary writes for external tooling to perform
// the client DSN swap from the old primary to the new PG17 cluster.
type DSNSwapSignal struct {
	Action        string    `json:"action"` // always "swap-dsn"
	ClusterName   string    `json:"cluster_name"`
	NewPrimaryDSN string    `json:"new_primary_dsn"`
	WrittenAt     time.Time `json:"written_at"`
}

// WriteDSNSwapSignal serializes the swap signal and persists it via write.
func WriteDSNSwapSignal(write func(path string, data []byte) error, path, newDSN, clusterName string) error {
	payload := DSNSwapSignal{
		Action:        "swap-dsn",
		ClusterName:   clusterName,
		NewPrimaryDSN: newDSN,
		WrittenAt:     time.Now().UTC(),
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("signal: marshal: %w", err)
	}
	if err := write(path, data); err != nil {
		return fmt.Errorf("signal: write %s: %w", path, err)
	}
	return nil
}
```

- [ ] **Step 5: Run + commit**
```bash
go build ./... && go test ./internal/phases/ -run TestDSNSwapSignal && gofmt -l internal/phases/ && go vet ./internal/phases/
git add internal/phases/deps.go internal/phases/signal.go internal/phases/signal_test.go
git commit -m "feat(pg-upgrade): Deps PG17/NewPatroni/WriteSignal + DSN-swap signal payload"
```

---

## Task 3: Phase 5 (Catchup)

**Files:**
- Create: `internal/phases/catchup.go`
- Test: `internal/phases/catchup_test.go`

The forward subscription connects PG17 → old primary's publication, reusing the
drained slot (`create_slot=false`, `slot_name`). `VerifyNewClusterHealthy` is the
delegated step: it verifies (via the new cluster's Patroni REST) that the
operator-formed cluster has a leader and at least one replica.

- [ ] **Step 1: Write the failing test `internal/phases/catchup_test.go`**
```go
package phases

import (
	"context"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// catchup-phase methods on the shared fakePG
func (f *fakePG) GetSubscriptionLag(_ context.Context, name string) (*pg.SubscriptionLag, error) {
	return f.subLag, nil // nil until CreateSubscription sets it
}
func (f *fakePG) CreateSubscription(_ context.Context, name, connStr, pubName, slotName string) error {
	f.createdSub = name
	f.subLag = &pg.SubscriptionLag{} // subscription now exists with zero lag
	return nil
}

func TestCatchupCreatesSubAndWaitsLag(t *testing.T) {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade", "catchup"} {
		require.NoError(t, mgr.Advance(p))
	}
	require.NoError(t, mgr.SetPrimaryHost("primary.host"))

	pg17 := &fakePG{} // PG17 "up" (Check skips Start); subscription created in the loop
	oldPrimary := &fakePG{}
	tools := &fakeTools{}
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "n1", Host: "n1", Role: "leader"},
		{Name: "n2", Host: "n2", Role: "replica"},
	}}}
	d := Deps{
		Cfg: config.Config{Upgrade: config.UpgradeConfig{
			SubscriptionName: "sub_up", PublicationName: "pub_up", SlotName: "slot_up",
			NewPGBindir: "/n", NewDataDir: "/nd",
		}, PG: config.PGConfig{SuperuserDSN: "host=tmpl"}},
		Mgr: mgr, Tools: tools, NewPatroni: newPat,
		PG17:    func(context.Context) (pg.Client, error) { return pg17, nil },
		Primary: func(context.Context) (pg.Client, error) { return oldPrimary, nil },
	}

	ph := NewCatchup(d)
	assert.Equal(t, "catchup", ph.ID())
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.Equal(t, "sub_up", pg17.createdSub)
}

func TestStartPG17Runs(t *testing.T) {
	tools := &fakeTools{}
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{NewPGBindir: "/n", NewDataDir: "/nd"}}, Tools: tools}
	require.NoError(t, (&startPG17{d}).Run(context.Background()))
	assert.True(t, tools.started)
}

func TestCatchupTransitionsToSwitchover(t *testing.T) {
	ph := NewCatchup(Deps{})
	tr := ph.Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "switchover", tr[0].To)
}
```
Add `subLag *pg.SubscriptionLag` and `createdSub string` fields to the shared `fakePG` struct in `internal/phases/prepare_test.go`.

- [ ] **Step 2: Run to verify failure** — `go test ./internal/phases/ -run TestCatchup` → FAIL (`undefined: NewCatchup`).

- [ ] **Step 3: Write `internal/phases/catchup.go`**
```go
package phases

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/connect"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewCatchup builds Phase 5: start PG17, subscribe to the old primary, catch up,
// and verify the operator-formed new Patroni cluster is healthy.
func NewCatchup(d Deps) runner.Phase {
	return &simplePhase{
		id: "catchup",
		steps: []runner.Step{
			&startPG17{d},
			&createForwardSubscription{d},
			&waitLagZero{d},
			&verifyNewClusterHealthy{d},
		},
		trans: []runner.Transition{{To: "switchover"}},
	}
}

// --- StartPG17OnN1 ---

type startPG17 struct{ d Deps }

func (s *startPG17) ID() runner.StepID { return "StartPG17OnN1" }
func (s *startPG17) Check(ctx context.Context) (bool, error) {
	// done if PG17 already accepts connections
	c, err := s.d.PG17(ctx)
	if err != nil {
		return false, nil // not up yet
	}
	if _, err := c.IsInRecovery(ctx); err != nil {
		return false, nil
	}
	return true, nil
}
func (s *startPG17) Run(ctx context.Context) error {
	return s.d.Tools.Start(ctx, s.d.Cfg.Upgrade.NewPGBindir, s.d.Cfg.Upgrade.NewDataDir)
}

// --- CreateForwardSubscription (PG17 subscribes to old primary's publication) ---

type createForwardSubscription struct{ d Deps }

func (s *createForwardSubscription) ID() runner.StepID { return "CreateForwardSubscription" }
func (s *createForwardSubscription) Check(ctx context.Context) (bool, error) {
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return false, err
	}
	lag, err := pg17.GetSubscriptionLag(ctx, s.d.Cfg.Upgrade.SubscriptionName)
	if err != nil {
		return false, err
	}
	return lag != nil, nil // subscription exists (has a stat row)
}
func (s *createForwardSubscription) Run(ctx context.Context) error {
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return err
	}
	primaryDSN, err := connect.DSNForHost(s.d.Cfg.PG.SuperuserDSN, s.d.Mgr.Get().Artifacts.PrimaryHost)
	if err != nil {
		return err
	}
	return pg17.CreateSubscription(ctx,
		s.d.Cfg.Upgrade.SubscriptionName, primaryDSN, s.d.Cfg.Upgrade.PublicationName, s.d.Cfg.Upgrade.SlotName)
}

// --- WaitLagZero ---

type waitLagZero struct{ d Deps }

func (s *waitLagZero) ID() runner.StepID { return "WaitLagZero" }
func (s *waitLagZero) Check(ctx context.Context) (bool, error) { return s.zero(ctx) }
func (s *waitLagZero) Run(ctx context.Context) error {
	zero, err := s.zero(ctx)
	if err != nil {
		return err
	}
	if !zero {
		return fmt.Errorf("catchup: subscription lag not yet zero; re-run pg-upgrade to retry")
	}
	return nil
}
func (s *waitLagZero) zero(ctx context.Context) (bool, error) {
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return false, err
	}
	lag, err := pg17.GetSubscriptionLag(ctx, s.d.Cfg.Upgrade.SubscriptionName)
	if err != nil {
		return false, err
	}
	if lag == nil {
		return false, fmt.Errorf("catchup: subscription %s not found", s.d.Cfg.Upgrade.SubscriptionName)
	}
	return lag.WriteLagMs == 0 && lag.FlushLagMs == 0 && lag.ReplayLagMs == 0, nil
}

// --- VerifyNewClusterHealthy (delegated formation; binary verifies) ---

type verifyNewClusterHealthy struct{ d Deps }

func (s *verifyNewClusterHealthy) ID() runner.StepID { return "VerifyNewClusterHealthy" }
func (s *verifyNewClusterHealthy) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifyNewClusterHealthy) Run(ctx context.Context) error {
	cluster, err := s.d.NewPatroni.GetCluster(ctx)
	if err != nil {
		return err
	}
	if cluster.Leader() == nil {
		return fmt.Errorf("catchup: new Patroni cluster has no leader (form the new cluster, then re-run)")
	}
	replicas := 0
	for _, m := range cluster.Members {
		if m.Role == "replica" {
			replicas++
		}
	}
	if replicas < 1 {
		return fmt.Errorf("catchup: new Patroni cluster has no replica yet (add replicas, then re-run)")
	}
	return nil
}

var (
	_ runner.Step = (*startPG17)(nil)
	_ runner.Step = (*createForwardSubscription)(nil)
	_ runner.Step = (*waitLagZero)(nil)
	_ runner.Step = (*verifyNewClusterHealthy)(nil)
)
```

- [ ] **Step 4: Run + commit**
```bash
go test ./internal/phases/ -run TestCatchup && go test ./internal/phases/ && gofmt -l internal/phases/ && go vet ./internal/phases/
git add internal/phases/catchup.go internal/phases/catchup_test.go internal/phases/prepare_test.go
git commit -m "feat(pg-upgrade): phase 5 Catchup (start PG17, subscribe, wait lag, verify new cluster)"
```

---

## Task 4: Phase 6 part 1 (Freeze, WaitFinalLagZero, SyncSequences)

**Files:**
- Create: `internal/phases/switchover.go`
- Test: `internal/phases/switchover_test.go`

- [ ] **Step 1: Write the failing test `internal/phases/switchover_test.go`**
```go
package phases

import (
	"context"
	"testing"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// switchover-phase methods on the shared fakePG
func (f *fakePG) FreezeForUpgrade(_ context.Context, dbname string) error { f.frozen = dbname; return nil }
func (f *fakePG) GetAllSequences(context.Context) ([]pg.SequenceInfo, error) {
	return f.sequences, nil
}
func (f *fakePG) SetSequenceValue(_ context.Context, schema, name string, value int64) error {
	f.setSeqs = append(f.setSeqs, seqSet{schema, name, value})
	return nil
}

func switchoverDeps(t *testing.T, pg17, oldPrimary *fakePG) Deps {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade", "catchup", "switchover"} {
		require.NoError(t, mgr.Advance(p))
	}
	require.NoError(t, mgr.SetPrimaryHost("primary.host"))
	return Deps{
		Cfg: config.Config{ClusterName: "prod", Upgrade: config.UpgradeConfig{
			SubscriptionName: "sub_up", ReversePubName: "pub_rb", ReverseSubName: "sub_rb",
			DBName: "app", PG17DSN: "host=n1 port=5433 dbname=app", SequenceBuffer: 1000,
			DSNSwapSignalPath: "/run/sig.json",
		}, PG: config.PGConfig{SuperuserDSN: "host=tmpl"}},
		Mgr:     mgr,
		PG17:    func(context.Context) (pg.Client, error) { return pg17, nil },
		Primary: func(context.Context) (pg.Client, error) { return oldPrimary, nil },
	}
}

func TestSwitchoverFreezeAndSyncSequences(t *testing.T) {
	pg17 := &fakePG{subLag: &pg.SubscriptionLag{}} // subscription exists, zero lag
	oldPrimary := &fakePG{sequences: []pg.SequenceInfo{{Schema: "public", Name: "s1", LastValue: 42}}}
	d := switchoverDeps(t, pg17, oldPrimary)

	steps := NewSwitchover(d).Steps()
	// run only the first three steps (freeze, wait final lag, sync sequences)
	for _, s := range steps[:3] {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.Equal(t, "app", oldPrimary.frozen)
	require.Len(t, pg17.setSeqs, 1)
	assert.Equal(t, int64(1042), pg17.setSeqs[0].value) // 42 + buffer 1000
	assert.True(t, d.Mgr.Get().Artifacts.SequencesSynced)
}
```
Add to the shared `fakePG` struct (in `prepare_test.go`): `frozen string`, `sequences []pg.SequenceInfo`, `setSeqs []seqSet`. Also add a small helper type to `prepare_test.go`:
```go
type seqSet struct {
	schema string
	name   string
	value  int64
}
```
`WaitFinalLagZero` reuses `GetSubscriptionLag` (defined on fakePG in catchup_test.go), so the switchover tests initialize `pg17` with a non-nil `subLag` to represent an existing zero-lag subscription.

- [ ] **Step 2: Run to verify failure** — `go test ./internal/phases/ -run TestSwitchoverFreeze` → FAIL (`undefined: NewSwitchover`).

- [ ] **Step 3: Write `internal/phases/switchover.go` (phase + first three steps)**
```go
package phases

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewSwitchover builds Phase 6: the critical section. Freeze the old primary,
// drain the final lag, sync sequences, set up reverse replication, signal the
// DSN swap, verify traffic moved, and disable the forward subscription. Terminal
// in this plan (the run pauses at the rollback window; Plan 4 adds Finalize).
// NOTE: Task 4 implements only the first three steps. Task 5 appends the
// remaining four to this slice (and defines their types), so the slice here
// lists ONLY the three steps Task 4 defines — otherwise the package won't build.
func NewSwitchover(d Deps) runner.Phase {
	return &simplePhase{
		id: "switchover",
		steps: []runner.Step{
			&freezeOldPrimary{d},
			&waitFinalLagZero{d},
			&syncSequences{d},
		},
		trans: nil, // terminal in Plan 3: paused at the rollback window
	}
}

// --- FreezeOldPrimary ---

type freezeOldPrimary struct{ d Deps }

func (s *freezeOldPrimary) ID() runner.StepID { return "FreezeOldPrimary" }
func (s *freezeOldPrimary) Check(context.Context) (bool, error) { return false, nil } // FreezeForUpgrade is idempotent
func (s *freezeOldPrimary) Run(ctx context.Context) error {
	old, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	return old.FreezeForUpgrade(ctx, s.d.Cfg.Upgrade.DBName)
}

// --- WaitFinalLagZero (drain the last changes after the freeze) ---

type waitFinalLagZero struct{ d Deps }

func (s *waitFinalLagZero) ID() runner.StepID { return "WaitFinalLagZero" }
func (s *waitFinalLagZero) Check(ctx context.Context) (bool, error) { return s.zero(ctx) }
func (s *waitFinalLagZero) Run(ctx context.Context) error {
	zero, err := s.zero(ctx)
	if err != nil {
		return err
	}
	if !zero {
		return fmt.Errorf("switchover: final lag not yet zero; re-run pg-upgrade to retry")
	}
	return nil
}
func (s *waitFinalLagZero) zero(ctx context.Context) (bool, error) {
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return false, err
	}
	lag, err := pg17.GetSubscriptionLag(ctx, s.d.Cfg.Upgrade.SubscriptionName)
	if err != nil {
		return false, err
	}
	if lag == nil {
		return false, fmt.Errorf("switchover: subscription %s not found", s.d.Cfg.Upgrade.SubscriptionName)
	}
	return lag.WriteLagMs == 0 && lag.FlushLagMs == 0 && lag.ReplayLagMs == 0, nil
}

// --- SyncSequences (read from frozen old primary, set on PG17 with a buffer) ---

type syncSequences struct{ d Deps }

func (s *syncSequences) ID() runner.StepID { return "SyncSequences" }
func (s *syncSequences) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.SequencesSynced, nil
}
func (s *syncSequences) Run(ctx context.Context) error {
	old, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return err
	}
	seqs, err := old.GetAllSequences(ctx)
	if err != nil {
		return err
	}
	for _, seq := range seqs {
		// Advance past the old value plus a safety buffer for cached/unflushed
		// nextval allocations on the old primary.
		if err := pg17.SetSequenceValue(ctx, seq.Schema, seq.Name, seq.LastValue+s.d.Cfg.Upgrade.SequenceBuffer); err != nil {
			return err
		}
	}
	return s.d.Mgr.SetSequencesSynced()
}
```

- [ ] **Step 4: Run + commit**
```bash
go test ./internal/phases/ -run TestSwitchoverFreeze && gofmt -l internal/phases/ && go vet ./internal/phases/
git add internal/phases/switchover.go internal/phases/switchover_test.go internal/phases/prepare_test.go
git commit -m "feat(pg-upgrade): phase 6 part 1 (freeze, final lag, sequence sync)"
```

---

## Task 5: Phase 6 part 2 (Reverse replication, DSN-swap signal, verify, disable forward)

**Files:**
- Modify: `internal/phases/switchover.go`, `internal/phases/switchover_test.go`

- [ ] **Step 1: Write the failing test** (append to `internal/phases/switchover_test.go`)
```go
// fakePG.CreatePublication already exists (prepare_test.go) and sets createdPub.
func (f *fakePG) CreateSubscriptionCreatingSlot(_ context.Context, name, connStr, pubName string) error {
	f.createdRevSub = name
	return nil
}
func (f *fakePG) DisableSubscription(_ context.Context, name string) error { f.disabledSub = name; return nil }
func (f *fakePG) CountAppBackends(context.Context) (int, error)            { return f.appBackends, nil }

func TestSwitchoverReverseSignalDisable(t *testing.T) {
	pg17 := &fakePG{subLag: &pg.SubscriptionLag{}, appBackends: 5}
	oldPrimary := &fakePG{}
	d := switchoverDeps(t, pg17, oldPrimary)
	var signalled string
	d.WriteSignal = func(path string, _ []byte) error { signalled = path; return nil }

	steps := NewSwitchover(d).Steps()
	for _, s := range steps[3:] { // reverse, notify, verify-traffic, disable-forward
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.Equal(t, "pub_rb", pg17.createdPub)        // reverse publication on PG17
	assert.Equal(t, "sub_rb", oldPrimary.createdRevSub) // reverse subscription on old primary
	assert.Equal(t, "/run/sig.json", signalled)
	assert.True(t, d.Mgr.Get().Artifacts.DSNSwapNotified)
	assert.Equal(t, "sub_up", pg17.disabledSub)
}
```
Add to the shared `fakePG` struct (in `prepare_test.go`): `createdRevSub string`, `disabledSub string`, `appBackends int`. (`createdPub` already exists.)

- [ ] **Step 2: Run to verify failure** — `go test ./internal/phases/ -run TestSwitchoverReverse` → FAIL (`undefined: CountAppBackends`, and the four step types/missing logic).

- [ ] **Step 3: Add a `CountAppBackends` pg method** (client.go) so VerifyTrafficOnNew can count non-system client connections.

Interface (after `DisableSubscription`):
```go
	CountAppBackends(ctx context.Context) (int, error)
```
internalClient:
```go
// CountAppBackends counts client backends excluding background/replication
// workers, used to confirm application traffic has moved to the new primary.
func (c *internalClient) CountAppBackends(ctx context.Context) (int, error) {
	var n int
	err := c.q.QueryRow(ctx,
		"SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()").
		Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("pg: count app backends: %w", err)
	}
	return n, nil
}
```
PoolClient:
```go
func (p *PoolClient) CountAppBackends(ctx context.Context) (int, error) {
	return p.ic().CountAppBackends(ctx)
}
```
Add a pgxmock test to `internal/clients/pg/client_test.go`:
```go
func TestCountAppBackends(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectQuery("client backend").WillReturnRows(pgxmock.NewRows([]string{"n"}).AddRow(3))
	c := pgclient.NewFromPool(mock)
	n, err := c.CountAppBackends(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 4: Append the four steps to `internal/phases/switchover.go`**

First extend `NewSwitchover`'s `steps` slice to include the four new steps after `&syncSequences{d}`:
```go
		steps: []runner.Step{
			&freezeOldPrimary{d},
			&waitFinalLagZero{d},
			&syncSequences{d},
			&setupReverseReplication{d},
			&notifyDSNSwap{d},
			&verifyTrafficOnNew{d},
			&disableForwardSubscription{d},
		},
```
Then append the four step types (and update the `var _ runner.Step` block to include all seven):
```go
// --- SetupReverseReplication (PG17 publishes; old primary subscribes back) ---

type setupReverseReplication struct{ d Deps }

func (s *setupReverseReplication) ID() runner.StepID { return "SetupReverseReplication" }
func (s *setupReverseReplication) Check(context.Context) (bool, error) { return false, nil }
func (s *setupReverseReplication) Run(ctx context.Context) error {
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return err
	}
	old, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	// Publication on the new primary.
	if err := pg17.CreatePublication(ctx, s.d.Cfg.Upgrade.ReversePubName); err != nil {
		return err
	}
	// Subscription on the old primary, pointing back at PG17 (creates its own slot
	// on PG17). The old primary's apply worker runs as session_replication_role
	// 'replica', so the DML freeze triggers do not fire for it.
	return old.CreateSubscriptionCreatingSlot(ctx, s.d.Cfg.Upgrade.ReverseSubName, s.d.Cfg.Upgrade.PG17DSN, s.d.Cfg.Upgrade.ReversePubName)
}

// --- NotifyDSNSwap (signal external tooling, then operator confirms via checkpoint) ---

type notifyDSNSwap struct{ d Deps }

func (s *notifyDSNSwap) ID() runner.StepID { return "NotifyDSNSwap" }
func (s *notifyDSNSwap) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.DSNSwapNotified, nil
}
func (s *notifyDSNSwap) Run(_ context.Context) error {
	if err := WriteDSNSwapSignal(s.d.WriteSignal, s.d.Cfg.Upgrade.DSNSwapSignalPath,
		s.d.Cfg.Upgrade.PG17DSN, s.d.Cfg.ClusterName); err != nil {
		return err
	}
	return s.d.Mgr.SetDSNSwapNotified()
}

// --- VerifyTrafficOnNew (delegated swap; binary verifies traffic arrived) ---

type verifyTrafficOnNew struct{ d Deps }

func (s *verifyTrafficOnNew) ID() runner.StepID { return "VerifyTrafficOnNew" }
func (s *verifyTrafficOnNew) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifyTrafficOnNew) Run(ctx context.Context) error {
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return err
	}
	n, err := pg17.CountAppBackends(ctx)
	if err != nil {
		return err
	}
	if n < 1 {
		return fmt.Errorf("switchover: no application traffic on the new primary yet (perform the DSN swap, then re-run)")
	}
	return nil
}

// --- DisableForwardSubscription (stop forward apply now that writes are on PG17) ---

type disableForwardSubscription struct{ d Deps }

func (s *disableForwardSubscription) ID() runner.StepID { return "DisableForwardSubscription" }
func (s *disableForwardSubscription) Check(context.Context) (bool, error) { return false, nil }
func (s *disableForwardSubscription) Run(ctx context.Context) error {
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return err
	}
	return pg17.DisableSubscription(ctx, s.d.Cfg.Upgrade.SubscriptionName)
}

var (
	_ runner.Step = (*freezeOldPrimary)(nil)
	_ runner.Step = (*waitFinalLagZero)(nil)
	_ runner.Step = (*syncSequences)(nil)
	_ runner.Step = (*setupReverseReplication)(nil)
	_ runner.Step = (*notifyDSNSwap)(nil)
	_ runner.Step = (*verifyTrafficOnNew)(nil)
	_ runner.Step = (*disableForwardSubscription)(nil)
)
```

- [ ] **Step 5: Run + commit**
```bash
go build ./... && go test ./internal/phases/ ./internal/clients/pg/ && gofmt -l . && go vet ./...
git add internal/phases/switchover.go internal/phases/switchover_test.go internal/phases/prepare_test.go internal/clients/pg/client.go internal/clients/pg/client_test.go
git commit -m "feat(pg-upgrade): phase 6 part 2 (reverse repl, DSN-swap signal, verify traffic, disable forward)"
```

---

## Task 6: Registry + checkpoint prompts + run wiring

**Files:**
- Modify: `internal/phases/registry.go`, `internal/phases/upgrade.go`, `internal/runner/checkpoint.go`, `cmd/pg-upgrade/main.go`
- Test: `internal/phases/registry_test.go`

- [ ] **Step 1: Add the `upgrade → catchup` transition (upgrade.go)** — change `NewUpgrade`'s `trans: nil` to:
```go
		trans: []runner.Transition{{To: "catchup"}},
```
(Upgrade is no longer terminal: phases 5-6 follow.) Then fix the now-stale assertion in `internal/phases/upgrade_test.go` `TestUpgradeHappyPath`: replace `assert.Empty(t, ph.Transitions()) // terminal in Plan 2` with:
```go
	require.Len(t, ph.Transitions(), 1)
	assert.Equal(t, "catchup", ph.Transitions()[0].To)
```

- [ ] **Step 2: Update the registry test (registry_test.go)** — replace `TestPhases1to4Registry` with:
```go
func TestPhases1to6Registry(t *testing.T) {
	ps := Phases1to6(Deps{})
	require.Len(t, ps, 6)
	ids := []string{}
	for _, p := range ps {
		ids = append(ids, p.ID())
	}
	assert.Equal(t, []string{"prepare", "isolate", "drain", "upgrade", "catchup", "switchover"}, ids)
}
```

- [ ] **Step 3: Run to verify failure** — `go test ./internal/phases/ -run TestPhases1to6Registry` → FAIL (`undefined: Phases1to6`).

- [ ] **Step 4: Replace `Phases1to4` with `Phases1to6` (registry.go)**
```go
// Phases1to6 returns the ordered phases implemented through Plan 3. The first
// phase ("prepare") is the run's entry point; "switchover" pauses at the
// rollback window (Plan 4 adds Finalize/Cleanup).
func Phases1to6(d Deps) []runner.Phase {
	return []runner.Phase{
		NewPrepare(d),
		NewIsolate(d),
		NewDrain(d),
		NewUpgrade(d),
		NewCatchup(d),
		NewSwitchover(d),
	}
}
```
Keep the `FirstPhase = "prepare"` const.

- [ ] **Step 5: Add catchup/switchover prompts (checkpoint.go)** — add to the `DefaultPrompts()` map:
```go
		"catchup":    "New cluster healthy, subscription at zero lag. Begin switchover (write freeze)?",
		"switchover": "DSN swapped, rollback window open. Proceed to Finalize (Plan 4 — no rollback after this)?",
```

- [ ] **Step 6: Wire the new Deps in `cmd/pg-upgrade/main.go`** — in `runCmd`'s RunE, build the PG17 provider, the new-cluster Patroni client, and the signal writer, and add them to the `phases.Deps` literal; switch `phases.Phases1to4` to `phases.Phases1to6`.

Add after the existing `primaryProvider, closePrimary := ...` block:
```go
			pg17Provider, closePG17 := newPG17Provider(cfg.Upgrade.PG17DSN)
			defer closePG17()
			newPat := patroni.NewHTTPClient(cfg.Upgrade.NewPatroniURL)
```
Add these fields to the `phases.Deps{...}` literal:
```go
				PG17:        pg17Provider,
				NewPatroni:  newPat,
				WriteSignal: func(path string, data []byte) error { return os.WriteFile(path, data, 0o644) },
```
Change `phases.Phases1to4(d)` to `phases.Phases1to6(d)`. Update the closing message printed after `r.Run` to:
```go
			fmt.Fprintln(os.Stdout, "\nReached the rollback window (DSN swapped, reverse replication active). Phases 7-8 (Finalize/Cleanup) arrive in Plan 4.")
```
Add the `newPG17Provider` helper (mirrors `newPrimaryProvider`, but the DSN is a fixed config value, not host-derived):
```go
// newPG17Provider lazily connects to the upgraded PG17 on N1, plus a closer.
func newPG17Provider(dsn string) (func(context.Context) (pgclient.Client, error), func()) {
	var cached pgclient.Client
	provider := func(ctx context.Context) (pgclient.Client, error) {
		if cached != nil {
			return cached, nil
		}
		c, err := pgclient.NewFromDSN(ctx, dsn)
		if err != nil {
			return nil, err
		}
		cached = c
		return cached, nil
	}
	closeFn := func() {
		if cached != nil {
			cached.Close()
		}
	}
	return provider, closeFn
}
```

- [ ] **Step 7: Full sweep + smoke + commit**
```bash
go build ./... && go vet ./... && gofmt -l . && go test ./...
go run ./cmd/pg-upgrade/ run --help     # still shows --state/--headless
git add internal/phases/registry.go internal/phases/registry_test.go internal/phases/upgrade.go internal/runner/checkpoint.go cmd/pg-upgrade/main.go
git commit -m "feat(pg-upgrade): wire phases 5-6 into run (Phases1to6, prompts, PG17/NewPatroni/signal)"
```

---

## Final verification

- [ ] `go build ./...`, `go test ./...`, `go vet ./...`, `gofmt -l .` all clean.
- [ ] `pg-upgrade run --help` lists `--state`/`--headless`.
- [ ] Spec coverage (Phases 5-6): StartPG17OnN1 (Task 3), CreateSubscription→CreateForwardSubscription (Task 3), WaitLagZero (Task 3), InitNewPatroniCluster/AddReplicas→delegated, VerifyNewClusterHealthy (Task 3); FreezeOldPrimary (Task 4), WaitFinalLagZero (Task 4), SyncSequences with buffer (Task 4), SetupReverseReplication (Task 5), NotifyDSNSwap via signal file (Task 5), VerifyTrafficOnNew (Task 5), DisableForwardSubscription (Task 5). Switchover terminal (rollback window). Dual PG17/old-primary connection model (Tasks 2, 6).

## Out of scope (Plan 4)
- Phases 7-8: DropReverseReplication, DropForwardSubscription, UnfreezeOldPrimary, RenamePatroniCluster (etcdctl), VerifyRenamedCluster; StopOldPostgres, ArchivePgUpgradeLogs, RemoveOldDCSKeys. Add the `switchover → finalize` transition then.
- Bootstrapping the new Patroni cluster / adding replicas / the DSN swap itself remain operator/external-tooling actions (the binary verifies them).
- The forward subscription's `WaitLagZero`/`WaitFinalLagZero` are one-shot (error + operator re-run), consistent with `WaitReplayComplete`; bounded polling is a possible later refinement.
