# Isolate-on-Shadow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Freeze the **shadow leader** at `target_lsn` and detach it from prod — replacing the risky prod-side isolate. No prod Patroni pause, no `primary_conninfo` races.

**Architecture:** A new `isolate-shadow` phase: promote the shadow leader via Patroni (`ClearStandbyCluster` → standalone PG13 primary), let replay settle to capture `target_lsn` (reuse the existing `waitReplaySettled`), drop the physical slot on prod, and record `target_lsn` with the existing slot-baseline invariant. Built as a NEW phase (`NewIsolateShadow`) so the current cannibalize-N1 `NewIsolate` stays intact (Plan 6 selects between them).

**Tech Stack:** Go, the existing `internal/clients/{pg,patroni}` + `internal/phases`. Reuses `waitReplaySettled` and `recordTargetLSN`'s invariant check.

Plan 4 of the shadow-cluster upgrade. **Resolves spec Open Question 1:** freeze via **Patroni promote** (not stop-recovery) — Patroni-native, leaves a clean standalone primary for `pg_upgrade`, and `target_lsn` = the last replayed LSN before the timeline switch (in prod's LSN space).

---

### Task 1: pg `DropReplicationSlot`

**Files:** Modify `internal/clients/pg/client.go`; Test `internal/clients/pg/client_test.go`.

- [ ] **Step 1: Failing test**

```go
func TestDropReplicationSlot(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectExec("pg_drop_replication_slot").
		WithArgs("shadow_phys").
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	c := pgclient.NewFromPool(mock)
	require.NoError(t, c.DropReplicationSlot(context.Background(), "shadow_phys"))
	assert.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run, expect FAIL** (`DropReplicationSlot` undefined).

Run: `go test ./internal/clients/pg/ -run TestDropReplicationSlot -v`

- [ ] **Step 3: Implement** — interface line `DropReplicationSlot(ctx context.Context, name string) error`; internalClient:

```go
// DropReplicationSlot removes a slot (physical or logical) by name. Idempotent:
// only drops when present, so re-entry doesn't error on an already-dropped slot.
func (c *internalClient) DropReplicationSlot(ctx context.Context, name string) error {
	_, err := c.q.Exec(ctx,
		`SELECT pg_drop_replication_slot($1)
		   WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, name)
	return err
}
```

PoolClient: `func (p *PoolClient) DropReplicationSlot(ctx context.Context, name string) error { return p.ic().DropReplicationSlot(ctx, name) }`

- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `git commit -am "feat(shadow): pg DropReplicationSlot"`

---

### Task 2: `isolate-shadow` phase

**Files:** Create `internal/phases/isolate_shadow.go`; Test `internal/phases/isolate_shadow_test.go`.

- [ ] **Step 1: Failing tests**

`internal/phases/isolate_shadow_test.go`:

```go
package phases

import (
	"context"
	"testing"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsolateShadowPromotesAndRecordsTarget(t *testing.T) {
	defer setReplayTimingForTest(t)()
	mgr := testMgr(t)
	require.NoError(t, mgr.SetSlotBaseline(&state.SlotBaseline{ConfirmedFlushLSN: "0/10"}))
	pat := &fakePatroni{standbySet: true}
	// shadow promoted: receiver gone, replay settled at 0/3FA20000, out of recovery
	shadow := &fakePG{walRcvActive: false, replayLSN: "0/3FA20000", inRecovery: false}
	prod := &fakePG{}
	d := Deps{Mgr: mgr, NewPatroni: pat,
		Shadow:  func(context.Context) (pg.Client, error) { return shadow, nil },
		Primary: func(context.Context) (pg.Client, error) { return prod, nil },
		Cfg:     config.Config{Upgrade: config.UpgradeConfig{PhysicalSlotName: "shadow_phys"}}}

	for _, s := range NewIsolateShadow(d).Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.False(t, pat.standbySet, "standby_cluster must be cleared (promote)")
	assert.Equal(t, "shadow_phys", prod.droppedSlot)
	assert.Equal(t, "0/3FA20000", mgr.Get().Artifacts.TargetLSN)
}

func TestIsolateShadowStepOrder(t *testing.T) {
	var names []string
	for _, s := range NewIsolateShadow(Deps{}).Steps() {
		names = append(names, string(s.ID()))
	}
	assert.Equal(t, []string{"PromoteShadow", "WaitShadowPromoted", "SettleShadowTarget", "DropPhysicalSlot", "RecordTargetLSN"}, names)
}
```

Add fakePG field `droppedSlot string` + method (in `prepare_test.go`):

```go
func (f *fakePG) DropReplicationSlot(_ context.Context, name string) error { f.droppedSlot = name; return nil }
```

- [ ] **Step 2: Run, expect FAIL** (`NewIsolateShadow` undefined).

Run: `go test ./internal/phases/ -run TestIsolateShadow -v`

- [ ] **Step 3: Implement `isolate_shadow.go`**

```go
package phases

import (
	"context"
	"fmt"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

var shadowPromoteTimeout = 60 * time.Second

// NewIsolateShadow freezes the shadow leader at target_lsn by promoting it out
// of standby-cluster mode, settling replay, and dropping the physical slot. The
// production cluster is untouched.
func NewIsolateShadow(d Deps) runner.Phase {
	return &simplePhase{
		id: "isolate",
		steps: []runner.Step{
			&promoteShadow{d},
			&waitShadowPromoted{d},
			&settleShadowTarget{d},
			&dropPhysicalSlot{d},
			&recordTargetLSNShadow{d},
		},
		trans: []runner.Transition{{To: "drain"}},
	}
}

type promoteShadow struct{ d Deps }

func (s *promoteShadow) ID() runner.StepID                   { return "PromoteShadow" }
func (s *promoteShadow) Check(context.Context) (bool, error) { return false, nil }
func (s *promoteShadow) Run(ctx context.Context) error {
	s.d.logf("промоутю лидер шэдоу: снимаю standby_cluster → Patroni поднимет его как standalone primary...")
	return s.d.NewPatroni.ClearStandbyCluster(ctx)
}

type waitShadowPromoted struct{ d Deps }

func (s *waitShadowPromoted) ID() runner.StepID { return "WaitShadowPromoted" }
func (s *waitShadowPromoted) Check(ctx context.Context) (bool, error) { return s.promoted(ctx) }
func (s *waitShadowPromoted) Run(ctx context.Context) error {
	s.d.logf("жду, пока Patroni промоутит лидер шэдоу (pg_is_in_recovery=false)...")
	wctx, cancel := context.WithTimeout(ctx, shadowPromoteTimeout)
	defer cancel()
	for {
		ok, err := s.promoted(wctx)
		if err == nil && ok {
			return nil
		}
		select {
		case <-wctx.Done():
			return fmt.Errorf("isolate-shadow: shadow leader not promoted in time: %w", wctx.Err())
		case <-time.After(time.Second):
		}
	}
}
func (s *waitShadowPromoted) promoted(ctx context.Context) (bool, error) {
	shadow, err := s.d.Shadow(ctx)
	if err != nil {
		return false, err
	}
	inRec, err := shadow.IsInRecovery(ctx)
	if err != nil {
		return false, err
	}
	return !inRec, nil
}

// settleShadowTarget reuses waitReplaySettled: with the receiver gone after
// promote, replay holds at the freeze point; that settled LSN is target_lsn.
type settleShadowTarget struct{ d Deps }

func (s *settleShadowTarget) ID() runner.StepID { return "SettleShadowTarget" }
func (s *settleShadowTarget) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.TargetLSN != "", nil
}
func (s *settleShadowTarget) Run(ctx context.Context) error {
	shadow, err := s.d.Shadow(ctx)
	if err != nil {
		return err
	}
	s.d.logf("жду стабилизации replay на лидере шэдоу (точка заморозки = target_lsn)...")
	wctx, cancel := context.WithTimeout(ctx, replayDrainTimeout)
	defer cancel()
	settled, err := waitReplaySettled(wctx, shadow, replayDrainInterval, replayStableSamples)
	if err != nil {
		return err
	}
	s.d.logf("replay устаканился на %s", settled)
	return s.d.Mgr.SetTargetLSN(settled)
}

type dropPhysicalSlot struct{ d Deps }

func (s *dropPhysicalSlot) ID() runner.StepID                   { return "DropPhysicalSlot" }
func (s *dropPhysicalSlot) Check(context.Context) (bool, error) { return false, nil }
func (s *dropPhysicalSlot) Run(ctx context.Context) error {
	prod, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	s.d.logf("дроп физического слота %q на проде (шэдоу больше не стримит физически)...", s.d.Cfg.Upgrade.PhysicalSlotName)
	return prod.DropReplicationSlot(ctx, s.d.Cfg.Upgrade.PhysicalSlotName)
}

// recordTargetLSNShadow asserts the slot-baseline invariant (confirmed_flush <=
// target). target_lsn was set by settleShadowTarget, so Check short-circuits.
type recordTargetLSNShadow struct{ d Deps }

func (s *recordTargetLSNShadow) ID() runner.StepID { return "RecordTargetLSN" }
func (s *recordTargetLSNShadow) Check(context.Context) (bool, error) {
	bl := s.d.Mgr.Get().Artifacts.SlotBaseline
	return s.d.Mgr.Get().Artifacts.TargetLSN != "" && bl != nil, nil
}
func (s *recordTargetLSNShadow) Run(ctx context.Context) error {
	return assertSlotBaselineBelowTarget(s.d.Mgr.Get().Artifacts.SlotBaseline, s.d.Mgr.Get().Artifacts.TargetLSN)
}

var (
	_ runner.Step = (*promoteShadow)(nil)
	_ runner.Step = (*waitShadowPromoted)(nil)
	_ runner.Step = (*settleShadowTarget)(nil)
	_ runner.Step = (*dropPhysicalSlot)(nil)
	_ runner.Step = (*recordTargetLSNShadow)(nil)
)
```

- [ ] **Step 4: Extract the invariant helper from the existing isolate**

In `internal/phases/isolate.go`, extract the `confirmed_flush ≤ target` invariant from `recordTargetLSN.Run` into a shared helper so both isolates use it:

```go
func assertSlotBaselineBelowTarget(bl *state.SlotBaseline, target string) error {
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
		return fmt.Errorf("isolate: FATAL invariant violated: confirmed_flush_lsn %s > target_lsn %s", bl.ConfirmedFlushLSN, target)
	}
	return nil
}
```

Then replace the inline invariant block in the existing `recordTargetLSN.Run` with a call to `assertSlotBaselineBelowTarget(bl, target)` (keep its existing behaviour; this is a pure refactor — run the existing isolate tests to confirm green). Add `"github.com/dmbabuev/pg-upgrade/internal/state"` import to `isolate.go` if not present.

- [ ] **Step 5: Run, expect PASS + full suite**

Run: `go test ./internal/phases/ -run 'TestIsolate' -v && go vet ./... && go test ./...`
Expected: PASS (new shadow tests + existing isolate tests still green after the refactor).

- [ ] **Step 6: Commit**

```bash
git add internal/phases/isolate_shadow.go internal/phases/isolate_shadow_test.go internal/phases/isolate.go internal/phases/prepare_test.go internal/clients/pg/client.go internal/clients/pg/client_test.go
git commit -m "feat(shadow): isolate-on-shadow phase (promote + settle target + drop physical slot)"
```

---

## Notes for the implementer

- **`target_lsn` after promote.** Patroni promote ends recovery and switches timeline; `pg_last_wal_replay_lsn()` on the promoted node returns the last record applied during recovery — the divergence point in prod's LSN space, which is exactly `target_lsn`. `waitReplaySettled` already guards against an active walreceiver (gone after promote) and a NULL replay LSN.
- **Why reuse `waitReplaySettled`.** It polls until replay holds steady across `replayStableSamples` reads and re-checks `IsWALReceiverActive` each iteration. On a freshly-promoted primary the receiver is gone and replay is fixed, so it returns the freeze point immediately — same correctness property as the prod-side fix, no new logic.
- **DML during the promote→upgrade gap.** The shadow leader is briefly a writable primary before `pg_upgrade`. It carries the DDL lock (inherited via physical repl, Plan 1) and serves no traffic, so no writes occur; we proceed to stop+upgrade promptly (Plan 6 sequencing).
- The current `NewIsolate` is untouched except the pure invariant-helper extraction; Plan 6 selects `NewIsolateShadow` vs `NewIsolate` by topology mode.
