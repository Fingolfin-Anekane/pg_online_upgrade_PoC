# Shadow FSM Assembly + Switchover/Finalize Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the shadow phases into a selectable FSM so the operator can run the shadow-cluster upgrade end to end, while the existing in-place (cannibalize-N1) FSM stays intact and default.

**Architecture:** A config `Mode` (`inplace` | `shadow`) selects which phase list `cmd` builds. A new `PhasesShadow(d)` registry assembles `provision → prepare → isolate(shadow) → drain → upgrade → catchup → rebuild-replicas → switchover → finalize → cleanup`. `catchup` is refactored so its steps are shared and only its transition differs (`switchover` in-place, `rebuild-replicas` in shadow). Switchover/finalize are reused unchanged — the DDL unlock landed in finalize in Plan 1.

**Tech Stack:** Go, the existing `internal/phases/registry.go`, `internal/config`, `cmd/pg-upgrade`. Depends on Plans 1–5.

Plan 6 (final) of the shadow-cluster upgrade. Per the branch rule, all of this lands on the dedicated feature branch, not master.

---

### Task 1: Config `Mode`

**Files:** Modify `internal/config/config.go`; Test `internal/config/config_test.go`.

- [ ] **Step 1: Failing test**

```go
func TestModeDefaultsAndParses(t *testing.T) {
	cfg, err := config.Parse([]byte("upgrade:\n  mode: shadow\n"))
	require.NoError(t, err)
	assert.Equal(t, "shadow", cfg.Upgrade.Mode)

	cfg2, err := config.Parse([]byte("upgrade: {}\n"))
	require.NoError(t, err)
	assert.Equal(t, "inplace", cfg2.Upgrade.EffectiveMode()) // default
}
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `go test ./internal/config/ -run TestModeDefaults -v`

- [ ] **Step 3: Implement** — add to `UpgradeConfig`:

```go
	// Mode selects the upgrade topology: "inplace" (default; cannibalize a live
	// replica) or "shadow" (parallel standby_cluster upgraded in place).
	Mode string `yaml:"mode"`
```

and a method on `Config` (next to `EffectiveNewScope`):

```go
// EffectiveMode returns Upgrade.Mode, defaulting to "inplace".
func (c Config) EffectiveMode() string {
	if c.Upgrade.Mode == "" {
		return "inplace"
	}
	return c.Upgrade.Mode
}
```

- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `git commit -am "feat(shadow): config Mode (inplace|shadow)"`

---

### Task 2: Share catchup steps so its transition can vary

**Files:** Modify `internal/phases/catchup.go`; Test `internal/phases/catchup_test.go`.

- [ ] **Step 1: Failing test**

```go
func TestCatchupShadowTransitionsToRebuildReplicas(t *testing.T) {
	tr := NewCatchupShadow(Deps{}).Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "rebuild-replicas", tr[0].To)
}

func TestCatchupInplaceStillTransitionsToSwitchover(t *testing.T) {
	tr := NewCatchup(Deps{}).Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "switchover", tr[0].To)
}
```

- [ ] **Step 2: Run, expect FAIL** (`NewCatchupShadow` undefined).

Run: `go test ./internal/phases/ -run 'TestCatchupShadowTransitions|TestCatchupInplaceStill' -v`

- [ ] **Step 3: Refactor `NewCatchup` + add `NewCatchupShadow`**

In `internal/phases/catchup.go`, extract the steps into a helper and have both constructors use it:

```go
func catchupSteps(d Deps) []runner.Step {
	return []runner.Step{
		&verifyOldClusterStopped{d},
		&patchNewPatroniConfig{d},
		&startPG17{d},
		&createForwardSubscription{d},
		&ensureDDLLockOnNew{d}, // from Plan 1
		&waitLagZero{d},
		&verifyNewClusterHealthy{d},
	}
}

func NewCatchup(d Deps) runner.Phase {
	return &simplePhase{id: "catchup", steps: catchupSteps(d), trans: []runner.Transition{{To: "switchover"}}}
}

// NewCatchupShadow is catchup for the shadow topology: same steps, but it hands
// off to rebuild-replicas (replicas are rebuilt before cutover).
func NewCatchupShadow(d Deps) runner.Phase {
	return &simplePhase{id: "catchup", steps: catchupSteps(d), trans: []runner.Transition{{To: "rebuild-replicas"}}}
}
```

- [ ] **Step 4: Run, expect PASS + the existing catchup flow test still green.**

Run: `go test ./internal/phases/ -run TestCatchup -v`

- [ ] **Step 5: Commit** — `git commit -am "refactor(catchup): share steps; add NewCatchupShadow → rebuild-replicas"`

---

### Task 3: `PhasesShadow` registry

**Files:** Modify `internal/phases/registry.go`; Test `internal/phases/registry_test.go` (create if absent).

- [ ] **Step 1: Failing test**

`internal/phases/registry_test.go`:

```go
package phases

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPhasesShadowSequence(t *testing.T) {
	ps := PhasesShadow(Deps{})
	var ids []string
	for _, p := range ps {
		ids = append(ids, string(p.ID()))
	}
	require.Equal(t, []string{
		"provision", "prepare", "isolate", "drain", "upgrade",
		"catchup", "rebuild-replicas", "switchover", "finalize", "cleanup",
	}, ids)
}
```

- [ ] **Step 2: Run, expect FAIL** (`PhasesShadow` undefined).

Run: `go test ./internal/phases/ -run TestPhasesShadowSequence -v`

- [ ] **Step 3: Implement** — add to `internal/phases/registry.go`:

```go
// PhasesShadow assembles the shadow-cluster topology (spec
// 2026-06-15-shadow-cluster-upgrade-design.md). It reuses prepare/drain/upgrade/
// switchover/finalize/cleanup, swaps in the shadow isolate, and adds provision
// and rebuild-replicas.
func PhasesShadow(d Deps) []runner.Phase {
	return []runner.Phase{
		NewProvision(d),
		NewPrepare(d),
		NewIsolateShadow(d),
		NewDrain(d),
		NewUpgrade(d),
		NewCatchupShadow(d),
		NewRebuildReplicas(d),
		NewSwitchover(d),
		NewFinalize(d),
		NewCleanup(d),
	}
}
```

- [ ] **Step 4: Run, expect PASS + full suite**

Run: `go test ./internal/phases/ -run TestPhasesShadow -v && go vet ./... && go test ./...`
Expected: PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(shadow): PhasesShadow registry"`

---

### Task 4: Select the registry + build shadow Deps in `cmd`

**Files:** Modify `cmd/pg-upgrade/main.go` (or wherever `Phases1to8` is called and `Deps` is built).

- [ ] **Step 1: Locate the assembly**

Find where `phases.Phases1to8(deps)` is passed to `runner.New(...)`. Confirm where `Deps` is constructed (the pg-client providers, patroni clients, tools).

- [ ] **Step 2: Select by mode + wire shadow Deps**

Replace the phase-list construction with:

```go
var phaseList []runner.Phase
switch cfg.EffectiveMode() {
case "shadow":
	phaseList = phases.PhasesShadow(deps)
default:
	phaseList = phases.Phases1to8(deps)
}
```

And, only in shadow mode, populate the new `Deps` fields built in Plans 2–5:

```go
if cfg.EffectiveMode() == "shadow" {
	// NewPatroni is the shadow cluster's Patroni REST.
	deps.NewPatroni = patroni.NewHTTPClient(cfg.Upgrade.ShadowPatroniURL, patroniToken)
	// Shadow pg client (PG13 standby leader, then PG17). Build from the shadow
	// leader's DSN (derive from ShadowSourceHost-style config or a dedicated DSN).
	deps.Shadow = func(ctx context.Context) (pg.Client, error) {
		return pg.NewFromDSN(ctx, shadowLeaderDSN)
	}
	deps.ShadowMember = func(apiURL string) patroni.Client {
		return patroni.NewHTTPClient(apiURL, patroniToken)
	}
	deps.DiskGuard = diskguard.Monitor{Slot: cfg.Upgrade.SlotName, Reader: prodReader}
}
```

(Resolve `shadowLeaderDSN`, `patroniToken`, and `prodReader` from the existing config/clients in `main.go`; `prodReader` is the prod-primary pg client — it already exists for the `Primary` provider. Add a `shadow_leader_dsn` config field if the shadow leader's DSN isn't otherwise derivable.)

- [ ] **Step 3: Build + manual smoke**

Run: `go build ./... && go vet ./...`
Expected: no errors. (End-to-end is exercised against the real shadow/prod clusters by the operator — there is no unit test for `main.go` wiring; keep the logic thin so the build is the check.)

- [ ] **Step 4: Commit** — `git commit -am "feat(shadow): select PhasesShadow by mode; wire shadow Deps"`

---

### Task 5: Checkpoint prompts for the new phases

**Files:** Modify wherever `runner.PhasePrompts` is defined (search `PhasePrompts` / interactive checkpoint prompts).

- [ ] **Step 1: Add prompts** for `provision` and `rebuild-replicas` so the interactive checkpoint explains them (mirror existing per-phase prompt strings). Example entries:

```go
"provision":        "Шэдоу-кластер превращён в standby прода и догнан. Продолжить к prepare?",
"rebuild-replicas": "Реплики шэдоу пересобраны на PG17 (HA). Продолжить к switchover (cutover)?",
```

- [ ] **Step 2: Build + commit**

Run: `go build ./...`

```bash
git commit -am "feat(shadow): checkpoint prompts for provision and rebuild-replicas"
```

---

## Notes for the implementer

- **Switchover/finalize are reused as-is.** Freeze prod, drain final lag, sync sequences, DSN swap, verify, disable forward subscription (switchover); release the DDL lock + drop slot/publication (finalize, the unlock added in Plan 1). The global DDL bypass (Plan 1, Task 2) lets `FreezeForUpgrade`'s `CREATE TRIGGER` run while the lock is active — no change needed there.
- **Open deployment question (spec Open Questions §1 fallout):** the `upgrade` phase runs `pg_upgrade --link` on the **shadow leader's** data dirs via `Tools`. That means the tool's local pieces (the `upgrade` phase, the `pg_ctl` calls) must run where the shadow leader is. Decide at execution: run the orchestrator on the shadow-leader node, or split local-vs-remote responsibilities. This plan does not change the `upgrade` phase code; it assumes `Deps.Tools` and the data-dir config point at the shadow leader in shadow mode.
- **`cleanup`** (decommission prod) is reused; ensure it targets the prod cluster, and that the old cluster's inherited DDL lock is irrelevant once prod is stopped (spec: dropped at teardown).
- After all 6 plans: run `go test ./... && go vet ./...` on the feature branch; the in-place path tests must remain green (the shadow work is additive — new files + additive registry/config, with only the pure catchup refactor and the isolate invariant-helper extraction touching existing code).
