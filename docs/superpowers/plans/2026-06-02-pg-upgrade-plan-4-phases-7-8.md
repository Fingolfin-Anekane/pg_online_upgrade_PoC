# pg-upgrade Plan 4: Phases 7-8 (Finalize + Cleanup) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete `pg-upgrade run` with phase 7 (Finalize) and phase 8 (Cleanup) — commit the upgrade by tearing down the rollback artifacts (reverse + forward replication, the write freeze), verifying the operator-performed Patroni cluster rename, archiving the pg_upgrade logs, and confirming the old primary is decommissioned.

**Architecture:** Two new terminal-bound phases (`finalize`, `cleanup`) added to `internal/phases`, reusing the Plan 2/3 `simplePhase`/`Deps`/`runner` machinery. The binary performs the SQL teardown (DROP SUBSCRIPTION/PUBLICATION, unfreeze triggers) and the local pg_upgrade-log archive itself; **genuinely-external/multi-node actions** (the etcdctl-based Patroni cluster rename, stopping the remote old primary, removing stale DCS keys) are operator-driven — the binary pauses at a checkpoint and verifies the result over Patroni REST / SQL. The run completes after cleanup.

**Tech Stack:** Go 1.25, pgx/v5, cobra, yaml.v3, testify, pgxmock/v3. Module `github.com/dmbabuev/pg-upgrade`.

**Spec:** `docs/superpowers/specs/2026-06-01-pg-upgrade-design.md` (Phases 7-8). This is the final plan; after it, `pg-upgrade run` covers all 8 phases.

**Scope decisions (consistent with Plan 3's boundary):**
- The binary does the SQL teardown (drops/unfreeze on the right node) and archives the local pg_upgrade logs.
- `RenamePatroniCluster` (etcdctl + per-node config edits + restarts) and `RemoveOldDCSKeys` (etcdctl) are operator/external; the binary checkpoints + verifies. `StopOldPostgres` targets the remote old-primary node, so it is operator-driven and the binary verifies the old primary is unreachable.

---

## Existing foundation reused

- `internal/runner`: `Runner`, `simplePhase` (in `prepare.go`), `Checkpoint`, `DefaultPrompts`.
- `internal/phases`: `Deps` (Cfg, Mgr, Patroni, NewPatroni, Tools, N1, Primary, PG17, WriteSignal, Drain), `Phases1to6`, `FirstPhase`, the shared `fakePG`/`fakePatroni`/`fakeTools` test fakes.
- `internal/clients/pg`: `DropSubscription(ctx, name)` / `DropPublication(ctx, name)` (both `DROP ... IF EXISTS` — idempotent), `UnfreezeAfterUpgrade(ctx, dbname)` (drops upgrade_freeze triggers + `raise_upgrade_readonly()` + `ALTER DATABASE ... RESET default_transaction_read_only`; idempotent), `IsInRecovery(ctx)`.
- `internal/clients/patroni`: `Client.GetCluster`; `ClusterInfo.Leader()`.
- `internal/state`: `Manager`; `Artifacts`. (No new artifacts required — the teardown SQL is idempotent and the archive uses a destination-exists check.)
- `internal/config`: `UpgradeConfig` (with `SubscriptionName`, `ReversePubName`, `ReverseSubName`, `DBName`, `NewPatroniURL`, ...), `ValidateForRun`.

---

## File Structure (new/modified)

```
internal/config/config.go        # add PgUpgradeLogDir, LogArchiveDir; extend ValidateForRun
internal/phases/archive.go       # copyTree helper (recursive directory copy)
internal/phases/finalize.go      # Phase 7 (DropReverseReplication, DropForwardSubscription, UnfreezeOldPrimary, VerifyRenamedCluster)
internal/phases/cleanup.go       # Phase 8 (ArchivePgUpgradeLogs, VerifyOldPrimaryStopped)
internal/phases/registry.go      # Phases1to8; switchover->finalize transition
internal/phases/switchover.go    # change trans: nil -> {To: "finalize"}
internal/runner/checkpoint.go    # add finalize/cleanup prompts
cmd/pg-upgrade/main.go           # Phases1to8; closing message
```

**Phase/Step IDs (exact strings):** phases `"finalize"`, `"cleanup"`. Steps: `DropReverseReplication`, `DropForwardSubscription`, `UnfreezeOldPrimary`, `VerifyRenamedCluster`, `ArchivePgUpgradeLogs`, `VerifyOldPrimaryStopped`.

---

## Task 1: Config additions

**Files:**
- Modify: `internal/config/config.go`, `internal/config/config_test.go`

- [ ] **Step 1: Add two fields to `UpgradeConfig`** (after `SequenceBuffer`):
```go
	PgUpgradeLogDir string `yaml:"pg_upgrade_log_dir"`
	LogArchiveDir   string `yaml:"log_archive_dir"`
```

- [ ] **Step 2: Extend `ValidateForRun`** — append to the `missing` slice (before the `len(missing) > 0` check), alongside the existing required-field checks:
```go
	if u.PgUpgradeLogDir == "" {
		missing = append(missing, "pg_upgrade_log_dir")
	}
	if u.LogArchiveDir == "" {
		missing = append(missing, "log_archive_dir")
	}
```

- [ ] **Step 3: Fix the existing tests** — two tests construct configs that must stay valid past the new required-field checks:
  - `internal/config/config_test.go` `TestValidateForRun`: add `PgUpgradeLogDir: "/data/pg_upgrade_output.d", LogArchiveDir: "/var/log/pg-upgrade"` to BOTH `UpgradeConfig` literals (the valid `cfg` and the same-dirs `cfg2`).
  - `internal/phases/switchover_test.go` `TestValidateForRunRejectsZeroSequenceBuffer`: its inline `config.Config` literal must also gain `PgUpgradeLogDir: "/d", LogArchiveDir: "/a"`, otherwise the missing-fields check returns first and the test no longer isolates the zero-buffer rejection.

- [ ] **Step 4: Run + commit**
```bash
go test ./internal/config/ && go build ./... && gofmt -l internal/config/ && go vet ./internal/config/
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat(pg-upgrade): config pg_upgrade_log_dir + log_archive_dir"
```

---

## Task 2: copyTree archive helper

**Files:**
- Create: `internal/phases/archive.go`
- Test: `internal/phases/archive_test.go`

- [ ] **Step 1: Write the failing test `internal/phases/archive_test.go`**
```go
package phases

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyTree(t *testing.T) {
	src := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(src, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("world"), 0o600))

	dst := filepath.Join(t.TempDir(), "archive")
	require.NoError(t, copyTree(src, dst))

	a, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, "hello", string(a))
	b, err := os.ReadFile(filepath.Join(dst, "sub", "b.txt"))
	require.NoError(t, err)
	assert.Equal(t, "world", string(b))
}

func TestCopyTreeMissingSource(t *testing.T) {
	err := copyTree(filepath.Join(t.TempDir(), "nope"), t.TempDir())
	require.Error(t, err)
}
```

- [ ] **Step 2: Run to verify failure** — `go test ./internal/phases/ -run TestCopyTree` → FAIL (`undefined: copyTree`).

- [ ] **Step 3: Write `internal/phases/archive.go`**
```go
package phases

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// copyTree recursively copies the directory at src into dst, preserving the
// relative layout and file permissions. dst is created if absent. It is used to
// archive pg_upgrade's output directory before the old cluster is removed.
func copyTree(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("archive: stat source %s: %w", src, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("archive: source %s is not a directory", src)
	}
	return filepath.Walk(src, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if fi.IsDir() {
			return os.MkdirAll(target, fi.Mode().Perm())
		}
		return copyFile(path, target, fi.Mode().Perm())
	})
}

func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("archive: open %s: %w", src, err)
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("archive: mkdir for %s: %w", dst, err)
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("archive: create %s: %w", dst, err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("archive: copy %s -> %s: %w", src, dst, err)
	}
	return nil
}
```

- [ ] **Step 4: Run + commit**
```bash
go test ./internal/phases/ -run TestCopyTree && gofmt -l internal/phases/ && go vet ./internal/phases/
git add internal/phases/archive.go internal/phases/archive_test.go
git commit -m "feat(pg-upgrade): recursive copyTree archive helper"
```

---

## Task 3: Phase 7 (Finalize)

**Files:**
- Create: `internal/phases/finalize.go`
- Test: `internal/phases/finalize_test.go`
- Modify: `internal/phases/prepare_test.go` (add fakePG fields)

Drops are idempotent (`DROP ... IF EXISTS`) so their steps always-run. `VerifyRenamedCluster` is the delegated step: the operator renames the cluster (etcdctl + config + restart); the binary verifies the new cluster's Patroni still reports a leader.

- [ ] **Step 1: Add fakePG fields (prepare_test.go)**
```go
	droppedSub  []string
	droppedPub  []string
	unfrozen    string
```

- [ ] **Step 2: Write the failing test `internal/phases/finalize_test.go`**
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

// finalize/cleanup methods on the shared fakePG
func (f *fakePG) DropSubscription(_ context.Context, name string) error {
	f.droppedSub = append(f.droppedSub, name)
	return nil
}
func (f *fakePG) DropPublication(_ context.Context, name string) error {
	f.droppedPub = append(f.droppedPub, name)
	return nil
}
func (f *fakePG) UnfreezeAfterUpgrade(_ context.Context, dbname string) error { f.unfrozen = dbname; return nil }

func finalizeDeps(t *testing.T, pg17, oldPrimary *fakePG, newPat patroni.Client) Deps {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade", "catchup", "switchover", "finalize"} {
		require.NoError(t, mgr.Advance(p))
	}
	return Deps{
		Cfg: config.Config{ClusterName: "prod", Upgrade: config.UpgradeConfig{
			SubscriptionName: "sub_up", ReversePubName: "pub_rb", ReverseSubName: "sub_rb", DBName: "app",
		}},
		Mgr: mgr, NewPatroni: newPat,
		PG17:    func(context.Context) (pg.Client, error) { return pg17, nil },
		Primary: func(context.Context) (pg.Client, error) { return oldPrimary, nil },
	}
}

func TestFinalizeDropsAndUnfreezes(t *testing.T) {
	pg17 := &fakePG{}
	oldPrimary := &fakePG{}
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "n1", Role: "leader"}, {Name: "n2", Role: "sync_standby"},
	}}}
	d := finalizeDeps(t, pg17, oldPrimary, newPat)

	ph := NewFinalize(d)
	assert.Equal(t, "finalize", ph.ID())
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.Contains(t, oldPrimary.droppedSub, "sub_rb") // reverse sub on old primary
	assert.Contains(t, pg17.droppedPub, "pub_rb")       // reverse pub on PG17
	assert.Contains(t, pg17.droppedSub, "sub_up")       // forward sub on PG17
	assert.Equal(t, "app", oldPrimary.unfrozen)
}

func TestFinalizeTransitionsToCleanup(t *testing.T) {
	ph := NewFinalize(Deps{})
	tr := ph.Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "cleanup", tr[0].To)
}

func TestVerifyRenamedClusterRejectsNoLeader(t *testing.T) {
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{{Name: "a", Role: "replica"}}}}
	err := (&verifyRenamedCluster{Deps{NewPatroni: newPat}}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no leader")
}
```

- [ ] **Step 3: Run to verify failure** — `go test ./internal/phases/ -run 'TestFinalize|TestVerifyRenamed'` → FAIL (`undefined: NewFinalize`).

- [ ] **Step 4: Write `internal/phases/finalize.go`**
```go
package phases

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewFinalize builds Phase 7: commit the upgrade by tearing down the rollback
// artifacts (reverse replication, forward subscription, write freeze) and
// verifying the operator-performed Patroni cluster rename.
func NewFinalize(d Deps) runner.Phase {
	return &simplePhase{
		id: "finalize",
		steps: []runner.Step{
			&dropReverseReplication{d},
			&dropForwardSubscription{d},
			&unfreezeOldPrimary{d},
			&verifyRenamedCluster{d},
		},
		trans: []runner.Transition{{To: "cleanup"}},
	}
}

// --- DropReverseReplication (sub_rollback on old primary; pub_rollback on PG17) ---

type dropReverseReplication struct{ d Deps }

func (s *dropReverseReplication) ID() runner.StepID { return "DropReverseReplication" }
func (s *dropReverseReplication) Check(context.Context) (bool, error) { return false, nil } // DROP IF EXISTS is idempotent
func (s *dropReverseReplication) Run(ctx context.Context) error {
	old, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	if err := old.DropSubscription(ctx, s.d.Cfg.Upgrade.ReverseSubName); err != nil {
		return err
	}
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return err
	}
	return pg17.DropPublication(ctx, s.d.Cfg.Upgrade.ReversePubName)
}

// --- DropForwardSubscription (sub_upgrade on PG17; also drops its slot on the old primary) ---

type dropForwardSubscription struct{ d Deps }

func (s *dropForwardSubscription) ID() runner.StepID { return "DropForwardSubscription" }
func (s *dropForwardSubscription) Check(context.Context) (bool, error) { return false, nil }
func (s *dropForwardSubscription) Run(ctx context.Context) error {
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return err
	}
	return pg17.DropSubscription(ctx, s.d.Cfg.Upgrade.SubscriptionName)
}

// --- UnfreezeOldPrimary (drop the DML freeze triggers on the old primary) ---

type unfreezeOldPrimary struct{ d Deps }

func (s *unfreezeOldPrimary) ID() runner.StepID { return "UnfreezeOldPrimary" }
func (s *unfreezeOldPrimary) Check(context.Context) (bool, error) { return false, nil } // UnfreezeAfterUpgrade is idempotent
func (s *unfreezeOldPrimary) Run(ctx context.Context) error {
	old, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	return old.UnfreezeAfterUpgrade(ctx, s.d.Cfg.Upgrade.DBName)
}

// --- VerifyRenamedCluster (operator renames via etcdctl; binary verifies health) ---

type verifyRenamedCluster struct{ d Deps }

func (s *verifyRenamedCluster) ID() runner.StepID { return "VerifyRenamedCluster" }
func (s *verifyRenamedCluster) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifyRenamedCluster) Run(ctx context.Context) error {
	cluster, err := s.d.NewPatroni.GetCluster(ctx)
	if err != nil {
		return err
	}
	if cluster.Leader() == nil {
		return fmt.Errorf("finalize: renamed cluster has no leader (rename the Patroni cluster, then re-run)")
	}
	return nil
}

var (
	_ runner.Step = (*dropReverseReplication)(nil)
	_ runner.Step = (*dropForwardSubscription)(nil)
	_ runner.Step = (*unfreezeOldPrimary)(nil)
	_ runner.Step = (*verifyRenamedCluster)(nil)
)
```

- [ ] **Step 5: Run + commit**
```bash
go test ./internal/phases/ -run 'TestFinalize|TestVerifyRenamed' && go test ./internal/phases/ && gofmt -l internal/phases/ && go vet ./internal/phases/
git add internal/phases/finalize.go internal/phases/finalize_test.go internal/phases/prepare_test.go
git commit -m "feat(pg-upgrade): phase 7 Finalize (drop reverse/forward repl, unfreeze, verify rename)"
```

---

## Task 4: Phase 8 (Cleanup)

**Files:**
- Create: `internal/phases/cleanup.go`
- Test: `internal/phases/cleanup_test.go`
- Modify: `internal/phases/prepare_test.go` (add an IsInRecovery error field to fakePG)

`ArchivePgUpgradeLogs` copies the local pg_upgrade output dir to the archive dir (skip if already archived). `VerifyOldPrimaryStopped` is delegated: the operator stops the remote old primary; the binary confirms it is no longer reachable.

- [ ] **Step 1: Add an error field to fakePG (prepare_test.go) and route IsInRecovery through it**
Add a field to the `fakePG` struct:
```go
	inRecoveryErr error
```
And change the existing `IsInRecovery` method (in prepare_test.go) from `return f.inRecovery, nil` to:
```go
func (f *fakePG) IsInRecovery(context.Context) (bool, error) { return f.inRecovery, f.inRecoveryErr }
```
(Default nil preserves all existing tests.)

- [ ] **Step 2: Write the failing test `internal/phases/cleanup_test.go`**
```go
package phases

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanupArchivesAndVerifiesStopped(t *testing.T) {
	srcDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "pg_upgrade.log"), []byte("ok"), 0o644))
	archiveDir := filepath.Join(t.TempDir(), "archive")

	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade", "catchup", "switchover", "finalize", "cleanup"} {
		require.NoError(t, mgr.Advance(p))
	}
	// old primary is down -> IsInRecovery errors
	oldPrimary := &fakePG{inRecoveryErr: errors.New("connection refused")}
	d := Deps{
		Cfg: config.Config{Upgrade: config.UpgradeConfig{
			PgUpgradeLogDir: srcDir, LogArchiveDir: archiveDir,
		}},
		Mgr:     mgr,
		Primary: func(context.Context) (pg.Client, error) { return oldPrimary, nil },
	}

	ph := NewCleanup(d)
	assert.Equal(t, "cleanup", ph.ID())
	assert.Empty(t, ph.Transitions()) // terminal
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	data, err := os.ReadFile(filepath.Join(archiveDir, "pg_upgrade.log"))
	require.NoError(t, err)
	assert.Equal(t, "ok", string(data))
}

func TestVerifyOldPrimaryStoppedErrorsWhenReachable(t *testing.T) {
	oldPrimary := &fakePG{} // IsInRecovery returns (false, nil) -> still reachable
	d := Deps{Primary: func(context.Context) (pg.Client, error) { return oldPrimary, nil }}
	err := (&verifyOldPrimaryStopped{d}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still reachable")
}
```

- [ ] **Step 3: Run to verify failure** — `go test ./internal/phases/ -run 'TestCleanup|TestVerifyOldPrimary'` → FAIL (`undefined: NewCleanup`).

- [ ] **Step 4: Write `internal/phases/cleanup.go`**
```go
package phases

import (
	"context"
	"fmt"
	"os"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewCleanup builds Phase 8: archive the pg_upgrade logs and confirm the old
// primary is decommissioned. Terminal — the run completes after this phase.
// Removing stale DCS keys (etcdctl) is an operator action noted in the closing
// message; it requires direct etcd access the binary deliberately avoids.
func NewCleanup(d Deps) runner.Phase {
	return &simplePhase{
		id: "cleanup",
		steps: []runner.Step{
			&archivePgUpgradeLogs{d},
			&verifyOldPrimaryStopped{d},
		},
		trans: nil, // terminal: upgrade complete
	}
}

// --- ArchivePgUpgradeLogs (copy the local pg_upgrade output dir to the archive) ---

type archivePgUpgradeLogs struct{ d Deps }

func (s *archivePgUpgradeLogs) ID() runner.StepID { return "ArchivePgUpgradeLogs" }
func (s *archivePgUpgradeLogs) Check(context.Context) (bool, error) {
	if _, err := os.Stat(s.d.Cfg.Upgrade.LogArchiveDir); err == nil {
		return true, nil // already archived
	}
	return false, nil
}
func (s *archivePgUpgradeLogs) Run(context.Context) error {
	return copyTree(s.d.Cfg.Upgrade.PgUpgradeLogDir, s.d.Cfg.Upgrade.LogArchiveDir)
}

// --- VerifyOldPrimaryStopped (operator stops the remote old primary; binary confirms it is down) ---

type verifyOldPrimaryStopped struct{ d Deps }

func (s *verifyOldPrimaryStopped) ID() runner.StepID { return "VerifyOldPrimaryStopped" }
func (s *verifyOldPrimaryStopped) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifyOldPrimaryStopped) Run(ctx context.Context) error {
	old, err := s.d.Primary(ctx)
	if err != nil {
		return nil // cannot build a client -> treat as down
	}
	// A successful query means the old primary is still up. We want it stopped.
	if _, err := old.IsInRecovery(ctx); err == nil {
		return fmt.Errorf("cleanup: old primary still reachable; stop it, then re-run")
	}
	return nil // query failed -> old primary is down (expected)
}

var (
	_ runner.Step = (*archivePgUpgradeLogs)(nil)
	_ runner.Step = (*verifyOldPrimaryStopped)(nil)
)
```

- [ ] **Step 5: Run + commit**
```bash
go test ./internal/phases/ -run 'TestCleanup|TestVerifyOldPrimary' && go test ./internal/phases/ && gofmt -l internal/phases/ && go vet ./internal/phases/
git add internal/phases/cleanup.go internal/phases/cleanup_test.go internal/phases/prepare_test.go
git commit -m "feat(pg-upgrade): phase 8 Cleanup (archive pg_upgrade logs, verify old primary stopped)"
```

---

## Task 5: Registry + transition + prompts + run wiring

**Files:**
- Modify: `internal/phases/registry.go`, `internal/phases/registry_test.go`, `internal/phases/switchover.go`, `internal/runner/checkpoint.go`, `cmd/pg-upgrade/main.go`

- [ ] **Step 1: Make switchover transition to finalize (switchover.go)** — change `NewSwitchover`'s `trans: nil` to:
```go
		trans: []runner.Transition{{To: "finalize"}},
```
Then fix the switchover test that asserted terminal: in `internal/phases/switchover_test.go`, if any test asserts `NewSwitchover(...).Transitions()` is empty, update it to expect `{To: "finalize"}`. (Grep for `Transitions()` in switchover_test.go; the `TestSwitchoverReverseSignalDisable` uses `require.Len(t, steps, 7)` on Steps(), not Transitions — leave that. If there is no transition assertion, add one:
```go
func TestSwitchoverTransitionsToFinalize(t *testing.T) {
	tr := NewSwitchover(Deps{}).Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "finalize", tr[0].To)
}
```)

- [ ] **Step 2: Update the registry test (registry_test.go)** — replace `TestPhases1to6Registry` with:
```go
func TestPhases1to8Registry(t *testing.T) {
	ps := Phases1to8(Deps{})
	require.Len(t, ps, 8)
	ids := []string{}
	for _, p := range ps {
		ids = append(ids, p.ID())
	}
	assert.Equal(t, []string{"prepare", "isolate", "drain", "upgrade", "catchup", "switchover", "finalize", "cleanup"}, ids)
}
```
Run `go test ./internal/phases/ -run TestPhases1to8Registry` → FAIL (undefined Phases1to8).

- [ ] **Step 3: Replace `Phases1to6` with `Phases1to8` (registry.go)**
```go
// Phases1to8 returns all eight ordered phases. The first phase ("prepare") is
// the run's entry point; "cleanup" is terminal (upgrade complete).
func Phases1to8(d Deps) []runner.Phase {
	return []runner.Phase{
		NewPrepare(d),
		NewIsolate(d),
		NewDrain(d),
		NewUpgrade(d),
		NewCatchup(d),
		NewSwitchover(d),
		NewFinalize(d),
		NewCleanup(d),
	}
}
```
Keep `const FirstPhase = "prepare"`. Delete `Phases1to6` (grep to confirm nothing else references it; the CLI is updated below).

- [ ] **Step 4: Add finalize/cleanup prompts (checkpoint.go)** — add to the `DefaultPrompts()` map:
```go
		"finalize": "Rollback artifacts dropped, cluster renamed. Proceed to cleanup (decommission old cluster)?",
		"cleanup":  "Cleanup complete.",
```

- [ ] **Step 5: Use `Phases1to8` and update the closing message (cmd/pg-upgrade/main.go)** — change `phases.Phases1to6(d)` to `phases.Phases1to8(d)`. Replace the post-run message after a successful `r.Run(ctx)` with:
```go
			fmt.Fprintln(os.Stdout, "\nUpgrade complete (all 8 phases). Operator follow-up: remove stale DCS keys for the old cluster (e.g. etcdctl del /service/<old-scope>/ --prefix).")
```
Also update the cobra command `Short` for the `run` subcommand from "phases 1-6 (Prepare → Switchover)" to "phases 1-8 (Prepare → Cleanup)".

- [ ] **Step 6: Full sweep + smoke + commit**
```bash
go build ./... && go vet ./... && gofmt -l . && go test ./...
go run ./cmd/pg-upgrade/ run --help     # still shows --state/--headless
git add internal/phases/registry.go internal/phases/registry_test.go internal/phases/switchover.go internal/phases/switchover_test.go internal/runner/checkpoint.go cmd/pg-upgrade/main.go
git commit -m "feat(pg-upgrade): wire phases 7-8 into run (Phases1to8, switchover->finalize, prompts)"
```

---

## Final verification

- [ ] `go build ./...`, `go test ./...`, `go vet ./...`, `gofmt -l .` all clean.
- [ ] `pg-upgrade run --help` lists `--state`/`--headless`.
- [ ] Spec coverage (Phases 7-8): DropReverseReplication (Task 3), DropForwardSubscription (Task 3), UnfreezeOldPrimary (Task 3), RenamePatroniCluster→operator-delegated + VerifyRenamedCluster (Task 3); ArchivePgUpgradeLogs (Task 4), StopOldPostgres→operator-delegated + VerifyOldPrimaryStopped (Task 4), RemoveOldDCSKeys→operator-delegated (closing message, Task 5). All 8 phases now reachable from `run`.

## Out of scope (operator/external, by the delegation boundary)
- The etcdctl-based Patroni cluster rename (RenamePatroniCluster) and stale-DCS-key removal (RemoveOldDCSKeys) — the binary deliberately avoids direct etcd access; it checkpoints + verifies.
- Stopping the remote old-primary postgres (StopOldPostgres) — a different node; the binary verifies it is unreachable.
- `VerifyOldPrimaryStopped` infers "down" from a failed connection; a transient network failure during the post-finalize cleanup phase could read as "down" (low risk — no rollback after finalize). A future hardening could distinguish connection-refused from transient errors.
