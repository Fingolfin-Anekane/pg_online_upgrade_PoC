package phases

import (
	"context"
	"fmt"
	"time"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/clients/pgbin"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewUpgrade builds Phase 4: promote N1, shut it down cleanly, initdb the new
// data dir, and run pg_upgrade --link (point of no return). The new cluster is
// brought up under Patroni later, in catchup. Transitions to catchup (Phase 5).
func NewUpgrade(d Deps) runner.Phase {
	return &simplePhase{
		id: "upgrade",
		steps: []runner.Step{
			&promoteN1{d},
			&shutdownN1Clean{d},
			&stopOldPatroni{d},
			&initNewDataDir{d},
			&runPgUpgradeCheck{d},
			&runPgUpgrade{d},
		},
		trans: []runner.Transition{{To: "catchup"}},
	}
}

func (d Deps) upgradeOpts() pgbin.UpgradeOptions {
	return pgbin.UpgradeOptions{
		OldBindir:  d.Cfg.Upgrade.OldPGBindir,
		NewBindir:  d.Cfg.Upgrade.NewPGBindir,
		OldDataDir: d.Cfg.Upgrade.DataDir,
		NewDataDir: d.Cfg.Upgrade.NewDataDir,
		WorkDir:    d.Cfg.Upgrade.PgUpgradeLogDir,
	}
}

// upgradeDone reports whether pg_upgrade has already completed (the point of no
// return). Past it the old cluster is gone — pg_upgrade renames its
// global/pg_control to global/pg_control.old so the old server can't be started
// over the --link'd files — so the pre-pg_upgrade steps that read the old control
// file must NOT re-read it on resume. If a checkpoint decline leaves Current at
// "upgrade", a re-run re-enters this phase; these steps report done from this
// terminal artifact instead, letting the phase pass straight through to catchup.
func (d Deps) upgradeDone() bool { return d.Mgr.Get().Artifacts.PgUpgradeDone }

// checkUpgradePaths enforces pg_upgrade's requirement of distinct old/new dirs.
func (d Deps) checkUpgradePaths() error {
	if d.Cfg.Upgrade.NewDataDir == "" {
		return fmt.Errorf("upgrade: new_data_dir not configured")
	}
	if d.Cfg.Upgrade.NewDataDir == d.Cfg.Upgrade.DataDir {
		return fmt.Errorf("upgrade: new_data_dir must differ from data_dir (pg_upgrade requires distinct old/new datadirs)")
	}
	return nil
}

// --- PromoteN1 ---

type promoteN1 struct{ d Deps }

func (s *promoteN1) ID() runner.StepID { return "PromoteN1" }
func (s *promoteN1) Check(ctx context.Context) (bool, error) {
	if s.d.upgradeDone() {
		return true, nil // pg_upgrade done: old control file is gone, phase is past this
	}
	// Read the control file rather than querying N1 live: it is authoritative and
	// readable whether or not the server is running, so a resumed run works even
	// after ShutdownN1Clean (a later step) has stopped N1.
	//   "in archive recovery"   -> running standby, promote it
	//   "shut down in recovery"  -> stopped standby, not yet promoted
	//   "in production"          -> already promoted (running)
	//   "shut down"              -> promoted then cleanly stopped by a later step
	cd, err := s.d.Tools.OldControlData(ctx, s.d.Cfg.Upgrade.DataDir)
	if err != nil {
		return false, err
	}
	switch cd.State {
	case "in production", "shut down":
		return true, nil // already promoted
	default:
		return false, nil
	}
}
func (s *promoteN1) Run(ctx context.Context) error {
	s.d.logf("промоутю N1 в primary (pg_ctl promote, datadir=%s)...", s.d.Cfg.Upgrade.DataDir)
	if err := s.d.Tools.Promote(ctx, s.d.Cfg.Upgrade.DataDir); err != nil {
		return err
	}
	s.d.logf("жду выхода N1 из recovery (poll IsInRecovery, до 60с)...")
	// pg_ctl promote -w waits on PG12+, but the -w flag is ignored for promote on
	// PG10/11 (it returns immediately). Poll the authoritative signal until N1
	// leaves recovery so a single run works across all supported versions.
	if err := waitOutOfRecovery(ctx, s.d.N1, 60, time.Second); err != nil {
		return err
	}
	s.d.logf("N1 вышел из recovery — теперь это primary")
	return nil
}

// waitOutOfRecovery polls IsInRecovery until it reports false, up to `attempts`
// times spaced by `interval`. It checks immediately before the first wait.
func waitOutOfRecovery(ctx context.Context, n1 pg.Client, attempts int, interval time.Duration) error {
	for i := 0; i < attempts; i++ {
		inRec, err := n1.IsInRecovery(ctx)
		if err != nil {
			return err
		}
		if !inRec {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
	return fmt.Errorf("upgrade: promote did not complete within %s (still in recovery)", time.Duration(attempts)*interval)
}

// --- StopOldPatroni: take the old Patroni off N1 before pg_upgrade --link ---
//
// The dangerous agent is the OLD Patroni on N1: while it runs it can restart the
// old postgres (a service restart resurrects it even when the cluster is paused),
// and after pg_upgrade --link a running old server corrupts the new cluster's
// hard-linked files. By this point ShutdownN1Clean has already stopped postgres,
// so this is a safe spot to take Patroni off N1. The binary no longer needs the
// old Patroni REST after isolate.
type stopOldPatroni struct{ d Deps }

func (s *stopOldPatroni) ID() runner.StepID { return "StopOldPatroniOnN1" }
func (s *stopOldPatroni) Check(context.Context) (bool, error) {
	// Re-verify on every run UNTIL pg_upgrade has completed; past that point the
	// link is done and re-stopping/re-checking the old Patroni is moot.
	return s.d.upgradeDone(), nil
}
func (s *stopOldPatroni) Run(ctx context.Context) error {
	if cmd := s.d.Cfg.Upgrade.OldPatroniStopCommand; cmd != "" {
		s.d.logf("останавливаю старый Patroni на N1: %q...", cmd)
		if err := s.d.Tools.StopPatroni(ctx, cmd); err != nil {
			return err
		}
	}
	// A reachable REST means the old Patroni still runs and could resurrect the
	// old server. (A transient REST error is treated as "down" — acceptable: this
	// is an operator-gated safety check, and the operator has stopped it.)
	if _, err := s.d.Patroni.GetCluster(ctx); err == nil {
		return fmt.Errorf("upgrade: the old Patroni on N1 is still reachable. It can resurrect the old server, and after pg_upgrade --link that corrupts the new cluster. Stop the old Patroni on this node now (postgres is already down for the upgrade), then re-run — or set upgrade.old_patroni_stop_command to have pg-upgrade stop it")
	}
	s.d.logf("старый Patroni на N1 недоступен — безопасно делать pg_upgrade")
	return nil
}

// --- ShutdownN1Clean ---

type shutdownN1Clean struct{ d Deps }

func (s *shutdownN1Clean) ID() runner.StepID { return "ShutdownN1Clean" }
func (s *shutdownN1Clean) Check(ctx context.Context) (bool, error) {
	if s.d.upgradeDone() {
		return true, nil // pg_upgrade done: old control file is gone, phase is past this
	}
	cd, err := s.d.Tools.OldControlData(ctx, s.d.Cfg.Upgrade.DataDir)
	if err != nil {
		return false, err
	}
	return cd.State == "shut down", nil
}
func (s *shutdownN1Clean) Run(ctx context.Context) error {
	// Flush dirty pages before stopping (spec: two checkpoints).
	s.d.logf("делаю два CHECKPOINT перед остановкой (сбрасываю грязные страницы)...")
	if err := s.d.N1.Checkpoint(ctx); err != nil {
		return err
	}
	if err := s.d.N1.Checkpoint(ctx); err != nil {
		return err
	}
	s.d.logf("останавливаю N1 чисто (pg_ctl stop -m fast) для pg_upgrade...")
	if err := s.d.Tools.StopClean(ctx, s.d.Cfg.Upgrade.DataDir); err != nil {
		return err
	}
	s.d.logf("N1 остановлен в состоянии \"shut down\"")
	return nil
}

// --- RunPgUpgradeCheck ---

type runPgUpgradeCheck struct{ d Deps }

func (s *runPgUpgradeCheck) ID() runner.StepID { return "RunPgUpgradeCheck" }
func (s *runPgUpgradeCheck) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.PgUpgradeCheckPassed, nil
}
func (s *runPgUpgradeCheck) Run(ctx context.Context) error {
	if err := s.d.checkUpgradePaths(); err != nil {
		return err
	}
	s.d.logf("запускаю pg_upgrade --check (old=%s new=%s)...", s.d.Cfg.Upgrade.OldPGBindir, s.d.Cfg.Upgrade.NewPGBindir)
	if err := s.d.Tools.UpgradeCheck(ctx, s.d.upgradeOpts()); err != nil {
		return err
	}
	s.d.logf("pg_upgrade --check пройден")
	return s.d.Mgr.SetPgUpgradeCheckPassed()
}

// --- RunPgUpgrade (point of no return) ---

// runPgUpgrade performs pg_upgrade --link — the point of no return. Resume
// limitation: if the process crashes after Tools.Upgrade() succeeds but before
// SetPgUpgradeDone persists (a microsecond window; the state write is atomic),
// a resumed run re-invokes pg_upgrade. pg_upgrade is self-protecting — it
// re-runs --check and refuses to link an already-migrated cluster, failing
// cleanly WITHOUT data corruption. Recovery is manual: confirm the upgrade
// completed and mark the state done. An automatic NewControlData probe is
// deliberately NOT used: it cannot distinguish an empty freshly-initdb'd new
// cluster from a completed upgrade, and a false "done" would skip the real
// migration. (Hardened auto-resume is a Plan 3 follow-up.)
type runPgUpgrade struct{ d Deps }

func (s *runPgUpgrade) ID() runner.StepID { return "RunPgUpgrade" }
func (s *runPgUpgrade) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.PgUpgradeDone, nil
}
func (s *runPgUpgrade) Run(ctx context.Context) error {
	if err := s.d.checkUpgradePaths(); err != nil {
		return err
	}
	s.d.logf("ТОЧКА НЕВОЗВРАТА: запускаю pg_upgrade --link (new_data_dir=%s)...", s.d.Cfg.Upgrade.NewDataDir)
	if err := s.d.Tools.Upgrade(ctx, s.d.upgradeOpts()); err != nil {
		return err
	}
	cd, err := s.d.Tools.NewControlData(ctx, s.d.Cfg.Upgrade.NewDataDir)
	if err != nil {
		return err
	}
	if cd.SystemID == "" {
		return fmt.Errorf("upgrade: could not read PG17 system identifier after pg_upgrade")
	}
	s.d.logf("pg_upgrade завершён, новый кластер system_identifier=%s", cd.SystemID)
	return s.d.Mgr.SetPgUpgradeDone(cd.SystemID)
}

var (
	_ runner.Step = (*promoteN1)(nil)
	_ runner.Step = (*shutdownN1Clean)(nil)
	_ runner.Step = (*stopOldPatroni)(nil)
	_ runner.Step = (*runPgUpgradeCheck)(nil)
	_ runner.Step = (*runPgUpgrade)(nil)
)
