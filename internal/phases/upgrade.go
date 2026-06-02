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
		NewDataDir: d.Cfg.Upgrade.NewDataDir,
	}
}

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
	inRec, err := s.d.N1.IsInRecovery(ctx)
	if err != nil {
		return false, err
	}
	return !inRec, nil // already promoted = done
}
func (s *promoteN1) Run(ctx context.Context) error {
	if err := s.d.Tools.Promote(ctx, s.d.Cfg.Upgrade.DataDir); err != nil {
		return err
	}
	// pg_ctl promote -w should have waited; confirm with the authoritative
	// signal. If still in recovery, a re-run self-heals (Check skips once promoted).
	inRec, err := s.d.N1.IsInRecovery(ctx)
	if err != nil {
		return err
	}
	if inRec {
		return fmt.Errorf("upgrade: promote not complete (still in recovery); re-run pg-upgrade")
	}
	return nil
}

// --- ShutdownN1Clean ---

type shutdownN1Clean struct{ d Deps }

func (s *shutdownN1Clean) ID() runner.StepID { return "ShutdownN1Clean" }
func (s *shutdownN1Clean) Check(ctx context.Context) (bool, error) {
	cd, err := s.d.Tools.OldControlData(ctx, s.d.Cfg.Upgrade.DataDir)
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
	if err := s.d.checkUpgradePaths(); err != nil {
		return err
	}
	if err := s.d.Tools.UpgradeCheck(ctx, s.d.upgradeOpts()); err != nil {
		return err
	}
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
	// NOTE(plan-3): minimal stub — a real Patroni config also needs restapi/
	// etcd/postgresql sections. Plan 3 extends this before starting Patroni.
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
