package phases

import (
	"context"
	"fmt"
	"os"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewCleanup builds Phase 8: archive the pg_upgrade logs and confirm the old
// primary is decommissioned. Terminal — the run completes after this phase.
//
// The spec's StopOldPostgres is operator-delegated (the old primary is a
// separate node the binary cannot pg_ctl), so VerifyOldPrimaryStopped only
// confirms the operator's stop. RemoveOldDCSKeys (etcdctl) is likewise operator
// follow-up, noted in the closing message — both require access the binary
// deliberately avoids.
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
	// "archive dir exists" is treated as already-archived. A partial copy left by
	// a mid-run crash can be re-archived by removing LogArchiveDir first; these
	// are diagnostic logs, not operational data.
	if _, err := os.Stat(s.d.Cfg.Upgrade.LogArchiveDir); err == nil {
		return true, nil
	}
	return false, nil
}
func (s *archivePgUpgradeLogs) Run(context.Context) error {
	return copyTree(s.d.Cfg.Upgrade.PgUpgradeLogDir, s.d.Cfg.Upgrade.LogArchiveDir)
}

// --- VerifyOldPrimaryStopped (operator stops the remote old primary; binary confirms it is down) ---

type verifyOldPrimaryStopped struct{ d Deps }

func (s *verifyOldPrimaryStopped) ID() runner.StepID                   { return "VerifyOldPrimaryStopped" }
func (s *verifyOldPrimaryStopped) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifyOldPrimaryStopped) Run(ctx context.Context) error {
	old, err := s.d.Primary(ctx)
	if err != nil {
		// A provider error is a config problem (the pool is built lazily, so it
		// is NOT a "down" signal); surface it rather than declaring success.
		return fmt.Errorf("cleanup: cannot reach old primary for verification: %w", err)
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
