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

func (s *verifyOldPrimaryStopped) ID() runner.StepID                   { return "VerifyOldPrimaryStopped" }
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
