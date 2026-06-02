package phases

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewSwitchover builds Phase 6: the critical section. Freeze the old primary,
// drain the final lag, sync sequences, set up reverse replication, signal the
// DSN swap, verify traffic moved, and disable the forward subscription. Terminal
// in Plan 3 (the run pauses at the rollback window; Plan 4 adds Finalize).
//
// NOTE: this task (Plan 3 Task 4) implements only the first three steps. Task 5
// appends the remaining four to this slice (and defines their types), so the
// slice lists ONLY the three steps defined below.
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

func (s *freezeOldPrimary) ID() runner.StepID                   { return "FreezeOldPrimary" }
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

func (s *waitFinalLagZero) ID() runner.StepID                       { return "WaitFinalLagZero" }
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

var (
	_ runner.Step = (*freezeOldPrimary)(nil)
	_ runner.Step = (*waitFinalLagZero)(nil)
	_ runner.Step = (*syncSequences)(nil)
)
