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
func NewSwitchover(d Deps) runner.Phase {
	return &simplePhase{
		id: "switchover",
		steps: []runner.Step{
			&freezeOldPrimary{d},
			&waitFinalLagZero{d},
			&syncSequences{d},
			&setupReverseReplication{d},
			&notifyDSNSwap{d},
			&verifyTrafficOnNew{d},
			&disableForwardSubscription{d},
		},
		trans: nil, // terminal in Plan 3: paused at the rollback window
	}
}

// --- FreezeOldPrimary ---

type freezeOldPrimary struct{ d Deps }

func (s *freezeOldPrimary) ID() runner.StepID                   { return "FreezeOldPrimary" }
func (s *freezeOldPrimary) Check(context.Context) (bool, error) { return false, nil } // always-run; FreezeForUpgrade re-applies idempotently (DROP IF EXISTS + CREATE)
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

// --- SetupReverseReplication (PG17 publishes; old primary subscribes back) ---

type setupReverseReplication struct{ d Deps }

func (s *setupReverseReplication) ID() runner.StepID { return "SetupReverseReplication" }
func (s *setupReverseReplication) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.ReverseReplSetUp, nil
}
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
	if err := old.CreateSubscriptionCreatingSlot(ctx, s.d.Cfg.Upgrade.ReverseSubName, s.d.Cfg.Upgrade.PG17DSN, s.d.Cfg.Upgrade.ReversePubName); err != nil {
		return err
	}
	return s.d.Mgr.SetReverseReplSetUp()
}

// --- NotifyDSNSwap (signal external tooling; operator confirms via checkpoint) ---

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

func (s *verifyTrafficOnNew) ID() runner.StepID                   { return "VerifyTrafficOnNew" }
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
func (s *disableForwardSubscription) Check(context.Context) (bool, error) {
	return false, nil
} // always-run: ALTER ... DISABLE is a no-op if already disabled
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
