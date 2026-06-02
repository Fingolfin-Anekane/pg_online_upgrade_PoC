package phases

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/dmbabuev/pg-upgrade/internal/slotdrain"
	"github.com/dmbabuev/pg-upgrade/internal/state"
)

// NewDrain builds Phase 3: advance the slot's confirmed_flush_lsn to the last
// commit <= target_lsn, leaving the tail for the PG17 subscription.
func NewDrain(d Deps) runner.Phase {
	return &simplePhase{
		id: "drain",
		steps: []runner.Step{
			&runSlotDrain{d},
			&verifySlotDrained{d},
		},
		trans: []runner.Transition{{To: "upgrade"}},
	}
}

// --- RunSlotDrain ---

type runSlotDrain struct{ d Deps }

func (s *runSlotDrain) ID() runner.StepID { return "RunSlotDrain" }
func (s *runSlotDrain) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.DrainReport != nil, nil
}
func (s *runSlotDrain) Run(ctx context.Context) error {
	target := s.d.Mgr.Get().Artifacts.TargetLSN
	if target == "" {
		return fmt.Errorf("drain: target_lsn not set")
	}
	report, err := s.d.Drain(ctx, slotdrain.Config{
		ConnString: s.d.Cfg.PG.SuperuserDSN,
		SlotName:   s.d.Cfg.Upgrade.SlotName,
		PubName:    s.d.Cfg.Upgrade.PublicationName,
		TargetLSN:  target,
	})
	if err != nil {
		return err
	}
	return s.d.Mgr.SetDrainReport(&state.DrainReport{
		CompletedAt:         report.CompletedAt,
		FinalFlushLSN:       report.FinalFlushLSN,
		TransactionsDrained: report.TransactionsDrained,
	})
}

// --- VerifySlotDrained: confirmed_flush_lsn is at the drain's final flush ---

type verifySlotDrained struct{ d Deps }

func (s *verifySlotDrained) ID() runner.StepID                   { return "VerifySlotDrained" }
func (s *verifySlotDrained) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifySlotDrained) Run(ctx context.Context) error {
	report := s.d.Mgr.Get().Artifacts.DrainReport
	if report == nil {
		return fmt.Errorf("drain: no drain report to verify")
	}
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	slot, err := primary.GetReplicationSlot(ctx, s.d.Cfg.Upgrade.SlotName)
	if err != nil {
		return err
	}
	if slot == nil {
		return fmt.Errorf("drain: slot %s missing after drain", s.d.Cfg.Upgrade.SlotName)
	}
	if slot.ConfirmedFlushLSN != report.FinalFlushLSN {
		return fmt.Errorf("drain: confirmed_flush_lsn %s != drained final %s", slot.ConfirmedFlushLSN, report.FinalFlushLSN)
	}
	return nil
}

var (
	_ runner.Step = (*runSlotDrain)(nil)
	_ runner.Step = (*verifySlotDrained)(nil)
)
