package phases

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"

	"github.com/dmbabuev/pg-upgrade/internal/connect"
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
	host := s.d.Mgr.Get().Artifacts.PrimaryHost
	if host == "" {
		return fmt.Errorf("drain: primary host not discovered")
	}
	connStr, err := connect.DSNForHost(s.d.Cfg.PG.SuperuserDSN, host)
	if err != nil {
		return err
	}
	s.d.logf("сливаю слот %q до target_lsn=%s (потребляю изменения через pgoutput)...", s.d.Cfg.Upgrade.SlotName, target)
	report, err := s.d.Drain(ctx, slotdrain.Config{
		ConnString: connStr,
		SlotName:   s.d.Cfg.Upgrade.SlotName,
		PubName:    s.d.Cfg.Upgrade.PublicationName,
		TargetLSN:  target,
	})
	if err != nil {
		return err
	}
	s.d.logf("слот слит: транзакций=%d, confirmed_flush_lsn=%s", report.TransactionsDrained, report.FinalFlushLSN)
	return s.d.Mgr.SetDrainReport(&state.DrainReport{
		CompletedAt:         report.CompletedAt,
		FinalFlushLSN:       report.FinalFlushLSN,
		LastCommitLSN:       report.LastCommitLSN,
		TransactionsDrained: report.TransactionsDrained,
	})
}

// --- VerifySlotDrained: confirmed_flush_lsn is at the drain's final flush ---

type verifySlotDrained struct{ d Deps }

func (s *verifySlotDrained) ID() runner.StepID                   { return "VerifySlotDrained" }
func (s *verifySlotDrained) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifySlotDrained) Run(ctx context.Context) error {
	s.d.logf("проверяю, что confirmed_flush_lsn слота совпал с итогом слива...")
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
	// Before trusting confirmed_flush, make sure the slot itself is still alive:
	// max_slot_wal_keep_size could have invalidated it (wal_status lost/unreserved),
	// in which case the post-target tail WAL is gone and catchup would lose data.
	if err := assertSlotReserved(slot); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	flushLSN, err := pglogrepl.ParseLSN(slot.ConfirmedFlushLSN)
	if err != nil {
		return fmt.Errorf("drain: parse confirmed_flush_lsn %q: %w", slot.ConfirmedFlushLSN, err)
	}
	target, err := pglogrepl.ParseLSN(report.FinalFlushLSN)
	if err != nil {
		return fmt.Errorf("drain: parse drain target %q: %w", report.FinalFlushLSN, err)
	}
	// The slot must sit at the boundary, not exactly on target: PostgreSQL clamps
	// confirmed_flush_lsn to the last record it decoded, which can be a few bytes
	// below target (non-decodable WAL fills the gap). The safe window is
	// LastCommitLSN <= confirmed_flush <= target — above target loses the tail,
	// below the last drained commit means the slot never advanced (incomplete).
	if flushLSN > target {
		return fmt.Errorf("drain: confirmed_flush_lsn %s overshot target %s — slot would skip post-target changes (data loss)", slot.ConfirmedFlushLSN, report.FinalFlushLSN)
	}
	if report.LastCommitLSN != "" {
		lastCommit, err := pglogrepl.ParseLSN(report.LastCommitLSN)
		if err != nil {
			return fmt.Errorf("drain: parse last commit %q: %w", report.LastCommitLSN, err)
		}
		if flushLSN < lastCommit {
			return fmt.Errorf("drain: confirmed_flush_lsn %s is behind the last drained commit %s — slot not advanced (drain incomplete)", slot.ConfirmedFlushLSN, report.LastCommitLSN)
		}
	}
	s.d.logf("слот корректно слит: confirmed_flush_lsn=%s (в окне [%s, target=%s])", slot.ConfirmedFlushLSN, report.LastCommitLSN, report.FinalFlushLSN)
	return nil
}

var (
	_ runner.Step = (*runSlotDrain)(nil)
	_ runner.Step = (*verifySlotDrained)(nil)
)
