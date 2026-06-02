package phases

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/jackc/pglogrepl"
)

// NewIsolate builds Phase 2: disconnect N1 from WAL and record the physical
// boundary target_lsn.
func NewIsolate(d Deps) runner.Phase {
	return &simplePhase{
		id: "isolate",
		steps: []runner.Step{
			&pausePatroni{d},
			&captureReceivedLSN{d},
			&disconnectN1{d},
			&waitReplayComplete{d},
			&recordTargetLSN{d},
		},
		trans: []runner.Transition{{To: "drain"}},
	}
}

// --- PausePatroni ---

type pausePatroni struct{ d Deps }

func (s *pausePatroni) ID() runner.StepID { return "PausePatroni" }
func (s *pausePatroni) Check(ctx context.Context) (bool, error) {
	c, err := s.d.Patroni.GetCluster(ctx)
	if err != nil {
		return false, err
	}
	if c == nil {
		return false, nil
	}
	return c.Paused, nil
}
func (s *pausePatroni) Run(ctx context.Context) error { return s.d.Patroni.Pause(ctx) }

// --- CaptureReceivedLSN (must run before disconnect; receiver goes empty after) ---

type captureReceivedLSN struct{ d Deps }

func (s *captureReceivedLSN) ID() runner.StepID { return "CaptureReceivedLSN" }
func (s *captureReceivedLSN) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.ReceivedLSN != "", nil
}
func (s *captureReceivedLSN) Run(ctx context.Context) error {
	lsn, err := s.d.N1.GetWALReceiverReceivedLSN(ctx)
	if err != nil {
		return err
	}
	if lsn == "" {
		return fmt.Errorf("isolate: wal receiver already empty; cannot capture received_lsn")
	}
	return s.d.Mgr.SetReceivedLSN(lsn)
}

// --- DisconnectN1FromWAL ---

type disconnectN1 struct{ d Deps }

func (s *disconnectN1) ID() runner.StepID { return "DisconnectN1FromWAL" }

// Check always returns false: DisconnectFromWAL clears primary_conninfo and is
// idempotent, so we run it unconditionally rather than skipping based on
// IsWALReceiverActive (the receiver may have already stopped, but
// primary_conninfo still needs clearing so Patroni cannot reconnect).
func (s *disconnectN1) Check(context.Context) (bool, error) { return false, nil }
func (s *disconnectN1) Run(ctx context.Context) error       { return s.d.N1.DisconnectFromWAL(ctx) }

// --- WaitReplayComplete: replay >= received ---

type waitReplayComplete struct{ d Deps }

func (s *waitReplayComplete) ID() runner.StepID { return "WaitReplayComplete" }
func (s *waitReplayComplete) Check(ctx context.Context) (bool, error) {
	return s.replayCaughtUp(ctx)
}
func (s *waitReplayComplete) Run(ctx context.Context) error {
	caught, err := s.replayCaughtUp(ctx)
	if err != nil {
		return err
	}
	if !caught {
		return fmt.Errorf("isolate: replay has not reached received_lsn yet; re-run pg-upgrade to retry")
	}
	return nil
}
func (s *waitReplayComplete) replayCaughtUp(ctx context.Context) (bool, error) {
	received := s.d.Mgr.Get().Artifacts.ReceivedLSN
	if received == "" {
		return false, fmt.Errorf("isolate: received_lsn not captured")
	}
	replayStr, err := s.d.N1.GetLastWALReplayLSN(ctx)
	if err != nil {
		return false, err
	}
	if replayStr == "" {
		return false, fmt.Errorf("isolate: replay_lsn is NULL (N1 has not replayed any WAL)")
	}
	recv, err := pglogrepl.ParseLSN(received)
	if err != nil {
		return false, fmt.Errorf("isolate: parse received_lsn: %w", err)
	}
	replay, err := pglogrepl.ParseLSN(replayStr)
	if err != nil {
		return false, fmt.Errorf("isolate: parse replay_lsn: %w", err)
	}
	return replay >= recv, nil
}

// --- RecordTargetLSN + post-phase invariant ---

type recordTargetLSN struct{ d Deps }

func (s *recordTargetLSN) ID() runner.StepID { return "RecordTargetLSN" }
func (s *recordTargetLSN) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.TargetLSN != "", nil
}
func (s *recordTargetLSN) Run(ctx context.Context) error {
	target, err := s.d.N1.GetLastWALReplayLSN(ctx)
	if err != nil {
		return err
	}
	if target == "" {
		return fmt.Errorf("isolate: replay_lsn is NULL; cannot record target_lsn")
	}
	if err := s.d.Mgr.SetTargetLSN(target); err != nil {
		return err
	}
	// Invariant: SlotBaseline.ConfirmedFlushLSN <= target_lsn, else changes
	// between baseline and target would be lost.
	bl := s.d.Mgr.Get().Artifacts.SlotBaseline
	if bl == nil {
		return fmt.Errorf("isolate: slot baseline missing")
	}
	conf, err := pglogrepl.ParseLSN(bl.ConfirmedFlushLSN)
	if err != nil {
		return fmt.Errorf("isolate: parse confirmed_flush_lsn: %w", err)
	}
	tgt, err := pglogrepl.ParseLSN(target)
	if err != nil {
		return fmt.Errorf("isolate: parse target_lsn: %w", err)
	}
	if conf > tgt {
		return fmt.Errorf("isolate: FATAL invariant violated: confirmed_flush_lsn %s > target_lsn %s (slot created after N1 disconnected)", bl.ConfirmedFlushLSN, target)
	}
	return nil
}

var (
	_ runner.Step = (*pausePatroni)(nil)
	_ runner.Step = (*captureReceivedLSN)(nil)
	_ runner.Step = (*disconnectN1)(nil)
	_ runner.Step = (*waitReplayComplete)(nil)
	_ runner.Step = (*recordTargetLSN)(nil)
)
