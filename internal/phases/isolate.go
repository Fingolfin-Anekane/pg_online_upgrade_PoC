package phases

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

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
			&verifyN1Detached{d},
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
func (s *pausePatroni) Run(ctx context.Context) error {
	s.d.logf("ставлю Patroni на паузу (отключаю автоматический failover)...")
	if err := s.d.Patroni.Pause(ctx); err != nil {
		return err
	}
	s.d.logf("Patroni на паузе")
	return nil
}

// --- CaptureReceivedLSN (must run before disconnect; receiver goes empty after) ---

type captureReceivedLSN struct{ d Deps }

func (s *captureReceivedLSN) ID() runner.StepID { return "CaptureReceivedLSN" }
func (s *captureReceivedLSN) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.ReceivedLSN != "", nil
}
func (s *captureReceivedLSN) Run(ctx context.Context) error {
	s.d.logf("фиксирую received_lsn WAL-приёмника N1 (до отключения от WAL)...")
	lsn, err := s.d.N1.GetWALReceiverReceivedLSN(ctx)
	if err != nil {
		return err
	}
	if lsn == "" {
		return fmt.Errorf("isolate: wal receiver already empty; cannot capture received_lsn")
	}
	s.d.logf("received_lsn=%s зафиксирован", lsn)
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
func (s *disconnectN1) Run(ctx context.Context) error {
	v, err := s.d.N1.ServerVersionNum(ctx)
	if err != nil {
		return err
	}
	switch {
	case v >= 130000:
		// PG13+: primary_conninfo is reloadable (PGC_SIGHUP).
		s.d.logf("PG%d: очищаю primary_conninfo через ALTER SYSTEM + reload (без рестарта)...", v/10000)
		if err := s.d.N1.DisconnectFromWAL(ctx); err != nil {
			return err
		}
		s.d.logf("N1 отключён от WAL")
		return nil
	case v >= 120000:
		// PG12: GUC but PGC_POSTMASTER -> clear via ALTER SYSTEM, then restart.
		s.d.logf("PG12: очищаю primary_conninfo (ALTER SYSTEM) и рестартую N1...")
		if err := s.d.N1.ClearPrimaryConninfo(ctx); err != nil {
			return err
		}
		if err := s.d.Tools.Restart(ctx, s.d.Cfg.Upgrade.DataDir); err != nil {
			return err
		}
		s.d.logf("N1 отключён от WAL (после рестарта)")
		return nil
	default:
		// PG10/11: primary_conninfo lives in recovery.conf (not a GUC); edit the
		// file (keep standby_mode) and restart.
		s.d.logf("PG%d: удаляю primary_conninfo из recovery.conf и рестартую N1...", v/10000)
		if err := removePrimaryConninfoFromRecoveryConf(s.d.Cfg.Upgrade.DataDir); err != nil {
			return err
		}
		if err := s.d.Tools.Restart(ctx, s.d.Cfg.Upgrade.DataDir); err != nil {
			return err
		}
		s.d.logf("N1 отключён от WAL (после рестарта)")
		return nil
	}
}

// removePrimaryConninfoFromRecoveryConf strips primary_conninfo from
// $DATADIR/recovery.conf (PG10/11), leaving standby_mode and everything else so
// a restart brings N1 up as a standby with no upstream (frozen at target_lsn).
func removePrimaryConninfoFromRecoveryConf(dataDir string) error {
	path := filepath.Join(dataDir, "recovery.conf")
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("isolate: read recovery.conf: %w", err)
	}
	if err := os.WriteFile(path, []byte(stripPrimaryConninfo(string(data))), 0o600); err != nil {
		return fmt.Errorf("isolate: write recovery.conf: %w", err)
	}
	return nil
}

// matches an active "primary_conninfo = ..." (or "primary_conninfo=...") line,
// with optional leading whitespace; does NOT match comments or other keys.
var primaryConninfoLineRe = regexp.MustCompile(`^\s*primary_conninfo\s*=`)

// stripPrimaryConninfo removes active primary_conninfo lines from a recovery.conf
// body, preserving comments and all other settings.
func stripPrimaryConninfo(content string) string {
	lines := strings.Split(content, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		if primaryConninfoLineRe.MatchString(line) {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// --- WaitReplayComplete: replay >= received ---

type waitReplayComplete struct{ d Deps }

func (s *waitReplayComplete) ID() runner.StepID { return "WaitReplayComplete" }
func (s *waitReplayComplete) Check(ctx context.Context) (bool, error) {
	return s.replayCaughtUp(ctx)
}
func (s *waitReplayComplete) Run(ctx context.Context) error {
	s.d.logf("проверяю, что N1 доиграл WAL до received_lsn=%s...", s.d.Mgr.Get().Artifacts.ReceivedLSN)
	caught, err := s.replayCaughtUp(ctx)
	if err != nil {
		return err
	}
	if !caught {
		return fmt.Errorf("isolate: replay has not reached received_lsn yet; re-run pg-upgrade to retry")
	}
	s.d.logf("replay_lsn >= received_lsn — N1 доиграл весь принятый WAL")
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

// --- VerifyN1Detached: the disconnect must still hold before we freeze ---

type verifyN1Detached struct{ d Deps }

func (s *verifyN1Detached) ID() runner.StepID { return "VerifyN1Detached" }

// Check always returns false: this is a live invariant, not a recorded fact —
// re-verify on every (re-)entry.
func (s *verifyN1Detached) Check(context.Context) (bool, error) { return false, nil }
func (s *verifyN1Detached) Run(ctx context.Context) error {
	s.d.logf("проверяю, что N1 не переподключился к WAL (walreceiver должен быть пуст)...")
	active, err := s.d.N1.IsWALReceiverActive(ctx)
	if err != nil {
		return err
	}
	if active {
		// DisconnectFromWAL cleared primary_conninfo, but something put N1 back on
		// the WAL stream — typically Patroni re-applying its managed
		// primary_conninfo even while paused. If we recorded target_lsn now, N1
		// would keep advancing past it and the upgrade boundary would break.
		return fmt.Errorf("isolate: N1 re-attached to WAL (walreceiver is active again, primary_conninfo was restored — usually paused Patroni reconciling it). Take Patroni out of N1's loop while keeping postgres up (e.g. SIGSTOP the Patroni process), re-clear primary_conninfo, then re-run; otherwise N1 drifts past target_lsn")
	}
	s.d.logf("N1 не переподключился — приёмник пуст")
	return nil
}

// --- RecordTargetLSN + post-phase invariant ---

type recordTargetLSN struct{ d Deps }

func (s *recordTargetLSN) ID() runner.StepID { return "RecordTargetLSN" }
func (s *recordTargetLSN) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.TargetLSN != "", nil
}
func (s *recordTargetLSN) Run(ctx context.Context) error {
	s.d.logf("записываю target_lsn (физическая граница, на которой замёрз N1)...")
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
	s.d.logf("target_lsn=%s записан; проверяю инвариант confirmed_flush_lsn <= target_lsn...", target)
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
	_ runner.Step = (*verifyN1Detached)(nil)
	_ runner.Step = (*recordTargetLSN)(nil)
)
