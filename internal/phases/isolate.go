package phases

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/jackc/pglogrepl"
)

// How long PausePatroni waits for the node to actually apply maintenance mode
// after the PATCH, and how often it polls. The default Patroni loop_wait is 10s,
// so 30s leaves room for a couple of HA cycles.
const (
	pauseApplyTimeout  = 30 * time.Second
	pauseApplyInterval = 1 * time.Second
)

// Settle/detach poll timings. Vars (not consts) so tests can shrink them.
var (
	// detachConfirm*: VerifyN1Detached polls for the walreceiver to go inactive
	// after DisconnectN1FromWAL. The receiver shutdown after a reload is async, so
	// tolerate a brief active window; a persistent active receiver is a re-attach.
	detachConfirmTimeout  = 15 * time.Second
	detachConfirmInterval = 1 * time.Second

	// replayDrain*: WaitReplayDrained polls replay_lsn until it stops advancing.
	// With the receiver gone, replay climbs to the end of received WAL then holds;
	// the held value is N1's true physical freeze point (X').
	replayDrainTimeout  = 60 * time.Second
	replayDrainInterval = 1 * time.Second
)

// replayStableSamples is how many consecutive equal replay_lsn reads (one
// replayDrainInterval apart) count as "settled". Three reads => two full
// intervals of no movement, which on a receiver-less standby means replay has
// drained all on-disk WAL.
const replayStableSamples = 3

// NewIsolate builds Phase 2: disconnect N1 from WAL and record the physical
// boundary target_lsn.
//
// StopPatroniOnN1 runs right after the cluster-wide pause: a paused Patroni
// still reconciles primary_conninfo on N1 and would re-attach its walreceiver
// after DisconnectN1FromWAL, so N1 must be taken out of Patroni's loop for the
// disconnect to hold. It is operator-gated by upgrade.old_patroni_stop_command
// (the same command used before pg_upgrade); when unset, isolate falls back to
// the VerifyN1Detached guard catching a re-attach.
func NewIsolate(d Deps) runner.Phase {
	return &simplePhase{
		id: "isolate",
		steps: []runner.Step{
			&pausePatroni{d},
			&stopPatroniOnN1{d},
			&captureReceivedLSN{d},
			&disconnectN1{d},
			&verifyN1Detached{d},
			&waitReplayDrained{d},
			&recordTargetLSN{d},
		},
		trans: []runner.Transition{{To: "drain"}},
	}
}

// --- PausePatroni ---

type pausePatroni struct{ d Deps }

func (s *pausePatroni) ID() runner.StepID { return "PausePatroni" }
func (s *pausePatroni) Check(ctx context.Context) (bool, error) {
	// Once StopPatroniOnN1 has run, N1's Patroni REST (localhost:8008) is down, so
	// any REST call would error on re-entry. The pause was already applied before
	// the stop, so treat this step as done.
	if s.d.Mgr.Get().Artifacts.PatroniStoppedOnN1 {
		return true, nil
	}
	// Use the node's APPLIED maintenance state, not GetCluster().Paused (which is
	// only the DCS flag): "done" must mean the node won't gracefully stop postgres
	// when StopPatroniOnN1 fires.
	return s.d.Patroni.NodePaused(ctx)
}
func (s *pausePatroni) Run(ctx context.Context) error {
	s.d.logf("ставлю Patroni на паузу (отключаю автоматический failover)...")
	if err := s.d.Patroni.Pause(ctx); err != nil {
		return err
	}
	// The PATCH only writes pause to DCS; the node applies maintenance mode on its
	// next HA loop. Wait for the node to actually be paused before StopPatroniOnN1
	// stops it — a paused node leaves postgres alone on shutdown, an un-paused one
	// gracefully stops it (the race that killed N1's postgres).
	s.d.logf("жду, пока нода применит maintenance mode (pause), прежде чем останавливать Patroni...")
	wctx, cancel := context.WithTimeout(ctx, pauseApplyTimeout)
	defer cancel()
	if err := waitNodePaused(wctx, s.d.Patroni, pauseApplyInterval); err != nil {
		return err
	}
	s.d.logf("Patroni на паузе (применено на ноде)")
	return nil
}

// waitNodePaused polls until the Patroni node reports it has applied maintenance
// mode, or ctx expires. Stopping Patroni before this is true risks a graceful
// PostgreSQL shutdown.
func waitNodePaused(ctx context.Context, p patroni.Client, interval time.Duration) error {
	for {
		paused, err := p.NodePaused(ctx)
		if err != nil {
			return err
		}
		if paused {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("isolate: Patroni node did not apply maintenance mode (pause) in time — stopping it now would gracefully shut down postgres; re-run: %w", ctx.Err())
		case <-time.After(interval):
		}
	}
}

// --- StopPatroniOnN1: take Patroni off N1 so the WAL disconnect holds ---

type stopPatroniOnN1 struct{ d Deps }

func (s *stopPatroniOnN1) ID() runner.StepID { return "StopPatroniOnN1" }
func (s *stopPatroniOnN1) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.PatroniStoppedOnN1, nil
}
func (s *stopPatroniOnN1) Run(ctx context.Context) error {
	cmd := s.d.Cfg.Upgrade.OldPatroniStopCommand
	if cmd == "" {
		// No stop command configured: leave Patroni running (pause stays in effect)
		// and rely on VerifyN1Detached to catch a re-attach. Don't record the
		// artifact, so PausePatroni keeps using the live REST.
		s.d.logf("old_patroni_stop_command не задан — Patroni на N1 не останавливаю; полагаюсь на guard VerifyN1Detached (без остановки Patroni может вернуть primary_conninfo)")
		return nil
	}
	s.d.logf("останавливаю Patroni на N1, чтобы он не переподключил WAL: %q...", cmd)
	if err := s.d.Tools.StopPatroni(ctx, cmd); err != nil {
		return err
	}
	s.d.logf("Patroni на N1 остановлен (postgres продолжает работать)")
	return s.d.Mgr.SetPatroniStoppedOnN1()
}

// --- CaptureReceivedLSN: a pre-disconnect LOWER BOUND on N1's frozen point ---
//
// received_lsn (pg_stat_wal_receiver) is read before DisconnectN1FromWAL while
// the receiver still exists. It is only a lower bound: the receiver keeps
// streaming in the gap before primary_conninfo is actually cleared, so N1's true
// final position X' >= this value. We no longer derive target_lsn from it (that
// caused the undershoot bug); WaitReplayDrained settles to X' after disconnect
// and asserts X' >= this captured bound as a sanity check.
type captureReceivedLSN struct{ d Deps }

func (s *captureReceivedLSN) ID() runner.StepID { return "CaptureReceivedLSN" }
func (s *captureReceivedLSN) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.ReceivedLSN != "", nil
}
func (s *captureReceivedLSN) Run(ctx context.Context) error {
	s.d.logf("фиксирую received_lsn WAL-приёмника N1 как нижнюю границу (до отключения от WAL)...")
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

// --- VerifyN1Detached: the disconnect must hold before we measure the freeze point ---
//
// Runs BEFORE WaitReplayDrained: the settle measurement is only valid if no new
// WAL can arrive while replay drains. The walreceiver shutdown after the reload
// is async, so poll for it to go inactive rather than failing on the first read;
// a receiver that never goes inactive is a re-attach (paused Patroni restoring
// primary_conninfo) and must fail loudly.
type verifyN1Detached struct{ d Deps }

func (s *verifyN1Detached) ID() runner.StepID { return "VerifyN1Detached" }

// Check always returns false: this is a live invariant, not a recorded fact —
// re-verify on every (re-)entry.
func (s *verifyN1Detached) Check(context.Context) (bool, error) { return false, nil }
func (s *verifyN1Detached) Run(ctx context.Context) error {
	s.d.logf("проверяю, что N1 не переподключился к WAL (жду, пока walreceiver станет пустым)...")
	wctx, cancel := context.WithTimeout(ctx, detachConfirmTimeout)
	defer cancel()
	if err := waitReceiverInactive(wctx, s.d.N1, detachConfirmInterval); err != nil {
		return err
	}
	s.d.logf("N1 не переподключился — приёмник пуст")
	return nil
}

// waitReceiverInactive polls until N1's walreceiver is inactive, or ctx expires.
// A persistently-active receiver after DisconnectFromWAL means primary_conninfo
// was restored (typically paused Patroni reconciling it) — N1 would drift past
// target_lsn, so surface it loudly.
func waitReceiverInactive(ctx context.Context, n1 pg.Client, interval time.Duration) error {
	for {
		active, err := n1.IsWALReceiverActive(ctx)
		if err != nil {
			return err
		}
		if !active {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("isolate: N1 re-attached to WAL (walreceiver still active, primary_conninfo was restored — usually paused Patroni reconciling it). Stop Patroni on N1 (systemctl stop patroni — leaves postgres running in this setup), re-clear primary_conninfo, then re-run; otherwise N1 drifts past target_lsn: %w", ctx.Err())
		case <-time.After(interval):
		}
	}
}

// --- WaitReplayDrained: let replay settle to N1's true freeze point (X') ---
//
// With the receiver confirmed gone (VerifyN1Detached), no new WAL can arrive, so
// replay_lsn climbs to the end of received WAL and then holds. The held value is
// X' — N1's true physical freeze point. Recording target_lsn from a still-
// climbing replay was the undershoot bug; here we wait for it to stop moving.
type waitReplayDrained struct{ d Deps }

func (s *waitReplayDrained) ID() runner.StepID { return "WaitReplayDrained" }

// Check short-circuits once target_lsn is recorded: replay was already drained
// and frozen on a prior pass, so don't re-poll.
func (s *waitReplayDrained) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.TargetLSN != "", nil
}
func (s *waitReplayDrained) Run(ctx context.Context) error {
	s.d.logf("жду, пока replay_lsn на N1 перестанет расти (полный слив принятого WAL после отключения)...")
	wctx, cancel := context.WithTimeout(ctx, replayDrainTimeout)
	defer cancel()
	settled, err := waitReplaySettled(wctx, s.d.N1, replayDrainInterval, replayStableSamples)
	if err != nil {
		return err
	}
	// Sanity: the settled point must be at least the pre-disconnect received_lsn
	// lower bound. Below it means we measured a stale early replay (or N1 never
	// drained the WAL it had already received), and target_lsn would undershoot.
	if recv := s.d.Mgr.Get().Artifacts.ReceivedLSN; recv != "" {
		r, err := pglogrepl.ParseLSN(recv)
		if err != nil {
			return fmt.Errorf("isolate: parse received_lsn: %w", err)
		}
		st, err := pglogrepl.ParseLSN(settled)
		if err != nil {
			return fmt.Errorf("isolate: parse settled replay_lsn: %w", err)
		}
		if st < r {
			return fmt.Errorf("isolate: settled replay_lsn %s is below the pre-disconnect received_lsn %s — N1 has not drained the WAL it received; re-run", settled, recv)
		}
	}
	s.d.logf("replay_lsn устаканился на %s — N1 слил весь принятый WAL (это и есть target)", settled)
	return nil
}

// waitReplaySettled polls replay_lsn until it stops advancing across `samples`
// consecutive reads `interval` apart, then returns the settled LSN. It re-checks
// that the walreceiver stays inactive each iteration so a Patroni re-attach
// (which would keep replay advancing forever) fails fast instead of timing out.
func waitReplaySettled(ctx context.Context, n1 pg.Client, interval time.Duration, samples int) (string, error) {
	var last string
	matches := 0
	for {
		active, err := n1.IsWALReceiverActive(ctx)
		if err != nil {
			return "", err
		}
		if active {
			return "", fmt.Errorf("isolate: N1 re-attached to WAL while replay was draining (walreceiver active again — primary_conninfo restored, usually paused Patroni). Stop Patroni on N1 and re-run; otherwise N1 drifts past target_lsn")
		}
		cur, err := n1.GetLastWALReplayLSN(ctx)
		if err != nil {
			return "", err
		}
		if cur == "" {
			return "", fmt.Errorf("isolate: replay_lsn is NULL (N1 has not replayed any WAL)")
		}
		if cur == last {
			matches++
		} else {
			matches = 1
			last = cur
		}
		if matches >= samples {
			return cur, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("isolate: replay_lsn did not settle (still advancing — N1 has not finished draining received WAL) before timeout: %w", ctx.Err())
		case <-time.After(interval):
		}
	}
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
	_ runner.Step = (*stopPatroniOnN1)(nil)
	_ runner.Step = (*captureReceivedLSN)(nil)
	_ runner.Step = (*disconnectN1)(nil)
	_ runner.Step = (*verifyN1Detached)(nil)
	_ runner.Step = (*waitReplayDrained)(nil)
	_ runner.Step = (*recordTargetLSN)(nil)
)
