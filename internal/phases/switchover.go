package phases

import (
	"context"
	"fmt"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewSwitchover builds Phase 6: the critical section. Freeze the old primary,
// drain the final lag, sync sequences, signal the DSN swap, verify traffic
// moved, and disable the forward subscription. Transitions to "finalize"
// (Plan 4).
//
// Reverse replication (PG17 -> old primary, rollback insurance) is currently
// omitted: it created a bidirectional setup that loops (a write on PG17 flows
// back to the old primary and is re-captured by the forward publication, which
// origin=none can't break on the PG13 subscriber), and its slot creation hangs
// on an idle PG17. Finalize's DropReverseReplication stays (DROP ... IF EXISTS),
// so any leftover reverse artifacts are still cleaned up.
func NewSwitchover(d Deps) runner.Phase {
	return &simplePhase{
		id: "switchover",
		steps: []runner.Step{
			&freezeOldPrimary{d},
			&waitFinalLagZero{d},
			&syncSequences{d},
			&notifyDSNSwap{d},
			&verifyTrafficOnNew{d},
			&disableForwardSubscription{d},
		},
		trans: []runner.Transition{{To: "finalize"}},
	}
}

// forwardSubDisabled reports whether DisableForwardSubscription has already run.
// Once it has, the critical section is effectively complete: the forward
// subscription's walsender on the publisher is torn down, so the lag gate can no
// longer observe it. The pre-disable steps must short-circuit to "done" on any
// re-entry past this point instead of re-doing freeze/lag work that now makes no
// sense (and would error with "no walsender ...").
func (d Deps) forwardSubDisabled() bool { return d.Mgr.Get().Artifacts.ForwardSubDisabled }

// --- FreezeOldPrimary ---

type freezeOldPrimary struct{ d Deps }

func (s *freezeOldPrimary) ID() runner.StepID { return "FreezeOldPrimary" }
func (s *freezeOldPrimary) Check(context.Context) (bool, error) {
	// Re-freezing after the cutover is pointless (writes already on PG17); skip
	// once the forward subscription has been disabled. Otherwise always-run:
	// FreezeForUpgrade re-applies idempotently (DROP IF EXISTS + CREATE).
	return s.d.forwardSubDisabled(), nil
}
func (s *freezeOldPrimary) Run(ctx context.Context) error {
	s.d.logf("замораживаю запись на старом primary через DML-триггеры (БД %q)...", s.d.Cfg.Upgrade.DBName)
	old, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	if err := old.FreezeForUpgrade(ctx, s.d.Cfg.Upgrade.DBName); err != nil {
		return err
	}
	s.d.logf("запись на старом primary заморожена (apply-воркер reverse-репликации не затронут)")
	return nil
}

// --- WaitFinalLagZero (drain the last changes after the freeze) ---

type waitFinalLagZero struct{ d Deps }

func (s *waitFinalLagZero) ID() runner.StepID { return "WaitFinalLagZero" }
func (s *waitFinalLagZero) Check(ctx context.Context) (bool, error) {
	// Past the cutover the forward subscription is disabled and its walsender is
	// gone, so zero() would error "no walsender ...". Derive "already past this
	// point" from live state, not just our artifact: a state file written by an
	// older binary records forward_sub_disabled=false even though the subscription
	// really is disabled on PG17. The artifact is only a fast-path.
	if s.d.forwardSubDisabled() {
		return true, nil
	}
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return false, err
	}
	enabled, err := pg17.SubscriptionEnabled(ctx, s.d.Cfg.Upgrade.SubscriptionName)
	if err != nil {
		return false, err
	}
	if !enabled {
		// Disabled or already dropped on the subscriber => the cutover happened and
		// the final lag was drained to zero before the disable; nothing left to wait
		// on (and no walsender to read).
		return true, nil
	}
	return s.zero(ctx)
}
func (s *waitFinalLagZero) Run(ctx context.Context) error {
	s.d.logf("жду нулевого финального лага после заморозки (bytes_behind=0 на PG17, до %s)...",
		time.Duration(lagPollAttempts)*lagPollInterval)
	if err := pollLagZero(ctx, s.zero, lagPollAttempts, lagPollInterval); err != nil {
		return fmt.Errorf("switchover: final %w", err)
	}
	s.d.logf("финальный лаг нулевой")
	return nil
}
func (s *waitFinalLagZero) zero(ctx context.Context) (bool, error) {
	// Lag lives on the publisher (the frozen old primary), in pg_stat_replication.
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return false, err
	}
	lag, err := primary.GetSubscriptionLag(ctx, s.d.Cfg.Upgrade.SubscriptionName)
	if err != nil {
		return false, err
	}
	if lag == nil {
		return false, fmt.Errorf("switchover: no walsender for subscription %s on the publisher", s.d.Cfg.Upgrade.SubscriptionName)
	}
	// Byte (LSN) lag, not the time-based *LagMs columns: after the freeze,
	// background WAL keeps the *_lag columns sampling small non-zero values, so
	// they almost never read exactly zero; bytes_behind settles to 0 once the
	// subscriber has replayed up to the publisher's WAL — the real cutover gate.
	return lag.ByteLag == 0, nil
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
	s.d.logf("синхронизирую последовательности: %d шт., буфер=+%d (читаю last_value со старого primary, ставлю на PG17)...",
		len(seqs), s.d.Cfg.Upgrade.SequenceBuffer)
	for _, seq := range seqs {
		// Advance past the old value plus a safety buffer for cached/unflushed
		// nextval allocations on the old primary.
		if err := pg17.SetSequenceValue(ctx, seq.Schema, seq.Name, seq.LastValue+s.d.Cfg.Upgrade.SequenceBuffer); err != nil {
			return err
		}
	}
	s.d.logf("последовательности синхронизированы (%d шт.)", len(seqs))
	return s.d.Mgr.SetSequencesSynced()
}

// --- NotifyDSNSwap (signal external tooling; operator confirms via checkpoint) ---

type notifyDSNSwap struct{ d Deps }

func (s *notifyDSNSwap) ID() runner.StepID { return "NotifyDSNSwap" }
func (s *notifyDSNSwap) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.DSNSwapNotified, nil
}
func (s *notifyDSNSwap) Run(_ context.Context) error {
	s.d.logf("пишу сигнал смены DSN в %s (новый primary -> PG17); оператор/прокси выполняет swap...", s.d.Cfg.Upgrade.DSNSwapSignalPath)
	if err := WriteDSNSwapSignal(s.d.WriteSignal, s.d.Cfg.Upgrade.DSNSwapSignalPath,
		s.d.Cfg.Upgrade.PG17DSN, s.d.Cfg.ClusterName); err != nil {
		return err
	}
	s.d.logf("сигнал смены DSN записан")
	return s.d.Mgr.SetDSNSwapNotified()
}

// --- VerifyTrafficOnNew (delegated swap; binary verifies traffic arrived) ---

type verifyTrafficOnNew struct{ d Deps }

func (s *verifyTrafficOnNew) ID() runner.StepID                   { return "VerifyTrafficOnNew" }
func (s *verifyTrafficOnNew) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifyTrafficOnNew) Run(ctx context.Context) error {
	s.d.logf("проверяю, что трафик приложения переехал на PG17 (считаю клиентские backend'ы)...")
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
	s.d.logf("трафик на PG17: клиентских backend'ов=%d", n)
	return nil
}

// --- DisableForwardSubscription (stop forward apply now that writes are on PG17) ---

type disableForwardSubscription struct{ d Deps }

func (s *disableForwardSubscription) ID() runner.StepID { return "DisableForwardSubscription" }
func (s *disableForwardSubscription) Check(context.Context) (bool, error) {
	return false, nil
} // always-run: ALTER ... DISABLE is a no-op if already disabled
func (s *disableForwardSubscription) Run(ctx context.Context) error {
	s.d.logf("отключаю прямую подписку %q на PG17 (запись теперь идёт на новый primary)...", s.d.Cfg.Upgrade.SubscriptionName)
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return err
	}
	if err := pg17.DisableSubscription(ctx, s.d.Cfg.Upgrade.SubscriptionName); err != nil {
		return err
	}
	s.d.logf("прямая подписка %q отключена", s.d.Cfg.Upgrade.SubscriptionName)
	// Record the cutover point so a switchover re-entry short-circuits the
	// pre-disable steps (freeze, final-lag) instead of erroring on the now-absent
	// walsender.
	return s.d.Mgr.SetForwardSubDisabled()
}

var (
	_ runner.Step = (*freezeOldPrimary)(nil)
	_ runner.Step = (*waitFinalLagZero)(nil)
	_ runner.Step = (*syncSequences)(nil)
	_ runner.Step = (*notifyDSNSwap)(nil)
	_ runner.Step = (*verifyTrafficOnNew)(nil)
	_ runner.Step = (*disableForwardSubscription)(nil)
)
