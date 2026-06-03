package phases

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/connect"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewCatchup builds Phase 5. The spec's InitNewPatroniCluster and AddReplicas
// steps are operator-driven (forming the new Patroni cluster on N1/N2/N3 is out
// of the single-binary scope); the binary verifies the result in
// VerifyNewClusterHealthy rather than performing the formation.
func NewCatchup(d Deps) runner.Phase {
	return &simplePhase{
		id: "catchup",
		steps: []runner.Step{
			&startPG17{d},
			&createForwardSubscription{d},
			&waitLagZero{d},
			&verifyNewClusterHealthy{d},
		},
		trans: []runner.Transition{{To: "switchover"}},
	}
}

// --- StartPG17OnN1 ---

type startPG17 struct{ d Deps }

func (s *startPG17) ID() runner.StepID { return "StartPG17OnN1" }
func (s *startPG17) Check(ctx context.Context) (bool, error) {
	// Use pg_ctl status (reads postmaster.pid locally) rather than a DSN probe:
	// it authoritatively reports whether the server is already up, so a resumed
	// run does not try to start a second postmaster (which fails). DSN
	// reachability is a separate concern surfaced at CreateForwardSubscription.
	return s.d.Tools.IsRunning(ctx, s.d.Cfg.Upgrade.NewPGBindir, s.d.Cfg.Upgrade.NewDataDir)
}
func (s *startPG17) Run(ctx context.Context) error {
	s.d.logf("стартую PG17 на N1 (bindir=%s datadir=%s)...", s.d.Cfg.Upgrade.NewPGBindir, s.d.Cfg.Upgrade.NewDataDir)
	if err := s.d.Tools.Start(ctx, s.d.Cfg.Upgrade.NewPGBindir, s.d.Cfg.Upgrade.NewDataDir); err != nil {
		return err
	}
	s.d.logf("PG17 запущен")
	return nil
}

// --- CreateForwardSubscription (PG17 subscribes to old primary's publication) ---

type createForwardSubscription struct{ d Deps }

func (s *createForwardSubscription) ID() runner.StepID { return "CreateForwardSubscription" }
func (s *createForwardSubscription) Check(ctx context.Context) (bool, error) {
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return false, err
	}
	return pg17.SubscriptionExists(ctx, s.d.Cfg.Upgrade.SubscriptionName)
}
func (s *createForwardSubscription) Run(ctx context.Context) error {
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return err
	}
	primaryDSN, err := connect.DSNForHost(s.d.Cfg.PG.SuperuserDSN, s.d.Mgr.Get().Artifacts.PrimaryHost)
	if err != nil {
		return err
	}
	s.d.logf("создаю прямую подписку %q на PG17 (publication=%q, переиспользую слит. слот %q, create_slot=false)...",
		s.d.Cfg.Upgrade.SubscriptionName, s.d.Cfg.Upgrade.PublicationName, s.d.Cfg.Upgrade.SlotName)
	if err := pg17.CreateSubscription(ctx,
		s.d.Cfg.Upgrade.SubscriptionName, primaryDSN, s.d.Cfg.Upgrade.PublicationName, s.d.Cfg.Upgrade.SlotName); err != nil {
		return err
	}
	s.d.logf("подписка %q создана — пошёл догон хвоста изменений", s.d.Cfg.Upgrade.SubscriptionName)
	return nil
}

// --- WaitLagZero ---

type waitLagZero struct{ d Deps }

func (s *waitLagZero) ID() runner.StepID                       { return "WaitLagZero" }
func (s *waitLagZero) Check(ctx context.Context) (bool, error) { return s.zero(ctx) }
func (s *waitLagZero) Run(ctx context.Context) error {
	s.d.logf("проверяю лаг прямой подписки %q (нужен write=flush=replay=0)...", s.d.Cfg.Upgrade.SubscriptionName)
	zero, err := s.zero(ctx)
	if err != nil {
		return err
	}
	if !zero {
		return fmt.Errorf("catchup: subscription lag not yet zero; re-run pg-upgrade to retry")
	}
	s.d.logf("лаг нулевой — PG17 догнал старый primary")
	return nil
}
func (s *waitLagZero) zero(ctx context.Context) (bool, error) {
	// Lag lives on the publisher (the old primary), in pg_stat_replication.
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return false, err
	}
	lag, err := primary.GetSubscriptionLag(ctx, s.d.Cfg.Upgrade.SubscriptionName)
	if err != nil {
		return false, err
	}
	if lag == nil {
		return false, fmt.Errorf("catchup: no walsender for subscription %s on the publisher yet", s.d.Cfg.Upgrade.SubscriptionName)
	}
	return lag.WriteLagMs == 0 && lag.FlushLagMs == 0 && lag.ReplayLagMs == 0, nil
}

// --- VerifyNewClusterHealthy (delegated formation; binary verifies) ---

type verifyNewClusterHealthy struct{ d Deps }

func (s *verifyNewClusterHealthy) ID() runner.StepID                   { return "VerifyNewClusterHealthy" }
func (s *verifyNewClusterHealthy) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifyNewClusterHealthy) Run(ctx context.Context) error {
	s.d.logf("проверяю здоровье нового кластера через Patroni (лидер + хотя бы одна реплика)...")
	cluster, err := s.d.NewPatroni.GetCluster(ctx)
	if err != nil {
		return err
	}
	if cluster.Leader() == nil {
		return fmt.Errorf("catchup: new Patroni cluster has no leader (form the new cluster, then re-run)")
	}
	standbys := 0
	for _, m := range cluster.Members {
		if m.Role != "leader" { // replica, sync_standby, etc.
			standbys++
		}
	}
	if standbys < 1 {
		return fmt.Errorf("catchup: new Patroni cluster has no standby yet (add a replica, then re-run)")
	}
	s.d.logf("новый кластер здоров: есть лидер и реплик=%d", standbys)
	return nil
}

var (
	_ runner.Step = (*startPG17)(nil)
	_ runner.Step = (*createForwardSubscription)(nil)
	_ runner.Step = (*waitLagZero)(nil)
	_ runner.Step = (*verifyNewClusterHealthy)(nil)
)
