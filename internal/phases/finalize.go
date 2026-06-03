package phases

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewFinalize builds Phase 7: commit the upgrade by tearing down the rollback
// artifacts (reverse replication, forward subscription, write freeze) and
// verifying the operator-performed Patroni cluster rename.
func NewFinalize(d Deps) runner.Phase {
	return &simplePhase{
		id: "finalize",
		steps: []runner.Step{
			&dropReverseReplication{d},
			&dropForwardSubscription{d},
			&unfreezeOldPrimary{d},
			&verifyRenamedCluster{d},
		},
		trans: []runner.Transition{{To: "cleanup"}},
	}
}

// --- DropReverseReplication (sub_rollback on old primary; pub_rollback on PG17) ---

type dropReverseReplication struct{ d Deps }

func (s *dropReverseReplication) ID() runner.StepID                   { return "DropReverseReplication" }
func (s *dropReverseReplication) Check(context.Context) (bool, error) { return false, nil } // DROP IF EXISTS is idempotent
func (s *dropReverseReplication) Run(ctx context.Context) error {
	s.d.logf("сношу обратную репликацию: подписку %q на старом primary...", s.d.Cfg.Upgrade.ReverseSubName)
	old, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	if err := old.DropSubscription(ctx, s.d.Cfg.Upgrade.ReverseSubName); err != nil {
		return err
	}
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return err
	}
	s.d.logf("сношу публикацию %q на PG17...", s.d.Cfg.Upgrade.ReversePubName)
	if err := pg17.DropPublication(ctx, s.d.Cfg.Upgrade.ReversePubName); err != nil {
		return err
	}
	s.d.logf("обратная репликация удалена — откат больше невозможен")
	return nil
}

// --- DropForwardSubscription (sub_upgrade on PG17; also drops its slot on the old primary) ---

type dropForwardSubscription struct{ d Deps }

func (s *dropForwardSubscription) ID() runner.StepID                   { return "DropForwardSubscription" }
func (s *dropForwardSubscription) Check(context.Context) (bool, error) { return false, nil }
func (s *dropForwardSubscription) Run(ctx context.Context) error {
	pg17, err := s.d.PG17(ctx)
	if err != nil {
		return err
	}
	s.d.logf("сношу прямую подписку %q на PG17 (заодно дропается её слот на старом primary)...", s.d.Cfg.Upgrade.SubscriptionName)
	// sub_upgrade is slot-bearing, so DROP SUBSCRIPTION contacts the old primary
	// to drop its replication slot (as the spec intends). This requires the old
	// primary to be reachable — guaranteed here because finalize always runs
	// before cleanup (which decommissions the old primary), and a re-entry into
	// finalize only happens before the run has advanced to cleanup.
	if err := pg17.DropSubscription(ctx, s.d.Cfg.Upgrade.SubscriptionName); err != nil {
		return err
	}
	s.d.logf("прямая подписка %q удалена", s.d.Cfg.Upgrade.SubscriptionName)
	return nil
}

// --- UnfreezeOldPrimary (drop the DML freeze triggers on the old primary) ---

type unfreezeOldPrimary struct{ d Deps }

func (s *unfreezeOldPrimary) ID() runner.StepID                   { return "UnfreezeOldPrimary" }
func (s *unfreezeOldPrimary) Check(context.Context) (bool, error) { return false, nil } // UnfreezeAfterUpgrade is idempotent
func (s *unfreezeOldPrimary) Run(ctx context.Context) error {
	s.d.logf("снимаю заморозку записи на старом primary (удаляю DML-триггеры, БД %q)...", s.d.Cfg.Upgrade.DBName)
	old, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	if err := old.UnfreezeAfterUpgrade(ctx, s.d.Cfg.Upgrade.DBName); err != nil {
		return err
	}
	s.d.logf("заморозка снята")
	return nil
}

// --- VerifyRenamedCluster (operator renames via etcdctl; binary verifies health) ---

type verifyRenamedCluster struct{ d Deps }

func (s *verifyRenamedCluster) ID() runner.StepID                   { return "VerifyRenamedCluster" }
func (s *verifyRenamedCluster) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifyRenamedCluster) Run(ctx context.Context) error {
	s.d.logf("проверяю переименованный кластер через Patroni (должен быть лидер)...")
	cluster, err := s.d.NewPatroni.GetCluster(ctx)
	if err != nil {
		return err
	}
	if cluster.Leader() == nil {
		return fmt.Errorf("finalize: renamed cluster has no leader (rename the Patroni cluster, then re-run)")
	}
	s.d.logf("переименованный кластер здоров: лидер %s", cluster.Leader().Host)
	return nil
}

var (
	_ runner.Step = (*dropReverseReplication)(nil)
	_ runner.Step = (*dropForwardSubscription)(nil)
	_ runner.Step = (*unfreezeOldPrimary)(nil)
	_ runner.Step = (*verifyRenamedCluster)(nil)
)
