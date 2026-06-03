package phases

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/connect"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// NewCatchup builds Phase 5. The binary brings up PG17 under Patroni on N1 (so
// the new cluster is Patroni-managed, not bare pg_ctl); adding the replicas
// N2/N3 stays operator-driven, and the binary verifies the formed cluster in
// VerifyNewClusterHealthy.
func NewCatchup(d Deps) runner.Phase {
	return &simplePhase{
		id: "catchup",
		steps: []runner.Step{
			&verifyNewPatroniConfig{d},
			&startPG17{d},
			&createForwardSubscription{d},
			&waitLagZero{d},
			&verifyNewClusterHealthy{d},
		},
		trans: []runner.Transition{{To: "switchover"}},
	}
}

// --- VerifyNewPatroniConfig: fail fast on a misconfigured new-cluster patroni.yml ---
//
// The upgraded PG17 cluster has a NEW system identifier. Patroni will refuse to
// start ("system ID mismatch") if pointed at the old cluster's scope, whose DCS
// /initialize key still holds the old sysid. So the new cluster's patroni.yml
// must declare a fresh scope and the upgraded data_dir. We verify that before
// starting Patroni, turning a cryptic mismatch into a clear error.

type verifyNewPatroniConfig struct{ d Deps }

func (s *verifyNewPatroniConfig) ID() runner.StepID                   { return "VerifyNewPatroniConfig" }
func (s *verifyNewPatroniConfig) Check(context.Context) (bool, error) { return false, nil } // always verify (cheap)
func (s *verifyNewPatroniConfig) Run(_ context.Context) error {
	path := s.d.Cfg.Upgrade.PatroniConfigPath
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("catchup: read new-cluster patroni config %s: %w", path, err)
	}
	scope, dataDir, err := parsePatroniScopeDataDir(data)
	if err != nil {
		return err
	}
	// scope/data_dir may be set via env rather than the file; only enforce what
	// the file actually declares to avoid false positives.
	if scope != "" && scope == s.d.Cfg.ClusterName {
		return fmt.Errorf("catchup: new-cluster patroni scope %q must differ from the old cluster %q — reusing it makes Patroni refuse to start (system ID mismatch: the upgraded cluster has a new sysid). Use a fresh scope (e.g. %q-17) and rename it to %q in finalize",
			scope, s.d.Cfg.ClusterName, s.d.Cfg.ClusterName, s.d.Cfg.ClusterName)
	}
	if dataDir != "" && dataDir != s.d.Cfg.Upgrade.NewDataDir {
		return fmt.Errorf("catchup: new-cluster patroni postgresql.data_dir %q must match upgrade.new_data_dir %q", dataDir, s.d.Cfg.Upgrade.NewDataDir)
	}
	s.d.logf("конфиг нового Patroni ок: scope=%q (≠ старого %q), data_dir=%q; PG17 sysid=%s",
		scope, s.d.Cfg.ClusterName, dataDir, s.d.Mgr.Get().Artifacts.PG17SYSID)
	return nil
}

// --- StartPG17OnN1: bring PG17 up under Patroni ---

type startPG17 struct{ d Deps }

func (s *startPG17) ID() runner.StepID { return "StartPG17OnN1" }
func (s *startPG17) Check(ctx context.Context) (bool, error) {
	// pg_ctl status (reads postmaster.pid locally) authoritatively reports whether
	// PG17 is already up — whether Patroni or a prior run started it — so a resumed
	// run does not start it twice. DSN reachability is surfaced later, at
	// CreateForwardSubscription.
	return s.d.Tools.IsRunning(ctx, s.d.Cfg.Upgrade.NewPGBindir, s.d.Cfg.Upgrade.NewDataDir)
}
func (s *startPG17) Run(ctx context.Context) error {
	cmd := s.d.Cfg.Upgrade.PatroniStartCommand
	s.d.logf("поднимаю новый кластер под Patroni на N1: %q...", cmd)
	if err := s.d.Tools.StartPatroni(ctx, cmd); err != nil {
		return err
	}
	s.d.logf("жду, пока Patroni поднимет PG17 (poll pg_ctl status, до 60с)...")
	if err := waitRunning(ctx, s.d.Tools, s.d.Cfg.Upgrade.NewPGBindir, s.d.Cfg.Upgrade.NewDataDir, 60, time.Second); err != nil {
		return err
	}
	s.d.logf("PG17 поднят под управлением Patroni")
	return nil
}

// waitRunning polls Tools.IsRunning until it reports the server is up, up to
// attempts times spaced by interval. Patroni's start command returns before
// Postgres is actually accepting connections, so we wait for it explicitly.
func waitRunning(ctx context.Context, tools interface {
	IsRunning(context.Context, string, string) (bool, error)
}, bindir, dataDir string, attempts int, interval time.Duration) error {
	for i := 0; i < attempts; i++ {
		running, err := tools.IsRunning(ctx, bindir, dataDir)
		if err == nil && running {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
	return fmt.Errorf("catchup: PG17 did not come up under Patroni within %s", time.Duration(attempts)*interval)
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
	_ runner.Step = (*verifyNewPatroniConfig)(nil)
	_ runner.Step = (*startPG17)(nil)
	_ runner.Step = (*createForwardSubscription)(nil)
	_ runner.Step = (*waitLagZero)(nil)
	_ runner.Step = (*verifyNewClusterHealthy)(nil)
)
