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
			&verifyOldClusterStopped{d},
			&patchNewPatroniConfig{d},
			&startPG17{d},
			&createForwardSubscription{d},
			&waitLagZero{d},
			&verifyNewClusterHealthy{d},
		},
		trans: []runner.Transition{{To: "switchover"}},
	}
}

// --- VerifyOldClusterStopped: refuse to bring up PG17 while the old server lives ---
//
// pg_upgrade --link hard-links the old and new data files, so a running OLD
// postgres on this node WILL corrupt the upgraded cluster (the docs forbid
// running the old cluster after --link). The old cluster's Patroni, if its
// service is restarted, can resurrect the old postmaster on the old data dir —
// pausing the cluster does not prevent that. So before starting PG17 we fail
// hard if any old postmaster is alive on DataDir.
type verifyOldClusterStopped struct{ d Deps }

func (s *verifyOldClusterStopped) ID() runner.StepID                   { return "VerifyOldClusterStopped" }
func (s *verifyOldClusterStopped) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifyOldClusterStopped) Run(ctx context.Context) error {
	running, err := s.d.Tools.IsRunning(ctx, s.d.Cfg.Upgrade.OldPGBindir, s.d.Cfg.Upgrade.DataDir)
	if err != nil {
		return err
	}
	if running {
		return fmt.Errorf("catchup: an OLD postgres is still running on %s. After pg_upgrade --link the old and new clusters share data files via hard links, so a running old server corrupts the upgraded cluster. Stop the OLD Patroni on this node (it can restart the old server; cluster-pause does not prevent that) and the old server, then re-run: pg_ctl -D %s stop -m fast",
			s.d.Cfg.Upgrade.DataDir, s.d.Cfg.Upgrade.DataDir)
	}
	s.d.logf("старый postgres на %s остановлен — безопасно поднимать PG17", s.d.Cfg.Upgrade.DataDir)
	return nil
}

// --- PatchNewPatroniConfig: rewrite the new-cluster patroni.yml for PG17 ---
//
// The upgraded PG17 cluster has a NEW system identifier and lives in a new data
// dir. Patroni will refuse to start ("system ID mismatch") under the old scope,
// and would manage the new data dir with the old binaries if bin_dir still
// points at the old major. So before starting Patroni we patch the new
// cluster's patroni.yml in place: scope -> EffectiveNewScope (default
// "<cluster_name>-17"), postgresql.data_dir -> new_data_dir, postgresql.bin_dir
// -> new_pg_bindir, and postgresql.config_dir -> new_config_dir when set. The
// operator's original is preserved once as <path>.bak.
type patchNewPatroniConfig struct{ d Deps }

func (s *patchNewPatroniConfig) ID() runner.StepID { return "PatchNewPatroniConfig" }
func (s *patchNewPatroniConfig) Check(_ context.Context) (bool, error) {
	data, err := os.ReadFile(s.d.Cfg.Upgrade.PatroniConfigPath)
	if err != nil {
		return false, fmt.Errorf("catchup: read new-cluster patroni config %s: %w", s.d.Cfg.Upgrade.PatroniConfigPath, err)
	}
	cur, err := parsePatroniManagedFields(data)
	if err != nil {
		return false, err
	}
	if cur.Scope != s.d.Cfg.EffectiveNewScope() {
		return false, nil
	}
	if cur.DataDir != s.d.Cfg.Upgrade.NewDataDir {
		return false, nil
	}
	if cur.BinDir != s.d.Cfg.Upgrade.NewPGBindir {
		return false, nil
	}
	if s.d.Cfg.Upgrade.NewConfigDir != "" && cur.ConfigDir != s.d.Cfg.Upgrade.NewConfigDir {
		return false, nil
	}
	return true, nil
}
func (s *patchNewPatroniConfig) Run(_ context.Context) error {
	path := s.d.Cfg.Upgrade.PatroniConfigPath
	scope := s.d.Cfg.EffectiveNewScope()
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("catchup: read new-cluster patroni config %s: %w", path, err)
	}
	// Preserve the operator's original once; never clobber an existing backup.
	bak := path + ".bak"
	if _, statErr := os.Stat(bak); os.IsNotExist(statErr) {
		if err := os.WriteFile(bak, data, 0o644); err != nil {
			return fmt.Errorf("catchup: write patroni config backup %s: %w", bak, err)
		}
	}
	s.d.logf("патчу новый patroni.yml %s: scope=%q, data_dir=%q, bin_dir=%q, config_dir=%q...",
		path, scope, s.d.Cfg.Upgrade.NewDataDir, s.d.Cfg.Upgrade.NewPGBindir, s.d.Cfg.Upgrade.NewConfigDir)
	patched, err := patchPatroniConfig(data, scope, s.d.Cfg.Upgrade.NewDataDir, s.d.Cfg.Upgrade.NewPGBindir, s.d.Cfg.Upgrade.NewConfigDir)
	if err != nil {
		return err
	}
	// Write atomically (tmp + rename within the same dir/filesystem) so Patroni,
	// which may read this file at any moment, never sees a half-written config.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, patched, 0o644); err != nil {
		return fmt.Errorf("catchup: write patched patroni config %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("catchup: replace patroni config %s: %w", path, err)
	}
	s.d.logf("новый patroni.yml приведён к PG17 (оригинал сохранён в %s)", bak)
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
	_ runner.Step = (*verifyOldClusterStopped)(nil)
	_ runner.Step = (*patchNewPatroniConfig)(nil)
	_ runner.Step = (*startPG17)(nil)
	_ runner.Step = (*createForwardSubscription)(nil)
	_ runner.Step = (*waitLagZero)(nil)
	_ runner.Step = (*verifyNewClusterHealthy)(nil)
)
