package phases

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	"github.com/dmbabuev/pg-upgrade/internal/diskguard"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

var (
	rebuildPollInterval = 5 * time.Second
	rebuildTimeout      = 60 * time.Minute
)

// NewRebuildReplicas builds the phase that rebuilds the shadow replicas as PG17
// standbys of the upgraded leader, throttled by disk pressure on the prod slot.
func NewRebuildReplicas(d Deps) runner.Phase {
	return &simplePhase{
		id:    "rebuild-replicas",
		steps: []runner.Step{&reinitShadowReplicas{d}, &waitReplicasHealthy{d}},
		trans: []runner.Transition{{To: "switchover"}},
	}
}

type reinitShadowReplicas struct{ d Deps }

func (s *reinitShadowReplicas) ID() runner.StepID                   { return "ReinitShadowReplicas" }
func (s *reinitShadowReplicas) Check(context.Context) (bool, error) { return false, nil }
func (s *reinitShadowReplicas) Run(ctx context.Context) error {
	cluster, err := s.d.NewPatroni.GetCluster(ctx)
	if err != nil {
		return err
	}
	for _, m := range cluster.Members {
		if m.Role == "leader" {
			continue
		}
		if err := s.throttleBeforeLoad(ctx); err != nil {
			return err
		}
		s.d.logf("reinit реплики шэдоу %q (basebackup с PG17-лидера)...", m.Name)
		mc := s.d.ShadowMember(memberAPIURL(s.d.Cfg.Upgrade.ShadowPatroniURL, m))
		if err := mc.Reinitialize(ctx); err != nil {
			return fmt.Errorf("rebuild-replicas: reinit %s: %w", m.Name, err)
		}
	}
	return nil
}

// throttleBeforeLoad blocks while the prod slot is in the throttle band and
// aborts if it crosses the cap, so basebackup load can't invalidate the slot.
func (s *reinitShadowReplicas) throttleBeforeLoad(ctx context.Context) error {
	if s.d.DiskGuard == nil {
		return nil
	}
	wctx, cancel := context.WithTimeout(ctx, rebuildTimeout)
	defer cancel()
	for {
		dec, retained, err := s.d.DiskGuard.Sample(wctx)
		if err != nil {
			return err
		}
		switch dec {
		case diskguard.Abort:
			return fmt.Errorf("rebuild-replicas: prod slot disk pressure too high (retained=%d) — aborting before invalidation; raise max_slot_wal_keep_size or free disk, then re-run", retained)
		case diskguard.OK:
			return nil
		case diskguard.Throttle:
			s.d.logf("⏳ слот под давлением (retained=%d) — пауза перед следующим reinit...", retained)
		}
		select {
		case <-wctx.Done():
			return fmt.Errorf("rebuild-replicas: slot stayed under pressure too long: %w", wctx.Err())
		case <-time.After(rebuildPollInterval):
		}
	}
}

type waitReplicasHealthy struct{ d Deps }

func (s *waitReplicasHealthy) ID() runner.StepID                       { return "WaitReplicasHealthy" }
func (s *waitReplicasHealthy) Check(ctx context.Context) (bool, error) { return s.healthy(ctx) }
func (s *waitReplicasHealthy) Run(ctx context.Context) error {
	s.d.logf("жду, пока все реплики шэдоу станут running и наберут полный состав...")
	wctx, cancel := context.WithTimeout(ctx, rebuildTimeout)
	defer cancel()
	for {
		ok, err := s.healthy(wctx)
		if err == nil && ok {
			s.d.logf("реплики шэдоу здоровы — новый кластер в HA")
			return nil
		}
		select {
		case <-wctx.Done():
			return fmt.Errorf("rebuild-replicas: replicas not healthy in time: %w", wctx.Err())
		case <-time.After(rebuildPollInterval):
		}
	}
}
func (s *waitReplicasHealthy) healthy(ctx context.Context) (bool, error) {
	cluster, err := s.d.NewPatroni.GetCluster(ctx)
	if err != nil {
		return false, err
	}
	if cluster.Leader() == nil || len(cluster.Members) < s.d.Cfg.Upgrade.ShadowNodeCount {
		return false, nil
	}
	return true, nil
}

// memberAPIURL takes the shadow Patroni base URL and returns the same URL with
// its host replaced by the member's host (API port is the same across members).
func memberAPIURL(base string, m patroni.Member) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	_, port, _ := net.SplitHostPort(u.Host)
	if port != "" {
		u.Host = net.JoinHostPort(m.Host, port)
	} else {
		u.Host = m.Host
	}
	return u.String()
}

var (
	_ runner.Step = (*reinitShadowReplicas)(nil)
	_ runner.Step = (*waitReplicasHealthy)(nil)
)
