package phases

import (
	"context"
	"fmt"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/jackc/pglogrepl"
)

// maxProvisionLagBytes is how close (in WAL bytes) the shadow leader's replay
// must be to prod's current WAL before provision is considered caught up.
const maxProvisionLagBytes = 16 * 1024 * 1024

var (
	provisionPollInterval = 5 * time.Second
	provisionTimeout      = 30 * time.Minute
)

// NewProvision builds Phase 0: turn the existing shadow cluster into a Patroni
// standby_cluster of prod, create the physical slot, and wait for it to sync.
func NewProvision(d Deps) runner.Phase {
	return &simplePhase{
		id: "provision",
		steps: []runner.Step{
			&createPhysicalSlot{d},
			&applyStandbyCluster{d},
			&waitShadowCaughtUp{d},
		},
		trans: []runner.Transition{{To: "prepare"}},
	}
}

type createPhysicalSlot struct{ d Deps }

func (s *createPhysicalSlot) ID() runner.StepID                   { return "CreatePhysicalSlot" }
func (s *createPhysicalSlot) Check(context.Context) (bool, error) { return false, nil } // idempotent SQL
func (s *createPhysicalSlot) Run(ctx context.Context) error {
	prod, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	s.d.logf("создаю физический слот %q на проде для стрима шэдоу...", s.d.Cfg.Upgrade.PhysicalSlotName)
	return prod.CreatePhysicalSlot(ctx, s.d.Cfg.Upgrade.PhysicalSlotName)
}

type applyStandbyCluster struct{ d Deps }

func (s *applyStandbyCluster) ID() runner.StepID                   { return "ApplyStandbyCluster" }
func (s *applyStandbyCluster) Check(context.Context) (bool, error) { return false, nil }
func (s *applyStandbyCluster) Run(ctx context.Context) error {
	s.d.logf("навешиваю standby_cluster на шэдоу (источник %s:%d, слот %q)...",
		s.d.Cfg.Upgrade.ShadowSourceHost, s.d.Cfg.Upgrade.ShadowSourcePort, s.d.Cfg.Upgrade.PhysicalSlotName)
	return s.d.NewPatroni.SetStandbyCluster(ctx,
		s.d.Cfg.Upgrade.ShadowSourceHost, s.d.Cfg.Upgrade.ShadowSourcePort, s.d.Cfg.Upgrade.PhysicalSlotName)
}

type waitShadowCaughtUp struct{ d Deps }

func (s *waitShadowCaughtUp) ID() runner.StepID                       { return "WaitShadowCaughtUp" }
func (s *waitShadowCaughtUp) Check(ctx context.Context) (bool, error) { return s.caughtUp(ctx) }
func (s *waitShadowCaughtUp) Run(ctx context.Context) error {
	s.d.logf("жду, пока шэдоу догонит прод (lag < %d байт) и наберёт %d нод...",
		maxProvisionLagBytes, s.d.Cfg.Upgrade.ShadowNodeCount)
	wctx, cancel := context.WithTimeout(ctx, provisionTimeout)
	defer cancel()
	for {
		ok, err := s.caughtUp(wctx)
		if err == nil && ok {
			s.d.logf("шэдоу догнан и в полном составе")
			return nil
		}
		select {
		case <-wctx.Done():
			return fmt.Errorf("provision: shadow not caught up in time: %w", wctx.Err())
		case <-time.After(provisionPollInterval):
		}
	}
}
func (s *waitShadowCaughtUp) caughtUp(ctx context.Context) (bool, error) {
	cluster, err := s.d.NewPatroni.GetCluster(ctx)
	if err != nil {
		return false, err
	}
	if len(cluster.Members) < s.d.Cfg.Upgrade.ShadowNodeCount || cluster.Leader() == nil {
		return false, nil
	}
	prod, err := s.d.Primary(ctx)
	if err != nil {
		return false, err
	}
	shadow, err := s.d.Shadow(ctx)
	if err != nil {
		return false, err
	}
	cur, err := prod.CurrentWALLSN(ctx)
	if err != nil {
		return false, err
	}
	rep, err := shadow.GetLastWALReplayLSN(ctx)
	if err != nil || rep == "" {
		return false, err
	}
	curL, err := pglogrepl.ParseLSN(cur)
	if err != nil {
		return false, err
	}
	repL, err := pglogrepl.ParseLSN(rep)
	if err != nil {
		return false, err
	}
	return int64(curL)-int64(repL) <= maxProvisionLagBytes, nil
}

var (
	_ runner.Step = (*createPhysicalSlot)(nil)
	_ runner.Step = (*applyStandbyCluster)(nil)
	_ runner.Step = (*waitShadowCaughtUp)(nil)
)
