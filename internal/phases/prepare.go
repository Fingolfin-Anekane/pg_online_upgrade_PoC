package phases

import (
	"context"
	"fmt"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/dmbabuev/pg-upgrade/internal/state"
)

// NewPrepare builds Phase 1: create the logical replication foundation on the
// primary before N1 is isolated.
func NewPrepare(d Deps) runner.Phase {
	return &simplePhase{
		id: "prepare",
		steps: []runner.Step{
			&discoverTopology{d},
			&verifyPrerequisites{d},
			&createPublication{d},
			&createLogicalSlot{d},
			&recordSlotBaseline{d},
		},
		trans: []runner.Transition{{To: "isolate"}},
	}
}

// simplePhase is a reusable Phase backed by fixed steps and transitions.
type simplePhase struct {
	id    runner.PhaseID
	steps []runner.Step
	trans []runner.Transition
}

func (p *simplePhase) ID() runner.PhaseID               { return p.id }
func (p *simplePhase) Steps() []runner.Step             { return p.steps }
func (p *simplePhase) Transitions() []runner.Transition { return p.trans }

// --- DiscoverTopology ---

type discoverTopology struct{ d Deps }

func (s *discoverTopology) ID() runner.StepID { return "DiscoverTopology" }
func (s *discoverTopology) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.PrimaryHost != "", nil
}
func (s *discoverTopology) Run(ctx context.Context) error {
	cluster, err := s.d.Patroni.GetCluster(ctx)
	if err != nil {
		return err
	}
	leader := cluster.Leader()
	if leader == nil {
		return fmt.Errorf("prepare: no leader in Patroni cluster")
	}
	return s.d.Mgr.SetPrimaryHost(leader.Host)
}

// --- VerifyPrerequisites ---

type verifyPrerequisites struct{ d Deps }

func (s *verifyPrerequisites) ID() runner.StepID                   { return "VerifyPrerequisites" }
func (s *verifyPrerequisites) Check(context.Context) (bool, error) { return false, nil } // always verify
func (s *verifyPrerequisites) Run(ctx context.Context) error {
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	level, err := primary.ShowWALLevel(ctx)
	if err != nil {
		return err
	}
	if level != "logical" {
		return fmt.Errorf("prepare: primary wal_level=%q, need logical", level)
	}
	inRec, err := s.d.N1.IsInRecovery(ctx)
	if err != nil {
		return err
	}
	if !inRec {
		return fmt.Errorf("prepare: target node %s is not a replica (not in recovery)", s.d.Cfg.Upgrade.TargetNode)
	}
	v, err := s.d.N1.ServerVersionNum(ctx)
	if err != nil {
		return err
	}
	if v < 100000 {
		return fmt.Errorf("prepare: N1 server_version_num %d is below the minimum supported PostgreSQL 10", v)
	}
	return nil
}

// --- CreatePublication ---

type createPublication struct{ d Deps }

func (s *createPublication) ID() runner.StepID { return "CreatePublication" }
func (s *createPublication) Check(ctx context.Context) (bool, error) {
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return false, err
	}
	// pg has no CREATE PUBLICATION IF NOT EXISTS, so skip the create when the
	// publication already exists (idempotent resume).
	return primary.PublicationExists(ctx, s.d.Cfg.Upgrade.PublicationName)
}
func (s *createPublication) Run(ctx context.Context) error {
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	return primary.CreatePublication(ctx, s.d.Cfg.Upgrade.PublicationName)
}

// --- CreateLogicalSlot ---

type createLogicalSlot struct{ d Deps }

func (s *createLogicalSlot) ID() runner.StepID { return "CreateLogicalSlot" }
func (s *createLogicalSlot) Check(ctx context.Context) (bool, error) {
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return false, err
	}
	slot, err := primary.GetReplicationSlot(ctx, s.d.Cfg.Upgrade.SlotName)
	if err != nil {
		return false, err
	}
	return slot != nil, nil
}
func (s *createLogicalSlot) Run(ctx context.Context) error {
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	_, err = primary.CreateLogicalSlot(ctx, s.d.Cfg.Upgrade.SlotName, "pgoutput")
	return err
}

// --- RecordSlotBaseline ---

type recordSlotBaseline struct{ d Deps }

func (s *recordSlotBaseline) ID() runner.StepID { return "RecordSlotBaseline" }
func (s *recordSlotBaseline) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.SlotBaseline != nil, nil
}
func (s *recordSlotBaseline) Run(ctx context.Context) error {
	primary, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	slot, err := primary.GetReplicationSlot(ctx, s.d.Cfg.Upgrade.SlotName)
	if err != nil {
		return err
	}
	if slot == nil {
		return fmt.Errorf("prepare: slot %s missing at baseline", s.d.Cfg.Upgrade.SlotName)
	}
	return s.d.Mgr.SetSlotBaseline(&state.SlotBaseline{
		CapturedAt:        time.Now(),
		RestartLSN:        slot.RestartLSN,
		ConfirmedFlushLSN: slot.ConfirmedFlushLSN,
		PrimaryHost:       s.d.Mgr.Get().Artifacts.PrimaryHost,
	})
}

// (interface assertions)
var (
	_ runner.Step = (*discoverTopology)(nil)
	_ runner.Step = (*verifyPrerequisites)(nil)
	_ runner.Step = (*createPublication)(nil)
	_ runner.Step = (*createLogicalSlot)(nil)
	_ runner.Step = (*recordSlotBaseline)(nil)
)
