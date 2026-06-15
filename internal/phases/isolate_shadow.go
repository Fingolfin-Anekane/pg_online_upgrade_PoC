package phases

import (
	"context"
	"fmt"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

var shadowPromoteTimeout = 60 * time.Second

// NewIsolateShadow freezes the shadow leader at target_lsn by promoting it out
// of standby-cluster mode, settling replay, and dropping the physical slot. The
// production cluster is untouched.
func NewIsolateShadow(d Deps) runner.Phase {
	return &simplePhase{
		id: "isolate",
		steps: []runner.Step{
			&promoteShadow{d},
			&waitShadowPromoted{d},
			&settleShadowTarget{d},
			&dropPhysicalSlot{d},
			&recordTargetLSNShadow{d},
		},
		trans: []runner.Transition{{To: "drain"}},
	}
}

type promoteShadow struct{ d Deps }

func (s *promoteShadow) ID() runner.StepID                   { return "PromoteShadow" }
func (s *promoteShadow) Check(context.Context) (bool, error) { return false, nil }
func (s *promoteShadow) Run(ctx context.Context) error {
	s.d.logf("промоутю лидер шэдоу: снимаю standby_cluster → Patroni поднимет его как standalone primary...")
	return s.d.NewPatroni.ClearStandbyCluster(ctx)
}

type waitShadowPromoted struct{ d Deps }

func (s *waitShadowPromoted) ID() runner.StepID                       { return "WaitShadowPromoted" }
func (s *waitShadowPromoted) Check(ctx context.Context) (bool, error) { return s.promoted(ctx) }
func (s *waitShadowPromoted) Run(ctx context.Context) error {
	s.d.logf("жду, пока Patroni промоутит лидер шэдоу (pg_is_in_recovery=false)...")
	wctx, cancel := context.WithTimeout(ctx, shadowPromoteTimeout)
	defer cancel()
	for {
		ok, err := s.promoted(wctx)
		if err == nil && ok {
			return nil
		}
		select {
		case <-wctx.Done():
			return fmt.Errorf("isolate-shadow: shadow leader not promoted in time: %w", wctx.Err())
		case <-time.After(time.Second):
		}
	}
}
func (s *waitShadowPromoted) promoted(ctx context.Context) (bool, error) {
	shadow, err := s.d.Shadow(ctx)
	if err != nil {
		return false, err
	}
	inRec, err := shadow.IsInRecovery(ctx)
	if err != nil {
		return false, err
	}
	return !inRec, nil
}

// settleShadowTarget reuses waitReplaySettled: with the receiver gone after
// promote, replay holds at the freeze point; that settled LSN is target_lsn.
type settleShadowTarget struct{ d Deps }

func (s *settleShadowTarget) ID() runner.StepID { return "SettleShadowTarget" }
func (s *settleShadowTarget) Check(context.Context) (bool, error) {
	return s.d.Mgr.Get().Artifacts.TargetLSN != "", nil
}
func (s *settleShadowTarget) Run(ctx context.Context) error {
	shadow, err := s.d.Shadow(ctx)
	if err != nil {
		return err
	}
	s.d.logf("жду стабилизации replay на лидере шэдоу (точка заморозки = target_lsn)...")
	wctx, cancel := context.WithTimeout(ctx, replayDrainTimeout)
	defer cancel()
	settled, err := waitReplaySettled(wctx, shadow, replayDrainInterval, replayStableSamples)
	if err != nil {
		return err
	}
	s.d.logf("replay устаканился на %s", settled)
	return s.d.Mgr.SetTargetLSN(settled)
}

type dropPhysicalSlot struct{ d Deps }

func (s *dropPhysicalSlot) ID() runner.StepID                   { return "DropPhysicalSlot" }
func (s *dropPhysicalSlot) Check(context.Context) (bool, error) { return false, nil }
func (s *dropPhysicalSlot) Run(ctx context.Context) error {
	prod, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	s.d.logf("дроп физического слота %q на проде (шэдоу больше не стримит физически)...", s.d.Cfg.Upgrade.PhysicalSlotName)
	return prod.DropReplicationSlot(ctx, s.d.Cfg.Upgrade.PhysicalSlotName)
}

// recordTargetLSNShadow asserts the slot-baseline invariant (confirmed_flush <=
// target). target_lsn was set by settleShadowTarget, so Check short-circuits.
type recordTargetLSNShadow struct{ d Deps }

func (s *recordTargetLSNShadow) ID() runner.StepID { return "RecordTargetLSN" }
func (s *recordTargetLSNShadow) Check(context.Context) (bool, error) {
	bl := s.d.Mgr.Get().Artifacts.SlotBaseline
	return s.d.Mgr.Get().Artifacts.TargetLSN != "" && bl != nil, nil
}
func (s *recordTargetLSNShadow) Run(ctx context.Context) error {
	return assertSlotBaselineBelowTarget(s.d.Mgr.Get().Artifacts.SlotBaseline, s.d.Mgr.Get().Artifacts.TargetLSN)
}

var (
	_ runner.Step = (*promoteShadow)(nil)
	_ runner.Step = (*waitShadowPromoted)(nil)
	_ runner.Step = (*settleShadowTarget)(nil)
	_ runner.Step = (*dropPhysicalSlot)(nil)
	_ runner.Step = (*recordTargetLSNShadow)(nil)
)
