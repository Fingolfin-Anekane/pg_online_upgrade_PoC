package runner

import (
	"context"
	"fmt"

	"github.com/dmbabuev/pg-upgrade/internal/state"
)

// Checkpoint is invoked at a phase boundary in Interactive mode. Returning an
// error aborts the run (operator declined). In Headless mode it is never called.
type Checkpoint func(ctx context.Context, from, to PhaseID) error

// Runner is the FSM engine: it executes the current phase's steps, then follows
// the first matching transition, persisting progress through the state Manager.
type Runner struct {
	phases map[PhaseID]Phase
	mgr    *state.Manager
	mode   RunMode
	cp     Checkpoint
}

// New builds a Runner from a phase list (indexed by ID), a state Manager, a run
// mode, and a checkpoint (used only in Interactive mode).
func New(phases []Phase, mgr *state.Manager, mode RunMode, cp Checkpoint) *Runner {
	idx := make(map[PhaseID]Phase, len(phases))
	for _, p := range phases {
		idx[p.ID()] = p
	}
	return &Runner{phases: idx, mgr: mgr, mode: mode, cp: cp}
}

// Run drives the FSM from state.Current until a phase has no matching transition
// (terminal) or a step/checkpoint fails.
func (r *Runner) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		cur := r.mgr.Get().Current
		phase, ok := r.phases[cur]
		if !ok {
			return fmt.Errorf("runner: unknown phase %q", cur)
		}
		if err := r.executePhase(ctx, phase); err != nil {
			return err
		}
		next, err := r.transition(phase)
		if err != nil {
			return err
		}
		if next == "" {
			return nil // terminal phase reached
		}
		if r.mode == Interactive && r.cp != nil {
			if err := r.cp(ctx, phase.ID(), next); err != nil {
				return fmt.Errorf("runner: aborted at %s: %w", phase.ID(), err)
			}
		}
		if err := r.mgr.Advance(next); err != nil {
			return err
		}
	}
}

// executePhase runs each step that Check() reports as not-yet-done, persisting
// per-step status. A failing step stops the phase.
func (r *Runner) executePhase(ctx context.Context, phase Phase) error {
	for _, step := range phase.Steps() {
		done, err := step.Check(ctx)
		if err != nil {
			_ = r.mgr.FailStep(phase.ID(), step.ID(), err.Error())
			return fmt.Errorf("runner: %s/%s check: %w", phase.ID(), step.ID(), err)
		}
		if done {
			if err := r.mgr.SkipStep(phase.ID(), step.ID()); err != nil {
				return err
			}
			continue
		}
		if err := step.Run(ctx); err != nil {
			_ = r.mgr.FailStep(phase.ID(), step.ID(), err.Error())
			return fmt.Errorf("runner: %s/%s run: %w", phase.ID(), step.ID(), err)
		}
		if err := r.mgr.CompleteStep(phase.ID(), step.ID()); err != nil {
			return err
		}
	}
	return nil
}

// transition returns the target of the first transition whose condition matches
// (nil condition always matches). If transitions exist but none match, the phase
// gate is unmet and an error is returned. If no transitions are declared the
// phase is terminal and "" is returned.
func (r *Runner) transition(phase Phase) (PhaseID, error) {
	st := r.mgr.Get()
	ts := phase.Transitions()
	for _, t := range ts {
		if t.Condition == nil || t.Condition(st) {
			return t.To, nil
		}
	}
	if len(ts) > 0 {
		// transitions exist but none matched: the phase gate is unmet, not done.
		return "", fmt.Errorf("runner: %s: no transition condition matched (phase incomplete)", phase.ID())
	}
	return "", nil // explicit terminal: phase declares no transitions
}
