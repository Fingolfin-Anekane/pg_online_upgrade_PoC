package runner

import (
	"context"

	"github.com/dmbabuev/pg-upgrade/internal/state"
)

// Step is a single idempotent unit of work within a phase.
type Step interface {
	ID() StepID
	// Check returns true if the step was already completed and should be skipped.
	Check(ctx context.Context) (bool, error)
	Run(ctx context.Context) error
}

// Transition describes a possible phase change.
// Condition nil means unconditional; first matching Transition wins.
type Transition struct {
	To        PhaseID
	Condition func(*state.State) bool
}

// Phase is an ordered group of steps with declared forward transitions.
type Phase interface {
	ID() PhaseID
	Steps() []Step
	Transitions() []Transition
}
