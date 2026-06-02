package runner

// PhaseID and StepID are type aliases (not distinct types) so they are
// interchangeable with string. Phases pass p.ID() to state.Manager methods
// that take plain strings without explicit conversion.
type PhaseID = string
type StepID = string

type StepStatus string

const (
	StepPending StepStatus = "pending"
	StepSkipped StepStatus = "skipped"
	StepRunning StepStatus = "running"
	StepDone    StepStatus = "done"
	StepFailed  StepStatus = "failed"
)

type RunMode int

const (
	Interactive RunMode = iota
	Headless
)
