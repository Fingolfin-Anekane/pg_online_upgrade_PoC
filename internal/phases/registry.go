package phases

import "github.com/dmbabuev/pg-upgrade/internal/runner"

// Phases1to4 returns the ordered phases implemented in Plan 2. The first phase
// ("prepare") is the run's entry point.
func Phases1to4(d Deps) []runner.Phase {
	return []runner.Phase{
		NewPrepare(d),
		NewIsolate(d),
		NewDrain(d),
		NewUpgrade(d),
	}
}

// FirstPhase is the entry-point phase id for a fresh run.
const FirstPhase = "prepare"
