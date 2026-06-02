package phases

import "github.com/dmbabuev/pg-upgrade/internal/runner"

// Phases1to8 returns all eight ordered phases. The first phase ("prepare") is
// the run's entry point; "cleanup" is terminal (upgrade complete).
func Phases1to8(d Deps) []runner.Phase {
	return []runner.Phase{
		NewPrepare(d),
		NewIsolate(d),
		NewDrain(d),
		NewUpgrade(d),
		NewCatchup(d),
		NewSwitchover(d),
		NewFinalize(d),
		NewCleanup(d),
	}
}

// FirstPhase is the entry-point phase id for a fresh run.
const FirstPhase = "prepare"
