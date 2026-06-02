package phases

import "github.com/dmbabuev/pg-upgrade/internal/runner"

// Phases1to6 returns the ordered phases implemented through Plan 3. The first
// phase ("prepare") is the run's entry point; "switchover" pauses at the
// rollback window (Plan 4 adds Finalize/Cleanup).
func Phases1to6(d Deps) []runner.Phase {
	return []runner.Phase{
		NewPrepare(d),
		NewIsolate(d),
		NewDrain(d),
		NewUpgrade(d),
		NewCatchup(d),
		NewSwitchover(d),
	}
}

// FirstPhase is the entry-point phase id for a fresh run.
const FirstPhase = "prepare"
