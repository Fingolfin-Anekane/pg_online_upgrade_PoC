package runner

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
)

// PhasePrompts maps the just-completed phase to the question shown before
// advancing. A phase with no entry falls back to a generic prompt.
type PhasePrompts = map[PhaseID]string

// InteractiveCheckpoint returns a Checkpoint that prints the phase's prompt and
// requires the operator to type "y" to proceed; anything else aborts.
func InteractiveCheckpoint(in io.Reader, out io.Writer, prompts PhasePrompts) Checkpoint {
	reader := bufio.NewReader(in)
	return func(_ context.Context, from, to PhaseID) error {
		msg := prompts[from]
		if msg == "" {
			msg = fmt.Sprintf("Phase %q complete. Proceed to %q?", from, to)
		}
		fmt.Fprintf(out, "\n>>> %s [y/N]: ", msg)
		line, _ := reader.ReadString('\n')
		if strings.ToLower(strings.TrimSpace(line)) != "y" {
			return fmt.Errorf("operator declined at phase %q", from)
		}
		return nil
	}
}

// DefaultPrompts are the checkpoint questions from the design spec.
func DefaultPrompts() PhasePrompts {
	return PhasePrompts{
		"prepare": "Logical slot created. Proceed to isolate N1?",
		"isolate": "N1 isolated, target_lsn recorded. Run slot drain?",
		"drain":   "Slot drained. Proceed to pg_upgrade (point of no return)?",
		"upgrade": "pg_upgrade complete. Proceed to catchup (Plan 3)?",
	}
}
