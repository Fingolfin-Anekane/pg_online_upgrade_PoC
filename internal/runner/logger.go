package runner

import (
	"fmt"
	"io"
)

// Logger receives human-facing progress events during a run. The Runner emits
// the phase/step framing (start, result); individual steps emit parameter-level
// Detail lines from within Run (e.g. "создаю слот ... с плагином pgoutput").
//
// All messages are in Russian to match operator-facing output.
type Logger interface {
	PhaseStart(id PhaseID)
	PhaseDone(id PhaseID)
	StepStart(phase PhaseID, step StepID)
	StepResult(phase PhaseID, step StepID, status StepStatus)
	// Detail prints an indented free-form line, nested under the current step.
	Detail(format string, args ...any)
}

// nopLogger discards every event. New() substitutes it when no logger is given,
// so the Runner and steps can call the logger unconditionally.
type nopLogger struct{}

func (nopLogger) PhaseStart(PhaseID)                     {}
func (nopLogger) PhaseDone(PhaseID)                      {}
func (nopLogger) StepStart(PhaseID, StepID)              {}
func (nopLogger) StepResult(PhaseID, StepID, StepStatus) {}
func (nopLogger) Detail(string, ...any)                  {}

// ConsoleLogger renders progress as indented Russian lines to out.
type ConsoleLogger struct{ out io.Writer }

// NewConsoleLogger writes human-readable progress to out (typically os.Stdout).
func NewConsoleLogger(out io.Writer) *ConsoleLogger { return &ConsoleLogger{out: out} }

func (l *ConsoleLogger) PhaseStart(id PhaseID) { fmt.Fprintf(l.out, "\n▶ Фаза %s\n", id) }
func (l *ConsoleLogger) PhaseDone(id PhaseID) {
	fmt.Fprintf(l.out, "✓ Фаза %s завершена\n", id)
}

func (l *ConsoleLogger) StepStart(_ PhaseID, step StepID) {
	fmt.Fprintf(l.out, "  • %s\n", step)
}

func (l *ConsoleLogger) StepResult(_ PhaseID, step StepID, status StepStatus) {
	switch status {
	case StepDone:
		fmt.Fprintf(l.out, "  ✓ %s — готово\n", step)
	case StepSkipped:
		fmt.Fprintf(l.out, "  ↷ %s — уже выполнено, пропуск\n", step)
	case StepFailed:
		fmt.Fprintf(l.out, "  ✗ %s — ошибка\n", step)
	}
}

func (l *ConsoleLogger) Detail(format string, args ...any) {
	fmt.Fprintf(l.out, "      "+format+"\n", args...)
}
