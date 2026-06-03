package runner

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// detailStep emits a parameter-level line from Run via the supplied logger.
type detailStep struct {
	id  StepID
	log Logger
}

func (s *detailStep) ID() StepID                          { return s.id }
func (s *detailStep) Check(context.Context) (bool, error) { return false, nil }
func (s *detailStep) Run(context.Context) error {
	s.log.Detail("делаю %s", "нечто")
	return nil
}

func TestConsoleLoggerFramesPhasesAndSteps(t *testing.T) {
	var buf bytes.Buffer
	log := NewConsoleLogger(&buf)

	skipped := &fakeStep{id: "AlreadyDone", checkRes: true}
	ran := &detailStep{id: "DoWork", log: log}
	a := &fakePhase{id: "prepare", steps: []Step{skipped, ran}, trans: []Transition{{To: "isolate"}}}
	b := &fakePhase{id: "isolate"} // terminal

	mgr := newMgr(t, "prepare")
	r := New([]Phase{a, b}, mgr, Headless, nil, log)
	require.NoError(t, r.Run(context.Background()))

	got := buf.String()
	// Framing: phase headers, skip marker, step bullet + result, nested detail.
	for _, want := range []string{
		"▶ Фаза prepare",
		"↷ AlreadyDone — уже выполнено, пропуск",
		"• DoWork",
		"      делаю нечто",
		"✓ DoWork — готово",
		"✓ Фаза prepare завершена",
		"▶ Фаза isolate",
	} {
		assert.Contains(t, got, want)
	}
	// The skipped step must not print a "• " start bullet (only the skip marker).
	assert.NotContains(t, got, "• AlreadyDone\n")
	// Detail is nested deeper than the step bullet.
	assert.True(t, strings.Contains(got, "      делаю нечто"))
}

func TestNilLoggerIsNoOp(t *testing.T) {
	s := &fakeStep{id: "s"}
	a := &fakePhase{id: "prepare", steps: []Step{s}} // terminal
	mgr := newMgr(t, "prepare")
	r := New([]Phase{a}, mgr, Headless, nil, nil) // nil logger -> nopLogger
	require.NoError(t, r.Run(context.Background()))
	assert.True(t, s.ran)
}
