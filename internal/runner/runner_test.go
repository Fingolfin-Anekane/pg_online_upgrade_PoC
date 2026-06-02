package runner

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStep records calls and returns scripted results.
type fakeStep struct {
	id       StepID
	checked  bool
	ran      bool
	checkRes bool
	checkErr error
	runErr   error
}

func (s *fakeStep) ID() StepID { return s.id }
func (s *fakeStep) Check(context.Context) (bool, error) {
	s.checked = true
	return s.checkRes, s.checkErr
}
func (s *fakeStep) Run(context.Context) error { s.ran = true; return s.runErr }

// fakePhase is a phase with fixed steps and transitions.
type fakePhase struct {
	id    PhaseID
	steps []Step
	trans []Transition
}

func (p *fakePhase) ID() PhaseID               { return p.id }
func (p *fakePhase) Steps() []Step             { return p.steps }
func (p *fakePhase) Transitions() []Transition { return p.trans }

func newMgr(t *testing.T, first PhaseID) *state.Manager {
	t.Helper()
	// NewManager(path, clusterName) starts Current at "prepare"; Advance moves it
	// to the phase the test wants to start from.
	m, err := state.NewManager(filepath.Join(t.TempDir(), "state.json"), "test")
	require.NoError(t, err)
	require.NoError(t, m.Advance(first))
	return m
}

func TestRunnerRunsStepsAndTransitions(t *testing.T) {
	s1 := &fakeStep{id: "s1"}
	s2 := &fakeStep{id: "s2"}
	a := &fakePhase{id: "a", steps: []Step{s1}, trans: []Transition{{To: "b"}}}
	b := &fakePhase{id: "b", steps: []Step{s2}} // no transition = terminal

	mgr := newMgr(t, "a")
	r := New([]Phase{a, b}, mgr, Headless, nil)
	require.NoError(t, r.Run(context.Background()))

	assert.True(t, s1.ran)
	assert.True(t, s2.ran)
	assert.Equal(t, "b", mgr.Get().Current)
}

func TestRunnerSkipsCompletedSteps(t *testing.T) {
	s1 := &fakeStep{id: "s1", checkRes: true} // already done
	a := &fakePhase{id: "a", steps: []Step{s1}}
	mgr := newMgr(t, "a")
	r := New([]Phase{a}, mgr, Headless, nil)
	require.NoError(t, r.Run(context.Background()))
	assert.True(t, s1.checked)
	assert.False(t, s1.ran) // skipped
}

func TestRunnerStopsOnStepError(t *testing.T) {
	s1 := &fakeStep{id: "s1", runErr: errors.New("boom")}
	a := &fakePhase{id: "a", steps: []Step{s1}, trans: []Transition{{To: "b"}}}
	mgr := newMgr(t, "a")
	r := New([]Phase{a}, mgr, Headless, nil)
	err := r.Run(context.Background())
	require.Error(t, err)
	assert.Equal(t, "a", mgr.Get().Current) // did not advance
}

func TestRunnerConditionalTransition(t *testing.T) {
	a := &fakePhase{id: "a", steps: nil, trans: []Transition{
		{To: "x", Condition: func(*state.State) bool { return false }},
		{To: "y", Condition: func(*state.State) bool { return true }},
	}}
	y := &fakePhase{id: "y"}
	mgr := newMgr(t, "a")
	r := New([]Phase{a, y}, mgr, Headless, nil)
	require.NoError(t, r.Run(context.Background()))
	assert.Equal(t, "y", mgr.Get().Current)
}

func TestRunnerInteractiveCheckpointAbort(t *testing.T) {
	a := &fakePhase{id: "a", trans: []Transition{{To: "b"}}}
	b := &fakePhase{id: "b"}
	mgr := newMgr(t, "a")
	cp := func(context.Context, PhaseID, PhaseID) error { return errors.New("operator aborted") }
	r := New([]Phase{a, b}, mgr, Interactive, cp)
	err := r.Run(context.Background())
	require.Error(t, err)
	assert.Equal(t, "a", mgr.Get().Current) // aborted before advancing
}

func TestRunnerStopsOnCheckError(t *testing.T) {
	s1 := &fakeStep{id: "s1", checkErr: errors.New("check boom")}
	a := &fakePhase{id: "a", steps: []Step{s1}, trans: []Transition{{To: "b"}}}
	mgr := newMgr(t, "a")
	r := New([]Phase{a}, mgr, Headless, nil)
	err := r.Run(context.Background())
	require.Error(t, err)
	assert.False(t, s1.ran) // never ran: check failed first
	assert.Equal(t, "a", mgr.Get().Current)
}

func TestRunnerUnknownPhase(t *testing.T) {
	mgr := newMgr(t, "ghost") // no phase with this id is registered
	r := New([]Phase{}, mgr, Headless, nil)
	err := r.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown phase")
}

func TestRunnerNoMatchingTransitionErrors(t *testing.T) {
	a := &fakePhase{id: "a", trans: []Transition{
		{To: "x", Condition: func(*state.State) bool { return false }},
	}}
	mgr := newMgr(t, "a")
	r := New([]Phase{a}, mgr, Headless, nil)
	err := r.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no transition")
}
