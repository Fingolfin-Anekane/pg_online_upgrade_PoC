package state_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewManager_CreatesFileWithInitialState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	m, err := state.NewManager(path, "prod")
	require.NoError(t, err)

	s := m.Get()
	assert.Equal(t, "prod", s.ClusterName)
	assert.Equal(t, "prepare", s.Current)
	assert.Equal(t, "1", s.Version)
	assert.WithinDuration(t, time.Now(), s.StartedAt, 5*time.Second)

	_, err = os.Stat(path)
	assert.NoError(t, err)

	_, err = os.Stat(path + ".tmp")
	assert.True(t, os.IsNotExist(err))
}

func TestLoadManager_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	m, err := state.NewManager(path, "prod")
	require.NoError(t, err)
	require.NoError(t, m.SetPrimaryHost("primary.internal"))

	loaded, err := state.LoadManager(path)
	require.NoError(t, err)

	assert.Equal(t, "primary.internal", loaded.Get().Artifacts.PrimaryHost)
}

func TestManager_CompleteStep(t *testing.T) {
	m := newTestManager(t)

	require.NoError(t, m.CompleteStep("prepare", "discover_topology"))

	s := m.Get()
	assert.Equal(t, state.StepStatusDone, s.Phases["prepare"].Steps["discover_topology"].Status)
	assert.NotNil(t, s.Phases["prepare"].Steps["discover_topology"].CompletedAt)
}

func TestManager_SkipStep(t *testing.T) {
	m := newTestManager(t)

	require.NoError(t, m.SkipStep("prepare", "create_publication"))

	assert.Equal(t, state.StepStatusSkipped, m.Get().Phases["prepare"].Steps["create_publication"].Status)
}

func TestManager_FailStep(t *testing.T) {
	m := newTestManager(t)

	require.NoError(t, m.FailStep("prepare", "verify_prerequisites", "wal_level is not logical"))

	s := m.Get()
	require.NotNil(t, s.LastError)
	assert.Equal(t, "prepare", s.LastError.Phase)
	assert.Equal(t, "verify_prerequisites", s.LastError.Step)
	assert.Equal(t, "wal_level is not logical", s.LastError.Message)
}

func TestManager_Advance(t *testing.T) {
	m := newTestManager(t)

	require.NoError(t, m.Advance("isolate"))

	assert.Equal(t, "isolate", m.Get().Current)
	assert.Contains(t, m.Get().Phases, "isolate")
}

func TestManager_SetSlotBaseline(t *testing.T) {
	m := newTestManager(t)

	b := &state.SlotBaseline{
		CapturedAt:        time.Now(),
		RestartLSN:        "0/1A2B3C4D",
		ConfirmedFlushLSN: "0/1A2B0000",
		PrimaryHost:       "primary.internal",
	}
	require.NoError(t, m.SetSlotBaseline(b))

	got := m.Get().Artifacts.SlotBaseline
	require.NotNil(t, got)
	assert.Equal(t, "0/1A2B3C4D", got.RestartLSN)
	assert.Equal(t, "0/1A2B0000", got.ConfirmedFlushLSN)
}

func TestManager_SetDrainReport(t *testing.T) {
	m := newTestManager(t)

	r := &state.DrainReport{
		CompletedAt:         time.Now(),
		FinalFlushLSN:       "0/2A2B0000",
		TransactionsDrained: 42,
	}
	require.NoError(t, m.SetDrainReport(r))

	got := m.Get().Artifacts.DrainReport
	require.NotNil(t, got)
	assert.Equal(t, "0/2A2B0000", got.FinalFlushLSN)
	assert.Equal(t, 42, got.TransactionsDrained)
}

func newTestManager(t *testing.T) *state.Manager {
	t.Helper()
	m, err := state.NewManager(filepath.Join(t.TempDir(), "state.json"), "prod")
	require.NoError(t, err)
	return m
}
