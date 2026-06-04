package state

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Manager struct {
	path  string
	state State
}

func NewManager(path, clusterName string) (*Manager, error) {
	m := &Manager{
		path: path,
		state: State{
			Version:     "1",
			ClusterName: clusterName,
			StartedAt:   time.Now(),
			Current:     "prepare",
			Phases:      make(map[string]PhaseState),
		},
	}
	return m, m.persist()
}

func LoadManager(path string) (*Manager, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse state: %w", err)
	}
	return &Manager{path: path, state: s}, nil
}

func (m *Manager) Get() *State { return &m.state }

func (m *Manager) Advance(phase string) error {
	m.state.Current = phase
	if _, ok := m.state.Phases[phase]; !ok {
		m.state.Phases[phase] = PhaseState{
			Status:    StepStatusRunning,
			StartedAt: time.Now(),
			Steps:     make(map[string]StepState),
		}
	}
	return m.persist()
}

func (m *Manager) CompleteStep(phase, step string) error {
	m.ensurePhase(phase)
	now := time.Now()
	ph := m.state.Phases[phase]
	ph.Steps[step] = StepState{Status: StepStatusDone, CompletedAt: &now}
	m.state.Phases[phase] = ph
	return m.persist()
}

func (m *Manager) SkipStep(phase, step string) error {
	m.ensurePhase(phase)
	now := time.Now()
	ph := m.state.Phases[phase]
	ph.Steps[step] = StepState{Status: StepStatusSkipped, CompletedAt: &now}
	m.state.Phases[phase] = ph
	return m.persist()
}

func (m *Manager) FailStep(phase, step, message string) error {
	m.ensurePhase(phase)
	m.state.LastError = &StepError{
		Phase:      phase,
		Step:       step,
		Message:    message,
		OccurredAt: time.Now(),
	}
	return m.persist()
}

func (m *Manager) SetPrimaryHost(host string) error {
	m.state.Artifacts.PrimaryHost = host
	return m.persist()
}

func (m *Manager) SetSlotBaseline(b *SlotBaseline) error {
	m.state.Artifacts.SlotBaseline = b
	return m.persist()
}

func (m *Manager) SetReceivedLSN(lsn string) error {
	m.state.Artifacts.ReceivedLSN = lsn
	return m.persist()
}

func (m *Manager) SetTargetLSN(lsn string) error {
	m.state.Artifacts.TargetLSN = lsn
	return m.persist()
}

func (m *Manager) SetDrainReport(r *DrainReport) error {
	m.state.Artifacts.DrainReport = r
	return m.persist()
}

func (m *Manager) SetPgUpgradeCheckPassed() error {
	m.state.Artifacts.PgUpgradeCheckPassed = true
	return m.persist()
}

func (m *Manager) SetPgUpgradeDone(sysid string) error {
	m.state.Artifacts.PgUpgradeDone = true
	m.state.Artifacts.PG17SYSID = sysid
	return m.persist()
}

func (m *Manager) SetSequencesSynced() error {
	m.state.Artifacts.SequencesSynced = true
	return m.persist()
}

func (m *Manager) SetDSNSwapNotified() error {
	m.state.Artifacts.DSNSwapNotified = true
	return m.persist()
}

func (m *Manager) SetForwardSubDisabled() error {
	m.state.Artifacts.ForwardSubDisabled = true
	return m.persist()
}

func (m *Manager) SetReverseReplSetUp() error {
	m.state.Artifacts.ReverseReplSetUp = true
	return m.persist()
}

func (m *Manager) ensurePhase(phase string) {
	if _, ok := m.state.Phases[phase]; !ok {
		m.state.Phases[phase] = PhaseState{
			Status:    StepStatusRunning,
			StartedAt: time.Now(),
			Steps:     make(map[string]StepState),
		}
	}
}

func (m *Manager) persist() error {
	data, err := json.MarshalIndent(m.state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return fmt.Errorf("write state tmp: %w", err)
	}
	return os.Rename(tmp, m.path)
}
