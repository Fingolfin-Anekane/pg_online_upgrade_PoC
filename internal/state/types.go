package state

import "time"

type StepStatus string

const (
	StepStatusPending StepStatus = "pending"
	StepStatusSkipped StepStatus = "skipped"
	StepStatusRunning StepStatus = "running"
	StepStatusDone    StepStatus = "done"
	StepStatusFailed  StepStatus = "failed"
)

type State struct {
	Version     string                `json:"version"`
	ClusterName string                `json:"cluster_name"`
	StartedAt   time.Time             `json:"started_at"`
	Current     string                `json:"current_phase"`
	Phases      map[string]PhaseState `json:"phases"`
	Artifacts   Artifacts             `json:"artifacts"`
	LastError   *StepError            `json:"last_error,omitempty"`
}

type PhaseState struct {
	Status      StepStatus           `json:"status"`
	StartedAt   time.Time            `json:"started_at"`
	CompletedAt *time.Time           `json:"completed_at,omitempty"`
	Steps       map[string]StepState `json:"steps"`
}

type StepState struct {
	Status      StepStatus `json:"status"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type StepError struct {
	Phase      string    `json:"phase"`
	Step       string    `json:"step"`
	Message    string    `json:"message"`
	OccurredAt time.Time `json:"occurred_at"`
}

type Artifacts struct {
	PrimaryHost          string        `json:"primary_host,omitempty"`
	PatroniStoppedOnN1   bool          `json:"patroni_stopped_on_n1"`
	SlotBaseline         *SlotBaseline `json:"slot_baseline,omitempty"`
	ReceivedLSN          string        `json:"received_lsn,omitempty"`
	TargetLSN            string        `json:"target_lsn,omitempty"`
	DrainReport          *DrainReport  `json:"drain_report,omitempty"`
	PgUpgradeCheckPassed bool          `json:"pg_upgrade_check_passed"`
	PgUpgradeDone        bool          `json:"pg_upgrade_done"`
	PG17SYSID            string        `json:"pg17_sysid,omitempty"`
	SequencesSynced      bool          `json:"sequences_synced"`
	DSNSwapNotified      bool          `json:"dsn_swap_notified"`
	ForwardSubDisabled   bool          `json:"forward_sub_disabled"`
	ReverseReplSetUp     bool          `json:"reverse_repl_set_up"`
}

type SlotBaseline struct {
	CapturedAt        time.Time `json:"captured_at"`
	RestartLSN        string    `json:"restart_lsn"`
	ConfirmedFlushLSN string    `json:"confirmed_flush_lsn"`
	PrimaryHost       string    `json:"primary_host"`
}

type DrainReport struct {
	CompletedAt         time.Time `json:"completed_at"`
	FinalFlushLSN       string    `json:"final_flush_lsn"`
	TransactionsDrained int       `json:"transactions_drained"`
}
