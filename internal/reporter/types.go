package reporter

import "time"

type EventType string

const (
	EventPhaseStart    EventType = "phase_start"
	EventPhaseComplete EventType = "phase_complete"
	EventStepStart     EventType = "step_start"
	EventStepSkipped   EventType = "step_skipped"
	EventStepDone      EventType = "step_done"
	EventStepFailed    EventType = "step_failed"
	EventCheckpoint    EventType = "checkpoint"
)

type Event struct {
	Type    EventType
	Phase   string
	Step    string
	Message string
	At      time.Time
}

type MetricSnapshot struct {
	Phase        string
	SlotLagBytes *int64 // non-nil during Drain phase
	SubLagMs     *int64 // non-nil during Catchup/Switchover
	ClusterState string // always: "cluster | primary: host | replicas: N/M"
}
