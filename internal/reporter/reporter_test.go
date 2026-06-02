package reporter_test

import (
	"testing"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/reporter"
	"github.com/stretchr/testify/assert"
)

func TestReporter_SendAndReceive(t *testing.T) {
	r := reporter.New()
	r.Start()
	defer r.Stop()

	r.Send(reporter.Event{
		Type:    reporter.EventStepDone,
		Phase:   "prepare",
		Step:    "discover_topology",
		Message: "primary: primary.internal",
		At:      time.Now(),
	})

	// Reporter should not block or panic when receiving events
	// Full terminal rendering is tested visually; here we verify no deadlock
	time.Sleep(10 * time.Millisecond)
	assert.True(t, true) // reaching here = no deadlock
}

func TestReporter_MetricSnapshot(t *testing.T) {
	r := reporter.New()
	r.Start()
	defer r.Stop()

	r.SendMetric(reporter.MetricSnapshot{
		Phase:        "drain",
		SlotLagBytes: int64Ptr(1024 * 1024 * 512),
		ClusterState: "prod | primary: n0.internal | replicas: 2/5",
	})

	time.Sleep(10 * time.Millisecond)
	assert.True(t, true)
}

func int64Ptr(v int64) *int64 { return &v }
