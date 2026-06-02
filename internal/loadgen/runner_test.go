package loadgen

import (
	"context"
	"testing"
	"time"

	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendLoopRunsAndIncrementsSeq(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	// allow any number of inserts
	for i := 0; i < 100; i++ {
		mock.ExpectExec("INSERT INTO events").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	}

	sel := NewSelector(mock, mock)
	w := newTestWriter(t)
	m := NewMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()

	n := appendLoop(ctx, sel, w, m, 1, 0) // writerID 1, unthrottled
	assert.Positive(t, n)                 // at least one op happened
}

func TestRecordOutcomeRoutes(t *testing.T) {
	m := NewMetrics()
	recordOutcome(m, "append", "a", StatusAcked, "")
	recordOutcome(m, "append", "a", StatusFailed, "read-only")
	recordOutcome(m, "append", "a", StatusIndoubt, "timeout")
	commits, errs := m.Snapshot()
	assert.Equal(t, int64(1), commits["append"])
	assert.Equal(t, int64(1), errs["read-only"])
	assert.Equal(t, int64(1), errs["timeout"])
}

func TestThrottleZeroIsAlwaysReady(t *testing.T) {
	ch, stop := throttle(0)
	defer stop()
	select {
	case <-ch: // closed channel is always ready
	default:
		t.Fatal("throttle(0) channel should be always-ready (closed)")
	}
}

func TestThrottlePositiveTicks(t *testing.T) {
	ch, stop := throttle(1000) // ~1ms period
	defer stop()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("throttle(1000) should tick within 1s")
	}
}
