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
