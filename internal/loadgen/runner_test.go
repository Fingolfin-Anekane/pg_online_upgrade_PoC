package loadgen

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cancelAfterFirstOp wraps a Pool and cancels ctx after the first completed
// write op — an autocommit Exec, or a transaction's Commit — so an otherwise
// infinite write loop performs exactly one iteration deterministically. The
// cancel fires AFTER the op completes (never mid-transaction), so the in-flight
// op runs on a live ctx and the loop only sees Done at the next iteration's top.
type cancelAfterFirstOp struct {
	Pool
	cancel func()
	fired  atomic.Bool
}

func (c *cancelAfterFirstOp) maybe() {
	if c.fired.CompareAndSwap(false, true) {
		c.cancel()
	}
}
func (c *cancelAfterFirstOp) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := c.Pool.Exec(ctx, sql, args...)
	c.maybe()
	return tag, err
}
func (c *cancelAfterFirstOp) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return tx, err
	}
	return &cancelTx{Tx: tx, c: c}, nil
}

type cancelTx struct {
	pgx.Tx
	c *cancelAfterFirstOp
}

func (t *cancelTx) Commit(ctx context.Context) error {
	err := t.Tx.Commit(ctx)
	t.c.maybe()
	return err
}

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

func TestAppendLoopResumesPastExistingRows(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	// On (re)start the worker first reads its persisted max client_seq...
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(client_seq\), 0\)`).WithArgs(1000).
		WillReturnRows(pgxmock.NewRows([]string{"coalesce"}).AddRow(int64(5)))
	// ...so its first INSERT continues at max+1 (=6), never colliding at 1.
	mock.ExpectExec("INSERT INTO events").WithArgs(1000, int64(6), "append").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	ctx, cancel := context.WithCancel(context.Background())
	sel := NewSelector(&cancelAfterFirstOp{Pool: mock, cancel: cancel}, mock)
	appendLoop(ctx, sel, newTestWriter(t), NewMetrics(), 1000, 0)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRYWLoopResumesPastExistingRows(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(client_seq\), 0\)`).WithArgs(2000).
		WillReturnRows(pgxmock.NewRows([]string{"coalesce"}).AddRow(int64(5)))
	mock.ExpectExec("INSERT INTO events").WithArgs(2000, int64(6), "ryw").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectQuery(`SELECT coalesce\(max\(client_seq\)`).WithArgs(2000).
		WillReturnRows(pgxmock.NewRows([]string{"coalesce"}).AddRow(int64(6)))

	ctx, cancel := context.WithCancel(context.Background())
	sel := NewSelector(&cancelAfterFirstOp{Pool: mock, cancel: cancel}, mock)
	rywLoop(ctx, sel, newTestWriter(t), NewMetrics(), 2000, 0)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLongTxnLoopResumesPastExistingRows(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(client_seq\), 0\)`).WithArgs(3000).
		WillReturnRows(pgxmock.NewRows([]string{"coalesce"}).AddRow(int64(5)))
	// First batch continues at max+1 (=6), then 7.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO events").WithArgs(3000, int64(6), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("INSERT INTO events").WithArgs(3000, int64(7), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	sel := NewSelector(&cancelAfterFirstOp{Pool: mock, cancel: cancel}, mock)
	longTxnLoop(ctx, sel, newTestWriter(t), NewMetrics(), 3000, 2, 0)

	assert.NoError(t, mock.ExpectationsWereMet())
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
