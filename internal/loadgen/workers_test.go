package loadgen

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pgconnPgError = pgconn.PgError

func newTestWriter(t *testing.T) *Writer {
	t.Helper()
	w, err := NewWriter(t.TempDir()+"/log.jsonl", &bytes.Buffer{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestAppendOnceAcked(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectExec("INSERT INTO events").
		WithArgs(7, int64(100), "hello").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	w := newTestWriter(t)
	status, class := appendOnce(context.Background(), mock, w, "a", "pre-switch", 7, 100, "hello")
	assert.Equal(t, StatusAcked, status)
	assert.Empty(t, class)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAppendOnceFailedOnPgError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectExec("INSERT INTO events").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(&pgconnPgError{Code: "25006"}) // read-only

	w := newTestWriter(t)
	status, class := appendOnce(context.Background(), mock, w, "a", "pre-switch", 7, 100, "x")
	assert.Equal(t, StatusFailed, status)
	assert.Equal(t, "read-only", class)
}

func TestTransferOnceCommits(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE accounts SET balance = balance -").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec("UPDATE accounts SET balance = balance +").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	status, _ := transferOnce(context.Background(), mock, 1, 2, 5)
	assert.Equal(t, StatusAcked, status)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLongTxnOnceInsertsBatch(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO events").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("INSERT INTO events").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	w := newTestWriter(t)
	status, _ := longTxnOnce(context.Background(), mock, w, "a", "pre-switch", 3, 200, 2, 0)
	assert.Equal(t, StatusAcked, status)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRYWOnceReadsBackMaxSeq(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectExec("INSERT INTO events").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectQuery(`SELECT coalesce\(max\(client_seq\)`).WithArgs(7).
		WillReturnRows(pgxmock.NewRows([]string{"coalesce"}).AddRow(int64(100)))

	w := newTestWriter(t)
	status, class, maxSeq := rywOnce(context.Background(), mock, w, "a", "pre-switch", 7, 100)
	assert.Equal(t, StatusAcked, status)
	assert.Empty(t, class)
	assert.Equal(t, int64(100), maxSeq)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeSeqReturnsPersistedMax(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(client_seq\), 0\) FROM events WHERE writer_id`).WithArgs(1000).
		WillReturnRows(pgxmock.NewRows([]string{"coalesce"}).AddRow(int64(42)))

	got := resumeSeq(context.Background(), mock, 1000)
	assert.Equal(t, int64(42), got)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResumeSeqZeroOnEmptyTable(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(client_seq\), 0\)`).WithArgs(1000).
		WillReturnRows(pgxmock.NewRows([]string{"coalesce"}).AddRow(int64(0)))

	assert.Equal(t, int64(0), resumeSeq(context.Background(), mock, 1000))
}

func TestResumeSeqZeroOnQueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectQuery(`SELECT COALESCE`).WithArgs(1000).WillReturnError(errors.New("boom"))

	// Best-effort: an unreachable/missing table must not crash and must not
	// resurrect the duplicate-key bug by starting below real rows; 0 is safe on a
	// fresh table, and on a populated+reachable table the query succeeds.
	assert.Equal(t, int64(0), resumeSeq(context.Background(), mock, 1000))
}

func TestAppendOnceIndoubtOnNetworkError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectExec("INSERT INTO events").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(errors.New("write: connection reset by peer"))

	w := newTestWriter(t)
	status, _ := appendOnce(context.Background(), mock, w, "a", "pre-switch", 7, 100, "x")
	assert.Equal(t, StatusIndoubt, status)
}
