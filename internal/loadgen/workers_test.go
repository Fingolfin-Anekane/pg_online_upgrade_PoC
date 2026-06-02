package loadgen

import (
	"bytes"
	"context"
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
