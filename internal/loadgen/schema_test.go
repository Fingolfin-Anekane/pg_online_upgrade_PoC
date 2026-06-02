package loadgen

import (
	"context"
	"testing"

	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitSchemaSeedsAccounts(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS events").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS accounts").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("INSERT INTO accounts").
		WithArgs(int64(1000), 100).
		WillReturnResult(pgxmock.NewResult("INSERT", 100))

	require.NoError(t, InitSchema(context.Background(), mock, 100, 1000, false))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInitSchemaResetTruncates(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS events").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS accounts").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("TRUNCATE").WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	mock.ExpectExec("INSERT INTO accounts").
		WithArgs(int64(1000), 100).
		WillReturnResult(pgxmock.NewResult("INSERT", 100))

	require.NoError(t, InitSchema(context.Background(), mock, 100, 1000, true))
	assert.NoError(t, mock.ExpectationsWereMet())
}
