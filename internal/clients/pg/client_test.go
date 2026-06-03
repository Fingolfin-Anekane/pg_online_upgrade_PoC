package pg_test

import (
	"context"
	"testing"

	pgclient "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShowWALLevel(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SHOW wal_level").
		WillReturnRows(pgxmock.NewRows([]string{"wal_level"}).AddRow("logical"))

	c := pgclient.NewFromPool(mock)
	level, err := c.ShowWALLevel(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "logical", level)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestIsInRecovery_True(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT pg_is_in_recovery\\(\\)").
		WillReturnRows(pgxmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))

	c := pgclient.NewFromPool(mock)
	inRecovery, err := c.IsInRecovery(context.Background())
	require.NoError(t, err)
	assert.True(t, inRecovery)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetWALReceiverReceivedLSN_VersionAgnostic(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// The query must not hardcode received_lsn (renamed to flushed_lsn in PG13);
	// it reads whichever of the two columns exists via to_jsonb + COALESCE.
	mock.ExpectQuery("to_jsonb").
		WillReturnRows(pgxmock.NewRows([]string{"lsn"}).AddRow("0/3FA20000"))

	c := pgclient.NewFromPool(mock)
	lsn, err := c.GetWALReceiverReceivedLSN(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "0/3FA20000", lsn)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetWALReceiverReceivedLSN_NoRow(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// No walreceiver row (already disconnected) -> empty string, no error.
	mock.ExpectQuery("pg_stat_wal_receiver").
		WillReturnRows(pgxmock.NewRows([]string{"lsn"}))

	c := pgclient.NewFromPool(mock)
	lsn, err := c.GetWALReceiverReceivedLSN(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "", lsn)
}

func TestGetLastWALReplayLSN(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT pg_last_wal_replay_lsn\\(\\)::text").
		WillReturnRows(pgxmock.NewRows([]string{"lsn"}).AddRow("0/3FA20000"))

	c := pgclient.NewFromPool(mock)
	lsn, err := c.GetLastWALReplayLSN(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "0/3FA20000", lsn)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckpoint(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("CHECKPOINT").WillReturnResult(pgxmock.NewResult("CHECKPOINT", 0))

	c := pgclient.NewFromPool(mock)
	err = c.Checkpoint(context.Background())
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetReplicationSlot_Exists(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT slot_name, restart_lsn::text, confirmed_flush_lsn::text").
		WithArgs("slot_upgrade").
		WillReturnRows(pgxmock.NewRows([]string{"slot_name", "restart_lsn", "confirmed_flush_lsn"}).
			AddRow("slot_upgrade", "0/1A000000", "0/1A000100"))

	c := pgclient.NewFromPool(mock)
	slot, err := c.GetReplicationSlot(context.Background(), "slot_upgrade")
	require.NoError(t, err)
	require.NotNil(t, slot)
	assert.Equal(t, "slot_upgrade", slot.Name)
	assert.Equal(t, "0/1A000000", slot.RestartLSN)
	assert.Equal(t, "0/1A000100", slot.ConfirmedFlushLSN)
}

func TestGetReplicationSlot_NotExists(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT slot_name, restart_lsn::text, confirmed_flush_lsn::text").
		WithArgs("slot_upgrade").
		WillReturnRows(pgxmock.NewRows([]string{"slot_name", "restart_lsn", "confirmed_flush_lsn"}))

	c := pgclient.NewFromPool(mock)
	slot, err := c.GetReplicationSlot(context.Background(), "slot_upgrade")
	require.NoError(t, err)
	assert.Nil(t, slot)
}

func TestCreateLogicalSlot_ReadsBackFromCatalog(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// pg_create_logical_replication_slot() returns only (slot_name, lsn), so the
	// create is a plain Exec; the restart/confirmed_flush LSNs come from a
	// follow-up read of pg_replication_slots.
	mock.ExpectExec("SELECT pg_create_logical_replication_slot").
		WithArgs("slot_upgrade", "pgoutput").
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery("SELECT slot_name, restart_lsn::text, confirmed_flush_lsn::text").
		WithArgs("slot_upgrade").
		WillReturnRows(pgxmock.NewRows([]string{"slot_name", "restart_lsn", "confirmed_flush_lsn"}).
			AddRow("slot_upgrade", "0/1A000000", "0/1A000100"))

	c := pgclient.NewFromPool(mock)
	slot, err := c.CreateLogicalSlot(context.Background(), "slot_upgrade", "pgoutput")
	require.NoError(t, err)
	require.NotNil(t, slot)
	assert.Equal(t, "slot_upgrade", slot.Name)
	assert.Equal(t, "0/1A000000", slot.RestartLSN)
	assert.Equal(t, "0/1A000100", slot.ConfirmedFlushLSN)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateSubscription_EscapesSingleQuotes(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// A single quote in the DSN (e.g. inside a password) must be escaped by
	// doubling it so it cannot break out of the SQL string literal.
	mock.ExpectExec("CONNECTION 'host=p''rimary'").
		WillReturnResult(pgxmock.NewResult("CREATE SUBSCRIPTION", 0))

	c := pgclient.NewFromPool(mock)
	err = c.CreateSubscription(context.Background(), "sub_upgrade", "host=p'rimary", "pub_upgrade", "slot_upgrade")
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestIsWALReceiverActive(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("FROM pg_stat_wal_receiver").
		WillReturnRows(pgxmock.NewRows([]string{"active"}).AddRow(true))

	c := pgclient.NewFromPool(mock)
	active, err := c.IsWALReceiverActive(context.Background())
	require.NoError(t, err)
	assert.True(t, active)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestIsWALReceiverActive_False(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// empty pg_stat_wal_receiver => count(*) > 0 is false (N1 disconnected).
	mock.ExpectQuery("FROM pg_stat_wal_receiver").
		WillReturnRows(pgxmock.NewRows([]string{"active"}).AddRow(false))

	c := pgclient.NewFromPool(mock)
	active, err := c.IsWALReceiverActive(context.Background())
	require.NoError(t, err)
	assert.False(t, active)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDisconnectFromWAL(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("ALTER SYSTEM SET primary_conninfo").WillReturnResult(pgxmock.NewResult("ALTER", 0))
	mock.ExpectExec("pg_reload_conf").WillReturnResult(pgxmock.NewResult("SELECT", 0))

	c := pgclient.NewFromPool(mock)
	require.NoError(t, c.DisconnectFromWAL(context.Background()))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPublicationExists(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("FROM pg_publication").
		WithArgs("pub_up").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))

	c := pgclient.NewFromPool(mock)
	exists, err := c.PublicationExists(context.Background(), "pub_up")
	require.NoError(t, err)
	assert.True(t, exists)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestServerVersionNum(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("server_version_num").
		WillReturnRows(pgxmock.NewRows([]string{"server_version_num"}).AddRow("120008"))

	c := pgclient.NewFromPool(mock)
	v, err := c.ServerVersionNum(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 120008, v)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestClearPrimaryConninfo(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("ALTER SYSTEM SET primary_conninfo").WillReturnResult(pgxmock.NewResult("ALTER", 0))

	c := pgclient.NewFromPool(mock)
	require.NoError(t, c.ClearPrimaryConninfo(context.Background()))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDisableSubscription(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectExec("ALTER SUBSCRIPTION .* DISABLE").WillReturnResult(pgxmock.NewResult("ALTER", 0))
	c := pgclient.NewFromPool(mock)
	require.NoError(t, c.DisableSubscription(context.Background(), "sub_upgrade"))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateSubscriptionCreatingSlot(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectExec("CREATE SUBSCRIPTION .* create_slot = true").
		WillReturnResult(pgxmock.NewResult("CREATE SUBSCRIPTION", 0))
	c := pgclient.NewFromPool(mock)
	require.NoError(t, c.CreateSubscriptionCreatingSlot(context.Background(), "sub_rollback", "host=n1", "pub_rollback"))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCountAppBackends(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectQuery("client backend").WillReturnRows(pgxmock.NewRows([]string{"n"}).AddRow(3))
	c := pgclient.NewFromPool(mock)
	n, err := c.CountAppBackends(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.NoError(t, mock.ExpectationsWereMet())
}
