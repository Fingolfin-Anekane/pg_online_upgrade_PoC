package pg

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Client is the interface used by phases to interact with PostgreSQL.
type Client interface {
	ShowWALLevel(ctx context.Context) (string, error)
	IsInRecovery(ctx context.Context) (bool, error)
	ServerVersionNum(ctx context.Context) (int, error)
	ClearPrimaryConninfo(ctx context.Context) error
	GetLastWALReplayLSN(ctx context.Context) (string, error)
	GetWALReceiverReceivedLSN(ctx context.Context) (string, error)
	IsWALReceiverActive(ctx context.Context) (bool, error)
	DisconnectFromWAL(ctx context.Context) error
	Checkpoint(ctx context.Context) error
	GetReplicationSlot(ctx context.Context, name string) (*ReplicationSlot, error)
	CreateLogicalSlot(ctx context.Context, name, plugin string) (*ReplicationSlot, error)
	CreatePublication(ctx context.Context, name string) error
	PublicationExists(ctx context.Context, name string) (bool, error)
	CreateSubscription(ctx context.Context, name, connStr, pubName, slotName string) error
	CreateSubscriptionCreatingSlot(ctx context.Context, name, connStr, pubName string) error
	DisableSubscription(ctx context.Context, name string) error
	CountAppBackends(ctx context.Context) (int, error)
	GetSubscriptionLag(ctx context.Context, name string) (*SubscriptionLag, error)
	GetAllSequences(ctx context.Context) ([]SequenceInfo, error)
	SetSequenceValue(ctx context.Context, schema, name string, value int64) error
	FreezeForUpgrade(ctx context.Context, dbname string) error
	UnfreezeAfterUpgrade(ctx context.Context, dbname string) error
	DropSubscription(ctx context.Context, name string) error
	DropPublication(ctx context.Context, name string) error
	Close()
}

type ReplicationSlot struct {
	Name              string
	RestartLSN        string
	ConfirmedFlushLSN string
}

type SubscriptionLag struct {
	WriteLagMs  int64
	FlushLagMs  int64
	ReplayLagMs int64
}

type SequenceInfo struct {
	Schema    string
	Name      string
	LastValue int64
}

// PoolClient wraps pgxpool.Pool.
type PoolClient struct {
	pool *pgxpool.Pool
}

// poolQuerier is the subset of pgxpool.Pool that internalClient needs.
type poolQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// internalClient implements most Client methods using a poolQuerier.
// pgxmock.NewPool() satisfies this interface, enabling unit tests without a real database.
type internalClient struct {
	q poolQuerier
}

func NewFromPool(q poolQuerier) *internalClient {
	return &internalClient{q: q}
}

func NewFromDSN(ctx context.Context, dsn string) (*PoolClient, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pg connect: %w", err)
	}
	return &PoolClient{pool: pool}, nil
}

func (c *internalClient) ShowWALLevel(ctx context.Context) (string, error) {
	var level string
	err := c.q.QueryRow(ctx, "SHOW wal_level").Scan(&level)
	return level, err
}

func (c *internalClient) IsInRecovery(ctx context.Context) (bool, error) {
	var v bool
	err := c.q.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&v)
	return v, err
}

// ServerVersionNum returns the integer server version (e.g. 120008 for 12.8),
// used to choose the version-appropriate WAL-disconnect mechanism.
func (c *internalClient) ServerVersionNum(ctx context.Context) (int, error) {
	var s string
	if err := c.q.QueryRow(ctx, "SHOW server_version_num").Scan(&s); err != nil {
		return 0, fmt.Errorf("pg: server_version_num: %w", err)
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("pg: server_version_num parse %q: %w", s, err)
	}
	return v, nil
}

// ClearPrimaryConninfo sets primary_conninfo empty via ALTER SYSTEM (writing
// postgresql.auto.conf) WITHOUT reloading. On PG 12 the parameter is
// PGC_POSTMASTER, so the caller must restart for it to take effect; on PG 13+
// prefer DisconnectFromWAL (which also reloads).
func (c *internalClient) ClearPrimaryConninfo(ctx context.Context) error {
	if _, err := c.q.Exec(ctx, `ALTER SYSTEM SET primary_conninfo = ''`); err != nil {
		return fmt.Errorf("pg: clear primary_conninfo: %w", err)
	}
	return nil
}

func (c *internalClient) GetLastWALReplayLSN(ctx context.Context) (string, error) {
	var lsn string
	err := c.q.QueryRow(ctx, "SELECT pg_last_wal_replay_lsn()::text").Scan(&lsn)
	return lsn, err
}

func (c *internalClient) GetWALReceiverReceivedLSN(ctx context.Context) (string, error) {
	var lsn *string
	err := c.q.QueryRow(ctx, "SELECT received_lsn::text FROM pg_stat_wal_receiver").Scan(&lsn)
	if err != nil || lsn == nil {
		return "", err
	}
	return *lsn, nil
}

// IsWALReceiverActive reports whether N1 is still streaming WAL from a primary.
// pg_stat_wal_receiver has one row while a walreceiver is connected, none after.
func (c *internalClient) IsWALReceiverActive(ctx context.Context) (bool, error) {
	var active bool
	err := c.q.QueryRow(ctx, `SELECT count(*) > 0 AS active FROM pg_stat_wal_receiver`).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("pg: query wal_receiver: %w", err)
	}
	return active, nil
}

// DisconnectFromWAL clears primary_conninfo and reloads, so N1 stops receiving
// WAL. Patroni must be paused first or it will revert this.
func (c *internalClient) DisconnectFromWAL(ctx context.Context) error {
	if _, err := c.q.Exec(ctx, `ALTER SYSTEM SET primary_conninfo = ''`); err != nil {
		return fmt.Errorf("pg: clear primary_conninfo: %w", err)
	}
	if _, err := c.q.Exec(ctx, `SELECT pg_reload_conf()`); err != nil {
		return fmt.Errorf("pg: reload conf: %w", err)
	}
	return nil
}

func (c *internalClient) Checkpoint(ctx context.Context) error {
	_, err := c.q.Exec(ctx, "CHECKPOINT")
	return err
}

func (c *internalClient) GetReplicationSlot(ctx context.Context, name string) (*ReplicationSlot, error) {
	var s ReplicationSlot
	err := c.q.QueryRow(ctx,
		"SELECT slot_name, restart_lsn::text, confirmed_flush_lsn::text "+
			"FROM pg_replication_slots WHERE slot_name = $1", name).
		Scan(&s.Name, &s.RestartLSN, &s.ConfirmedFlushLSN)
	// No row means the slot does not exist — a normal "not found", not an error.
	// CreateLogicalSlot deliberately does NOT swallow ErrNoRows: a create that
	// returns no row IS a real failure.
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &s, err
}

func (c *internalClient) CreateLogicalSlot(ctx context.Context, name, plugin string) (*ReplicationSlot, error) {
	var s ReplicationSlot
	err := c.q.QueryRow(ctx,
		"SELECT slot_name, restart_lsn::text, confirmed_flush_lsn::text "+
			"FROM pg_create_logical_replication_slot($1, $2)", name, plugin).
		Scan(&s.Name, &s.RestartLSN, &s.ConfirmedFlushLSN)
	return &s, err
}

func (c *internalClient) CreatePublication(ctx context.Context, name string) error {
	_, err := c.q.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", pgx.Identifier{name}.Sanitize()))
	return err
}

// PublicationExists reports whether a publication with the given name exists.
func (c *internalClient) PublicationExists(ctx context.Context, name string) (bool, error) {
	var exists bool
	err := c.q.QueryRow(ctx, `SELECT count(*) > 0 FROM pg_publication WHERE pubname = $1`, name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("pg: query publication: %w", err)
	}
	return exists, nil
}

func (c *internalClient) CreateSubscription(ctx context.Context, name, connStr, pubName, slotName string) error {
	sql := fmt.Sprintf(
		"CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s "+
			"WITH (copy_data = false, create_slot = false, slot_name = %s, enabled = true)",
		pgx.Identifier{name}.Sanitize(),
		quoteString(connStr),
		pgx.Identifier{pubName}.Sanitize(),
		quoteString(slotName),
	)
	_, err := c.q.Exec(ctx, sql)
	return err
}

// CreateSubscriptionCreatingSlot creates a subscription that CREATES its own
// replication slot on the publisher (create_slot=true). Used for the reverse
// rollback subscription, whose slot does not pre-exist.
func (c *internalClient) CreateSubscriptionCreatingSlot(ctx context.Context, name, connStr, pubName string) error {
	sql := fmt.Sprintf(
		"CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s "+
			"WITH (copy_data = false, create_slot = true, enabled = true)",
		pgx.Identifier{name}.Sanitize(), quoteString(connStr), pgx.Identifier{pubName}.Sanitize())
	_, err := c.q.Exec(ctx, sql)
	return err
}

// DisableSubscription stops a subscription's apply worker without dropping it.
func (c *internalClient) DisableSubscription(ctx context.Context, name string) error {
	_, err := c.q.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s DISABLE", pgx.Identifier{name}.Sanitize()))
	return err
}

// CountAppBackends counts client backends excluding background/replication
// workers, used to confirm application traffic has moved to the new primary.
func (c *internalClient) CountAppBackends(ctx context.Context) (int, error) {
	var n int
	err := c.q.QueryRow(ctx,
		"SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()").
		Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("pg: count app backends: %w", err)
	}
	return n, nil
}

func (c *internalClient) GetSubscriptionLag(ctx context.Context, name string) (*SubscriptionLag, error) {
	var lag SubscriptionLag
	err := c.q.QueryRow(ctx,
		"SELECT COALESCE(EXTRACT(EPOCH FROM write_lag)*1000, 0)::bigint, "+
			"COALESCE(EXTRACT(EPOCH FROM flush_lag)*1000, 0)::bigint, "+
			"COALESCE(EXTRACT(EPOCH FROM replay_lag)*1000, 0)::bigint "+
			"FROM pg_stat_subscription WHERE subname = $1", name).
		Scan(&lag.WriteLagMs, &lag.FlushLagMs, &lag.ReplayLagMs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &lag, err
}

// GetAllSequences is not supported on internalClient because pgxmock does not
// easily mock multi-row Query calls. Call this only via PoolClient in production.
func (c *internalClient) GetAllSequences(_ context.Context) ([]SequenceInfo, error) {
	return nil, fmt.Errorf("pg: GetAllSequences requires PoolClient (not available in test mode)")
}

func (c *internalClient) SetSequenceValue(ctx context.Context, schema, name string, value int64) error {
	_, err := c.q.Exec(ctx,
		fmt.Sprintf("SELECT setval('%s.%s', $1)",
			pgx.Identifier{schema}.Sanitize(),
			pgx.Identifier{name}.Sanitize()),
		value)
	return err
}

// FreezeForUpgrade installs DML triggers on all user tables that raise an
// error for any INSERT/UPDATE/DELETE/TRUNCATE. Enforcement is purely
// trigger-based: sessions with session_replication_role='replica' (the
// reverse-apply worker) bypass the triggers and can still write, preserving
// the rollback window. The database-level default_transaction_read_only is
// intentionally NOT set — it would block the replica-role apply worker too.
func (c *internalClient) FreezeForUpgrade(ctx context.Context, dbname string) error {
	_, err := c.q.Exec(ctx, `
		CREATE OR REPLACE FUNCTION raise_upgrade_readonly() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'database is read-only during upgrade window'
				USING ERRCODE = 'read_only_sql_transaction';
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql;

		DO $$
		DECLARE t text;
		BEGIN
			FOR t IN
				SELECT format('%I.%I', schemaname, tablename)
				FROM pg_tables
				WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
			LOOP
				EXECUTE format('DROP TRIGGER IF EXISTS upgrade_freeze ON %s', t);
				EXECUTE format(
					'CREATE TRIGGER upgrade_freeze
					 BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s
					 FOR EACH STATEMENT EXECUTE PROCEDURE raise_upgrade_readonly()',
					t);
			END LOOP;
		END;
		$$;
	`)
	return err
}

func (c *internalClient) UnfreezeAfterUpgrade(ctx context.Context, dbname string) error {
	_, err := c.q.Exec(ctx, `
		DO $$
		DECLARE r record;
		BEGIN
			FOR r IN
				SELECT trigger_schema, trigger_name, event_object_schema, event_object_table
				FROM information_schema.triggers
				WHERE trigger_name = 'upgrade_freeze'
			LOOP
				EXECUTE format('DROP TRIGGER IF EXISTS upgrade_freeze ON %I.%I',
					r.event_object_schema, r.event_object_table);
			END LOOP;
		END;
		$$;
		DROP FUNCTION IF EXISTS raise_upgrade_readonly();
	`)
	if err != nil {
		return err
	}
	_, err = c.q.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s RESET default_transaction_read_only",
		pgx.Identifier{dbname}.Sanitize()))
	return err
}

func (c *internalClient) DropSubscription(ctx context.Context, name string) error {
	_, err := c.q.Exec(ctx, fmt.Sprintf("DROP SUBSCRIPTION IF EXISTS %s", pgx.Identifier{name}.Sanitize()))
	return err
}

func (c *internalClient) DropPublication(ctx context.Context, name string) error {
	_, err := c.q.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{name}.Sanitize()))
	return err
}

func (c *internalClient) Close() {}

// ic returns an internalClient backed by the real pool, used for delegation.
func (p *PoolClient) ic() *internalClient { return &internalClient{q: p.pool} }

func (p *PoolClient) ShowWALLevel(ctx context.Context) (string, error) {
	return p.ic().ShowWALLevel(ctx)
}
func (p *PoolClient) IsInRecovery(ctx context.Context) (bool, error) {
	return p.ic().IsInRecovery(ctx)
}
func (p *PoolClient) ServerVersionNum(ctx context.Context) (int, error) {
	return p.ic().ServerVersionNum(ctx)
}
func (p *PoolClient) ClearPrimaryConninfo(ctx context.Context) error {
	return p.ic().ClearPrimaryConninfo(ctx)
}
func (p *PoolClient) GetLastWALReplayLSN(ctx context.Context) (string, error) {
	return p.ic().GetLastWALReplayLSN(ctx)
}
func (p *PoolClient) GetWALReceiverReceivedLSN(ctx context.Context) (string, error) {
	return p.ic().GetWALReceiverReceivedLSN(ctx)
}
func (p *PoolClient) IsWALReceiverActive(ctx context.Context) (bool, error) {
	return p.ic().IsWALReceiverActive(ctx)
}
func (p *PoolClient) DisconnectFromWAL(ctx context.Context) error {
	return p.ic().DisconnectFromWAL(ctx)
}
func (p *PoolClient) Checkpoint(ctx context.Context) error { return p.ic().Checkpoint(ctx) }
func (p *PoolClient) GetReplicationSlot(ctx context.Context, name string) (*ReplicationSlot, error) {
	return p.ic().GetReplicationSlot(ctx, name)
}
func (p *PoolClient) CreateLogicalSlot(ctx context.Context, name, plugin string) (*ReplicationSlot, error) {
	return p.ic().CreateLogicalSlot(ctx, name, plugin)
}
func (p *PoolClient) CreatePublication(ctx context.Context, name string) error {
	return p.ic().CreatePublication(ctx, name)
}
func (p *PoolClient) PublicationExists(ctx context.Context, name string) (bool, error) {
	return p.ic().PublicationExists(ctx, name)
}
func (p *PoolClient) CreateSubscription(ctx context.Context, name, connStr, pubName, slotName string) error {
	return p.ic().CreateSubscription(ctx, name, connStr, pubName, slotName)
}
func (p *PoolClient) CreateSubscriptionCreatingSlot(ctx context.Context, name, connStr, pubName string) error {
	return p.ic().CreateSubscriptionCreatingSlot(ctx, name, connStr, pubName)
}
func (p *PoolClient) DisableSubscription(ctx context.Context, name string) error {
	return p.ic().DisableSubscription(ctx, name)
}
func (p *PoolClient) CountAppBackends(ctx context.Context) (int, error) {
	return p.ic().CountAppBackends(ctx)
}
func (p *PoolClient) GetSubscriptionLag(ctx context.Context, name string) (*SubscriptionLag, error) {
	return p.ic().GetSubscriptionLag(ctx, name)
}
func (p *PoolClient) SetSequenceValue(ctx context.Context, schema, name string, value int64) error {
	return p.ic().SetSequenceValue(ctx, schema, name, value)
}
func (p *PoolClient) FreezeForUpgrade(ctx context.Context, dbname string) error {
	return p.ic().FreezeForUpgrade(ctx, dbname)
}
func (p *PoolClient) UnfreezeAfterUpgrade(ctx context.Context, dbname string) error {
	return p.ic().UnfreezeAfterUpgrade(ctx, dbname)
}
func (p *PoolClient) DropSubscription(ctx context.Context, name string) error {
	return p.ic().DropSubscription(ctx, name)
}
func (p *PoolClient) DropPublication(ctx context.Context, name string) error {
	return p.ic().DropPublication(ctx, name)
}

// GetAllSequences uses pool.Query directly (multi-row; not available via internalClient).
func (p *PoolClient) GetAllSequences(ctx context.Context) ([]SequenceInfo, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT schemaname, sequencename, COALESCE(last_value, 0)
		FROM pg_sequences
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY schemaname, sequencename
	`)
	if err != nil {
		return nil, fmt.Errorf("pg: get sequences: %w", err)
	}
	defer rows.Close()

	var seqs []SequenceInfo
	for rows.Next() {
		var s SequenceInfo
		if err := rows.Scan(&s.Schema, &s.Name, &s.LastValue); err != nil {
			return nil, err
		}
		seqs = append(seqs, s)
	}
	return seqs, rows.Err()
}

func (p *PoolClient) Close() { p.pool.Close() }

// quoteString renders s as a SQL string literal, escaping embedded single
// quotes by doubling them so a value such as a DSN password containing a quote
// cannot break out of the literal or inject SQL.
func quoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// Compile-time interface assertions.
var _ Client = (*PoolClient)(nil)
var _ Client = (*internalClient)(nil)
