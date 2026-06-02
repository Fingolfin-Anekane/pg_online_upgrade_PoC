package loadgen

import (
	"context"
	"fmt"
)

const ddlEvents = `CREATE TABLE IF NOT EXISTS events (
    id         bigserial PRIMARY KEY,
    writer_id  int        NOT NULL,
    client_seq bigint     NOT NULL,
    batch_id   uuid       NULL,
    payload    text       NOT NULL,
    ts         timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (writer_id, client_seq)
)`

const ddlAccounts = `CREATE TABLE IF NOT EXISTS accounts (
    id      int    PRIMARY KEY,
    balance bigint NOT NULL
)`

// seedAccounts inserts K accounts each with balance B; ON CONFLICT keeps it
// idempotent when accounts already exist.
const seedAccounts = `INSERT INTO accounts (id, balance)
SELECT g, $1 FROM generate_series(1, $2) g
ON CONFLICT (id) DO NOTHING`

// InitSchema creates both tables and seeds K accounts with balance B. When
// reset is true it TRUNCATEs first, so the seed re-applies cleanly.
func InitSchema(ctx context.Context, db Pool, accounts int, balance int64, reset bool) error {
	if _, err := db.Exec(ctx, ddlEvents); err != nil {
		return fmt.Errorf("init events: %w", err)
	}
	if _, err := db.Exec(ctx, ddlAccounts); err != nil {
		return fmt.Errorf("init accounts: %w", err)
	}
	if reset {
		if _, err := db.Exec(ctx, "TRUNCATE events, accounts RESTART IDENTITY"); err != nil {
			return fmt.Errorf("init truncate: %w", err)
		}
	}
	if _, err := db.Exec(ctx, seedAccounts, balance, accounts); err != nil {
		return fmt.Errorf("init seed accounts: %w", err)
	}
	return nil
}
