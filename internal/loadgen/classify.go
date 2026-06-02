package loadgen

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// classifyStatus maps the error from the decisive call (single-statement Exec
// or tx Commit) to a three-set status. A PgError is a server response, so the
// statement was rejected and did NOT commit (failed). A network error leaves
// the outcome unknown (indoubt). nil is acked.
func classifyStatus(err error) string {
	if err == nil {
		return StatusAcked
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return StatusFailed
	}
	return StatusIndoubt
}

// errorClass buckets an error for the live (non-authoritative) metrics.
func errorClass(err error) string {
	if err == nil {
		return ""
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "25006" { // read_only_sql_transaction
			return "read-only"
		}
		return "other"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"):
		return "conn-refused"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	default:
		return "other"
	}
}
