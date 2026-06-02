package loadgen

import (
	"context"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Pool is the subset of *pgxpool.Pool used by workers and reconciliation. Both
// *pgxpool.Pool and pgxmock pools satisfy it, so callers unit-test on mocks.
type Pool interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Selector holds the two pools and an atomic switch. Workers call Active() on
// every op, so a SIGHUP-driven Switch() takes effect on the next operation.
type Selector struct {
	a, b     Pool
	switched atomic.Bool
}

func NewSelector(a, b Pool) *Selector { return &Selector{a: a, b: b} }

// Switch flips the active pool from A to B. Idempotent.
func (s *Selector) Switch() { s.switched.Store(true) }

// Switched reports whether the swap has happened.
func (s *Selector) Switched() bool { return s.switched.Load() }

// Active returns the live pool, its label ("a"/"b"), and the phase tag.
func (s *Selector) Active() (Pool, string, string) {
	if s.switched.Load() {
		return s.b, "b", "post-switch"
	}
	return s.a, "a", "pre-switch"
}
