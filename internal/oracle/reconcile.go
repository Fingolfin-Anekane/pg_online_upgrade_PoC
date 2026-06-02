package oracle

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Querier is the read subset used at rest. *pgxpool.Pool and pgxmock satisfy it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Findings collects every reconciliation result. Empty == PASS.
type Findings struct {
	Lost        []string // op_id: acked but missing in DB
	Dup         []string // op_id: present more than once
	Phantom     []string // op_id: failed but present in DB
	Partial     []string // op_id: in-doubt batch only partially present
	DupID       int64    // duplicate events.id rows
	SeqBehind   bool     // last_value < max(id)
	LastValue   int64
	MaxID       int64
	SumExpected int64
	SumActual   int64
}

// Failed reports whether any finding fails the run.
func (f *Findings) Failed() bool {
	return len(f.Lost) > 0 || len(f.Dup) > 0 || len(f.Phantom) > 0 ||
		len(f.Partial) > 0 || f.DupID > 0 || f.SeqBehind ||
		f.SumExpected != f.SumActual
}

type key struct {
	writer int
	seq    int64
}

// Reconcile compares the resolved ops against the database at rest.
func Reconcile(ctx context.Context, db Querier, ops []Op, balance int64) (*Findings, error) {
	present, err := loadMembership(ctx, db)
	if err != nil {
		return nil, err
	}

	f := &Findings{}
	// An op's rows occupy client_seq [ClientSeq, ClientSeq+Rows). Note an op may
	// appear in more than one finding list — e.g. a torn acked batch with one
	// duplicated row and one missing row is both Dup and Lost; that is intended.
	for _, op := range ops {
		var seen, missing, dups int64
		for i := int64(0); i < op.Rows; i++ {
			c := present[key{op.WriterID, op.ClientSeq + i}]
			if c > 1 {
				dups++
			}
			if c >= 1 {
				seen++
			} else {
				missing++
			}
		}
		if dups > 0 {
			f.Dup = append(f.Dup, op.OpID) // once per op, not once per duplicated row
		}
		switch op.Status {
		case "acked":
			if missing > 0 {
				f.Lost = append(f.Lost, op.OpID)
			}
		case "failed":
			if seen > 0 {
				f.Phantom = append(f.Phantom, op.OpID)
			}
		case "indoubt":
			if seen > 0 && missing > 0 { // partial batch is never acceptable
				f.Partial = append(f.Partial, op.OpID)
			}
		}
	}

	if err := loadAggregates(ctx, db, f, balance); err != nil {
		return nil, err
	}
	return f, nil
}

func loadMembership(ctx context.Context, db Querier) (map[key]int64, error) {
	rows, err := db.Query(ctx, `SELECT writer_id, client_seq, count(*) FROM events GROUP BY writer_id, client_seq`)
	if err != nil {
		return nil, fmt.Errorf("reconcile membership: %w", err)
	}
	defer rows.Close()
	m := map[key]int64{}
	for rows.Next() {
		var w int
		var seq, c int64
		if err := rows.Scan(&w, &seq, &c); err != nil {
			return nil, fmt.Errorf("reconcile membership scan: %w", err)
		}
		m[key{w, seq}] = c
	}
	return m, rows.Err()
}

func loadAggregates(ctx context.Context, db Querier, f *Findings, balance int64) error {
	if err := db.QueryRow(ctx,
		`SELECT count(*) FROM (SELECT id FROM events GROUP BY id HAVING count(*) > 1) d`,
	).Scan(&f.DupID); err != nil {
		return fmt.Errorf("reconcile dup id: %w", err)
	}
	if err := db.QueryRow(ctx, `SELECT last_value FROM events_id_seq`).Scan(&f.LastValue); err != nil {
		return fmt.Errorf("reconcile seq: %w", err)
	}
	if err := db.QueryRow(ctx, `SELECT coalesce(max(id),0) FROM events`).Scan(&f.MaxID); err != nil {
		return fmt.Errorf("reconcile max id: %w", err)
	}
	f.SeqBehind = f.LastValue < f.MaxID

	var accounts int64
	if err := db.QueryRow(ctx,
		`SELECT coalesce(sum(balance),0), count(*) FROM accounts`,
	).Scan(&f.SumActual, &accounts); err != nil {
		return fmt.Errorf("reconcile sum: %w", err)
	}
	f.SumExpected = accounts * balance
	return nil
}
