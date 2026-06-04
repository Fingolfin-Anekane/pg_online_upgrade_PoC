package loadgen

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const (
	insertEvent        = `INSERT INTO events (writer_id, client_seq, batch_id, payload) VALUES ($1, $2, $3, $4)`
	insertEventNoBatch = `INSERT INTO events (writer_id, client_seq, payload) VALUES ($1, $2, $3)`
	maxClientSeq       = `SELECT COALESCE(MAX(client_seq), 0) FROM events WHERE writer_id = $1`
)

// resumeSeq returns the highest client_seq already persisted for writerID (0 if
// none), so a restarted worker continues past existing rows instead of colliding
// on the UNIQUE (writer_id, client_seq) constraint. writerID ranges are stable
// across runs while the in-memory counter restarts at 0, so without this a
// re-run replays seq 1,2,3… onto rows the prior run already wrote (SQLSTATE
// 23505). Best-effort: on a query error it returns 0 — a fresh/just-reset table
// reads 0 anyway, and an unreachable DB fails every write regardless.
func resumeSeq(ctx context.Context, db Pool, writerID int) int64 {
	var maxSeq int64
	_ = db.QueryRow(ctx, maxClientSeq, writerID).Scan(&maxSeq)
	return maxSeq
}

// appendOnce inserts one event row in autocommit and logs the op. Returns the
// resolved status and the error class ("" when acked) for live metrics.
func appendOnce(ctx context.Context, db Pool, w *Writer, dsn, phase string, writerID int, seq int64, payload string) (string, string) {
	opID := uuid.NewString()
	if err := w.WriteAttempt(Record{
		OpID: opID, Kind: "append", WriterID: writerID, ClientSeq: seq, Rows: 1,
		DSN: dsn, Phase: phase,
	}); err != nil {
		// Could not durably record intent; do NOT issue the DB write, else a
		// committed row would have no ground-truth entry. Abort the op.
		return StatusFailed, "log-write"
	}
	_, err := db.Exec(ctx, insertEventNoBatch, writerID, seq, payload)
	status := classifyStatus(err)
	_ = w.WriteResult(opID, status, errMsg(err))
	return status, errorClass(err)
}

// rywOnce does an append, then reads back this writer's max client_seq from the
// active pool. The read result is observation only (a stale read is lag, not a
// verdict). Returns status, error class, and the observed max client_seq.
func rywOnce(ctx context.Context, db Pool, w *Writer, dsn, phase string, writerID int, seq int64) (string, string, int64) {
	status, class := appendOnce(ctx, db, w, dsn, phase, writerID, seq, "ryw")
	var maxSeq int64 = -1
	_ = db.QueryRow(ctx, `SELECT coalesce(max(client_seq), -1) FROM events WHERE writer_id = $1`, writerID).Scan(&maxSeq)
	// maxSeq is for the caller to stream as a live observation; it is NOT used
	// in reconciliation (observation only — a stale read is lag, not a verdict).
	return status, class, maxSeq
}

// transferOnce moves `amount` between two accounts in one transaction. Not
// logged — verified by the SUM(balance) invariant. Status is resolved from the
// decisive Commit call; intermediate statement errors mean a rolled-back txn.
func transferOnce(ctx context.Context, db Pool, from, to, amount int) (string, string) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return classifyStatus(err), errorClass(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance - $1 WHERE id = $2`, amount, from); err != nil {
		_ = tx.Rollback(ctx)
		return StatusFailed, errorClass(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance + $1 WHERE id = $2`, amount, to); err != nil {
		_ = tx.Rollback(ctx)
		return StatusFailed, errorClass(err)
	}
	commitErr := tx.Commit(ctx)
	return classifyStatus(commitErr), errorClass(commitErr)
}

// longTxnOnce inserts `rows` event rows sharing a batch_id in one slow
// transaction, occupying the client_seq range [startSeq, startSeq+rows). The
// hold duration makes commits straddle the isolation point.
func longTxnOnce(ctx context.Context, db Pool, w *Writer, dsn, phase string, writerID int, startSeq, rows int64, hold time.Duration) (string, string) {
	opID := uuid.NewString()
	batchID := uuid.NewString()
	if err := w.WriteAttempt(Record{
		OpID: opID, Kind: "long-txn", WriterID: writerID, ClientSeq: startSeq, Rows: rows,
		BatchID: batchID, DSN: dsn, Phase: phase,
	}); err != nil {
		// See appendOnce: abort if intent isn't durable.
		return StatusFailed, "log-write"
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		status := classifyStatus(err)
		_ = w.WriteResult(opID, status, errMsg(err))
		return status, errorClass(err)
	}
	for i := int64(0); i < rows; i++ {
		if _, err := tx.Exec(ctx, insertEvent, writerID, startSeq+i, batchID, "long"); err != nil {
			_ = tx.Rollback(ctx)
			_ = w.WriteResult(opID, StatusFailed, errMsg(err))
			return StatusFailed, errorClass(err)
		}
	}
	if hold > 0 {
		timer := time.NewTimer(hold)
		select {
		case <-timer.C:
		case <-ctx.Done():
			// Harness is shutting down; fall through to Commit so the in-flight
			// long txn is logged as indoubt (outcome unknown) rather than failed.
			// pgx discards the unclean connection.
		}
		timer.Stop()
	}
	commitErr := tx.Commit(ctx)
	status := classifyStatus(commitErr)
	_ = w.WriteResult(opID, status, errMsg(commitErr))
	return status, errorClass(commitErr)
}

func errMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
