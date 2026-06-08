package slotdrain

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Config holds parameters for a slot drain operation.
type Config struct {
	ConnString string // must include dbname; replication=database is appended automatically
	SlotName   string
	PubName    string
	TargetLSN  string // hex LSN, e.g. "0/3FA20000"
}

func (c *Config) Validate() error {
	if c.ConnString == "" {
		return fmt.Errorf("slotdrain: conn_string is required")
	}
	if c.SlotName == "" {
		return fmt.Errorf("slotdrain: slot_name is required")
	}
	if c.PubName == "" {
		return fmt.Errorf("slotdrain: pub_name is required")
	}
	if _, err := pglogrepl.ParseLSN(c.TargetLSN); err != nil {
		return fmt.Errorf("slotdrain: target_lsn %q invalid: %w", c.TargetLSN, err)
	}
	return nil
}

// Report is the result of a completed drain.
type Report struct {
	CompletedAt   time.Time
	FinalFlushLSN string // the target we confirmed flush at (what we aimed the slot to)
	// LastCommitLSN is the commit_lsn of the last transaction ACKed (<= target).
	// PostgreSQL clamps confirmed_flush_lsn to this, which can sit a few bytes
	// below target (non-decodable WAL fills the gap), so verification checks
	// LastCommitLSN <= confirmed_flush <= target rather than strict equality.
	LastCommitLSN       string
	TransactionsDrained int
}

// Drain reads transactions from the logical slot and ACKs each transaction whose
// commit_lsn <= targetLSN. It stops without ACKing the first transaction whose
// commit_lsn > targetLSN, leaving it for the PG17 subscription to deliver.
//
// The function returns when either the slot is drained to targetLSN or the
// context is cancelled.
func Drain(ctx context.Context, cfg Config) (*Report, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	targetLSN, _ := pglogrepl.ParseLSN(cfg.TargetLSN)

	connStr := cfg.ConnString + " replication=database"
	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("slotdrain connect: %w", err)
	}
	defer conn.Close(ctx)

	if err := pglogrepl.StartReplication(ctx, conn, cfg.SlotName, 0,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", cfg.PubName),
			},
		}); err != nil {
		return nil, fmt.Errorf("slotdrain start replication: %w", err)
	}

	report := &Report{}
	var lastFlushLSN pglogrepl.LSN

	// finalize advances confirmed_flush to exactly targetLSN and returns the
	// report. Data committed at or before target is already in the PG17 baseline
	// (N1 was physically frozen at target_lsn); the tail — every transaction whose
	// commit_lsn > target, including long transactions that started before target
	// but commit after it — stays in the slot for the PG17 subscription, because
	// the slot redelivers anything committing after confirmed_flush.
	finalize := func() (*Report, error) {
		if err := sendStatusUpdate(ctx, conn, targetLSN); err != nil {
			return nil, err
		}
		// Gracefully end copy-both mode. The status update above is fire-and-forget:
		// the slot's confirmed_flush_lsn advances only when the walsender *reads* our
		// feedback, and nothing forces it to do so before the deferred conn.Close
		// tears down the connection. SendStandbyCopyDone sends CopyDone and reads the
		// server's reply through ReadyForQuery, which makes the walsender drain its
		// input — processing our target ACK — before we disconnect. Without it,
		// confirmed_flush can stick below the LSNs we ACKed (it was observed landing
		// below the last drained commit), failing VerifySlotDrained nondeterministically.
		if _, err := pglogrepl.SendStandbyCopyDone(ctx, conn); err != nil {
			return nil, fmt.Errorf("slotdrain copy-done: %w", err)
		}
		report.CompletedAt = time.Now()
		report.FinalFlushLSN = targetLSN.String()
		report.LastCommitLSN = lastFlushLSN.String()
		return report, nil
	}

	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("slotdrain receive: %w", err)
		}

		cd, ok := msg.(*pgproto3.CopyData)
		if !ok || len(cd.Data) == 0 {
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("slotdrain parse xlog: %w", err)
			}
			stop, err := handleXLogData(ctx, conn, xld, targetLSN, report, &lastFlushLSN)
			if err != nil {
				return nil, err
			}
			// Stop either because we saw a commit beyond target, or because the
			// server's WAL stream has itself advanced to/over target — the latter
			// is what prevents an indefinite wait when no post-target commit ever
			// arrives (idle primary, empty transactions, or writes to relations
			// outside the publication).
			if stop || reachedTarget(xld.ServerWALEnd, targetLSN) {
				return finalize()
			}

		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pka, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("slotdrain parse keepalive: %w", err)
			}
			if reachedTarget(pka.ServerWALEnd, targetLSN) {
				return finalize()
			}
			if pka.ReplyRequested {
				if err := sendStatusUpdate(ctx, conn, lastFlushLSN); err != nil {
					return nil, err
				}
			}
		}
	}
}

// reachedTarget reports whether the server's streamed WAL position has advanced
// to or past target, which means every transaction committing at or before
// target has already been delivered (logical decoding streams in commit order).
func reachedTarget(serverWALEnd, target pglogrepl.LSN) bool {
	return serverWALEnd >= target
}

// handleXLogData ACKs a commit at or before target and returns stop=true when it
// sees the first commit beyond target (the rest is the tail for PG17). Non-commit
// messages and undecodable payloads are skipped.
func handleXLogData(
	ctx context.Context,
	conn *pgconn.PgConn,
	xld pglogrepl.XLogData,
	targetLSN pglogrepl.LSN,
	report *Report,
	lastFlushLSN *pglogrepl.LSN,
) (bool, error) {
	if len(xld.WALData) == 0 {
		return false, nil
	}

	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return false, nil
	}

	commitMsg, ok := logicalMsg.(*pglogrepl.CommitMessage)
	if !ok {
		return false, nil
	}

	if commitMsg.CommitLSN <= targetLSN {
		if err := sendStatusUpdate(ctx, conn, commitMsg.CommitLSN); err != nil {
			return false, err
		}
		*lastFlushLSN = commitMsg.CommitLSN
		report.TransactionsDrained++
		return false, nil
	}

	return true, nil // commit_lsn > targetLSN: stop, leave it for PG17
}

func sendStatusUpdate(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ReplyRequested:   false,
	})
}
