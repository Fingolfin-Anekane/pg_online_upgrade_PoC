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
	CompletedAt         time.Time
	FinalFlushLSN       string
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

	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("slotdrain receive: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				continue
			}
			switch m.Data[0] {
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					return nil, fmt.Errorf("slotdrain parse xlog: %w", err)
				}

				if err := handleXLogData(ctx, conn, xld, targetLSN, report, &lastFlushLSN); err != nil {
					if err == errStopDrain {
						report.CompletedAt = time.Now()
						report.FinalFlushLSN = lastFlushLSN.String()
						return report, nil
					}
					return nil, err
				}

			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pka, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					return nil, fmt.Errorf("slotdrain parse keepalive: %w", err)
				}
				if pka.ReplyRequested {
					if err := sendStatusUpdate(ctx, conn, lastFlushLSN); err != nil {
						return nil, err
					}
				}
			}
		}
	}
}

var errStopDrain = fmt.Errorf("stop drain")

func handleXLogData(
	ctx context.Context,
	conn *pgconn.PgConn,
	xld pglogrepl.XLogData,
	targetLSN pglogrepl.LSN,
	report *Report,
	lastFlushLSN *pglogrepl.LSN,
) error {
	if len(xld.WALData) == 0 {
		return nil
	}

	// Parse the logical replication message. We only care about Commit messages
	// (transaction boundaries); everything else is skipped. Parse errors for
	// message types we don't decode are non-fatal — just continue.
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return nil
	}

	commitMsg, ok := logicalMsg.(*pglogrepl.CommitMessage)
	if !ok {
		return nil
	}

	if commitMsg.CommitLSN <= targetLSN {
		if err := sendStatusUpdate(ctx, conn, commitMsg.CommitLSN); err != nil {
			return err
		}
		*lastFlushLSN = commitMsg.CommitLSN
		report.TransactionsDrained++
		return nil
	}

	// commit_lsn > targetLSN: stop without ACKing
	return errStopDrain
}

func sendStatusUpdate(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ReplyRequested:   false,
	})
}
