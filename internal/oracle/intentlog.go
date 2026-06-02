// Package oracle is the authoritative verdict layer: it reads the durable
// intent-log and the database at rest and reconciles them. Nothing here reads
// live during the workload.
package oracle

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// Status strings shared with the loadgen intent-log writer. They MUST match
// loadgen's Status* constants; a skew would silently misclassify operations.
const (
	statusAttempt = "attempt"
	statusIndoubt = "indoubt"
)

// Op is a resolved operation: attempt identity + terminal status.
type Op struct {
	OpID      string
	Kind      string
	WriterID  int
	ClientSeq int64
	Rows      int64
	BatchID   string
	Status    string // acked / failed / indoubt
}

type rawRecord struct {
	OpID      string `json:"op_id"`
	Kind      string `json:"kind"`
	WriterID  int    `json:"writer_id"`
	ClientSeq int64  `json:"client_seq"`
	Rows      int64  `json:"rows"`
	BatchID   string `json:"batch_id"`
	Status    string `json:"status"`
}

// ReadLog parses the JSONL intent-log and pairs attempt/result by op_id. An
// attempt with no terminal result is classified indoubt.
func ReadLog(path string) ([]Op, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("oracle open log: %w", err)
	}
	defer f.Close()

	ops := map[string]*Op{}
	order := []string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var r rawRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("oracle parse line: %w", err)
		}
		if r.Status == statusAttempt {
			if _, ok := ops[r.OpID]; !ok {
				order = append(order, r.OpID)
			}
			// Overwrite is safe: writers mint a fresh uuid op_id per operation,
			// so the same op_id never has two attempt records.
			ops[r.OpID] = &Op{
				OpID: r.OpID, Kind: r.Kind, WriterID: r.WriterID,
				ClientSeq: r.ClientSeq, Rows: max(r.Rows, 1), BatchID: r.BatchID,
				Status: statusIndoubt, // until a result arrives
			}
			continue
		}
		if op, ok := ops[r.OpID]; ok {
			op.Status = r.Status
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("oracle scan: %w", err)
	}

	out := make([]Op, 0, len(order))
	for _, id := range order {
		out = append(out, *ops[id])
	}
	return out, nil
}
