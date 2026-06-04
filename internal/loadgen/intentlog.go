package loadgen

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Status values for an intent-log record.
const (
	StatusAttempt = "attempt"
	StatusAcked   = "acked"
	StatusFailed  = "failed"
	StatusIndoubt = "indoubt" // wire value; prose docs spell it "in-doubt"
)

// Record is one line of the append-only JSONL intent-log. Attempt records carry
// the full operation identity; result records carry op_id + status (+ error).
//
// omitempty invariant: WriterID (>=1000), ClientSeq (>=1), and Rows (>=1) are
// assigned by the runner and are never zero in attempt records, so omitempty
// never silently drops a meaningful value. Result records deliberately omit
// these fields to stay minimal (op_id + status + error + ts only).
type Record struct {
	OpID      string    `json:"op_id"`
	Kind      string    `json:"kind,omitempty"`
	WriterID  int       `json:"writer_id,omitempty"`
	ClientSeq int64     `json:"client_seq,omitempty"`
	Rows      int64     `json:"rows,omitempty"`
	BatchID   string    `json:"batch_id,omitempty"`
	DSN       string    `json:"dsn,omitempty"`
	Phase     string    `json:"phase,omitempty"`
	Status    string    `json:"status"`
	Error     string    `json:"error,omitempty"`
	TS        time.Time `json:"ts"`
}

// Writer is the single serialized owner of the durable intent-log and the
// human-readable stdout stream. All workers share one Writer; the mutex
// guarantees non-interleaved JSONL.
type Writer struct {
	mu  sync.Mutex
	f   *os.File
	out io.Writer
}

// TruncateIntentLog empties the intent-log at path (creating it if absent) so a
// fresh run started after `init --reset` does not leave stale records from prior
// runs. The log is append-only across runs, and `verify` reconciles the WHOLE
// file against the (just-truncated) DB; without this, rows wiped by the reset
// surface as false LOST/PHANTOM. An empty path is a no-op.
func TruncateIntentLog(path string) error {
	if path == "" {
		return nil
	}
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		return fmt.Errorf("intentlog truncate %s: %w", path, err)
	}
	return nil
}

// NewWriter opens (creates/appends) the intent-log file and tees a human stream.
func NewWriter(path string, stdout io.Writer) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("intentlog open %s: %w", path, err)
	}
	return &Writer{f: f, out: stdout}, nil
}

// WriteAttempt persists an attempt record and fsyncs before returning, so the
// ground-truth entry is durable before the caller issues COMMIT.
func (w *Writer) WriteAttempt(rec Record) error {
	rec.Status = StatusAttempt
	rec.TS = time.Now().UTC()
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writeLocked(rec); err != nil {
		return err
	}
	return w.f.Sync()
}

// WriteResult persists the terminal status for an op. Not fsynced individually;
// it is flushed by the OS and on Close.
func (w *Writer) WriteResult(opID, status, errMsg string) error {
	rec := Record{OpID: opID, Status: status, Error: errMsg, TS: time.Now().UTC()}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeLocked(rec)
}

func (w *Writer) writeLocked(rec Record) error {
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("intentlog marshal: %w", err)
	}
	line = append(line, '\n')
	if _, err := w.f.Write(line); err != nil {
		return fmt.Errorf("intentlog write: %w", err)
	}
	_, _ = w.out.Write(line) // human stream is best-effort
	return nil
}

// Close fsyncs and closes the underlying file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("intentlog sync on close: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("intentlog close: %w", err)
	}
	return nil
}
