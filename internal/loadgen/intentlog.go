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
	StatusIndoubt = "indoubt"
)

// Record is one line of the append-only JSONL intent-log. Attempt records carry
// the full operation identity; result records carry op_id + status (+ error).
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
		return err
	}
	return w.f.Close()
}
