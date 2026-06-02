package oracle

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeLog(t *testing.T, lines string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "log.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(lines), 0o644))
	return path
}

func TestReadLogPairsRecords(t *testing.T) {
	path := writeLog(t, `{"op_id":"o1","kind":"append","writer_id":7,"client_seq":5,"rows":1,"status":"attempt","ts":"2026-06-02T00:00:00Z"}
{"op_id":"o1","status":"acked","ts":"2026-06-02T00:00:01Z"}
{"op_id":"o2","kind":"long-txn","writer_id":3,"client_seq":10,"rows":2,"status":"attempt","ts":"2026-06-02T00:00:02Z"}
{"op_id":"o2","status":"failed","ts":"2026-06-02T00:00:03Z"}
{"op_id":"o3","kind":"append","writer_id":7,"client_seq":6,"rows":1,"status":"attempt","ts":"2026-06-02T00:00:04Z"}
`)
	ops, err := ReadLog(path)
	require.NoError(t, err)
	require.Len(t, ops, 3)

	byID := map[string]Op{}
	for _, op := range ops {
		byID[op.OpID] = op
	}
	assert.Equal(t, "acked", byID["o1"].Status)
	assert.Equal(t, int64(1), byID["o1"].Rows)
	assert.Equal(t, "failed", byID["o2"].Status)
	assert.Equal(t, int64(2), byID["o2"].Rows)
	assert.Equal(t, "indoubt", byID["o3"].Status) // attempt with no result
}
