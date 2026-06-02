package loadgen

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readRecords(t *testing.T, path string) []Record {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var recs []Record
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r Record
		require.NoError(t, json.Unmarshal(sc.Bytes(), &r))
		recs = append(recs, r)
	}
	return recs
}

func TestWriterAttemptThenResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.jsonl")
	var stdout bytes.Buffer
	w, err := NewWriter(path, &stdout)
	require.NoError(t, err)

	rec := Record{OpID: "op1", Kind: "append", WriterID: 3, ClientSeq: 41, DSN: "a", Phase: "pre-switch"}
	require.NoError(t, w.WriteAttempt(rec))
	require.NoError(t, w.WriteResult("op1", StatusAcked, ""))
	require.NoError(t, w.Close())

	recs := readRecords(t, path)
	require.Len(t, recs, 2)
	assert.Equal(t, StatusAttempt, recs[0].Status)
	assert.Equal(t, "op1", recs[0].OpID)
	assert.Equal(t, 41, int(recs[0].ClientSeq))
	assert.Equal(t, StatusAcked, recs[1].Status)
	assert.Equal(t, "op1", recs[1].OpID)
	assert.NotEmpty(t, stdout.String()) // human stream also written
}
