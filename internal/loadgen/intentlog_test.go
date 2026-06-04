package loadgen

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	require.NoError(t, sc.Err())
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
	assert.Equal(t, 2, strings.Count(stdout.String(), "\n")) // human stream also written
	assert.False(t, recs[0].TS.IsZero(), "attempt TS must be set")
}

func TestWriterConcurrentWritesAreWellFormed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.jsonl")
	w, err := NewWriter(path, io.Discard)
	require.NoError(t, err)

	const workers, iters = 8, 50
	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				opID := fmt.Sprintf("w%d-%d", id, i)
				require.NoError(t, w.WriteAttempt(Record{OpID: opID, Kind: "append", WriterID: id + 1, ClientSeq: int64(i + 1)}))
				require.NoError(t, w.WriteResult(opID, StatusAcked, ""))
			}
		}(g)
	}
	wg.Wait()
	require.NoError(t, w.Close())

	recs := readRecords(t, path)
	assert.Len(t, recs, workers*iters*2) // every line parsed cleanly => no interleaving
}

func TestTruncateIntentLogEmptiesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{\"op_id\":\"a\"}\n{\"op_id\":\"b\"}\n"), 0o644))

	require.NoError(t, TruncateIntentLog(path))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Empty(t, data, "intent-log must be emptied so verify can't reconcile stale runs")
}

func TestTruncateIntentLogNoopOnEmptyPath(t *testing.T) {
	require.NoError(t, TruncateIntentLog(""))
}
