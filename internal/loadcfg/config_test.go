package loadcfg

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultsApplied(t *testing.T) {
	cfg := Default()
	assert.Equal(t, 100, cfg.Accounts)
	assert.Equal(t, int64(1000), cfg.Balance)
	assert.Equal(t, "loadtool-intent.jsonl", cfg.IntentLog)
	assert.Equal(t, 4, cfg.Workers.Append.Count)
	assert.Equal(t, 1, cfg.Workers.LongTxn.Count)
	assert.Equal(t, 2, cfg.Workers.Transfer.Count)
	assert.Equal(t, 1, cfg.Workers.RYW.Count)
}

func TestLoadYAMLOverridesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	require.NoError(t, os.WriteFile(path, []byte(
		"accounts: 50\nbalance: 500\nduration: 30s\nworkers:\n  append:\n    count: 8\n    rate: 20\n"), 0o644))

	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, 50, cfg.Accounts)
	assert.Equal(t, int64(500), cfg.Balance)
	assert.Equal(t, 30*time.Second, time.Duration(cfg.Duration))
	assert.Equal(t, 8, cfg.Workers.Append.Count)
	assert.Equal(t, 20, cfg.Workers.Append.Rate)
	// untouched fields keep defaults
	assert.Equal(t, 2, cfg.Workers.Transfer.Count)
}

func TestValidateRequiresDSNs(t *testing.T) {
	cfg := Default()
	assert.Error(t, cfg.ValidateRun())
	cfg.DSNA = "postgres://a"
	assert.Error(t, cfg.ValidateRun())
	cfg.DSNB = "postgres://b"
	assert.NoError(t, cfg.ValidateRun())
}
