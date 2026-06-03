package phases

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseInitdbOptions_FlagsAndKeyValues(t *testing.T) {
	yaml := []byte(`
bootstrap:
  initdb:
    - data-checksums
    - encoding: UTF8
    - locale: en_US.UTF-8
`)
	opts, err := parseInitdbOptions(yaml)
	require.NoError(t, err)
	assert.Equal(t, []string{"--data-checksums", "--encoding=UTF8", "--locale=en_US.UTF-8"}, opts)
}

func TestParseInitdbOptions_Empty(t *testing.T) {
	opts, err := parseInitdbOptions([]byte("scope: prod\n"))
	require.NoError(t, err)
	assert.Empty(t, opts)
}

func TestInitNewDataDir_SkipsWhenAlreadyInitialized(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "PG_VERSION"), []byte("17\n"), 0o644))
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{NewDataDir: dir}}}
	done, err := (&initNewDataDir{d}).Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done)
}

func TestInitNewDataDir_ErrorsWithoutConfig(t *testing.T) {
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{NewDataDir: t.TempDir() + "/empty"}}}
	err := (&initNewDataDir{d}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "patroni_initdb_config")
}
