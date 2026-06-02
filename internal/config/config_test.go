package config_test

import (
	"os"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Valid(t *testing.T) {
	f := writeTempFile(t, `
cluster_name: prod
upgrade:
  target_node: n1.internal
  slot_name: slot_upgrade
  publication_name: pub_upgrade
  new_pg_bindir: /usr/lib/postgresql/17/bin
pg:
  superuser_dsn: "host=primary port=5432 dbname=postgres user=postgres password=s3cr3t"
`)
	cfg, err := config.Load(f)
	require.NoError(t, err)

	assert.Equal(t, "prod", cfg.ClusterName)
	assert.Equal(t, "n1.internal", cfg.Upgrade.TargetNode)
	assert.Equal(t, "slot_upgrade", cfg.Upgrade.SlotName)
	assert.Equal(t, "pub_upgrade", cfg.Upgrade.PublicationName)
	assert.Equal(t, "/usr/lib/postgresql/17/bin", cfg.Upgrade.NewPGBindir)
	assert.Equal(t, "host=primary port=5432 dbname=postgres user=postgres password=s3cr3t", cfg.PG.SuperuserDSN)
}

func TestLoad_MissingClusterName(t *testing.T) {
	f := writeTempFile(t, `
upgrade:
  slot_name: slot_upgrade
  publication_name: pub_upgrade
  new_pg_bindir: /usr/lib/postgresql/17/bin
pg:
  superuser_dsn: "host=primary port=5432 dbname=postgres user=postgres"
`)
	_, err := config.Load(f)
	assert.ErrorContains(t, err, "cluster_name")
}

func TestLoad_MissingSlotName(t *testing.T) {
	f := writeTempFile(t, `
cluster_name: prod
upgrade:
  publication_name: pub_upgrade
  new_pg_bindir: /usr/lib/postgresql/17/bin
pg:
  superuser_dsn: "host=primary port=5432 dbname=postgres user=postgres"
`)
	_, err := config.Load(f)
	assert.ErrorContains(t, err, "slot_name")
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := config.Load("/nonexistent/path.yaml")
	assert.ErrorContains(t, err, "read config")
}

func TestValidateForRun(t *testing.T) {
	cfg := &config.Config{Upgrade: config.UpgradeConfig{
		TargetNode: "n1", OldPGBindir: "/o", NewPGBindir: "/n",
		DataDir: "/old", NewDataDir: "/new", PatroniConfigPath: "/p.yml",
		SubscriptionName: "sub_upgrade", ReversePubName: "pub_rb", ReverseSubName: "sub_rb",
		DBName: "app", PG17DSN: "host=localhost port=5433", NewPatroniURL: "http://localhost:8009",
		DSNSwapSignalPath: "/run/sig.json", SequenceBuffer: 1000,
	}}
	require.NoError(t, cfg.ValidateForRun())

	cfg.Upgrade.DataDir = ""
	assert.ErrorContains(t, cfg.ValidateForRun(), "data_dir")

	cfg2 := &config.Config{Upgrade: config.UpgradeConfig{
		TargetNode: "n1", OldPGBindir: "/o", DataDir: "/same", NewDataDir: "/same", PatroniConfigPath: "/p",
		SubscriptionName: "sub_upgrade", ReversePubName: "pub_rb", ReverseSubName: "sub_rb",
		DBName: "app", PG17DSN: "host=localhost port=5433", NewPatroniURL: "http://localhost:8009",
		DSNSwapSignalPath: "/run/sig.json", SequenceBuffer: 1000,
	}}
	assert.ErrorContains(t, cfg2.ValidateForRun(), "new_data_dir must differ")
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp("", "pg-upgrade-*.yaml")
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(f.Name()) })
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}
