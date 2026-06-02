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
