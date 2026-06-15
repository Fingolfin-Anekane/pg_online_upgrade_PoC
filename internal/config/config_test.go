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
	assert.Equal(t, "postgres", cfg.PG.OSUser, "os_user defaults to postgres when omitted")
}

func TestLoad_PatroniInitdbConfigDefaultsToPatroniConfigPath(t *testing.T) {
	f := writeTempFile(t, `
cluster_name: prod
upgrade:
  slot_name: slot_upgrade
  publication_name: pub_upgrade
  new_pg_bindir: /usr/lib/postgresql/17/bin
  patroni_config_path: /etc/patroni/patroni.yml
pg:
  superuser_dsn: "host=primary port=5432 dbname=postgres user=postgres"
`)
	cfg, err := config.Load(f)
	require.NoError(t, err)
	assert.Equal(t, "/etc/patroni/patroni.yml", cfg.Upgrade.PatroniInitdbConfig)
}

func TestLoad_PatroniStartCommandDefault(t *testing.T) {
	f := writeTempFile(t, `
cluster_name: prod
upgrade:
  slot_name: slot_upgrade
  publication_name: pub_upgrade
  new_pg_bindir: /usr/lib/postgresql/17/bin
pg:
  superuser_dsn: "host=primary port=5432 dbname=postgres user=postgres"
`)
	cfg, err := config.Load(f)
	require.NoError(t, err)
	assert.Equal(t, "systemctl start patroni", cfg.Upgrade.PatroniStartCommand)
}

func TestLoad_OSUserExplicitOverridesDefault(t *testing.T) {
	f := writeTempFile(t, `
cluster_name: prod
upgrade:
  slot_name: slot_upgrade
  publication_name: pub_upgrade
  new_pg_bindir: /usr/lib/postgresql/17/bin
pg:
  superuser_dsn: "host=primary port=5432 dbname=postgres user=postgres"
  os_user: pgowner
`)
	cfg, err := config.Load(f)
	require.NoError(t, err)
	assert.Equal(t, "pgowner", cfg.PG.OSUser)
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
		PgUpgradeLogDir: "/data/pg_upgrade_output.d", LogArchiveDir: "/var/log/pg-upgrade",
	}}
	require.NoError(t, cfg.ValidateForRun())

	cfg.Upgrade.DataDir = ""
	assert.ErrorContains(t, cfg.ValidateForRun(), "data_dir")

	cfg2 := &config.Config{Upgrade: config.UpgradeConfig{
		TargetNode: "n1", OldPGBindir: "/o", DataDir: "/same", NewDataDir: "/same", PatroniConfigPath: "/p",
		SubscriptionName: "sub_upgrade", ReversePubName: "pub_rb", ReverseSubName: "sub_rb",
		DBName: "app", PG17DSN: "host=localhost port=5433", NewPatroniURL: "http://localhost:8009",
		DSNSwapSignalPath: "/run/sig.json", SequenceBuffer: 1000,
		PgUpgradeLogDir: "/data/pg_upgrade_output.d", LogArchiveDir: "/var/log/pg-upgrade",
	}}
	assert.ErrorContains(t, cfg2.ValidateForRun(), "new_data_dir must differ")
}

func validRunConfig() config.Config {
	return config.Config{
		ClusterName: "prod-pg",
		Upgrade: config.UpgradeConfig{
			TargetNode: "n1", SlotName: "s", PublicationName: "p",
			NewPGBindir: "/new/bin", OldPGBindir: "/old/bin",
			DataDir: "/data/old", NewDataDir: "/data/new",
			PatroniConfigPath: "/etc/patroni.yml", SubscriptionName: "sub",
			ReversePubName: "rpub", ReverseSubName: "rsub", DBName: "appdb",
			PG17DSN: "host=h dbname=appdb", NewPatroniURL: "http://h:8008",
			DSNSwapSignalPath: "/run/swap.json", SequenceBuffer: 1000,
			PgUpgradeLogDir: "/var/log/pgu", LogArchiveDir: "/var/log/arch",
		},
		PG: config.PGConfig{SuperuserDSN: "host=h dbname=appdb"},
	}
}

func TestEffectiveNewScope_DefaultDerivesFromClusterName(t *testing.T) {
	c := config.Config{ClusterName: "prod-pg"}
	assert.Equal(t, "prod-pg-17", c.EffectiveNewScope())
}

func TestEffectiveNewScope_UsesConfiguredValue(t *testing.T) {
	c := config.Config{ClusterName: "prod-pg", Upgrade: config.UpgradeConfig{NewScope: "pg17-cluster"}}
	assert.Equal(t, "pg17-cluster", c.EffectiveNewScope())
}

func TestValidateForRun_RejectsNewScopeEqualToClusterName(t *testing.T) {
	c := validRunConfig()
	c.Upgrade.NewScope = c.ClusterName
	err := c.ValidateForRun()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "new_scope")
}

func TestShadowConfigParses(t *testing.T) {
	f := writeTempFile(t, `
cluster_name: prod
upgrade:
  slot_name: slot_upgrade
  publication_name: pub_upgrade
  new_pg_bindir: /usr/lib/postgresql/17/bin
  shadow_patroni_url: http://shadow-leader:8008
  shadow_source_host: prod-primary
  shadow_source_port: 5432
  physical_slot_name: shadow_phys
  shadow_node_count: 3
pg:
  superuser_dsn: "host=primary port=5432 dbname=postgres user=postgres"
`)
	cfg, err := config.Load(f)
	require.NoError(t, err)
	assert.Equal(t, "http://shadow-leader:8008", cfg.Upgrade.ShadowPatroniURL)
	assert.Equal(t, "prod-primary", cfg.Upgrade.ShadowSourceHost)
	assert.Equal(t, 5432, cfg.Upgrade.ShadowSourcePort)
	assert.Equal(t, "shadow_phys", cfg.Upgrade.PhysicalSlotName)
	assert.Equal(t, 3, cfg.Upgrade.ShadowNodeCount)
}

func TestModeDefaultsAndParses(t *testing.T) {
	f := writeTempFile(t, `
cluster_name: prod
upgrade:
  slot_name: slot_upgrade
  publication_name: pub_upgrade
  new_pg_bindir: /usr/lib/postgresql/17/bin
  mode: shadow
pg:
  superuser_dsn: "host=primary port=5432 dbname=postgres user=postgres"
`)
	cfg, err := config.Load(f)
	require.NoError(t, err)
	assert.Equal(t, "shadow", cfg.Upgrade.Mode)
	assert.Equal(t, "shadow", cfg.EffectiveMode())

	f2 := writeTempFile(t, `
cluster_name: prod
upgrade:
  slot_name: slot_upgrade
  publication_name: pub_upgrade
  new_pg_bindir: /usr/lib/postgresql/17/bin
pg:
  superuser_dsn: "host=primary port=5432 dbname=postgres user=postgres"
`)
	cfg2, err := config.Load(f2)
	require.NoError(t, err)
	assert.Equal(t, "inplace", cfg2.EffectiveMode())
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
