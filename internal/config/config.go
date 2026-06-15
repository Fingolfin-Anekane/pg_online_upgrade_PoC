package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ClusterName string        `yaml:"cluster_name"`
	Upgrade     UpgradeConfig `yaml:"upgrade"`
	PG          PGConfig      `yaml:"pg"`
	Patroni     PatroniConfig `yaml:"patroni"`
}

// PatroniConfig holds the credentials Patroni requires for unsafe REST methods
// (PATCH /config for pause/resume) when restapi.authentication is enabled.
// Leave empty for clusters whose REST API is unauthenticated.
type PatroniConfig struct {
	RESTUsername string `yaml:"rest_username"`
	RESTPassword string `yaml:"rest_password"`
}

type UpgradeConfig struct {
	TargetNode        string `yaml:"target_node"`
	SlotName          string `yaml:"slot_name"`
	PublicationName   string `yaml:"publication_name"`
	NewPGBindir       string `yaml:"new_pg_bindir"`
	OldPGBindir       string `yaml:"old_pg_bindir"`
	DataDir           string `yaml:"data_dir"`
	NewDataDir        string `yaml:"new_data_dir"`
	PatroniConfigPath string `yaml:"patroni_config_path"`
	SubscriptionName  string `yaml:"subscription_name"`
	ReversePubName    string `yaml:"reverse_pub_name"`
	ReverseSubName    string `yaml:"reverse_sub_name"`
	DBName            string `yaml:"dbname"`
	PG17DSN           string `yaml:"pg17_dsn"`
	NewPatroniURL     string `yaml:"new_patroni_url"`
	DSNSwapSignalPath string `yaml:"dsn_swap_signal_path"`
	SequenceBuffer    int64  `yaml:"sequence_buffer"`
	PgUpgradeLogDir   string `yaml:"pg_upgrade_log_dir"`
	LogArchiveDir     string `yaml:"log_archive_dir"`
	// PatroniInitdbConfig is the path to the patroni.yml whose bootstrap.initdb
	// options are passed to initdb when creating new_data_dir, so the new cluster
	// matches the old one's encoding/locale/checksums. Defaults to
	// PatroniConfigPath. Unused when new_data_dir is already initialized.
	PatroniInitdbConfig string `yaml:"patroni_initdb_config"`
	// PatroniStartCommand brings up Patroni on N1 during catchup so the new PG17
	// cluster comes up under Patroni management (not bare pg_ctl). It must return
	// promptly (start the service in the background), e.g. "systemctl start
	// patroni" — not run Patroni in the foreground. Defaults to DefaultPatroniStart.
	PatroniStartCommand string `yaml:"patroni_start_command"`
	// OldPatroniStopCommand, if set, is run to stop the OLD Patroni on N1 at two
	// points: during isolate (so a paused Patroni cannot re-apply primary_conninfo
	// and re-attach N1's walreceiver after the WAL disconnect — it must leave
	// postgres running, e.g. `systemctl stop patroni`), and again before
	// pg_upgrade (a running old server corrupts the --link'd new cluster). If
	// empty, isolate relies on the VerifyN1Detached guard and the upgrade step
	// only VERIFIES the old Patroni is unreachable and fails otherwise (so you can
	// stop it manually) — useful where auto-stopping is risky (e.g. Patroni as
	// PID 1 in a k8s pod shared with the orchestrator).
	OldPatroniStopCommand string `yaml:"old_patroni_stop_command"`
	// NewScope is the DCS scope (Patroni cluster name) the upgraded PG17 cluster
	// runs under. It MUST differ from cluster_name: the upgraded cluster has a new
	// system identifier, and reusing the old scope makes Patroni refuse to start
	// with a system-ID mismatch. Empty defaults to "<cluster_name>-17" (see
	// Config.EffectiveNewScope). The catchup PatchNewPatroniConfig step writes it
	// into the new cluster's patroni.yml.
	NewScope string `yaml:"new_scope"`
	// NewConfigDir is the PG17 config_dir written into the new cluster's
	// patroni.yml (postgresql.config_dir). Empty means leave config_dir untouched
	// (Patroni defaults config_dir to data_dir). There is no safe default path, so
	// it is only patched when set.
	NewConfigDir string `yaml:"new_config_dir"`

	// --- shadow-cluster upgrade ---
	// ShadowPatroniURL is the REST endpoint of the shadow cluster's Patroni
	// (the cluster that becomes the new PG17 cluster).
	ShadowPatroniURL string `yaml:"shadow_patroni_url"`
	// ShadowSourceHost/Port is the prod primary the shadow physically replicates
	// from (the standby_cluster source).
	ShadowSourceHost string `yaml:"shadow_source_host"`
	ShadowSourcePort int    `yaml:"shadow_source_port"`
	// PhysicalSlotName is the physical replication slot created on prod for the
	// shadow's stream (separate from the logical SlotName).
	PhysicalSlotName string `yaml:"physical_slot_name"`
	// ShadowNodeCount is the expected node count of the shadow cluster; provision
	// waits until that many members are healthy.
	ShadowNodeCount int `yaml:"shadow_node_count"`

	// Mode selects the upgrade topology: "inplace" (default; cannibalize a live
	// replica) or "shadow" (parallel standby_cluster upgraded in place).
	Mode string `yaml:"mode"`
}

type PGConfig struct {
	SuperuserDSN string `yaml:"superuser_dsn"`
	// OSUser is the OS account that owns the data directory and under which the PG
	// CLI tools (pg_ctl, pg_upgrade, pg_controldata) must run — they refuse to run
	// as root. Used only when the orchestrator itself runs as root. Defaults to
	// DefaultOSUser when omitted.
	OSUser string `yaml:"os_user"`
}

// DefaultOSUser is the OS account PG tools run as when pg.os_user is not set.
const DefaultOSUser = "postgres"

// DefaultPatroniStart is the command used to bring up Patroni when
// upgrade.patroni_start_command is not set.
const DefaultPatroniStart = "systemctl start patroni"

// EffectiveNewScope is the DCS scope for the upgraded PG17 cluster: the
// explicit upgrade.new_scope when set, otherwise "<cluster_name>-17".
func (c Config) EffectiveNewScope() string {
	if c.Upgrade.NewScope != "" {
		return c.Upgrade.NewScope
	}
	return c.ClusterName + "-17"
}

// EffectiveMode returns Upgrade.Mode, defaulting to "inplace".
func (c Config) EffectiveMode() string {
	if c.Upgrade.Mode == "" {
		return "inplace"
	}
	return c.Upgrade.Mode
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.PG.OSUser == "" {
		cfg.PG.OSUser = DefaultOSUser
	}
	// initdb's options default to the same patroni.yml the run already points at;
	// its bootstrap.initdb is read before patroni_config_path is rewritten.
	if cfg.Upgrade.PatroniInitdbConfig == "" {
		cfg.Upgrade.PatroniInitdbConfig = cfg.Upgrade.PatroniConfigPath
	}
	if cfg.Upgrade.PatroniStartCommand == "" {
		cfg.Upgrade.PatroniStartCommand = DefaultPatroniStart
	}

	return &cfg, cfg.validate()
}

// ValidateForRun checks the additional fields the full `run` (phases 1-6)
// requires beyond the base Load() validation. drain-slot/status do not call it.
func (c *Config) ValidateForRun() error {
	u := c.Upgrade
	var missing []string
	if u.TargetNode == "" {
		missing = append(missing, "target_node")
	}
	if u.OldPGBindir == "" {
		missing = append(missing, "old_pg_bindir")
	}
	if u.DataDir == "" {
		missing = append(missing, "data_dir")
	}
	if u.NewDataDir == "" {
		missing = append(missing, "new_data_dir")
	}
	if u.PatroniConfigPath == "" {
		missing = append(missing, "patroni_config_path")
	}
	if u.SubscriptionName == "" {
		missing = append(missing, "subscription_name")
	}
	if u.DBName == "" {
		missing = append(missing, "dbname")
	}
	if u.PG17DSN == "" {
		missing = append(missing, "pg17_dsn")
	}
	if u.NewPatroniURL == "" {
		missing = append(missing, "new_patroni_url")
	}
	if u.DSNSwapSignalPath == "" {
		missing = append(missing, "dsn_swap_signal_path")
	}
	if u.ReversePubName == "" {
		missing = append(missing, "reverse_pub_name")
	}
	if u.ReverseSubName == "" {
		missing = append(missing, "reverse_sub_name")
	}
	if u.PgUpgradeLogDir == "" {
		missing = append(missing, "pg_upgrade_log_dir")
	}
	if u.LogArchiveDir == "" {
		missing = append(missing, "log_archive_dir")
	}
	if len(missing) > 0 {
		return fmt.Errorf("config: run requires upgrade fields: %s", strings.Join(missing, ", "))
	}
	if u.NewDataDir == u.DataDir {
		return fmt.Errorf("config: new_data_dir must differ from data_dir")
	}
	if u.SequenceBuffer <= 0 {
		return fmt.Errorf("config: sequence_buffer must be positive (got %d)", u.SequenceBuffer)
	}
	if c.EffectiveNewScope() == c.ClusterName {
		return fmt.Errorf("config: upgrade.new_scope must differ from cluster_name %q (the upgraded cluster has a new system identifier; reusing the scope makes Patroni refuse to start)", c.ClusterName)
	}
	return nil
}

func (c *Config) validate() error {
	if c.ClusterName == "" {
		return fmt.Errorf("config: cluster_name is required")
	}
	if c.Upgrade.SlotName == "" {
		return fmt.Errorf("config: upgrade.slot_name is required")
	}
	if c.Upgrade.PublicationName == "" {
		return fmt.Errorf("config: upgrade.publication_name is required")
	}
	if c.Upgrade.NewPGBindir == "" {
		return fmt.Errorf("config: upgrade.new_pg_bindir is required")
	}
	if c.PG.SuperuserDSN == "" {
		return fmt.Errorf("config: pg.superuser_dsn is required")
	}
	return nil
}
