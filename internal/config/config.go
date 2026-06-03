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
}

type PGConfig struct {
	SuperuserDSN string `yaml:"superuser_dsn"`
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
