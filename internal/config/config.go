package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ClusterName string        `yaml:"cluster_name"`
	Upgrade     UpgradeConfig `yaml:"upgrade"`
	PG          PGConfig      `yaml:"pg"`
}

type UpgradeConfig struct {
	TargetNode        string `yaml:"target_node"`
	SlotName          string `yaml:"slot_name"`
	PublicationName   string `yaml:"publication_name"`
	NewPGBindir       string `yaml:"new_pg_bindir"`
	OldPGBindir       string `yaml:"old_pg_bindir"`
	DataDir           string `yaml:"data_dir"`
	PatroniConfigPath string `yaml:"patroni_config_path"`
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
