package phases

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

// --- InitNewDataDir: initdb the new-version data directory before pg_upgrade ---
//
// pg_upgrade requires an already-initialized (empty) new cluster. We run initdb
// with the options declared in the new cluster's Patroni bootstrap.initdb so the
// new cluster's encoding/locale/checksums match the old one (a mismatch makes
// pg_upgrade --check fail) and match what Patroni expects when it later adopts
// the cluster.
type initNewDataDir struct{ d Deps }

func (s *initNewDataDir) ID() runner.StepID { return "InitNewDataDir" }

func (s *initNewDataDir) Check(context.Context) (bool, error) {
	// A PG_VERSION file means the data dir is already initialized (idempotent
	// resume, or the operator initialized it themselves).
	if _, err := os.Stat(filepath.Join(s.d.Cfg.Upgrade.NewDataDir, "PG_VERSION")); err == nil {
		return true, nil
	}
	return false, nil
}

func (s *initNewDataDir) Run(ctx context.Context) error {
	cfgPath := s.d.Cfg.Upgrade.PatroniInitdbConfig
	if cfgPath == "" {
		return fmt.Errorf("upgrade: new_data_dir %s is not initialized and upgrade.patroni_initdb_config is unset; cannot run initdb", s.d.Cfg.Upgrade.NewDataDir)
	}
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return fmt.Errorf("upgrade: read patroni_initdb_config: %w", err)
	}
	opts, err := parseInitdbOptions(data)
	if err != nil {
		return err
	}
	s.d.logf("initdb новой data dir %s (опции из %s bootstrap.initdb: %v)...", s.d.Cfg.Upgrade.NewDataDir, cfgPath, opts)
	if err := s.d.Tools.InitDB(ctx, s.d.Cfg.Upgrade.NewPGBindir, s.d.Cfg.Upgrade.NewDataDir, opts); err != nil {
		return err
	}
	s.d.logf("новая data dir инициализирована")
	return nil
}

// parsePatroniScopeDataDir extracts the scope and postgresql.data_dir from a
// Patroni config. Either may be empty if set via environment variables instead
// of the file; callers must tolerate that.
func parsePatroniScopeDataDir(patroniYAML []byte) (scope, dataDir string, err error) {
	var doc struct {
		Scope      string `yaml:"scope"`
		Postgresql struct {
			DataDir string `yaml:"data_dir"`
		} `yaml:"postgresql"`
	}
	if err := yaml.Unmarshal(patroniYAML, &doc); err != nil {
		return "", "", fmt.Errorf("catchup: parse patroni config: %w", err)
	}
	return doc.Scope, doc.Postgresql.DataDir, nil
}

// parseInitdbOptions turns a Patroni config's bootstrap.initdb list into initdb
// command-line flags. Patroni encodes each entry either as a bare string (a flag
// like "data-checksums") or a single-key mapping (an option like "encoding: UTF8").
func parseInitdbOptions(patroniYAML []byte) ([]string, error) {
	var doc struct {
		Bootstrap struct {
			Initdb []any `yaml:"initdb"`
		} `yaml:"bootstrap"`
	}
	if err := yaml.Unmarshal(patroniYAML, &doc); err != nil {
		return nil, fmt.Errorf("upgrade: parse patroni bootstrap.initdb: %w", err)
	}
	var args []string
	for _, item := range doc.Bootstrap.Initdb {
		switch v := item.(type) {
		case string:
			args = append(args, "--"+v)
		case map[string]any:
			for k, val := range v {
				args = append(args, fmt.Sprintf("--%s=%v", k, val))
			}
		default:
			return nil, fmt.Errorf("upgrade: unsupported bootstrap.initdb entry %v (%T)", item, item)
		}
	}
	return args, nil
}

var _ runner.Step = (*initNewDataDir)(nil)
