// Package loadcfg holds loadtool's configuration: defaults, YAML loading, and
// validation. Flags are layered on top of these structs by cmd/loadtool.
package loadcfg

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration so YAML can express it as a string ("30s", "5m")
// rather than a raw nanosecond count. Integers are still accepted as ns.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	// yaml.v3 coerces integer nodes to string on Decode(&s), so check the tag
	// to route integer YAML values (nanoseconds) before attempting string parse.
	if value.Tag == "!!int" {
		var n int64
		if err := value.Decode(&n); err != nil {
			return fmt.Errorf(`loadcfg: duration must be a string like "30s" or an integer ns: %w`, err)
		}
		*d = Duration(n)
		return nil
	}
	var s string
	if err := value.Decode(&s); err == nil {
		parsed, perr := time.ParseDuration(s)
		if perr != nil {
			return fmt.Errorf("loadcfg: invalid duration %q: %w", s, perr)
		}
		*d = Duration(parsed)
		return nil
	}
	return fmt.Errorf(`loadcfg: duration must be a string like "30s" or an integer ns`)
}

type WorkerSet struct {
	Count int `yaml:"count"`
	Rate  int `yaml:"rate"` // ops/sec/worker; 0 = unthrottled
}

type LongTxnSet struct {
	Count       int      `yaml:"count"`
	TxnDuration Duration `yaml:"txn_duration"`
	BatchSize   int      `yaml:"batch_size"`
}

type Workers struct {
	Append   WorkerSet  `yaml:"append"`
	LongTxn  LongTxnSet `yaml:"long_txn"`
	Transfer WorkerSet  `yaml:"transfer"`
	RYW      WorkerSet  `yaml:"ryw"`
}

type Config struct {
	DSNA      string   `yaml:"dsn_a"`
	DSNB      string   `yaml:"dsn_b"`
	IntentLog string   `yaml:"intent_log"`
	Accounts  int      `yaml:"accounts"`
	Balance   int64    `yaml:"balance"`
	Duration  Duration `yaml:"duration"`
	Workers   Workers  `yaml:"workers"`
}

// Default returns a Config with sensible defaults already applied.
func Default() Config {
	return Config{
		IntentLog: "loadtool-intent.jsonl",
		Accounts:  100,
		Balance:   1000,
		Workers: Workers{
			Append:   WorkerSet{Count: 4, Rate: 0},
			LongTxn:  LongTxnSet{Count: 1, TxnDuration: Duration(5 * time.Second), BatchSize: 10},
			Transfer: WorkerSet{Count: 2, Rate: 0},
			RYW:      WorkerSet{Count: 1, Rate: 0},
		},
	}
}

// Load reads YAML from path on top of Default(); unset YAML fields keep defaults.
func Load(path string) (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("loadcfg read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("loadcfg parse %s: %w", path, err)
	}
	return cfg, nil
}

// ValidateRun checks the fields required by the `run` subcommand.
// It is a separate exported method rather than being called inside Load because
// only the `run` subcommand requires DSNs — `init` and `verify` deliberately do not.
// It does NOT require Duration > 0: a zero Duration means "run until SIGINT"
// and is a valid, intended configuration.
func (c Config) ValidateRun() error {
	if c.DSNA == "" {
		return fmt.Errorf("loadcfg: dsn_a is required")
	}
	if c.DSNB == "" {
		return fmt.Errorf("loadcfg: dsn_b is required")
	}
	if c.IntentLog == "" {
		return fmt.Errorf("loadcfg: intent_log is required")
	}
	return nil
}
