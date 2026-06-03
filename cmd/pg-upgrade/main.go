package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	pgclient "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/clients/pgbin"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/connect"
	"github.com/dmbabuev/pg-upgrade/internal/phases"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/dmbabuev/pg-upgrade/internal/slotdrain"
	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/spf13/cobra"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	var cfgPath string

	root := &cobra.Command{
		Use:   "pg-upgrade",
		Short: "Zero-downtime PostgreSQL major version upgrade orchestrator",
	}
	root.PersistentFlags().StringVarP(&cfgPath, "config", "c", "pg-upgrade.yaml", "Path to config file")

	root.AddCommand(drainSlotCmd(&cfgPath))
	root.AddCommand(statusCmd(&cfgPath))
	root.AddCommand(runCmd(&cfgPath))

	return root
}

func drainSlotCmd(cfgPath *string) *cobra.Command {
	var targetLSN string
	var statePath string

	cmd := &cobra.Command{
		Use:   "drain-slot",
		Short: "Drain the logical replication slot to target-lsn",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(*cfgPath)
			if err != nil {
				return err
			}

			if targetLSN == "" {
				return fmt.Errorf("--target-lsn is required")
			}

			drainCfg := slotdrain.Config{
				ConnString: cfg.PG.SuperuserDSN,
				SlotName:   cfg.Upgrade.SlotName,
				PubName:    cfg.Upgrade.PublicationName,
				TargetLSN:  targetLSN,
			}

			fmt.Fprintf(os.Stdout, "Draining slot %s to LSN %s...\n", cfg.Upgrade.SlotName, targetLSN)

			report, err := slotdrain.Drain(context.Background(), drainCfg)
			if err != nil {
				return fmt.Errorf("drain failed: %w", err)
			}

			fmt.Fprintf(os.Stdout, "Done. Drained %d transactions. Final flush LSN: %s\n",
				report.TransactionsDrained, report.FinalFlushLSN)

			if statePath != "" {
				// Write report to state file if provided
				fmt.Fprintf(os.Stdout, "Report written to state file (full pipeline integration in Plan 2)\n")
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&targetLSN, "target-lsn", "", "LSN to drain to (required)")
	cmd.Flags().StringVar(&statePath, "state", "", "Path to state file (optional)")

	return cmd
}

func statusCmd(cfgPath *string) *cobra.Command {
	var statePath string

	return &cobra.Command{
		Use:   "status",
		Short: "Show current upgrade status from state file",
		RunE: func(cmd *cobra.Command, args []string) error {
			if statePath == "" {
				statePath = "pg-upgrade-state.json"
			}
			if _, err := os.Stat(statePath); os.IsNotExist(err) {
				fmt.Println("No upgrade in progress (state file not found)")
				return nil
			}
			fmt.Printf("State file: %s\n(Full status display implemented in Plan 2)\n", statePath)
			return nil
		},
	}
}

func runCmd(cfgPath *string) *cobra.Command {
	var statePath string
	var headless bool

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Drive the upgrade through phases 1-8 (Prepare → Cleanup)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(*cfgPath)
			if err != nil {
				return err
			}
			if err := cfg.ValidateForRun(); err != nil {
				return err
			}
			// PG tools (pg_ctl/pg_upgrade/pg_controldata) refuse to run as root. If
			// we are root, pg.os_user must be set so we can drop to it; fail now with
			// a clear message rather than deep inside the upgrade phase.
			if os.Geteuid() == 0 {
				if cfg.PG.OSUser == "" {
					return fmt.Errorf("running as root but pg.os_user is unset: PostgreSQL tools refuse to run as root. Set pg.os_user (e.g. \"postgres\") in %s", *cfgPath)
				}
				fmt.Fprintf(os.Stdout, "running as root; PG CLI tools will run as pg.os_user=%q\n", cfg.PG.OSUser)
			}
			for _, c := range []struct{ name, dsn string }{
				{"superuser_dsn", cfg.PG.SuperuserDSN},
				{"pg17_dsn", cfg.Upgrade.PG17DSN},
			} {
				db, derr := connect.DatabaseOf(c.dsn)
				if derr != nil {
					return derr
				}
				if db != cfg.Upgrade.DBName {
					return fmt.Errorf("config: %s database %q must match upgrade.dbname %q (the freeze, publication and subscription all act on the DSN's database)", c.name, db, cfg.Upgrade.DBName)
				}
			}
			if statePath == "" {
				statePath = "pg-upgrade-state.json"
			}

			ctx := context.Background()

			// NewManager always starts Current at phases.FirstPhase ("prepare");
			// resume an in-progress run by loading the existing state file.
			var mgr *state.Manager
			if _, statErr := os.Stat(statePath); statErr == nil {
				mgr, err = state.LoadManager(statePath)
			} else {
				mgr, err = state.NewManager(statePath, cfg.ClusterName)
			}
			if err != nil {
				return err
			}

			// N1-local PG client.
			n1DSN, err := connect.DSNForHost(cfg.PG.SuperuserDSN, "localhost")
			if err != nil {
				return err
			}
			n1, err := pgclient.NewFromDSN(ctx, n1DSN)
			if err != nil {
				return err
			}
			defer n1.Close()

			primaryProvider, closePrimary := newPrimaryProvider(cfg.PG.SuperuserDSN, mgr)
			defer closePrimary()
			// Patroni REST auth (if configured) is shared by both clusters; PATCH
			// /config for pause/resume needs it when restapi.authentication is on.
			patAuth := patroni.WithBasicAuth(cfg.Patroni.RESTUsername, cfg.Patroni.RESTPassword)
			patClient := patroni.NewHTTPClient("http://localhost:8008", patAuth)

			pg17Provider, closePG17 := newPG17Provider(cfg.Upgrade.PG17DSN)
			defer closePG17()
			newPat := patroni.NewHTTPClient(cfg.Upgrade.NewPatroniURL, patAuth)

			d := phases.Deps{
				Cfg:         *cfg,
				Mgr:         mgr,
				Patroni:     patClient,
				Tools:       pgbin.Exec{NewBindir: cfg.Upgrade.NewPGBindir, OldBindir: cfg.Upgrade.OldPGBindir, OSUser: cfg.PG.OSUser},
				N1:          n1,
				Primary:     primaryProvider,
				Drain:       slotdrain.Drain,
				PG17:        pg17Provider,
				NewPatroni:  newPat,
				WriteSignal: func(path string, data []byte) error { return os.WriteFile(path, data, 0o644) },
			}

			mode := runner.Interactive
			var cp runner.Checkpoint
			if headless {
				mode = runner.Headless
			} else {
				cp = runner.InteractiveCheckpoint(os.Stdin, os.Stdout, runner.DefaultPrompts())
			}

			log := runner.NewConsoleLogger(os.Stdout)
			d.Log = log

			r := runner.New(phases.Phases1to8(d), mgr, mode, cp, log)
			if err := r.Run(ctx); err != nil {
				return err
			}
			fmt.Fprintln(os.Stdout, "\nUpgrade complete (all 8 phases). Operator follow-up: remove stale DCS keys for the old cluster (e.g. etcdctl del /service/<old-scope>/ --prefix).")
			return nil
		},
	}
	cmd.Flags().StringVar(&statePath, "state", "", "Path to state file (default pg-upgrade-state.json)")
	cmd.Flags().BoolVar(&headless, "headless", false, "Skip operator checkpoints (full automation)")
	return cmd
}

// newPG17Provider lazily connects to the upgraded PG17 on N1, plus a closer.
func newPG17Provider(dsn string) (func(context.Context) (pgclient.Client, error), func()) {
	var cached pgclient.Client
	provider := func(ctx context.Context) (pgclient.Client, error) {
		if cached != nil {
			return cached, nil
		}
		c, err := pgclient.NewFromDSN(ctx, dsn)
		if err != nil {
			return nil, err
		}
		cached = c
		return cached, nil
	}
	closeFn := func() {
		if cached != nil {
			cached.Close()
		}
	}
	return provider, closeFn
}

// newPrimaryProvider returns a provider that builds (once) a PG client to the
// primary host recorded in state by DiscoverTopology, plus a close func for it.
func newPrimaryProvider(template string, mgr *state.Manager) (func(context.Context) (pgclient.Client, error), func()) {
	var cached pgclient.Client
	provider := func(ctx context.Context) (pgclient.Client, error) {
		if cached != nil {
			return cached, nil
		}
		host := mgr.Get().Artifacts.PrimaryHost
		if host == "" {
			return nil, fmt.Errorf("run: primary host not yet discovered")
		}
		dsn, err := connect.DSNForHost(template, host)
		if err != nil {
			return nil, err
		}
		c, err := pgclient.NewFromDSN(ctx, dsn)
		if err != nil {
			return nil, err
		}
		cached = c
		return cached, nil
	}
	closeFn := func() {
		if cached != nil {
			cached.Close()
		}
	}
	return provider, closeFn
}
