// Command loadtool drives a verifiable workload against a Patroni cluster during
// an online upgrade, simulates the in-flight DSN swap (SIGHUP), and renders a
// consistency verdict from a durable intent-log.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/dmbabuev/pg-upgrade/internal/loadcfg"
	"github.com/dmbabuev/pg-upgrade/internal/loadgen"
	"github.com/dmbabuev/pg-upgrade/internal/oracle"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "loadtool",
		Short:         "Load + correctness harness for pg-upgrade",
		SilenceErrors: true, // main is the single error reporter
		SilenceUsage:  true, // don't dump usage on a runtime error
	}
	root.AddCommand(initCmd(), runCmd(), verifyCmd())
	return root
}

// loadConfig layers an optional YAML file under the defaults.
func loadConfig(path string) (loadcfg.Config, error) {
	if path == "" {
		return loadcfg.Default(), nil
	}
	return loadcfg.Load(path)
}

// openPool adapts pgxpool to loadgen.Pool and returns a closer.
func openPool(ctx context.Context, dsn string) (loadgen.Pool, func(), error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	return pool, pool.Close, nil
}

func initCmd() *cobra.Command {
	var cfgPath, dsnA string
	var accounts int
	var balance int64
	var reset bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create schema and seed accounts",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(cfgPath)
			if err != nil {
				return err
			}
			if dsnA != "" {
				cfg.DSNA = dsnA
			}
			if accounts > 0 {
				cfg.Accounts = accounts
			}
			if balance > 0 {
				cfg.Balance = balance
			}
			if cfg.DSNA == "" {
				return fmt.Errorf("init: --dsn-a (or config dsn_a) is required")
			}
			ctx := cmd.Context()
			pool, closeFn, err := openPool(ctx, cfg.DSNA)
			if err != nil {
				return err
			}
			defer closeFn()
			if err := loadgen.InitSchema(ctx, pool, cfg.Accounts, cfg.Balance, reset); err != nil {
				return err
			}
			fmt.Printf("init: %d accounts seeded with balance %d\n", cfg.Accounts, cfg.Balance)
			if reset {
				// The reset TRUNCATEd the DB; clear the append-only intent-log too,
				// else verify reconciles prior runs' records against the wiped DB and
				// reports false LOST/PHANTOM.
				if err := loadgen.TruncateIntentLog(cfg.IntentLog); err != nil {
					return err
				}
				fmt.Printf("init: intent-log %s truncated\n", cfg.IntentLog)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "YAML config path")
	cmd.Flags().StringVar(&dsnA, "dsn-a", "", "old primary DSN")
	cmd.Flags().IntVar(&accounts, "accounts", 0, "number of accounts (K)")
	cmd.Flags().Int64Var(&balance, "balance", 0, "per-account balance (B)")
	cmd.Flags().BoolVar(&reset, "reset", false, "TRUNCATE before seeding")
	return cmd
}

func runCmd() *cobra.Command {
	var cfgPath, dsnA, dsnB, intentLog string
	var duration time.Duration
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Generate load; SIGHUP swaps DSN A->B",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(cfgPath)
			if err != nil {
				return err
			}
			if dsnA != "" {
				cfg.DSNA = dsnA
			}
			if dsnB != "" {
				cfg.DSNB = dsnB
			}
			if intentLog != "" {
				cfg.IntentLog = intentLog
			}
			if duration > 0 {
				cfg.Duration = loadcfg.Duration(duration)
			}
			if err := cfg.ValidateRun(); err != nil {
				return err
			}
			m, err := loadgen.Run(cmd.Context(), cfg, openPool)
			if err != nil {
				return err
			}
			commits, errs := m.Snapshot()
			fmt.Printf("run complete. commits=%v errors=%v\n", commits, errs)
			if _, _, dur, ok := m.Window(); ok {
				fmt.Printf("unavailability window: %s\n", dur)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "YAML config path")
	cmd.Flags().StringVar(&dsnA, "dsn-a", "", "old primary DSN")
	cmd.Flags().StringVar(&dsnB, "dsn-b", "", "new primary DSN")
	cmd.Flags().StringVar(&intentLog, "intent-log", "", "intent-log path")
	cmd.Flags().DurationVar(&duration, "duration", 0, "run duration (0 = until SIGINT)")
	return cmd
}

func verifyCmd() *cobra.Command {
	var cfgPath, dsnB, intentLog string
	var balance int64
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Reconcile intent-log against DB and print verdict",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(cfgPath)
			if err != nil {
				return err
			}
			if dsnB != "" {
				cfg.DSNB = dsnB
			}
			if intentLog != "" {
				cfg.IntentLog = intentLog
			}
			if balance > 0 {
				cfg.Balance = balance
			}
			if cfg.DSNB == "" {
				return fmt.Errorf("verify: --dsn-b (or config dsn_b) is required")
			}
			ops, err := oracle.ReadLog(cfg.IntentLog)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			// verify needs only oracle.Querier (read-only), which *pgxpool.Pool
			// satisfies directly, so it doesn't go through openPool.
			pool, err := pgxpool.New(ctx, cfg.DSNB)
			if err != nil {
				return err
			}
			defer pool.Close()
			f, err := oracle.Reconcile(ctx, pool, ops, cfg.Balance)
			if err != nil {
				return err
			}
			if err := oracle.Render(os.Stdout, f); err != nil {
				return err
			}
			// os.Exit skips the deferred pool.Close above; that's fine, the
			// process is exiting and the OS reclaims the connections.
			if f.Failed() {
				os.Exit(2)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "YAML config path")
	cmd.Flags().StringVar(&dsnB, "dsn-b", "", "new primary DSN")
	cmd.Flags().StringVar(&intentLog, "intent-log", "", "intent-log path")
	cmd.Flags().Int64Var(&balance, "balance", 0, "per-account balance (B), must match init")
	return cmd
}
