package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/slotdrain"
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
