// Package pgbin wraps the PostgreSQL command-line tools (pg_ctl, pg_upgrade,
// pg_controldata) that the upgrade phase shells out to. The exec wrappers are
// thin; only the pg_controldata parser carries logic worth unit-testing.
package pgbin

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

// ControlData is the subset of pg_controldata output the orchestrator needs.
type ControlData struct {
	State    string // e.g. "in production", "shut down"
	SystemID string // database system identifier
}

// UpgradeOptions are the paths pg_upgrade needs.
type UpgradeOptions struct {
	OldBindir  string
	NewBindir  string
	OldDataDir string
	NewDataDir string
}

// PGTools is the seam the upgrade steps depend on, so their idempotency checks
// (which read pg_controldata) are unit-testable with a fake.
type PGTools interface {
	// OldControlData reads pg_controldata using OldBindir's binary (the
	// pre-upgrade PG10 cluster); NewControlData uses NewBindir (post-upgrade PG17).
	OldControlData(ctx context.Context, dataDir string) (*ControlData, error)
	NewControlData(ctx context.Context, dataDir string) (*ControlData, error)
	Promote(ctx context.Context, dataDir string) error
	StopClean(ctx context.Context, dataDir string) error
	Restart(ctx context.Context, dataDir string) error
	UpgradeCheck(ctx context.Context, o UpgradeOptions) error
	Upgrade(ctx context.Context, o UpgradeOptions) error
	Start(ctx context.Context, bindir, dataDir string) error
}

// Exec is the real PGTools, invoking binaries under the given bindirs.
type Exec struct {
	NewBindir string
	OldBindir string
}

func (e Exec) bin(dir, name string) string { return filepath.Join(dir, name) }

// OldControlData reads pg_controldata with the OLD bindir (pre-upgrade cluster).
func (e Exec) OldControlData(ctx context.Context, dataDir string) (*ControlData, error) {
	return e.controlData(ctx, e.OldBindir, dataDir)
}

// NewControlData reads pg_controldata with the NEW bindir (post-upgrade cluster).
func (e Exec) NewControlData(ctx context.Context, dataDir string) (*ControlData, error) {
	return e.controlData(ctx, e.NewBindir, dataDir)
}

func (e Exec) controlData(ctx context.Context, bindir, dataDir string) (*ControlData, error) {
	out, err := exec.CommandContext(ctx, e.bin(bindir, "pg_controldata"), "-D", dataDir).Output()
	if err != nil {
		return nil, fmt.Errorf("pgbin: pg_controldata: %w", err)
	}
	return parseControlData(string(out)), nil
}

func (e Exec) Promote(ctx context.Context, dataDir string) error {
	return run(exec.CommandContext(ctx, e.bin(e.OldBindir, "pg_ctl"), "promote", "-w", "-D", dataDir), "promote")
}

func (e Exec) StopClean(ctx context.Context, dataDir string) error {
	return run(exec.CommandContext(ctx, e.bin(e.OldBindir, "pg_ctl"), "stop", "-m", "fast", "-D", dataDir), "stop")
}

// Restart stops (fast) and starts the cluster, waiting for readiness. Used to
// apply a primary_conninfo change on PG < 13 where it is not reloadable.
func (e Exec) Restart(ctx context.Context, dataDir string) error {
	return run(exec.CommandContext(ctx, e.bin(e.OldBindir, "pg_ctl"), "restart", "-m", "fast", "-w", "-D", dataDir), "restart")
}

// Start launches a stopped cluster with the given bindir's pg_ctl, waiting for
// readiness. Used to bring PG17 up after pg_upgrade for the catchup subscription.
func (e Exec) Start(ctx context.Context, bindir, dataDir string) error {
	return run(exec.CommandContext(ctx, e.bin(bindir, "pg_ctl"), "start", "-w", "-D", dataDir), "start")
}

func (e Exec) UpgradeCheck(ctx context.Context, o UpgradeOptions) error {
	return run(e.upgradeCmd(ctx, o, true), "pg_upgrade --check")
}

func (e Exec) Upgrade(ctx context.Context, o UpgradeOptions) error {
	return run(e.upgradeCmd(ctx, o, false), "pg_upgrade --link")
}

func (e Exec) upgradeCmd(ctx context.Context, o UpgradeOptions, check bool) *exec.Cmd {
	args := []string{
		"--old-bindir", o.OldBindir, "--new-bindir", o.NewBindir,
		"--old-datadir", o.OldDataDir, "--new-datadir", o.NewDataDir,
		"--link",
	}
	if check {
		args = append(args, "--check")
	}
	return exec.CommandContext(ctx, e.bin(o.NewBindir, "pg_upgrade"), args...)
}

func run(cmd *exec.Cmd, label string) error {
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pgbin: %s: %w: %s", label, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// parseControlData extracts the fields the orchestrator needs from
// pg_controldata's "Label:   value" lines.
func parseControlData(out string) *ControlData {
	cd := &ControlData{}
	for _, line := range strings.Split(out, "\n") {
		idx := strings.Index(line, ":")
		if idx < 0 {
			continue
		}
		label := strings.TrimSpace(line[:idx])
		value := strings.TrimSpace(line[idx+1:])
		switch label {
		case "Database cluster state":
			cd.State = value
		case "Database system identifier":
			cd.SystemID = value
		}
	}
	return cd
}

// compile-time assertion that Exec satisfies PGTools.
var _ PGTools = Exec{}
