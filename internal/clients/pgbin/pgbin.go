// Package pgbin wraps the PostgreSQL command-line tools (pg_ctl, pg_upgrade,
// pg_controldata) that the upgrade phase shells out to. The exec wrappers are
// thin; only the pg_controldata parser carries logic worth unit-testing.
package pgbin

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
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
	// WorkDir, when set, is the working directory pg_upgrade runs in (where it
	// writes pg_upgrade_output.d / log files). It must be writable by OSUser.
	WorkDir string
}

// PGTools is the seam the upgrade steps depend on, so their idempotency checks
// (which read pg_controldata) are unit-testable with a fake.
type PGTools interface {
	// OldControlData reads pg_controldata using OldBindir's binary (the
	// pre-upgrade PG10 cluster); NewControlData uses NewBindir (post-upgrade PG17).
	OldControlData(ctx context.Context, dataDir string) (*ControlData, error)
	NewControlData(ctx context.Context, dataDir string) (*ControlData, error)
	// InitDB initializes a new-version data directory (bindir's initdb), passing
	// opts (initdb flags) so the new cluster matches the old one's settings.
	InitDB(ctx context.Context, bindir, dataDir string, opts []string) error
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
	// OSUser is the OS account that owns the PostgreSQL data directory. The PG
	// binaries (pg_ctl, pg_upgrade, pg_controldata) refuse to run as root, so when
	// the orchestrator runs as root we drop privileges to this user. Empty leaves
	// commands running as the current user (correct when already running as it).
	OSUser string
}

func (e Exec) bin(dir, name string) string { return filepath.Join(dir, name) }

// command builds an exec.Cmd, dropping privileges to OSUser when the orchestrator
// runs as root. The PG tools abort with "cannot be run as root", so this is how
// they end up running as postgres.
func (e Exec) command(ctx context.Context, name string, args ...string) (*exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	u, drop, err := e.targetUser()
	if err != nil {
		return nil, err
	}
	if !drop {
		return cmd, nil // nothing to switch, or not privileged to switch
	}
	cred, err := credential(u)
	if err != nil {
		return nil, err
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: cred}
	// PG tools consult HOME/USER (e.g. for .pgpass); point them at the target user.
	cmd.Env = append(os.Environ(), "HOME="+u.HomeDir, "USER="+e.OSUser, "LOGNAME="+e.OSUser)
	return cmd, nil
}

// targetUser resolves the OS user that PG tools should run as. drop is true only
// when a privilege drop is both requested (OSUser set) and possible (we are
// root); otherwise commands run as the current user.
func (e Exec) targetUser() (u *user.User, drop bool, err error) {
	if e.OSUser == "" || os.Geteuid() != 0 {
		return nil, false, nil
	}
	u, err = user.Lookup(e.OSUser)
	if err != nil {
		return nil, false, fmt.Errorf("pgbin: lookup os_user %q: %w", e.OSUser, err)
	}
	return u, true, nil
}

// ensureDir creates dir with the given perm and, when dropping privileges,
// chowns it to the target user so a PG tool running as that user can write there.
func (e Exec) ensureDir(dir string, perm os.FileMode) error {
	if err := os.MkdirAll(dir, perm); err != nil {
		return fmt.Errorf("pgbin: create dir %s: %w", dir, err)
	}
	u, drop, err := e.targetUser()
	if err != nil {
		return err
	}
	if !drop {
		return nil // running as the current user: it already owns what it created
	}
	cred, err := credential(u)
	if err != nil {
		return err
	}
	if err := os.Chown(dir, int(cred.Uid), int(cred.Gid)); err != nil {
		return fmt.Errorf("pgbin: chown dir %s to %s: %w", dir, e.OSUser, err)
	}
	return nil
}

// credential converts a resolved OS user into a syscall credential (uid/gid).
func credential(u *user.User) (*syscall.Credential, error) {
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return nil, fmt.Errorf("pgbin: parse uid %q: %w", u.Uid, err)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		return nil, fmt.Errorf("pgbin: parse gid %q: %w", u.Gid, err)
	}
	return &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}, nil
}

// OldControlData reads pg_controldata with the OLD bindir (pre-upgrade cluster).
func (e Exec) OldControlData(ctx context.Context, dataDir string) (*ControlData, error) {
	return e.controlData(ctx, e.OldBindir, dataDir)
}

// NewControlData reads pg_controldata with the NEW bindir (post-upgrade cluster).
func (e Exec) NewControlData(ctx context.Context, dataDir string) (*ControlData, error) {
	return e.controlData(ctx, e.NewBindir, dataDir)
}

func (e Exec) controlData(ctx context.Context, bindir, dataDir string) (*ControlData, error) {
	cmd, err := e.command(ctx, e.bin(bindir, "pg_controldata"), "-D", dataDir)
	if err != nil {
		return nil, err
	}
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("pgbin: pg_controldata: %w", err)
	}
	return parseControlData(string(out)), nil
}

func (e Exec) Promote(ctx context.Context, dataDir string) error {
	cmd, err := e.command(ctx, e.bin(e.OldBindir, "pg_ctl"), "promote", "-w", "-D", dataDir)
	if err != nil {
		return err
	}
	return run(cmd, "promote")
}

func (e Exec) StopClean(ctx context.Context, dataDir string) error {
	cmd, err := e.command(ctx, e.bin(e.OldBindir, "pg_ctl"), "stop", "-m", "fast", "-D", dataDir)
	if err != nil {
		return err
	}
	return run(cmd, "stop")
}

// startupLog is the file pg_ctl redirects the postmaster's stdout/stderr to via
// -l. Without it the daemonized server inherits and holds open our captured
// stdout pipe, so run()'s CombinedOutput would block forever after the server
// starts (even on a successful start). It lives in the data dir, which OSUser
// owns and can write.
func startupLog(dataDir string) string { return filepath.Join(dataDir, "pg_ctl_startup.log") }

// Restart stops (fast) and starts the cluster, waiting for readiness. Used to
// apply a primary_conninfo change on PG < 13 where it is not reloadable.
func (e Exec) Restart(ctx context.Context, dataDir string) error {
	cmd, err := e.command(ctx, e.bin(e.OldBindir, "pg_ctl"), "restart", "-m", "fast", "-w", "-D", dataDir, "-l", startupLog(dataDir))
	if err != nil {
		return err
	}
	return run(cmd, "restart")
}

// Start launches a stopped cluster with the given bindir's pg_ctl, waiting for
// readiness. Used to bring PG17 up after pg_upgrade for the catchup subscription.
func (e Exec) Start(ctx context.Context, bindir, dataDir string) error {
	cmd, err := e.command(ctx, e.bin(bindir, "pg_ctl"), "start", "-w", "-D", dataDir, "-l", startupLog(dataDir))
	if err != nil {
		return err
	}
	return run(cmd, "start")
}

func (e Exec) UpgradeCheck(ctx context.Context, o UpgradeOptions) error {
	cmd, err := e.upgradeCmd(ctx, o, true)
	if err != nil {
		return err
	}
	return run(cmd, "pg_upgrade --check")
}

func (e Exec) Upgrade(ctx context.Context, o UpgradeOptions) error {
	cmd, err := e.upgradeCmd(ctx, o, false)
	if err != nil {
		return err
	}
	return run(cmd, "pg_upgrade --link")
}

func (e Exec) upgradeCmd(ctx context.Context, o UpgradeOptions, check bool) (*exec.Cmd, error) {
	args := []string{
		"--old-bindir", o.OldBindir, "--new-bindir", o.NewBindir,
		"--old-datadir", o.OldDataDir, "--new-datadir", o.NewDataDir,
		"--link",
	}
	if check {
		args = append(args, "--check")
	}
	cmd, err := e.command(ctx, e.bin(o.NewBindir, "pg_upgrade"), args...)
	if err != nil {
		return nil, err
	}
	// pg_upgrade writes its output (pg_upgrade_output.d / log files) to the working
	// directory; create it (owned by the target user) and run from there.
	if o.WorkDir != "" {
		if err := e.ensureDir(o.WorkDir, 0o750); err != nil {
			return nil, err
		}
		cmd.Dir = o.WorkDir
	}
	return cmd, nil
}

// InitDB initializes a fresh data directory for the new cluster (required before
// pg_upgrade). opts are extra initdb flags (e.g. --data-checksums, --encoding=…)
// derived from the new cluster's Patroni bootstrap.initdb so the new cluster
// matches what pg_upgrade and Patroni expect. The data dir is created owned by
// OSUser so initdb, running as that user, can populate it.
func (e Exec) InitDB(ctx context.Context, bindir, dataDir string, opts []string) error {
	if err := e.ensureDir(dataDir, 0o700); err != nil {
		return err
	}
	args := append([]string{"-D", dataDir}, opts...)
	cmd, err := e.command(ctx, e.bin(bindir, "initdb"), args...)
	if err != nil {
		return err
	}
	return run(cmd, "initdb")
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
