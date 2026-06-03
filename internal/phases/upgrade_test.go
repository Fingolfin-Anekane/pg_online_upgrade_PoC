package phases

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/clients/pgbin"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Checkpoint on the shared fakePG (used by ShutdownN1Clean).
func (f *fakePG) Checkpoint(context.Context) error { f.checkpoints++; return nil }

// fakeTools implements pgbin.PGTools. OldControlData reports the pre-upgrade
// cluster state; NewControlData reports the post-upgrade sysid.
type fakeTools struct {
	oldState       string // State returned by OldControlData (pre-upgrade)
	sysID          string // SystemID returned by NewControlData (post-upgrade)
	promoted       bool
	stopped        bool
	checked        bool
	upgraded       bool
	restarted      bool
	started        bool
	initted        bool
	initOpts       []string
	running        bool   // IsRunning for newDataDir (the new cluster)
	oldRunning     bool   // IsRunning for any other datadir (the old cluster)
	newDataDir     string // which dataDir counts as "new" in IsRunning
	patroniStarted string // command passed to StartPatroni
	onPromote      func()
}

func (f *fakeTools) OldControlData(context.Context, string) (*pgbin.ControlData, error) {
	return &pgbin.ControlData{State: f.oldState}, nil
}
func (f *fakeTools) NewControlData(context.Context, string) (*pgbin.ControlData, error) {
	return &pgbin.ControlData{State: "in production", SystemID: f.sysID}, nil
}
func (f *fakeTools) InitDB(_ context.Context, _, _ string, opts []string) error {
	f.initted = true
	f.initOpts = opts
	return nil
}
func (f *fakeTools) IsRunning(_ context.Context, _, dataDir string) (bool, error) {
	if f.newDataDir != "" && dataDir == f.newDataDir {
		return f.running, nil
	}
	return f.oldRunning, nil
}
func (f *fakeTools) StartPatroni(_ context.Context, command string) error {
	f.patroniStarted = command
	f.running = true // Patroni brings PG17 up
	return nil
}
func (f *fakeTools) Promote(context.Context, string) error {
	if f.onPromote != nil {
		f.onPromote()
	}
	f.promoted = true
	f.oldState = "in production" // promotion updates the control file
	return nil
}
func (f *fakeTools) StopClean(context.Context, string) error {
	f.stopped = true
	f.oldState = "shut down" // a clean stop of the promoted primary
	return nil
}
func (f *fakeTools) UpgradeCheck(context.Context, pgbin.UpgradeOptions) error {
	f.checked = true
	return nil
}
func (f *fakeTools) Upgrade(context.Context, pgbin.UpgradeOptions) error {
	f.upgraded = true
	return nil
}
func (f *fakeTools) Restart(context.Context, string) error       { f.restarted = true; return nil }
func (f *fakeTools) Start(context.Context, string, string) error { f.started = true; return nil }

func TestUpgradeHappyPath(t *testing.T) {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade"} {
		require.NoError(t, mgr.Advance(p))
	}
	dir := t.TempDir()
	initdbCfg := filepath.Join(dir, "patroni-source.yml")
	require.NoError(t, os.WriteFile(initdbCfg, []byte("bootstrap:\n  initdb:\n    - data-checksums\n    - encoding: UTF8\n"), 0o644))
	n1 := &fakePG{inRecovery: true} // must be in recovery so PromoteN1.Run is called
	tools := &fakeTools{
		oldState:  "in archive recovery", // running standby: PromoteN1 must run
		sysID:     "7361852939023499998",
		onPromote: func() { n1.inRecovery = false },
	}
	d := Deps{
		Cfg: config.Config{Upgrade: config.UpgradeConfig{
			NewPGBindir: "/n", OldPGBindir: "/o", DataDir: filepath.Join(dir, "data"),
			NewDataDir:          filepath.Join(dir, "newdata"),
			PatroniInitdbConfig: initdbCfg,
		}},
		Mgr: mgr, N1: n1, Tools: tools,
	}

	ph := NewUpgrade(d)
	assert.Equal(t, "upgrade", ph.ID())
	require.Len(t, ph.Transitions(), 1)
	assert.Equal(t, "catchup", ph.Transitions()[0].To)

	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.True(t, tools.promoted)
	assert.True(t, tools.stopped)
	assert.True(t, tools.initted)
	assert.Equal(t, []string{"--data-checksums", "--encoding=UTF8"}, tools.initOpts)
	assert.True(t, tools.checked)
	assert.True(t, tools.upgraded)
	assert.Equal(t, 2, n1.checkpoints)
	assert.True(t, mgr.Get().Artifacts.PgUpgradeDone)
	assert.Equal(t, "7361852939023499998", mgr.Get().Artifacts.PG17SYSID)
}

func TestUpgradeRejectsSameDataDirs(t *testing.T) {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade"} {
		require.NoError(t, mgr.Advance(p))
	}
	d := Deps{
		Cfg: config.Config{Upgrade: config.UpgradeConfig{DataDir: "/same", NewDataDir: "/same"}},
		Mgr: mgr, Tools: &fakeTools{},
	}
	step := &runPgUpgradeCheck{d}
	err := step.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "distinct")
}

func TestUpgradeErrorsOnEmptySysID(t *testing.T) {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade"} {
		require.NoError(t, mgr.Advance(p))
	}
	d := Deps{
		Cfg: config.Config{Upgrade: config.UpgradeConfig{DataDir: "/old", NewDataDir: "/new"}},
		Mgr: mgr, Tools: &fakeTools{sysID: ""}, // NewControlData returns empty sysid
	}
	step := &runPgUpgrade{d}
	err := step.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "system identifier")
}

func TestUpgradeSkipsPromoteWhenAlreadyPrimary(t *testing.T) {
	tools := &fakeTools{oldState: "in production"} // already promoted
	d := Deps{Mgr: testMgr(t), Tools: tools}
	step := &promoteN1{d}
	done, err := step.Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done) // skip promote
	assert.False(t, tools.promoted)
}

// Resume after ShutdownN1Clean (a later step) has stopped N1: PromoteN1.Check
// must report done from the control file, not fail trying to connect to a
// server that is intentionally down.
func TestUpgradeSkipsPromoteWhenShutDown(t *testing.T) {
	tools := &fakeTools{oldState: "shut down"} // promoted then stopped earlier
	d := Deps{Mgr: testMgr(t), Tools: tools}
	done, err := (&promoteN1{d}).Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done)
	assert.False(t, tools.promoted)
}

func TestUpgradePromotesRunningStandby(t *testing.T) {
	tools := &fakeTools{oldState: "in archive recovery"} // running standby
	d := Deps{Mgr: testMgr(t), Tools: tools}
	done, err := (&promoteN1{d}).Check(context.Background())
	require.NoError(t, err)
	assert.False(t, done) // not promoted yet -> Run will promote
}

func TestPromoteConfirmsOutOfRecovery(t *testing.T) {
	n1 := &fakePG{inRecovery: true}
	tools := &fakeTools{onPromote: func() { n1.inRecovery = false }}
	d := Deps{Mgr: testMgr(t), N1: n1, Tools: tools, Cfg: config.Config{Upgrade: config.UpgradeConfig{DataDir: "/d"}}}
	require.NoError(t, (&promoteN1{d}).Run(context.Background()))
	assert.True(t, tools.promoted)
}

func TestPromoteErrorsIfStillInRecovery(t *testing.T) {
	n1 := &fakePG{inRecovery: true} // never leaves recovery
	tools := &fakeTools{}           // no onPromote -> stays in recovery
	d := Deps{Mgr: testMgr(t), N1: n1, Tools: tools, Cfg: config.Config{Upgrade: config.UpgradeConfig{DataDir: "/d"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := (&promoteN1{d}).Run(ctx)
	require.Error(t, err) // poll never sees out-of-recovery; ctx deadline ends it
}
