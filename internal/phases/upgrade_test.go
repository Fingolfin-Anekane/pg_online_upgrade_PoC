package phases

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
	oldState  string // State returned by OldControlData (pre-upgrade)
	sysID     string // SystemID returned by NewControlData (post-upgrade)
	promoted  bool
	stopped   bool
	checked   bool
	upgraded  bool
	restarted bool
}

func (f *fakeTools) OldControlData(context.Context, string) (*pgbin.ControlData, error) {
	return &pgbin.ControlData{State: f.oldState}, nil
}
func (f *fakeTools) NewControlData(context.Context, string) (*pgbin.ControlData, error) {
	return &pgbin.ControlData{State: "in production", SystemID: f.sysID}, nil
}
func (f *fakeTools) Promote(context.Context, string) error   { f.promoted = true; return nil }
func (f *fakeTools) StopClean(context.Context, string) error { f.stopped = true; return nil }
func (f *fakeTools) UpgradeCheck(context.Context, pgbin.UpgradeOptions) error {
	f.checked = true
	return nil
}
func (f *fakeTools) Upgrade(context.Context, pgbin.UpgradeOptions) error {
	f.upgraded = true
	return nil
}
func (f *fakeTools) Restart(context.Context, string) error { f.restarted = true; return nil }

func TestUpgradeHappyPath(t *testing.T) {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade"} {
		require.NoError(t, mgr.Advance(p))
	}
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "patroni.yml")
	n1 := &fakePG{inRecovery: true} // must be in recovery so PromoteN1.Run is called
	tools := &fakeTools{
		oldState: "in production", // promoted/running, not yet shut down at check time
		sysID:    "7361852939023499998",
	}
	d := Deps{
		Cfg: config.Config{Upgrade: config.UpgradeConfig{
			NewPGBindir: "/n", OldPGBindir: "/o", DataDir: filepath.Join(dir, "data"),
			NewDataDir:        filepath.Join(dir, "newdata"),
			PatroniConfigPath: cfgPath,
		}},
		Mgr: mgr, N1: n1, Tools: tools,
	}

	ph := NewUpgrade(d)
	assert.Equal(t, "upgrade", ph.ID())
	assert.Empty(t, ph.Transitions()) // terminal in Plan 2

	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.True(t, tools.promoted)
	assert.True(t, tools.stopped)
	assert.True(t, tools.checked)
	assert.True(t, tools.upgraded)
	assert.Equal(t, 2, n1.checkpoints)
	assert.True(t, mgr.Get().Artifacts.PgUpgradeDone)
	assert.Equal(t, "7361852939023499998", mgr.Get().Artifacts.PG17SYSID)

	data, err := os.ReadFile(cfgPath)
	require.NoError(t, err)
	assert.Contains(t, string(data), "7361852939023499998")
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
	n1 := &fakePG{inRecovery: false} // already promoted
	tools := &fakeTools{}
	d := Deps{Mgr: testMgr(t), N1: n1, Tools: tools}
	step := &promoteN1{d}
	done, err := step.Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done) // skip promote
	assert.False(t, tools.promoted)
}
