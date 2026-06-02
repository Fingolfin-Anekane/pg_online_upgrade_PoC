package phases

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isolate-phase methods on the shared fakePG
func (f *fakePG) GetWALReceiverReceivedLSN(context.Context) (string, error) {
	return f.receivedLSN, nil
}
func (f *fakePG) IsWALReceiverActive(context.Context) (bool, error)   { return f.walRcvActive, nil }
func (f *fakePG) DisconnectFromWAL(context.Context) error             { f.disconnected = true; return nil }
func (f *fakePG) GetLastWALReplayLSN(context.Context) (string, error) { return f.replayLSN, nil }
func (f *fakePG) ServerVersionNum(context.Context) (int, error)       { return f.serverVersion, nil }
func (f *fakePG) ClearPrimaryConninfo(context.Context) error          { f.conninfoCleared = true; return nil }

func TestIsolateRecordsTargetLSN(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	require.NoError(t, mgr.SetSlotBaseline(&state.SlotBaseline{ConfirmedFlushLSN: "0/10"}))

	n1 := &fakePG{receivedLSN: "0/3FA20000", walRcvActive: false, replayLSN: "0/3FA20000", serverVersion: 130005}
	pat := &fakePatroni{}
	d := Deps{Mgr: mgr, Patroni: pat, N1: n1}

	ph := NewIsolate(d)
	assert.Equal(t, "isolate", ph.ID())
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.True(t, pat.paused)
	assert.True(t, n1.disconnected)
	assert.Equal(t, "0/3FA20000", mgr.Get().Artifacts.ReceivedLSN)
	assert.Equal(t, "0/3FA20000", mgr.Get().Artifacts.TargetLSN)
}

func TestIsolateInvariantViolation(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	require.NoError(t, mgr.SetSlotBaseline(&state.SlotBaseline{ConfirmedFlushLSN: "0/FF000000"}))

	n1 := &fakePG{replayLSN: "0/10"}
	step := &recordTargetLSN{Deps{Mgr: mgr, N1: n1}}
	err := step.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invariant")
}

func TestIsolateTransitionsToDrain(t *testing.T) {
	ph := NewIsolate(Deps{})
	tr := ph.Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "drain", tr[0].To)
}

func TestCaptureReceivedLSNEmptyErrors(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	n1 := &fakePG{receivedLSN: ""} // wal receiver already empty
	step := &captureReceivedLSN{Deps{Mgr: mgr, N1: n1}}
	err := step.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wal receiver already empty")
}

func TestWaitReplayNotCaughtUp(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	require.NoError(t, mgr.SetReceivedLSN("0/3FA20000"))
	n1 := &fakePG{replayLSN: "0/10"} // replay behind received
	step := &waitReplayComplete{Deps{Mgr: mgr, N1: n1}}
	done, err := step.Check(context.Background())
	require.NoError(t, err)
	assert.False(t, done)
	err = step.Run(context.Background())
	require.Error(t, err)
}

func TestDisconnectPG13Reload(t *testing.T) {
	n1 := &fakePG{serverVersion: 130005}
	tools := &fakeTools{}
	d := Deps{Mgr: testMgr(t), N1: n1, Tools: tools, Cfg: config.Config{Upgrade: config.UpgradeConfig{DataDir: "/d"}}}
	require.NoError(t, (&disconnectN1{d}).Run(context.Background()))
	assert.True(t, n1.disconnected) // reload path
	assert.False(t, tools.restarted)
}

func TestDisconnectPG12Restart(t *testing.T) {
	n1 := &fakePG{serverVersion: 120008}
	tools := &fakeTools{}
	d := Deps{Mgr: testMgr(t), N1: n1, Tools: tools, Cfg: config.Config{Upgrade: config.UpgradeConfig{DataDir: "/d"}}}
	require.NoError(t, (&disconnectN1{d}).Run(context.Background()))
	assert.True(t, n1.conninfoCleared)
	assert.True(t, tools.restarted)
	assert.False(t, n1.disconnected)
}

func TestDisconnectPG10RecoveryConf(t *testing.T) {
	dir := t.TempDir()
	rc := filepath.Join(dir, "recovery.conf")
	require.NoError(t, os.WriteFile(rc,
		[]byte("standby_mode = 'on'\nprimary_conninfo = 'host=primary user=repl'\nrestore_command = 'cp %p'\n"), 0o600))
	n1 := &fakePG{serverVersion: 100015}
	tools := &fakeTools{}
	d := Deps{Mgr: testMgr(t), N1: n1, Tools: tools, Cfg: config.Config{Upgrade: config.UpgradeConfig{DataDir: dir}}}
	require.NoError(t, (&disconnectN1{d}).Run(context.Background()))
	assert.True(t, tools.restarted)
	data, err := os.ReadFile(rc)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "primary_conninfo = 'host=primary")
	assert.Contains(t, string(data), "standby_mode") // other settings preserved
	assert.Contains(t, string(data), "restore_command")
}

func TestStripPrimaryConninfo(t *testing.T) {
	in := "standby_mode = 'on'\nprimary_conninfo = 'host=p'\n#primary_conninfo = 'old'\nrestore_command = 'x'\n"
	out := stripPrimaryConninfo(in)
	assert.NotContains(t, out, "primary_conninfo = 'host=p'")
	assert.Contains(t, out, "standby_mode")
	assert.Contains(t, out, "#primary_conninfo") // commented line preserved
	assert.Contains(t, out, "restore_command")
}
