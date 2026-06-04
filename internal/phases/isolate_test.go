package phases

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	pat := &fakePatroni{} // not yet paused; PausePatroni.Run pauses, then the node applies it
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

func TestVerifyN1DetachedFailsWhenReceiverReattached(t *testing.T) {
	// Paused Patroni can re-apply primary_conninfo, reconnecting N1's walreceiver
	// after DisconnectFromWAL. The guard must catch that (receiver active again)
	// and fail loudly instead of letting isolate record a stale target_lsn while
	// N1 silently drifts.
	n1 := &fakePG{walRcvActive: true}
	err := (&verifyN1Detached{Deps{N1: n1}}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "re-attached")
}

func TestVerifyN1DetachedPassesWhenReceiverGone(t *testing.T) {
	n1 := &fakePG{walRcvActive: false}
	require.NoError(t, (&verifyN1Detached{Deps{N1: n1}}).Run(context.Background()))
}

func TestIsolateRunsVerifyN1DetachedBeforeRecordingTarget(t *testing.T) {
	steps := NewIsolate(Deps{}).Steps()
	var names []string
	for _, s := range steps {
		names = append(names, string(s.ID()))
	}
	assert.Equal(t, []string{
		"PausePatroni", "StopPatroniOnN1", "CaptureReceivedLSN", "DisconnectN1FromWAL",
		"WaitReplayComplete", "VerifyN1Detached", "RecordTargetLSN",
	}, names)
}

func TestStopPatroniOnN1RunsCommandAndRecords(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	tools := &fakeTools{}
	d := Deps{Mgr: mgr, Tools: tools, Cfg: config.Config{Upgrade: config.UpgradeConfig{
		OldPatroniStopCommand: "systemctl stop patroni",
	}}}
	step := &stopPatroniOnN1{d}

	done, err := step.Check(context.Background())
	require.NoError(t, err)
	assert.False(t, done)
	require.NoError(t, step.Run(context.Background()))
	assert.Equal(t, "systemctl stop patroni", tools.patroniStopped)
	assert.True(t, mgr.Get().Artifacts.PatroniStoppedOnN1)

	// recorded -> skip on re-entry (so it isn't re-run pointlessly)
	done, err = step.Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done)
}

func TestStopPatroniOnN1NoCommandIsNoop(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	tools := &fakeTools{}
	d := Deps{Mgr: mgr, Tools: tools, Cfg: config.Config{Upgrade: config.UpgradeConfig{OldPatroniStopCommand: ""}}}

	require.NoError(t, (&stopPatroniOnN1{d}).Run(context.Background()))
	assert.Empty(t, tools.patroniStopped)
	// nothing was stopped, so the pause step must keep using the live REST
	assert.False(t, mgr.Get().Artifacts.PatroniStoppedOnN1)
}

func TestWaitNodePausedReturnsWhenApplied(t *testing.T) {
	require.NoError(t, waitNodePaused(context.Background(), &fakePatroni{nodePaused: true}, time.Millisecond))
}

func TestWaitNodePausedTimesOutWhenNeverApplied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := waitNodePaused(ctx, &fakePatroni{nodePaused: false}, 5*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maintenance")
}

func TestPausePatroniRunWaitsForAppliedPause(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	pat := &fakePatroni{nodePaused: true} // node already in maintenance -> wait returns at once
	require.NoError(t, (&pausePatroni{Deps{Mgr: mgr, Patroni: pat}}).Run(context.Background()))
	assert.True(t, pat.paused, "must PATCH pause")
}

func TestPausePatroniCheckUsesAppliedNodeState(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	// DCS may say paused, but until the NODE applies it Check must report not-done.
	notApplied := &pausePatroni{Deps{Mgr: mgr, Patroni: &fakePatroni{nodePaused: false}}}
	done, err := notApplied.Check(context.Background())
	require.NoError(t, err)
	assert.False(t, done)

	applied := &pausePatroni{Deps{Mgr: mgr, Patroni: &fakePatroni{nodePaused: true}}}
	done, err = applied.Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done)
}

func TestPausePatroniSkippedAfterN1PatroniStopped(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	require.NoError(t, mgr.SetPatroniStoppedOnN1())
	// We already stopped Patroni on N1, so its REST is down; Check must NOT hit it.
	d := Deps{Mgr: mgr, Patroni: &fakePatroni{err: errors.New("connection refused")}}
	done, err := (&pausePatroni{d}).Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done)
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

	// a different key that merely shares the prefix must be preserved
	out2 := stripPrimaryConninfo("primary_conninfo_extra = 'keep me'\nprimary_conninfo = 'drop'\n")
	assert.Contains(t, out2, "primary_conninfo_extra = 'keep me'")
	assert.NotContains(t, out2, "primary_conninfo = 'drop'")
}
