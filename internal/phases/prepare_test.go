package phases

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLogger records Detail lines so tests can assert on emitted warnings.
type captureLogger struct{ details []string }

func (c *captureLogger) PhaseStart(runner.PhaseID)                                   {}
func (c *captureLogger) PhaseDone(runner.PhaseID)                                    {}
func (c *captureLogger) StepStart(runner.PhaseID, runner.StepID)                     {}
func (c *captureLogger) StepResult(runner.PhaseID, runner.StepID, runner.StepStatus) {}
func (c *captureLogger) Detail(format string, args ...any) {
	c.details = append(c.details, fmt.Sprintf(format, args...))
}

// fakePG embeds pg.Client; only overridden methods are safe to call. Fields are
// declared here and reused by isolate_test.go / upgrade_test.go (same package).
type fakePG struct {
	pg.Client
	walLevel        string
	inRecovery      bool
	slot            *pg.ReplicationSlot
	createdPub      string
	createdSlot     string
	receivedLSN     string
	walRcvActive    bool
	walRcvActiveSeq []bool // successive IsWALReceiverActive results; falls back to walRcvActive
	disconnected    bool
	replayLSN       string
	replaySeq       []string // successive GetLastWALReplayLSN results; falls back to replayLSN
	checkpoints     int
	serverVersion   int
	conninfoCleared bool
	subLag          *pg.SubscriptionLag
	subExists       bool
	subEnabled      bool
	createdSub      string
	frozen          string
	sequences       []pg.SequenceInfo
	setSeqs         []seqSet
	createdRevSub   string
	disabledSub     string
	appBackends     int
	droppedSub      []string
	droppedPub      []string
	unfrozen        string
	inRecoveryErr   error
	maxSlotWALKeep  string
	oldestTxnAge    time.Duration
	ddlLocked       bool
	ddlUnlocked     bool
	physicalSlot    string
	walCurrent      string
}

func (f *fakePG) CreatePhysicalSlot(_ context.Context, name string) error {
	f.physicalSlot = name
	return nil
}
func (f *fakePG) CurrentWALLSN(context.Context) (string, error) { return f.walCurrent, nil }

func (f *fakePG) LockDDL(context.Context) error   { f.ddlLocked = true; return nil }
func (f *fakePG) UnlockDDL(context.Context) error { f.ddlUnlocked = true; return nil }

func (f *fakePG) MaxSlotWALKeepSize(context.Context) (string, error)  { return f.maxSlotWALKeep, nil }
func (f *fakePG) OldestTxnAge(context.Context) (time.Duration, error) { return f.oldestTxnAge, nil }

func (f *fakePG) ShowWALLevel(context.Context) (string, error) { return f.walLevel, nil }
func (f *fakePG) IsInRecovery(context.Context) (bool, error)   { return f.inRecovery, f.inRecoveryErr }
func (f *fakePG) GetReplicationSlot(_ context.Context, name string) (*pg.ReplicationSlot, error) {
	// If a slot was created via CreateLogicalSlot, reflect it in subsequent reads.
	if f.slot == nil && f.createdSlot == name {
		return &pg.ReplicationSlot{Name: name, RestartLSN: "0/10", ConfirmedFlushLSN: "0/10"}, nil
	}
	return f.slot, nil
}
func (f *fakePG) CreatePublication(_ context.Context, name string) error {
	f.createdPub = name
	return nil
}
func (f *fakePG) CreateLogicalSlot(_ context.Context, name, plugin string) (*pg.ReplicationSlot, error) {
	f.createdSlot = name
	return &pg.ReplicationSlot{Name: name, RestartLSN: "0/10", ConfirmedFlushLSN: "0/10"}, nil
}
func (f *fakePG) PublicationExists(_ context.Context, name string) (bool, error) {
	return f.createdPub == name, nil
}

// fakePatroni implements patroni.Client.
type fakePatroni struct {
	cluster    *patroni.ClusterInfo
	paused     bool
	nodePaused bool  // applied maintenance state reported by NodePaused
	err        error // returned by GetCluster (e.g. to simulate an unreachable REST)
	standbySet bool  // toggled by SetStandbyCluster/ClearStandbyCluster
}

func (f *fakePatroni) GetCluster(context.Context) (*patroni.ClusterInfo, error) {
	return f.cluster, f.err
}

// NodePaused reports applied maintenance: either preset, or because Pause() was
// called on this fake (models the node picking up the PATCH on its HA loop).
func (f *fakePatroni) NodePaused(context.Context) (bool, error) { return f.nodePaused || f.paused, nil }
func (f *fakePatroni) Pause(context.Context) error              { f.paused = true; return nil }
func (f *fakePatroni) Resume(context.Context) error             { f.paused = false; return nil }

func (f *fakePatroni) SetStandbyCluster(context.Context, string, int, string) error {
	f.standbySet = true
	return nil
}
func (f *fakePatroni) ClearStandbyCluster(context.Context) error {
	f.standbySet = false
	return nil
}

func testMgr(t *testing.T) *state.Manager {
	t.Helper()
	// NewManager starts Current at "prepare"; isolate/drain/upgrade tests call
	// Advance to move forward.
	m, err := state.NewManager(filepath.Join(t.TempDir(), "s.json"), "test")
	require.NoError(t, err)
	return m
}

func TestPrepareDiscoverAndCreate(t *testing.T) {
	primary := &fakePG{walLevel: "logical", inRecovery: false, slot: nil}
	n1 := &fakePG{inRecovery: true, serverVersion: 120008}
	pat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "p", Host: "primary.host", Role: "leader"},
		{Name: "n1", Host: "n1.host", Role: "replica"},
	}}}
	mgr := testMgr(t)
	d := Deps{
		Cfg: config.Config{Upgrade: config.UpgradeConfig{TargetNode: "n1", SlotName: "slot_up", PublicationName: "pub_up"}},
		Mgr: mgr, Patroni: pat, N1: n1,
		Primary: func(context.Context) (pg.Client, error) { return primary, nil },
	}

	ph := NewPrepare(d)
	assert.Equal(t, "prepare", ph.ID())
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}

	assert.Equal(t, "primary.host", mgr.Get().Artifacts.PrimaryHost)
	assert.Equal(t, "pub_up", primary.createdPub)
	assert.Equal(t, "slot_up", primary.createdSlot)
	require.NotNil(t, mgr.Get().Artifacts.SlotBaseline)
	assert.Equal(t, "0/10", mgr.Get().Artifacts.SlotBaseline.ConfirmedFlushLSN)
}

func TestPrepareLocksDDLOnOldPrimary(t *testing.T) {
	primary := &fakePG{}
	d := Deps{Mgr: testMgr(t),
		Primary: func(context.Context) (pg.Client, error) { return primary, nil }}
	require.NoError(t, (&lockDDL{d}).Run(context.Background()))
	assert.True(t, primary.ddlLocked)
}

func TestPrepareTransitionsToIsolate(t *testing.T) {
	ph := NewPrepare(Deps{})
	tr := ph.Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "isolate", tr[0].To)
}

func TestPrepareNoLeaderFails(t *testing.T) {
	pat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "a", Host: "a", Role: "replica"},
		{Name: "b", Host: "b", Role: "replica"},
	}}}
	d := Deps{Mgr: testMgr(t), Patroni: pat}
	step := &discoverTopology{d}
	err := step.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no leader")
}

func TestPrepareRejectsNonLogicalWALLevel(t *testing.T) {
	primary := &fakePG{walLevel: "replica"}
	d := Deps{Mgr: testMgr(t), N1: &fakePG{inRecovery: true},
		Primary: func(context.Context) (pg.Client, error) { return primary, nil }}
	step := &verifyPrerequisites{d}
	err := step.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wal_level")
}

func TestPrepareRejectsNonReplicaN1(t *testing.T) {
	primary := &fakePG{walLevel: "logical"}
	n1 := &fakePG{inRecovery: false} // not a replica
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{TargetNode: "n1"}},
		Mgr: testMgr(t), N1: n1,
		Primary: func(context.Context) (pg.Client, error) { return primary, nil }}
	step := &verifyPrerequisites{d}
	err := step.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in recovery")
}

func TestVerifyPrerequisitesWarnsOnSlotRisks(t *testing.T) {
	// max_slot_wal_keep_size != -1 and a long-running transaction are non-fatal
	// but must surface as warnings (the hard stop is assertSlotReserved later).
	primary := &fakePG{walLevel: "logical", maxSlotWALKeep: "0", oldestTxnAge: 10 * time.Minute}
	n1 := &fakePG{inRecovery: true, serverVersion: 130005}
	log := &captureLogger{}
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{TargetNode: "n1"}},
		Mgr: testMgr(t), N1: n1, Log: log,
		Primary: func(context.Context) (pg.Client, error) { return primary, nil }}
	require.NoError(t, (&verifyPrerequisites{d}).Run(context.Background()))
	joined := strings.Join(log.details, "\n")
	assert.Contains(t, joined, "max_slot_wal_keep_size")
	assert.Contains(t, joined, "транзакц")
}

func TestVerifyPrerequisitesNoWarnOnSafeConfig(t *testing.T) {
	// Unlimited keep size, no long txn -> no slot-risk warnings.
	primary := &fakePG{walLevel: "logical", maxSlotWALKeep: "-1", oldestTxnAge: 0}
	n1 := &fakePG{inRecovery: true, serverVersion: 130005}
	log := &captureLogger{}
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{TargetNode: "n1"}},
		Mgr: testMgr(t), N1: n1, Log: log,
		Primary: func(context.Context) (pg.Client, error) { return primary, nil }}
	require.NoError(t, (&verifyPrerequisites{d}).Run(context.Background()))
	assert.NotContains(t, strings.Join(log.details, "\n"), "ПРЕДУПРЕЖДЕНИЕ")
}

func TestPrepareRejectsBelowPG10(t *testing.T) {
	primary := &fakePG{walLevel: "logical"}
	n1 := &fakePG{inRecovery: true, serverVersion: 90605} // PG 9.6
	d := Deps{Mgr: testMgr(t), N1: n1,
		Primary: func(context.Context) (pg.Client, error) { return primary, nil }}
	err := (&verifyPrerequisites{d}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PostgreSQL 10")
}

type seqSet struct {
	schema string
	name   string
	value  int64
}
