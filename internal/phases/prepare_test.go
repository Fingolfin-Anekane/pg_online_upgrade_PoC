package phases

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	disconnected    bool
	replayLSN       string
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
}

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
	cluster *patroni.ClusterInfo
	paused  bool
	err     error // returned by GetCluster (e.g. to simulate an unreachable REST)
}

func (f *fakePatroni) GetCluster(context.Context) (*patroni.ClusterInfo, error) {
	return f.cluster, f.err
}
func (f *fakePatroni) Pause(context.Context) error  { f.paused = true; return nil }
func (f *fakePatroni) Resume(context.Context) error { f.paused = false; return nil }

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
