package phases

import (
	"context"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// finalize/cleanup methods on the shared fakePG
func (f *fakePG) DropSubscription(_ context.Context, name string) error {
	f.droppedSub = append(f.droppedSub, name)
	return nil
}
func (f *fakePG) DropPublication(_ context.Context, name string) error {
	f.droppedPub = append(f.droppedPub, name)
	return nil
}
func (f *fakePG) UnfreezeAfterUpgrade(_ context.Context, dbname string) error {
	f.unfrozen = dbname
	return nil
}

func finalizeDeps(t *testing.T, pg17, oldPrimary *fakePG, newPat patroni.Client) Deps {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade", "catchup", "switchover", "finalize"} {
		require.NoError(t, mgr.Advance(p))
	}
	return Deps{
		Cfg: config.Config{ClusterName: "prod", Upgrade: config.UpgradeConfig{
			SubscriptionName: "sub_up", ReversePubName: "pub_rb", ReverseSubName: "sub_rb", DBName: "app",
		}},
		Mgr: mgr, NewPatroni: newPat,
		PG17:    func(context.Context) (pg.Client, error) { return pg17, nil },
		Primary: func(context.Context) (pg.Client, error) { return oldPrimary, nil },
	}
}

func TestFinalizeDropsRollbackArtifactsAndKeepsFreeze(t *testing.T) {
	pg17 := &fakePG{}
	oldPrimary := &fakePG{}
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "n1", Role: "leader"}, {Name: "n2", Role: "sync_standby"},
	}}}
	d := finalizeDeps(t, pg17, oldPrimary, newPat)

	ph := NewFinalize(d)
	assert.Equal(t, "finalize", ph.ID())
	require.Len(t, ph.Steps(), 3) // no UnfreezeOldPrimary step
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.Contains(t, oldPrimary.droppedSub, "sub_rb") // reverse sub on old primary
	assert.Contains(t, pg17.droppedPub, "pub_rb")       // reverse pub on PG17
	assert.Contains(t, pg17.droppedSub, "sub_up")       // forward sub on PG17
	// The old primary stays frozen (no split-brain): finalize must NOT unfreeze it.
	assert.Empty(t, oldPrimary.unfrozen, "finalize must leave the old primary frozen")
}

func TestFinalizeTransitionsToCleanup(t *testing.T) {
	ph := NewFinalize(Deps{})
	tr := ph.Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "cleanup", tr[0].To)
}

func TestVerifyRenamedClusterRejectsNoLeader(t *testing.T) {
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{{Name: "a", Role: "replica"}}}}
	err := (&verifyRenamedCluster{Deps{NewPatroni: newPat}}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no leader")
}

func TestVerifyRenamedClusterAcceptsLeader(t *testing.T) {
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{{Name: "n1", Role: "leader"}}}}
	require.NoError(t, (&verifyRenamedCluster{Deps{NewPatroni: newPat}}).Run(context.Background()))
}
