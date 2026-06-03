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

// catchup-phase methods on the shared fakePG
func (f *fakePG) GetSubscriptionLag(_ context.Context, name string) (*pg.SubscriptionLag, error) {
	return f.subLag, nil // nil until CreateSubscription sets it
}
func (f *fakePG) CreateSubscription(_ context.Context, name, connStr, pubName, slotName string) error {
	f.createdSub = name
	f.subLag = &pg.SubscriptionLag{} // subscription now exists with zero lag
	return nil
}

func TestCatchupCreatesSubAndWaitsLag(t *testing.T) {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade", "catchup"} {
		require.NoError(t, mgr.Advance(p))
	}
	require.NoError(t, mgr.SetPrimaryHost("primary.host"))

	pg17 := &fakePG{} // subscription created in the loop
	oldPrimary := &fakePG{}
	tools := &fakeTools{running: true} // PG17 already up -> StartPG17OnN1 skipped
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "n1", Host: "n1", Role: "leader"},
		{Name: "n2", Host: "n2", Role: "replica"},
	}}}
	d := Deps{
		Cfg: config.Config{Upgrade: config.UpgradeConfig{
			SubscriptionName: "sub_up", PublicationName: "pub_up", SlotName: "slot_up",
			NewPGBindir: "/n", NewDataDir: "/nd",
		}, PG: config.PGConfig{SuperuserDSN: "host=tmpl"}},
		Mgr: mgr, Tools: tools, NewPatroni: newPat,
		PG17:    func(context.Context) (pg.Client, error) { return pg17, nil },
		Primary: func(context.Context) (pg.Client, error) { return oldPrimary, nil },
	}

	ph := NewCatchup(d)
	assert.Equal(t, "catchup", ph.ID())
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.Equal(t, "sub_up", pg17.createdSub)
}

func TestStartPG17Runs(t *testing.T) {
	tools := &fakeTools{}
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{NewPGBindir: "/n", NewDataDir: "/nd"}}, Tools: tools}
	require.NoError(t, (&startPG17{d}).Run(context.Background()))
	assert.True(t, tools.started)
}

func TestStartPG17SkipsWhenAlreadyRunning(t *testing.T) {
	tools := &fakeTools{running: true} // pg_ctl status reports a live postmaster
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{NewPGBindir: "/n", NewDataDir: "/nd"}}, Tools: tools}
	done, err := (&startPG17{d}).Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done) // already up -> do not start a second postmaster
}

func TestCatchupTransitionsToSwitchover(t *testing.T) {
	ph := NewCatchup(Deps{})
	tr := ph.Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "switchover", tr[0].To)
}

func TestCreateForwardSubscriptionSkipsWhenExists(t *testing.T) {
	pg17 := &fakePG{subLag: &pg.SubscriptionLag{}} // subscription already exists
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{SubscriptionName: "sub_up"}},
		PG17: func(context.Context) (pg.Client, error) { return pg17, nil }}
	done, err := (&createForwardSubscription{d}).Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done)
	assert.Equal(t, "", pg17.createdSub) // create skipped
}

func TestVerifyNewClusterHealthyRejectsNoLeader(t *testing.T) {
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{{Name: "a", Role: "replica"}}}}
	err := (&verifyNewClusterHealthy{Deps{NewPatroni: newPat}}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no leader")
}

func TestVerifyNewClusterHealthyRejectsNoReplica(t *testing.T) {
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{{Name: "n1", Role: "leader"}}}}
	err := (&verifyNewClusterHealthy{Deps{NewPatroni: newPat}}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no standby")
}

func TestVerifyNewClusterHealthyAcceptsSyncStandby(t *testing.T) {
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "n1", Role: "leader"}, {Name: "n2", Role: "sync_standby"},
	}}}
	require.NoError(t, (&verifyNewClusterHealthy{Deps{NewPatroni: newPat}}).Run(context.Background()))
}

func TestWaitLagZeroErrorsWhenBehind(t *testing.T) {
	pg17 := &fakePG{subLag: &pg.SubscriptionLag{ReplayLagMs: 500}}
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{SubscriptionName: "sub_up"}},
		PG17: func(context.Context) (pg.Client, error) { return pg17, nil }}
	step := &waitLagZero{d}
	done, err := step.Check(context.Background())
	require.NoError(t, err)
	assert.False(t, done)
	require.Error(t, step.Run(context.Background()))
}
