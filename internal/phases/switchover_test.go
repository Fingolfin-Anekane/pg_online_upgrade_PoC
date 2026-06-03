package phases

import (
	"context"
	"testing"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// switchover-phase methods on the shared fakePG
func (f *fakePG) FreezeForUpgrade(_ context.Context, dbname string) error {
	f.frozen = dbname
	return nil
}
func (f *fakePG) GetAllSequences(context.Context) ([]pg.SequenceInfo, error) {
	return f.sequences, nil
}
func (f *fakePG) SetSequenceValue(_ context.Context, schema, name string, value int64) error {
	f.setSeqs = append(f.setSeqs, seqSet{schema, name, value})
	return nil
}

func switchoverDeps(t *testing.T, pg17, oldPrimary *fakePG) Deps {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade", "catchup", "switchover"} {
		require.NoError(t, mgr.Advance(p))
	}
	require.NoError(t, mgr.SetPrimaryHost("primary.host"))
	return Deps{
		Cfg: config.Config{ClusterName: "prod", Upgrade: config.UpgradeConfig{
			SubscriptionName: "sub_up", ReversePubName: "pub_rb", ReverseSubName: "sub_rb",
			DBName: "app", PG17DSN: "host=n1 port=5433 dbname=app", SequenceBuffer: 1000,
			DSNSwapSignalPath: "/run/sig.json",
		}, PG: config.PGConfig{SuperuserDSN: "host=tmpl"}},
		Mgr:     mgr,
		PG17:    func(context.Context) (pg.Client, error) { return pg17, nil },
		Primary: func(context.Context) (pg.Client, error) { return oldPrimary, nil },
	}
}

func TestSwitchoverTransitionsToFinalize(t *testing.T) {
	tr := NewSwitchover(Deps{}).Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "finalize", tr[0].To)
}

func TestSwitchoverFreezeAndSyncSequences(t *testing.T) {
	pg17 := &fakePG{}
	// publisher (old primary): zero lag + the sequence to sync
	oldPrimary := &fakePG{subLag: &pg.SubscriptionLag{}, sequences: []pg.SequenceInfo{{Schema: "public", Name: "s1", LastValue: 42}}}
	d := switchoverDeps(t, pg17, oldPrimary)

	steps := NewSwitchover(d).Steps()
	for _, s := range steps[:3] { // freeze, wait final lag, sync sequences
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.Equal(t, "app", oldPrimary.frozen)
	require.Len(t, pg17.setSeqs, 1)
	assert.Equal(t, int64(1042), pg17.setSeqs[0].value) // 42 + buffer 1000
	assert.True(t, d.Mgr.Get().Artifacts.SequencesSynced)
}

func TestWaitFinalLagZeroErrorsWhenBehind(t *testing.T) {
	// lag is reported by the publisher (old primary), not PG17
	d := switchoverDeps(t, &fakePG{}, &fakePG{subLag: &pg.SubscriptionLag{ByteLag: 250}})
	step := &waitFinalLagZero{d}
	done, err := step.Check(context.Background())
	require.NoError(t, err)
	assert.False(t, done)
	require.Error(t, step.Run(context.Background()))
}

func TestSyncSequencesMultiple(t *testing.T) {
	pg17 := &fakePG{subLag: &pg.SubscriptionLag{}}
	oldPrimary := &fakePG{sequences: []pg.SequenceInfo{
		{Schema: "public", Name: "a", LastValue: 10},
		{Schema: "app", Name: "b", LastValue: 7},
	}}
	d := switchoverDeps(t, pg17, oldPrimary)
	require.NoError(t, (&syncSequences{d}).Run(context.Background()))
	require.Len(t, pg17.setSeqs, 2)
	assert.Equal(t, int64(1010), pg17.setSeqs[0].value)
	assert.Equal(t, int64(1007), pg17.setSeqs[1].value)
	assert.True(t, d.Mgr.Get().Artifacts.SequencesSynced)
}

// switchover part-2 methods on the shared fakePG (CreatePublication already exists in prepare_test.go)
func (f *fakePG) CreateSubscriptionCreatingSlot(_ context.Context, name, connStr, pubName string) error {
	f.createdRevSub = name
	return nil
}
func (f *fakePG) DisableSubscription(_ context.Context, name string) error {
	f.disabledSub = name
	return nil
}
func (f *fakePG) CountAppBackends(context.Context) (int, error) { return f.appBackends, nil }

func TestSwitchoverSignalAndDisable(t *testing.T) {
	pg17 := &fakePG{subLag: &pg.SubscriptionLag{}, appBackends: 5}
	oldPrimary := &fakePG{}
	d := switchoverDeps(t, pg17, oldPrimary)
	var signalled string
	d.WriteSignal = func(path string, _ []byte) error { signalled = path; return nil }

	steps := NewSwitchover(d).Steps()
	require.Len(t, steps, 6)      // no reverse-replication step
	for _, s := range steps[3:] { // notify, verify-traffic, disable-forward
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.Equal(t, "/run/sig.json", signalled)
	assert.True(t, d.Mgr.Get().Artifacts.DSNSwapNotified)
	assert.Equal(t, "sub_up", pg17.disabledSub)
}

func TestVerifyTrafficOnNewErrorsWhenNoBackends(t *testing.T) {
	pg17 := &fakePG{appBackends: 0}
	d := switchoverDeps(t, pg17, &fakePG{})
	err := (&verifyTrafficOnNew{d}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no application traffic")
}

func TestValidateForRunRejectsZeroSequenceBuffer(t *testing.T) {
	// constructed inline so the rest of the config is valid; only SequenceBuffer is 0
	cfg := &config.Config{Upgrade: config.UpgradeConfig{
		TargetNode: "n1", OldPGBindir: "/o", DataDir: "/old", NewDataDir: "/new",
		PatroniConfigPath: "/p.yml", SubscriptionName: "s", ReversePubName: "rp",
		ReverseSubName: "rs", DBName: "app", PG17DSN: "host=n1", NewPatroniURL: "http://x",
		DSNSwapSignalPath: "/sig", SequenceBuffer: 0,
		PgUpgradeLogDir: "/d", LogArchiveDir: "/a",
	}}
	require.Error(t, cfg.ValidateForRun())
}
