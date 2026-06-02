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

func TestSwitchoverFreezeAndSyncSequences(t *testing.T) {
	pg17 := &fakePG{subLag: &pg.SubscriptionLag{}} // subscription exists, zero lag
	oldPrimary := &fakePG{sequences: []pg.SequenceInfo{{Schema: "public", Name: "s1", LastValue: 42}}}
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
