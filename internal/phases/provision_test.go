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

func clusterWithLeader(n int) *patroni.ClusterInfo {
	ms := []patroni.Member{{Name: "l", Host: "l", Role: "leader"}}
	for i := 1; i < n; i++ {
		ms = append(ms, patroni.Member{Name: "r", Host: "r", Role: "replica"})
	}
	return &patroni.ClusterInfo{Members: ms}
}

func TestProvisionAppliesStandbyAndCreatesSlot(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.SetPrimaryHost("prod-primary"))
	pat := &fakePatroni{}
	prod := &fakePG{}
	d := Deps{Mgr: mgr, NewPatroni: pat, Primary: func(context.Context) (pg.Client, error) { return prod, nil },
		Cfg: config.Config{Upgrade: config.UpgradeConfig{
			ShadowSourceHost: "prod-primary", ShadowSourcePort: 5432, PhysicalSlotName: "shadow_phys",
		}}}

	require.NoError(t, (&applyStandbyCluster{d}).Run(context.Background()))
	assert.True(t, pat.standbySet)
	require.NoError(t, (&createPhysicalSlot{d}).Run(context.Background()))
	assert.Equal(t, "shadow_phys", prod.physicalSlot)
}

func TestProvisionCaughtUpGate(t *testing.T) {
	prod := &fakePG{walCurrent: "0/100"}
	shadow := &fakePG{replayLSN: "0/100"}
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{ShadowNodeCount: 1}},
		Primary:    func(context.Context) (pg.Client, error) { return prod, nil },
		Shadow:     func(context.Context) (pg.Client, error) { return shadow, nil },
		NewPatroni: &fakePatroni{cluster: clusterWithLeader(1)}}
	ok, err := (&waitShadowCaughtUp{d}).caughtUp(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
}
