package phases

import (
	"context"
	"testing"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsolateShadowPromotesAndRecordsTarget(t *testing.T) {
	defer setReplayTimingForTest(t)()
	mgr := testMgr(t)
	require.NoError(t, mgr.SetSlotBaseline(&state.SlotBaseline{ConfirmedFlushLSN: "0/10"}))
	pat := &fakePatroni{standbySet: true}
	// shadow promoted: receiver gone, replay settled at 0/3FA20000, out of recovery
	shadow := &fakePG{walRcvActive: false, replayLSN: "0/3FA20000", inRecovery: false}
	prod := &fakePG{}
	d := Deps{Mgr: mgr, NewPatroni: pat,
		Shadow:  func(context.Context) (pg.Client, error) { return shadow, nil },
		Primary: func(context.Context) (pg.Client, error) { return prod, nil },
		Cfg:     config.Config{Upgrade: config.UpgradeConfig{PhysicalSlotName: "shadow_phys"}}}

	for _, s := range NewIsolateShadow(d).Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.False(t, pat.standbySet, "standby_cluster must be cleared (promote)")
	assert.Equal(t, "shadow_phys", prod.droppedSlot)
	assert.Equal(t, "0/3FA20000", mgr.Get().Artifacts.TargetLSN)
}

func TestIsolateShadowStepOrder(t *testing.T) {
	var names []string
	for _, s := range NewIsolateShadow(Deps{}).Steps() {
		names = append(names, string(s.ID()))
	}
	assert.Equal(t, []string{"PromoteShadow", "WaitShadowPromoted", "SettleShadowTarget", "DropPhysicalSlot", "RecordTargetLSN"}, names)
}
