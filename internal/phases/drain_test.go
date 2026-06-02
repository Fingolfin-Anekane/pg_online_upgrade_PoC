package phases

import (
	"context"
	"testing"
	"time"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/slotdrain"
	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDrainRunsAndVerifies(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	require.NoError(t, mgr.Advance("drain"))
	require.NoError(t, mgr.SetTargetLSN("0/3FA20000"))
	require.NoError(t, mgr.SetPrimaryHost("primary.host"))

	var called bool
	drainFn := func(_ context.Context, cfg slotdrain.Config) (*slotdrain.Report, error) {
		called = true
		assert.Equal(t, "0/3FA20000", cfg.TargetLSN)
		assert.Contains(t, cfg.ConnString, "host=primary.host")
		return &slotdrain.Report{CompletedAt: time.Now(), FinalFlushLSN: "0/3FA20000", TransactionsDrained: 3}, nil
	}
	primary := &fakePG{slot: &pg.ReplicationSlot{Name: "slot_up", ConfirmedFlushLSN: "0/3FA20000"}}
	d := Deps{
		Cfg:     config.Config{Upgrade: config.UpgradeConfig{SlotName: "slot_up", PublicationName: "pub_up"}, PG: config.PGConfig{SuperuserDSN: "host=primary"}},
		Mgr:     mgr,
		Primary: func(context.Context) (pg.Client, error) { return primary, nil },
		Drain:   drainFn,
	}

	ph := NewDrain(d)
	assert.Equal(t, "drain", ph.ID())
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.True(t, called)
	require.NotNil(t, mgr.Get().Artifacts.DrainReport)
	assert.Equal(t, 3, mgr.Get().Artifacts.DrainReport.TransactionsDrained)
}

func TestDrainTransitionsToUpgrade(t *testing.T) {
	ph := NewDrain(Deps{})
	tr := ph.Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "upgrade", tr[0].To)
}

func TestDrainErrorsWithoutTargetLSN(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	require.NoError(t, mgr.Advance("drain"))
	d := Deps{Mgr: mgr, Drain: func(context.Context, slotdrain.Config) (*slotdrain.Report, error) { return nil, nil }}
	step := &runSlotDrain{d}
	err := step.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target_lsn not set")
}

func TestVerifySlotDrainedMismatch(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	require.NoError(t, mgr.Advance("drain"))
	require.NoError(t, mgr.SetDrainReport(&state.DrainReport{FinalFlushLSN: "0/3FA20000"}))
	primary := &fakePG{slot: &pg.ReplicationSlot{Name: "slot_up", ConfirmedFlushLSN: "0/10"}} // mismatch
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{SlotName: "slot_up"}}, Mgr: mgr,
		Primary: func(context.Context) (pg.Client, error) { return primary, nil }}
	step := &verifySlotDrained{d}
	err := step.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "!=")
}

func TestVerifySlotDrainedMissingSlot(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.Advance("isolate"))
	require.NoError(t, mgr.Advance("drain"))
	require.NoError(t, mgr.SetDrainReport(&state.DrainReport{FinalFlushLSN: "0/3FA20000"}))
	primary := &fakePG{slot: nil}
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{SlotName: "slot_up"}}, Mgr: mgr,
		Primary: func(context.Context) (pg.Client, error) { return primary, nil }}
	step := &verifySlotDrained{d}
	err := step.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing")
}
