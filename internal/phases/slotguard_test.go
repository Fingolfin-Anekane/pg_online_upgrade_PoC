package phases

import (
	"testing"
	"time"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaxSlotWALKeepSizeRisky(t *testing.T) {
	assert.False(t, maxSlotWALKeepSizeRisky(""))   // pre-PG13: GUC absent
	assert.False(t, maxSlotWALKeepSizeRisky("-1")) // unlimited
	assert.True(t, maxSlotWALKeepSizeRisky("0"))   // most dangerous
	assert.True(t, maxSlotWALKeepSizeRisky("1024MB"))
}

func TestSlotRiskWarnings(t *testing.T) {
	const thr = 5 * time.Minute
	assert.Empty(t, slotRiskWarnings("-1", time.Minute, thr), "safe config -> no warnings")

	w := slotRiskWarnings("512MB", 0, thr)
	require.Len(t, w, 1)
	assert.Contains(t, w[0], "max_slot_wal_keep_size")

	w = slotRiskWarnings("-1", 10*time.Minute, thr)
	require.Len(t, w, 1)
	assert.Contains(t, w[0], "транзакц")

	assert.Len(t, slotRiskWarnings("0", 10*time.Minute, thr), 2) // both conditions
}

func TestSlotWALStatusReserved(t *testing.T) {
	for _, ok := range []string{"", "reserved", "extended"} {
		assert.True(t, slotWALStatusReserved(ok), "status %q must be safe", ok)
	}
	for _, bad := range []string{"unreserved", "lost"} {
		assert.False(t, slotWALStatusReserved(bad), "status %q must trip the guard", bad)
	}
}

func TestAssertSlotReserved(t *testing.T) {
	assert.NoError(t, assertSlotReserved(nil)) // missing slot handled by callers
	assert.NoError(t, assertSlotReserved(&pg.ReplicationSlot{Name: "s", WALStatus: "reserved"}))
	assert.NoError(t, assertSlotReserved(&pg.ReplicationSlot{Name: "s", WALStatus: ""})) // pre-PG13

	err := assertSlotReserved(&pg.ReplicationSlot{Name: "slot_up", WALStatus: "lost"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalidated")
	assert.Contains(t, err.Error(), "lost")
}
