package phases

import (
	"fmt"
	"time"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
)

// longTxnWarnThreshold is how old an open transaction on the primary must be
// before preflight warns about it: a long transaction pins the slot's
// restart_lsn/catalog_xmin far back (pg_wal + catalog bloat) and can stall
// pg_create_logical_replication_slot until it finishes.
const longTxnWarnThreshold = 5 * time.Minute

// maxSlotWALKeepSizeRisky reports whether max_slot_wal_keep_size can invalidate
// the slot. "" (pre-PG13, GUC absent) and "-1" (unlimited) are safe; any other
// value bounds retained WAL, so the slot can be dropped if it falls behind.
func maxSlotWALKeepSizeRisky(setting string) bool {
	return setting != "" && setting != "-1"
}

// slotRiskWarnings returns advisory (non-fatal) messages for conditions that
// threaten the logical slot during the upgrade window. The HARD stop, if a slot
// actually gets invalidated, is assertSlotReserved in drain/catchup.
func slotRiskWarnings(maxKeep string, oldestTxn, txnThreshold time.Duration) []string {
	var w []string
	if maxSlotWALKeepSizeRisky(maxKeep) {
		w = append(w, fmt.Sprintf("max_slot_wal_keep_size=%s (не -1): если слот отстанет больше этого размера, PostgreSQL ИНВАЛИДИРУЕТ его (wal_status=lost) и снесёт WAL — хвост изменений потеряется. На время апгрейда поставь -1 или заведомо большое значение", maxKeep))
	}
	if oldestTxn >= txnThreshold {
		w = append(w, fmt.Sprintf("на primary открыта транзакция возрастом %s: она пиннит restart_lsn/catalog_xmin слота далеко назад (распухание pg_wal и каталога) и может подвесить создание слота до её завершения — заверши долгие транзакции перед апгрейдом", oldestTxn.Round(time.Second)))
	}
	return w
}

// slotWALStatusReserved reports whether a slot's wal_status means its WAL is
// still safely retained for the upgrade. Empty = PG10–12 (no
// max_slot_wal_keep_size, slots never size-invalidated). reserved/extended =
// safe. unreserved = the WAL the slot needs is no longer guaranteed (loss
// imminent); lost = that WAL has already been removed (slot dead).
func slotWALStatusReserved(status string) bool {
	switch status {
	case "", "reserved", "extended":
		return true
	default: // "unreserved", "lost"
		return false
	}
}

// assertSlotReserved fails loudly when a logical slot is being invalidated by
// max_slot_wal_keep_size. This is the one real data-loss path a long-running
// transaction introduces: it pins restart_lsn far back, the old primary keeps
// writing during the upgrade window, and once WAL exceeds max_slot_wal_keep_size
// PostgreSQL drops it and the slot can no longer deliver the post-target tail.
// Catching it here turns a silent tail loss into a hard stop. A nil slot is the
// caller's "slot missing" case, handled separately.
func assertSlotReserved(slot *pg.ReplicationSlot) error {
	if slot == nil || slotWALStatusReserved(slot.WALStatus) {
		return nil
	}
	return fmt.Errorf("slot %q is being invalidated by max_slot_wal_keep_size (wal_status=%q): the WAL holding the post-target tail is being removed, so the forward subscription would silently lose changes. Raise max_slot_wal_keep_size (or set -1) / reduce write volume, then restart the upgrade from prepare so the slot is recreated", slot.Name, slot.WALStatus)
}
