package slotdrain_test

import (
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/slotdrain"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
)

func TestLSNComparison_BelowTarget(t *testing.T) {
	target, _ := pglogrepl.ParseLSN("0/3FA20000")
	commit, _ := pglogrepl.ParseLSN("0/3FA10000")
	assert.True(t, commit <= target, "commit below target should be ACKed")
}

func TestLSNComparison_AboveTarget(t *testing.T) {
	target, _ := pglogrepl.ParseLSN("0/3FA20000")
	commit, _ := pglogrepl.ParseLSN("0/3FA30000")
	assert.True(t, commit > target, "commit above target should stop drain")
}

func TestConfig_Validate_MissingConnString(t *testing.T) {
	cfg := slotdrain.Config{
		SlotName:  "slot_upgrade",
		PubName:   "pub_upgrade",
		TargetLSN: "0/3FA20000",
	}
	assert.ErrorContains(t, cfg.Validate(), "conn_string")
}

func TestConfig_Validate_MissingSlotName(t *testing.T) {
	cfg := slotdrain.Config{
		ConnString: "host=primary port=5432 dbname=postgres user=postgres",
		PubName:    "pub_upgrade",
		TargetLSN:  "0/3FA20000",
	}
	assert.ErrorContains(t, cfg.Validate(), "slot_name")
}

func TestConfig_Validate_InvalidTargetLSN(t *testing.T) {
	cfg := slotdrain.Config{
		ConnString: "host=primary port=5432 dbname=postgres user=postgres",
		SlotName:   "slot_upgrade",
		PubName:    "pub_upgrade",
		TargetLSN:  "not-a-lsn",
	}
	assert.ErrorContains(t, cfg.Validate(), "target_lsn")
}

func TestConfig_Validate_Valid(t *testing.T) {
	cfg := slotdrain.Config{
		ConnString: "host=primary port=5432 dbname=postgres user=postgres",
		SlotName:   "slot_upgrade",
		PubName:    "pub_upgrade",
		TargetLSN:  "0/3FA20000",
	}
	assert.NoError(t, cfg.Validate())
}
