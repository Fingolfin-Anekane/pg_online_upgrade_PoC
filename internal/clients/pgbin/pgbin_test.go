package pgbin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseControlData(t *testing.T) {
	out := `pg_control version number:            1300
Database cluster state:               in production
Database system identifier:           7361852939023499998
Latest checkpoint location:           0/3FA20000`

	cd := parseControlData(out)
	assert.Equal(t, "in production", cd.State)
	assert.Equal(t, "7361852939023499998", cd.SystemID)
}

func TestParseControlDataShutDown(t *testing.T) {
	out := `Database cluster state:               shut down
Database system identifier:           42`
	cd := parseControlData(out)
	assert.Equal(t, "shut down", cd.State)
	assert.Equal(t, "42", cd.SystemID)
}

func TestParseControlDataMissingFields(t *testing.T) {
	cd := parseControlData("garbage\nno fields here")
	assert.Equal(t, "", cd.State)
	assert.Equal(t, "", cd.SystemID)
}

func TestParseControlDataCRLF(t *testing.T) {
	cd := parseControlData("Database cluster state:               in production\r\nDatabase system identifier:           42\r\n")
	assert.Equal(t, "in production", cd.State)
	assert.Equal(t, "42", cd.SystemID)
}
