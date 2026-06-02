package phases

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDSNSwapSignalPayload(t *testing.T) {
	var captured []byte
	write := func(_ string, data []byte) error { captured = data; return nil }
	err := WriteDSNSwapSignal(write, "/run/sig.json", "host=n1 port=5433 dbname=app", "prod")
	require.NoError(t, err)

	var p DSNSwapSignal
	require.NoError(t, json.Unmarshal(captured, &p))
	assert.Equal(t, "host=n1 port=5433 dbname=app", p.NewPrimaryDSN)
	assert.Equal(t, "prod", p.ClusterName)
	assert.Equal(t, "swap-dsn", p.Action)
}
