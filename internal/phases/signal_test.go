package phases

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDSNSwapSignalPayload(t *testing.T) {
	var captured []byte
	var capturedPath string
	write := func(p string, data []byte) error { capturedPath = p; captured = data; return nil }
	err := WriteDSNSwapSignal(write, "/run/sig.json", "host=n1 port=5433 dbname=app", "prod")
	require.NoError(t, err)
	assert.Equal(t, "/run/sig.json", capturedPath)

	var p DSNSwapSignal
	require.NoError(t, json.Unmarshal(captured, &p))
	assert.Equal(t, "host=n1 port=5433 dbname=app", p.NewPrimaryDSN)
	assert.Equal(t, "prod", p.ClusterName)
	assert.Equal(t, "swap-dsn", p.Action)
	assert.False(t, p.WrittenAt.IsZero(), "written_at must be set and survive round-trip")
}

func TestDSNSwapSignalWriteError(t *testing.T) {
	writeErr := errors.New("disk full")
	err := WriteDSNSwapSignal(func(_ string, _ []byte) error { return writeErr }, "/p", "dsn", "cl")
	require.ErrorIs(t, err, writeErr)
}
