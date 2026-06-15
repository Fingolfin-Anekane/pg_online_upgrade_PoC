package diskguard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeReader struct {
	retained int64
	capStr   string
}

func (f fakeReader) SlotRetainedBytes(context.Context, string) (int64, error) { return f.retained, nil }
func (f fakeReader) MaxSlotWALKeepSize(context.Context) (string, error)       { return f.capStr, nil }

func TestMonitorSample(t *testing.T) {
	m := Monitor{Slot: "s", Reader: fakeReader{retained: 800, capStr: "1000"}}
	d, retained, err := m.Sample(context.Background())
	require.NoError(t, err)
	assert.Equal(t, Throttle, d)
	assert.Equal(t, int64(800), retained)
}

func TestMonitorUnbounded(t *testing.T) {
	m := Monitor{Slot: "s", Reader: fakeReader{retained: 1 << 40, capStr: "-1"}}
	d, _, err := m.Sample(context.Background())
	require.NoError(t, err)
	assert.Equal(t, OK, d)
}
