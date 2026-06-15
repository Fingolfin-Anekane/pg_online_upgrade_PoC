package diskguard

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecide(t *testing.T) {
	// cap unbounded -> always OK
	assert.Equal(t, OK, Decide(1<<40, 0))
	assert.Equal(t, OK, Decide(1<<40, -1))
	// below throttle fraction (0.75) -> OK
	assert.Equal(t, OK, Decide(700, 1000))
	// in [0.75, 1.0) -> Throttle
	assert.Equal(t, Throttle, Decide(800, 1000))
	// at/over cap -> Abort
	assert.Equal(t, Abort, Decide(1000, 1000))
	assert.Equal(t, Abort, Decide(1200, 1000))
}

func TestParseSize(t *testing.T) {
	b, err := ParseSize("1024MB")
	assert.NoError(t, err)
	assert.Equal(t, int64(1024)*1024*1024, b)
	b, err = ParseSize("-1")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), b)
	b, err = ParseSize("")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), b)
	_, err = ParseSize("garbage")
	assert.Error(t, err)
}
