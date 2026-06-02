package loadgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// fakePool embeds Pool so it satisfies the interface without implementing every
// method; only pointer identity matters for the selector test.
type fakePool struct {
	Pool
	name string
}

func TestSelectorSwitchFlipsActive(t *testing.T) {
	a := &fakePool{name: "a"}
	b := &fakePool{name: "b"}
	s := NewSelector(a, b)

	pool, label, phase := s.Active()
	assert.Same(t, a, pool)
	assert.Equal(t, "a", label)
	assert.Equal(t, "pre-switch", phase)

	s.Switch()

	pool, label, phase = s.Active()
	assert.Same(t, b, pool)
	assert.Equal(t, "b", label)
	assert.Equal(t, "post-switch", phase)
}
