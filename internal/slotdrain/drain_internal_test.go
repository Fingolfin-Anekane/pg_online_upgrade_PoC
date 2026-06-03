package slotdrain

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
)

// reachedTarget is the stop predicate that fixes the indefinite-wait hang: the
// drain must terminate once the server's WAL stream reaches target, even if no
// transaction ever commits beyond target.
func TestReachedTarget(t *testing.T) {
	target, _ := pglogrepl.ParseLSN("0/3FA20000")
	below, _ := pglogrepl.ParseLSN("0/3FA10000")
	above, _ := pglogrepl.ParseLSN("0/3FA30000")

	assert.False(t, reachedTarget(below, target), "below target: keep draining")
	assert.True(t, reachedTarget(target, target), "exactly at target: done")
	assert.True(t, reachedTarget(above, target), "past target: done")
	assert.False(t, reachedTarget(0, target), "no position reported yet: keep draining")
}
