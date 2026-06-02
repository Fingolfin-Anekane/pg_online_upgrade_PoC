package loadgen

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUnavailabilityWindow(t *testing.T) {
	m := NewMetrics()
	base := time.Unix(1000, 0)

	m.RecordCommit("append", "a", base) // pre-switch success, ignored for window
	m.RecordSwitch(base.Add(1 * time.Second))
	m.RecordError("read-only", base.Add(2*time.Second))    // first error after switch
	m.RecordError("read-only", base.Add(3*time.Second))    // later error, does not move start
	m.RecordCommit("append", "b", base.Add(5*time.Second)) // first success on B closes window

	start, end, dur, ok := m.Window()
	assert.True(t, ok)
	assert.Equal(t, base.Add(2*time.Second), start)
	assert.Equal(t, base.Add(5*time.Second), end)
	assert.Equal(t, 3*time.Second, dur)
}

func TestUnavailabilityWindowSeamless(t *testing.T) {
	m := NewMetrics()
	base := time.Unix(2000, 0)
	m.RecordSwitch(base)
	m.RecordCommit("append", "b", base.Add(10*time.Millisecond)) // success, no prior error
	_, _, dur, ok := m.Window()
	assert.True(t, ok)
	assert.Equal(t, time.Duration(0), dur)
}

func TestWindowNotReadyBeforeCommitOnB(t *testing.T) {
	m := NewMetrics()
	base := time.Unix(3000, 0)
	m.RecordSwitch(base)
	m.RecordError("read-only", base.Add(time.Second))
	_, _, _, ok := m.Window()
	assert.False(t, ok) // no success on B yet
}

func TestPostSwitchCommitOnADoesNotCloseWindow(t *testing.T) {
	m := NewMetrics()
	base := time.Unix(4000, 0)
	m.RecordSwitch(base)
	m.RecordError("read-only", base.Add(time.Second))
	m.RecordCommit("append", "a", base.Add(2*time.Second)) // still on A, must not close window
	_, _, _, ok := m.Window()
	assert.False(t, ok)
	m.RecordCommit("append", "b", base.Add(3*time.Second)) // now B closes it
	_, _, dur, ok := m.Window()
	assert.True(t, ok)
	assert.Equal(t, 2*time.Second, dur)
}

func TestSnapshotIsolation(t *testing.T) {
	m := NewMetrics()
	m.RecordCommit("append", "a", time.Unix(5000, 0))
	commits, _ := m.Snapshot()
	commits["append"] = 999 // mutate the returned copy
	commits2, _ := m.Snapshot()
	assert.Equal(t, int64(1), commits2["append"]) // internal state unaffected
}
