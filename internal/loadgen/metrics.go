package loadgen

import (
	"sync"
	"time"
)

// Metrics aggregates non-authoritative live observations: per-workload commit
// counts, per-class error counts, and the unavailability window around the swap.
type Metrics struct {
	mu       sync.Mutex
	commits  map[string]int64
	errors   map[string]int64
	switched bool
	firstErr time.Time // first error after switch
	firstOKB time.Time // first success on pool B after switch
}

func NewMetrics() *Metrics {
	return &Metrics{commits: map[string]int64{}, errors: map[string]int64{}}
}

func (m *Metrics) RecordSwitch(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.switched = true
}

func (m *Metrics) RecordCommit(kind, dsn string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commits[kind]++
	if m.switched && dsn == "b" && m.firstOKB.IsZero() {
		m.firstOKB = t
	}
}

func (m *Metrics) RecordError(class string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors[class]++
	if m.switched && m.firstErr.IsZero() {
		m.firstErr = t
	}
}

// Window returns the unavailability interval. ok is false until the first
// post-switch success on B is seen. If no error preceded that success the
// window is zero-length (seamless swap).
func (m *Metrics) Window() (start, end time.Time, dur time.Duration, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.firstOKB.IsZero() {
		return time.Time{}, time.Time{}, 0, false
	}
	end = m.firstOKB
	start = m.firstErr
	if start.IsZero() || start.After(end) {
		start = end // seamless (no error) or error arrived after recovery
	}
	return start, end, end.Sub(start), true
}

// Snapshot returns copies of the counters for rendering.
func (m *Metrics) Snapshot() (commits, errors map[string]int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	commits, errors = map[string]int64{}, map[string]int64{}
	for k, v := range m.commits {
		commits[k] = v
	}
	for k, v := range m.errors {
		errors[k] = v
	}
	return commits, errors
}
