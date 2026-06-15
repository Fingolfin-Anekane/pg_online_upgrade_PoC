package diskguard

import "context"

// Reader is the subset of the pg client the monitor needs (the prod primary).
type Reader interface {
	SlotRetainedBytes(ctx context.Context, slot string) (int64, error)
	MaxSlotWALKeepSize(ctx context.Context) (string, error)
}

// Monitor samples the slot's disk pressure and turns it into a Decision.
type Monitor struct {
	Slot   string
	Reader Reader
}

// Sample reads the slot's retained bytes and the cap, and returns the current
// Decision plus retained bytes (for logging).
func (m Monitor) Sample(ctx context.Context) (Decision, int64, error) {
	retained, err := m.Reader.SlotRetainedBytes(ctx, m.Slot)
	if err != nil {
		return OK, 0, err
	}
	capStr, err := m.Reader.MaxSlotWALKeepSize(ctx)
	if err != nil {
		return OK, retained, err
	}
	capBytes, err := ParseSize(capStr)
	if err != nil {
		return OK, retained, err
	}
	return Decide(retained, capBytes), retained, nil
}
