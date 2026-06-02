package phases

import (
	"encoding/json"
	"fmt"
	"time"
)

// DSNSwapSignal is the payload the binary writes for external tooling to perform
// the client DSN swap from the old primary to the new PG17 cluster.
type DSNSwapSignal struct {
	Action        string    `json:"action"` // always "swap-dsn"
	ClusterName   string    `json:"cluster_name"`
	NewPrimaryDSN string    `json:"new_primary_dsn"`
	WrittenAt     time.Time `json:"written_at"`
}

// WriteDSNSwapSignal serializes the swap signal and persists it via write.
func WriteDSNSwapSignal(write func(path string, data []byte) error, path, newDSN, clusterName string) error {
	payload := DSNSwapSignal{
		Action:        "swap-dsn",
		ClusterName:   clusterName,
		NewPrimaryDSN: newDSN,
		WrittenAt:     time.Now().UTC(),
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("signal: marshal: %w", err)
	}
	if err := write(path, data); err != nil {
		return fmt.Errorf("signal: write %s: %w", path, err)
	}
	return nil
}
