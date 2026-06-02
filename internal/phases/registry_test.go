package phases

import (
	"path/filepath"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPhases1to4Registry(t *testing.T) {
	ps := Phases1to4(Deps{})
	require.Len(t, ps, 4)
	ids := []string{}
	for _, p := range ps {
		ids = append(ids, p.ID())
	}
	assert.Equal(t, []string{"prepare", "isolate", "drain", "upgrade"}, ids)
}

func TestFirstPhaseMatchesManagerDefault(t *testing.T) {
	m, err := state.NewManager(filepath.Join(t.TempDir(), "s.json"), "test")
	require.NoError(t, err)
	assert.Equal(t, FirstPhase, m.Get().Current)
}
