package phases

import (
	"testing"

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
