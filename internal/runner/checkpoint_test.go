package runner

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInteractiveCheckpointAccepts(t *testing.T) {
	in := strings.NewReader("y\n")
	var out bytes.Buffer
	cp := InteractiveCheckpoint(in, &out, map[PhaseID]string{"prepare": "Proceed to isolate?"})
	require.NoError(t, cp(context.Background(), "prepare", "isolate"))
	assert.Contains(t, out.String(), "Proceed to isolate?")
}

func TestInteractiveCheckpointDeclines(t *testing.T) {
	in := strings.NewReader("n\n")
	var out bytes.Buffer
	cp := InteractiveCheckpoint(in, &out, nil)
	err := cp(context.Background(), "prepare", "isolate")
	assert.Error(t, err)
}

func TestInteractiveCheckpointEmptyDeclines(t *testing.T) {
	in := strings.NewReader("\n") // default is No
	var out bytes.Buffer
	cp := InteractiveCheckpoint(in, &out, nil)
	assert.Error(t, cp(context.Background(), "prepare", "isolate"))
}
