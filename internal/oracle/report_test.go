package oracle

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderPass(t *testing.T) {
	var b bytes.Buffer
	f := &Findings{SumExpected: 100000, SumActual: 100000, LastValue: 50, MaxID: 50}
	require.NoError(t, Render(&b, f))
	assert.Contains(t, b.String(), "VERDICT: PASS")
}

func TestRenderFailListsFindings(t *testing.T) {
	var b bytes.Buffer
	f := &Findings{Lost: []string{"o2"}, DupID: 1, SumExpected: 100000, SumActual: 99000}
	require.NoError(t, Render(&b, f))
	out := b.String()
	assert.Contains(t, out, "FAIL")
	assert.Contains(t, out, "LOST")
	assert.Contains(t, out, "o2")
	assert.Contains(t, out, "duplicate id")
	assert.Contains(t, out, "sum mismatch")
}

func TestRenderSeqBehindMarker(t *testing.T) {
	var b bytes.Buffer
	f := &Findings{SumExpected: 100000, SumActual: 100000, LastValue: 4, MaxID: 5, SeqBehind: true}
	require.NoError(t, Render(&b, f))
	out := b.String()
	assert.Contains(t, out, "SEQUENCE BEHIND")
	assert.Contains(t, out, "VERDICT: FAIL")
}
