package phases

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyTree(t *testing.T) {
	src := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(src, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("world"), 0o600))

	dst := filepath.Join(t.TempDir(), "archive")
	require.NoError(t, copyTree(src, dst))

	a, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, "hello", string(a))
	b, err := os.ReadFile(filepath.Join(dst, "sub", "b.txt"))
	require.NoError(t, err)
	assert.Equal(t, "world", string(b))

	// permissions are preserved
	st, err := os.Stat(filepath.Join(dst, "sub", "b.txt"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), st.Mode().Perm())
}

func TestCopyTreeMissingSource(t *testing.T) {
	err := copyTree(filepath.Join(t.TempDir(), "nope"), t.TempDir())
	require.Error(t, err)
}
