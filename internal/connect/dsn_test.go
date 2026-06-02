package connect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDSNForHostSwapsHost(t *testing.T) {
	out, err := DSNForHost("host=primary.old port=5432 user=postgres dbname=app", "n1.internal")
	require.NoError(t, err)
	assert.Contains(t, out, "host=n1.internal")
	assert.Contains(t, out, "user=postgres")
	assert.Contains(t, out, "dbname=app")
	assert.NotContains(t, out, "primary.old")
}

func TestDSNForHostURLForm(t *testing.T) {
	out, err := DSNForHost("postgres://postgres:secret@primary.old:5432/app", "n1.internal")
	require.NoError(t, err)
	assert.Contains(t, out, "host=n1.internal")
	assert.Contains(t, out, "user=postgres")
	assert.Contains(t, out, "dbname=app")
}

func TestDSNForHostInvalid(t *testing.T) {
	_, err := DSNForHost("=not a dsn=", "n1")
	assert.Error(t, err)
}
