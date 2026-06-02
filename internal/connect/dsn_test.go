package connect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseOf(t *testing.T) {
	db, err := DatabaseOf("host=p user=postgres dbname=app port=5432")
	require.NoError(t, err)
	assert.Equal(t, "app", db)

	db, err = DatabaseOf("postgres://postgres@p:5432/app")
	require.NoError(t, err)
	assert.Equal(t, "app", db)
}

func TestDSNForHostSwapsHost(t *testing.T) {
	out, err := DSNForHost("host=primary.old port=5432 user=postgres dbname=app", "n1.internal")
	require.NoError(t, err)
	assert.Contains(t, out, "host=n1.internal")
	assert.Contains(t, out, "user=postgres")
	assert.Contains(t, out, "dbname=app")
	assert.NotContains(t, out, "primary.old")
	assert.Contains(t, out, "port=5432")
}

func TestDSNForHostURLForm(t *testing.T) {
	out, err := DSNForHost("postgres://postgres:secret@primary.old:5432/app", "n1.internal")
	require.NoError(t, err)
	assert.Contains(t, out, "host=n1.internal")
	assert.Contains(t, out, "user=postgres")
	assert.Contains(t, out, "dbname=app")
	assert.Contains(t, out, "password=secret")
}

func TestDSNForHostInvalid(t *testing.T) {
	_, err := DSNForHost("=not a dsn=", "n1")
	assert.Error(t, err)
}

func TestDSNForHostQuotesSpaces(t *testing.T) {
	out, err := DSNForHost("host=p user=postgres dbname=app application_name='my app'", "n1")
	require.NoError(t, err)
	assert.Contains(t, out, "application_name='my app'")
	// round-trips: the output must itself parse back without error
	_, perr := DSNForHost(out, "n2")
	require.NoError(t, perr)
}
