package pgbin

import (
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureWorkDirCreatesNestedDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pg_upgrade_output", "run1")
	// OSUser empty -> no chown branch, so this is exercisable without root.
	require.NoError(t, Exec{}.ensureWorkDir(dir))
	st, err := os.Stat(dir)
	require.NoError(t, err)
	assert.True(t, st.IsDir())
}

func TestCredentialParsesUIDGID(t *testing.T) {
	cred, err := credential(&user.User{Uid: "26", Gid: "26"})
	require.NoError(t, err)
	assert.Equal(t, uint32(26), cred.Uid)
	assert.Equal(t, uint32(26), cred.Gid)
}

func TestCredentialRejectsNonNumericUID(t *testing.T) {
	_, err := credential(&user.User{Uid: "postgres", Gid: "26"})
	assert.ErrorContains(t, err, "parse uid")
}

// command() leaves SysProcAttr nil when no OSUser is configured, so non-root /
// already-correct-user runs are unaffected.
func TestCommandNoUserSwitchByDefault(t *testing.T) {
	cmd, err := Exec{}.command(t.Context(), "/bin/true")
	require.NoError(t, err)
	assert.Nil(t, cmd.SysProcAttr)
}

func TestParseControlData(t *testing.T) {
	out := `pg_control version number:            1300
Database cluster state:               in production
Database system identifier:           7361852939023499998
Latest checkpoint location:           0/3FA20000`

	cd := parseControlData(out)
	assert.Equal(t, "in production", cd.State)
	assert.Equal(t, "7361852939023499998", cd.SystemID)
}

func TestParseControlDataShutDown(t *testing.T) {
	out := `Database cluster state:               shut down
Database system identifier:           42`
	cd := parseControlData(out)
	assert.Equal(t, "shut down", cd.State)
	assert.Equal(t, "42", cd.SystemID)
}

func TestParseControlDataMissingFields(t *testing.T) {
	cd := parseControlData("garbage\nno fields here")
	assert.Equal(t, "", cd.State)
	assert.Equal(t, "", cd.SystemID)
}

func TestParseControlDataCRLF(t *testing.T) {
	cd := parseControlData("Database cluster state:               in production\r\nDatabase system identifier:           42\r\n")
	assert.Equal(t, "in production", cd.State)
	assert.Equal(t, "42", cd.SystemID)
}
