package phases

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanupArchivesAndVerifiesStopped(t *testing.T) {
	srcDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "pg_upgrade.log"), []byte("ok"), 0o644))
	archiveDir := filepath.Join(t.TempDir(), "archive")

	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade", "catchup", "switchover", "finalize", "cleanup"} {
		require.NoError(t, mgr.Advance(p))
	}
	// old primary is down -> IsInRecovery errors
	oldPrimary := &fakePG{inRecoveryErr: errors.New("connection refused")}
	d := Deps{
		Cfg: config.Config{Upgrade: config.UpgradeConfig{
			PgUpgradeLogDir: srcDir, LogArchiveDir: archiveDir,
		}},
		Mgr:     mgr,
		Primary: func(context.Context) (pg.Client, error) { return oldPrimary, nil },
	}

	ph := NewCleanup(d)
	assert.Equal(t, "cleanup", ph.ID())
	assert.Empty(t, ph.Transitions()) // terminal
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	data, err := os.ReadFile(filepath.Join(archiveDir, "pg_upgrade.log"))
	require.NoError(t, err)
	assert.Equal(t, "ok", string(data))
}

func TestVerifyOldPrimaryStoppedErrorsWhenReachable(t *testing.T) {
	oldPrimary := &fakePG{} // IsInRecovery returns (false, nil) -> still reachable
	d := Deps{Primary: func(context.Context) (pg.Client, error) { return oldPrimary, nil }}
	err := (&verifyOldPrimaryStopped{d}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still reachable")
}

func TestVerifyOldPrimaryStoppedErrorsOnProviderFailure(t *testing.T) {
	d := Deps{Primary: func(context.Context) (pg.Client, error) { return nil, errors.New("bad dsn") }}
	err := (&verifyOldPrimaryStopped{d}).Run(context.Background())
	require.Error(t, err) // provider error is surfaced, not treated as "down"
	assert.Contains(t, err.Error(), "cannot reach old primary")
}
