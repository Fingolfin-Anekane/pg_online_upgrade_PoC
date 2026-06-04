package phases

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// catchup-phase methods on the shared fakePG. SubscriptionExists is the
// subscriber-side existence check (PG17); GetSubscriptionLag is the
// publisher-side lag (old primary).
func (f *fakePG) SubscriptionExists(_ context.Context, name string) (bool, error) {
	return f.subExists, nil
}
func (f *fakePG) SubscriptionEnabled(_ context.Context, name string) (bool, error) {
	return f.subEnabled, nil
}
func (f *fakePG) GetSubscriptionLag(_ context.Context, name string) (*pg.SubscriptionLag, error) {
	return f.subLag, nil
}
func (f *fakePG) CreateSubscription(_ context.Context, name, connStr, pubName, slotName string) error {
	f.createdSub = name
	f.subExists = true // subscription now exists on the subscriber
	return nil
}

func TestCatchupCreatesSubAndWaitsLag(t *testing.T) {
	mgr := testMgr(t)
	for _, p := range []string{"isolate", "drain", "upgrade", "catchup"} {
		require.NoError(t, mgr.Advance(p))
	}
	require.NoError(t, mgr.SetPrimaryHost("primary.host"))

	patroniCfg := filepath.Join(t.TempDir(), "patroni.yml")
	// scope/data_dir/bin_dir already match targets -> PatchNewPatroniConfig is
	// skipped, keeping this test focused on the subscription + lag steps.
	require.NoError(t, os.WriteFile(patroniCfg, []byte("scope: prod-17\npostgresql:\n  data_dir: /nd\n  bin_dir: /n\n"), 0o644))
	pg17 := &fakePG{}                                    // subscription created in the loop (subExists flips true)
	oldPrimary := &fakePG{subLag: &pg.SubscriptionLag{}} // publisher: zero lag
	// old cluster stopped (oldRunning false), PG17 already up (running) -> start skipped
	tools := &fakeTools{running: true, newDataDir: "/nd"}
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "n1", Host: "n1", Role: "leader"},
		{Name: "n2", Host: "n2", Role: "replica"},
	}}}
	d := Deps{
		Cfg: config.Config{ClusterName: "prod", Upgrade: config.UpgradeConfig{
			SubscriptionName: "sub_up", PublicationName: "pub_up", SlotName: "slot_up",
			NewPGBindir: "/n", NewDataDir: "/nd", PatroniConfigPath: patroniCfg,
		}, PG: config.PGConfig{SuperuserDSN: "host=tmpl"}},
		Mgr: mgr, Tools: tools, NewPatroni: newPat,
		PG17:    func(context.Context) (pg.Client, error) { return pg17, nil },
		Primary: func(context.Context) (pg.Client, error) { return oldPrimary, nil },
	}

	ph := NewCatchup(d)
	assert.Equal(t, "catchup", ph.ID())
	for _, s := range ph.Steps() {
		done, err := s.Check(context.Background())
		require.NoError(t, err)
		if !done {
			require.NoError(t, s.Run(context.Background()))
		}
	}
	assert.Equal(t, "sub_up", pg17.createdSub)
}

func TestStartPG17Runs(t *testing.T) {
	tools := &fakeTools{newDataDir: "/nd"} // not running yet; StartPatroni flips running=true
	writable := &fakePG{inRecovery: false} // Patroni promoted PG17 to a writable primary
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{
		NewPGBindir: "/n", NewDataDir: "/nd", PatroniStartCommand: "systemctl start patroni",
	}}, Tools: tools, PG17: func(context.Context) (pg.Client, error) { return writable, nil }}
	require.NoError(t, (&startPG17{d}).Run(context.Background()))
	assert.Equal(t, "systemctl start patroni", tools.patroniStarted)
	assert.True(t, tools.running) // PG17 came up under Patroni
}

func TestWaitWritableReturnsWhenPrimary(t *testing.T) {
	writable := func(context.Context) (pg.Client, error) { return &fakePG{inRecovery: false}, nil }
	require.NoError(t, waitWritable(context.Background(), writable, 3, time.Millisecond))
}

func TestWaitWritableTimesOutWhileStandby(t *testing.T) {
	// Patroni starts PG17 read-only (in recovery) before promoting it; creating a
	// subscription then fails 25006, so we must wait out this window.
	standby := func(context.Context) (pg.Client, error) { return &fakePG{inRecovery: true}, nil }
	err := waitWritable(context.Background(), standby, 2, time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "writable primary")
}

func TestStartPG17SkipsWhenAlreadyRunning(t *testing.T) {
	tools := &fakeTools{running: true, newDataDir: "/nd"} // pg_ctl status reports a live postmaster
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{NewPGBindir: "/n", NewDataDir: "/nd"}}, Tools: tools}
	done, err := (&startPG17{d}).Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done) // already up -> do not start a second postmaster
}

func TestVerifyOldClusterStopped_RejectsRunningOldServer(t *testing.T) {
	tools := &fakeTools{oldRunning: true} // old postgres still alive on the old data dir
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{OldPGBindir: "/o", DataDir: "/data/old"}}, Tools: tools}
	err := (&verifyOldClusterStopped{d}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OLD postgres is still running")
}

func TestVerifyOldClusterStopped_AcceptsStoppedOldServer(t *testing.T) {
	tools := &fakeTools{oldRunning: false}
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{OldPGBindir: "/o", DataDir: "/data/old"}}, Tools: tools}
	require.NoError(t, (&verifyOldClusterStopped{d}).Run(context.Background()))
}

// patchPatroniDeps writes body to a temp patroni.yml and returns Deps pointing
// the catchup PatchNewPatroniConfig step at it. Returns the path too.
func patchPatroniDeps(t *testing.T, body string) (Deps, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "patroni.yml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
	d := Deps{Mgr: testMgr(t), Cfg: config.Config{ClusterName: "prod", Upgrade: config.UpgradeConfig{
		PatroniConfigPath: p, NewDataDir: "/data/pg17", NewPGBindir: "/usr/lib/postgresql/17/bin",
	}}}
	return d, p
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}

func TestPatchNewPatroniConfig_PatchesStaleFileAndBacksUp(t *testing.T) {
	d, p := patchPatroniDeps(t, "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /usr/lib/postgresql/13/bin\n")
	done, err := (&patchNewPatroniConfig{d}).Check(context.Background())
	require.NoError(t, err)
	assert.False(t, done)

	require.NoError(t, (&patchNewPatroniConfig{d}).Run(context.Background()))

	fields, err := parsePatroniManagedFields(readFile(t, p))
	require.NoError(t, err)
	assert.Equal(t, "prod-17", fields.Scope)
	assert.Equal(t, "/data/pg17", fields.DataDir)
	assert.Equal(t, "/usr/lib/postgresql/17/bin", fields.BinDir)

	bak := readFile(t, p+".bak")
	assert.Contains(t, string(bak), "data_dir: /data/pg13")

	done, err = (&patchNewPatroniConfig{d}).Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done)
}

func TestPatchNewPatroniConfig_DoesNotOverwriteExistingBak(t *testing.T) {
	d, p := patchPatroniDeps(t, "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /b13\n")
	require.NoError(t, os.WriteFile(p+".bak", []byte("ORIGINAL-BAK"), 0o644))
	require.NoError(t, (&patchNewPatroniConfig{d}).Run(context.Background()))
	assert.Equal(t, "ORIGINAL-BAK", string(readFile(t, p+".bak")))
}

func TestPatchNewPatroniConfig_PatchesConfigDirWhenSet(t *testing.T) {
	d, p := patchPatroniDeps(t, "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /b13\n  config_dir: /etc/postgresql/13/main\n")
	d.Cfg.Upgrade.NewConfigDir = "/etc/postgresql/17/main"
	require.NoError(t, (&patchNewPatroniConfig{d}).Run(context.Background()))
	fields, err := parsePatroniManagedFields(readFile(t, p))
	require.NoError(t, err)
	assert.Equal(t, "/etc/postgresql/17/main", fields.ConfigDir)
}

func TestCatchupTransitionsToSwitchover(t *testing.T) {
	ph := NewCatchup(Deps{})
	tr := ph.Transitions()
	require.Len(t, tr, 1)
	assert.Equal(t, "switchover", tr[0].To)
}

func TestCreateForwardSubscriptionSkipsWhenExists(t *testing.T) {
	pg17 := &fakePG{subExists: true} // subscription already exists on the subscriber
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{SubscriptionName: "sub_up"}},
		PG17: func(context.Context) (pg.Client, error) { return pg17, nil }}
	done, err := (&createForwardSubscription{d}).Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done)
	assert.Equal(t, "", pg17.createdSub) // create skipped
}

func TestVerifyNewClusterHealthyRejectsNoLeader(t *testing.T) {
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{{Name: "a", Role: "replica"}}}}
	err := (&verifyNewClusterHealthy{Deps{NewPatroni: newPat}}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no leader")
}

func TestVerifyNewClusterHealthyAcceptsLeaderOnly(t *testing.T) {
	// A single-node cluster (leader, no standby yet) is healthy enough for catchup;
	// the standby is an HA nicety the operator can add before switchover.
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{{Name: "n1", Role: "leader"}}}}
	require.NoError(t, (&verifyNewClusterHealthy{Deps{NewPatroni: newPat}}).Run(context.Background()))
}

func TestVerifyNewClusterHealthyAcceptsSyncStandby(t *testing.T) {
	newPat := &fakePatroni{cluster: &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "n1", Role: "leader"}, {Name: "n2", Role: "sync_standby"},
	}}}
	require.NoError(t, (&verifyNewClusterHealthy{Deps{NewPatroni: newPat}}).Run(context.Background()))
}

func TestWaitLagZeroErrorsWhenBehind(t *testing.T) {
	primary := &fakePG{subLag: &pg.SubscriptionLag{ByteLag: 500}} // publisher reports lag (bytes behind)
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{SubscriptionName: "sub_up"}},
		Primary: func(context.Context) (pg.Client, error) { return primary, nil }}
	step := &waitLagZero{d}
	done, err := step.Check(context.Background())
	require.NoError(t, err)
	assert.False(t, done)
	require.Error(t, step.Run(context.Background()))
}
