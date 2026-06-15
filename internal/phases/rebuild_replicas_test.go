package phases

import (
	"context"
	"testing"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/diskguard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeMemberPatroni implements the FULL patroni.Client interface. If the
// interface has methods beyond those below, add them (no-op) so it compiles.
type fakeMemberPatroni struct{ reinitialized bool }

func (f *fakeMemberPatroni) GetCluster(context.Context) (*patroni.ClusterInfo, error) { return nil, nil }
func (f *fakeMemberPatroni) NodePaused(context.Context) (bool, error)                 { return false, nil }
func (f *fakeMemberPatroni) Pause(context.Context) error                              { return nil }
func (f *fakeMemberPatroni) Resume(context.Context) error                             { return nil }
func (f *fakeMemberPatroni) SetStandbyCluster(context.Context, string, int, string) error {
	return nil
}
func (f *fakeMemberPatroni) ClearStandbyCluster(context.Context) error { return nil }
func (f *fakeMemberPatroni) Reinitialize(context.Context) error        { f.reinitialized = true; return nil }

type fakeGuard struct{ d diskguard.Decision }

func (g fakeGuard) Sample(context.Context) (diskguard.Decision, int64, error) { return g.d, 0, nil }

func setRebuildTimingForTest(t *testing.T) func() {
	t.Helper()
	o1, o2 := rebuildPollInterval, rebuildTimeout
	rebuildPollInterval, rebuildTimeout = time.Millisecond, time.Second
	return func() { rebuildPollInterval, rebuildTimeout = o1, o2 }
}

func TestRebuildReplicasReinitsEachReplica(t *testing.T) {
	defer setRebuildTimingForTest(t)()
	member := &fakeMemberPatroni{}
	cluster := &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "l", Host: "l", Role: "leader"},
		{Name: "r2", Host: "r2", Role: "replica"},
	}}
	d := Deps{
		NewPatroni:   &fakePatroni{cluster: cluster},
		ShadowMember: func(string) patroni.Client { return member },
		DiskGuard:    fakeGuard{d: diskguard.OK},
		Cfg:          config.Config{Upgrade: config.UpgradeConfig{ShadowPatroniURL: "http://l:8008"}},
	}
	require.NoError(t, (&reinitShadowReplicas{d}).Run(context.Background()))
	assert.True(t, member.reinitialized)
}

func TestRebuildReplicasAbortsOnDiskGuard(t *testing.T) {
	defer setRebuildTimingForTest(t)()
	cluster := &patroni.ClusterInfo{Members: []patroni.Member{
		{Name: "l", Host: "l", Role: "leader"}, {Name: "r2", Host: "r2", Role: "replica"},
	}}
	d := Deps{NewPatroni: &fakePatroni{cluster: cluster},
		ShadowMember: func(string) patroni.Client { return &fakeMemberPatroni{} },
		DiskGuard:    fakeGuard{d: diskguard.Abort},
		Cfg:          config.Config{Upgrade: config.UpgradeConfig{ShadowPatroniURL: "http://l:8008"}}}
	err := (&reinitShadowReplicas{d}).Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk")
}
