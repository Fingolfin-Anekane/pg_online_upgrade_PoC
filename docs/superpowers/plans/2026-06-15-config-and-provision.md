# Config + Provision Phase Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the configuration and the new `provision` phase that turns the pre-existing shadow cluster into a Patroni `standby_cluster` of prod, creates the physical slot on prod, and waits until the shadow is caught up.

**Architecture:** New `UpgradeConfig` fields describe the shadow cluster. A new Patroni method patches the shadow's dynamic config with a `standby_cluster` block; a new pg method creates the physical slot on prod. The `provision` phase strings these together plus a catch-up wait (compare prod `pg_current_wal_lsn()` to the shadow leader's `pg_last_wal_replay_lsn()` — valid because physical replication preserves LSNs).

**Tech Stack:** Go, pgx/v5, pgxmock, net/http + httptest, the existing `internal/clients/{pg,patroni}`, `internal/config`, `internal/phases` packages.

Plan 3 of the shadow-cluster upgrade. Depends on no other plan (but lives alongside them). Resolves spec decision: provision is **tool-driven** (patch + create + wait).

---

### Task 1: Config fields for the shadow cluster

**Files:**
- Modify: `internal/config/config.go` (`UpgradeConfig`)
- Test: `internal/config/config_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/config/config_test.go`:

```go
func TestShadowConfigParses(t *testing.T) {
	y := []byte(`
upgrade:
  shadow_patroni_url: http://shadow-leader:8008
  shadow_source_host: prod-primary
  shadow_source_port: 5432
  physical_slot_name: shadow_phys
  shadow_node_count: 3
`)
	cfg, err := config.Parse(y)
	require.NoError(t, err)
	assert.Equal(t, "http://shadow-leader:8008", cfg.Upgrade.ShadowPatroniURL)
	assert.Equal(t, "prod-primary", cfg.Upgrade.ShadowSourceHost)
	assert.Equal(t, 5432, cfg.Upgrade.ShadowSourcePort)
	assert.Equal(t, "shadow_phys", cfg.Upgrade.PhysicalSlotName)
	assert.Equal(t, 3, cfg.Upgrade.ShadowNodeCount)
}
```

(Use the project's existing config loader name; if it is not `config.Parse`, match the existing test helper in `config_test.go`.)

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/config/ -run TestShadowConfigParses -v`
Expected: FAIL — unknown fields / undefined.

- [ ] **Step 3: Add the fields**

In `internal/config/config.go`, inside `UpgradeConfig`, add:

```go
	// --- shadow-cluster upgrade ---
	// ShadowPatroniURL is the REST endpoint of the shadow cluster's Patroni
	// (the cluster that becomes the new PG17 cluster).
	ShadowPatroniURL string `yaml:"shadow_patroni_url"`
	// ShadowSourceHost/Port is the prod primary the shadow physically replicates
	// from (the standby_cluster source).
	ShadowSourceHost string `yaml:"shadow_source_host"`
	ShadowSourcePort int    `yaml:"shadow_source_port"`
	// PhysicalSlotName is the physical replication slot created on prod for the
	// shadow's stream (separate from the logical SlotName).
	PhysicalSlotName string `yaml:"physical_slot_name"`
	// ShadowNodeCount is the expected node count of the shadow cluster; provision
	// waits until that many members are healthy.
	ShadowNodeCount int `yaml:"shadow_node_count"`
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/config/ -run TestShadowConfigParses -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat(shadow): config fields for the shadow cluster"
```

---

### Task 2: Patroni `SetStandbyCluster`

Mirror the existing private `patchConfig`. Set a `standby_cluster` block in the shadow's dynamic config so its leader follows prod.

**Files:**
- Modify: `internal/clients/patroni/client.go` (interface + HTTPClient)
- Test: `internal/clients/patroni/client_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/clients/patroni/client_test.go`:

```go
func TestSetStandbyCluster(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)
		assert.Equal(t, "/config", r.URL.Path)
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL, "")
	require.NoError(t, c.SetStandbyCluster(context.Background(), "prod-primary", 5432, "shadow_phys"))

	sc, ok := gotBody["standby_cluster"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod-primary", sc["host"])
	assert.Equal(t, float64(5432), sc["port"])
	assert.Equal(t, "shadow_phys", sc["primary_slot_name"])
}
```

(Match the existing constructor name in `patroni` — the test file already constructs `HTTPClient`; reuse that pattern. Ensure imports `encoding/json`, `net/http`, `net/http/httptest`.)

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/clients/patroni/ -run TestSetStandbyCluster -v`
Expected: FAIL — `SetStandbyCluster` undefined.

- [ ] **Step 3: Add the interface method + implementation**

In the `patroni.Client` interface add:

```go
	SetStandbyCluster(ctx context.Context, host string, port int, slotName string) error
	ClearStandbyCluster(ctx context.Context) error
```

On `HTTPClient` (near `Pause`):

```go
// SetStandbyCluster makes this cluster a Patroni standby cluster following the
// remote primary at host:port through primary_slot_name. Patroni reinitializes
// the nodes from the remote primary.
func (c *HTTPClient) SetStandbyCluster(ctx context.Context, host string, port int, slotName string) error {
	return c.patchConfig(ctx, map[string]any{
		"standby_cluster": map[string]any{
			"host":                  host,
			"port":                  port,
			"primary_slot_name":     slotName,
			"create_replica_methods": []string{"basebackup"},
		},
	})
}

// ClearStandbyCluster removes the standby_cluster block, which makes Patroni
// promote the standby leader to a standalone primary.
func (c *HTTPClient) ClearStandbyCluster(ctx context.Context) error {
	return c.patchConfig(ctx, map[string]any{"standby_cluster": nil})
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/clients/patroni/ -run TestSetStandbyCluster -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/clients/patroni/client.go internal/clients/patroni/client_test.go
git commit -m "feat(shadow): Patroni Set/ClearStandbyCluster"
```

---

### Task 3: pg `CreatePhysicalSlot` + `CurrentWALLSN`

**Files:**
- Modify: `internal/clients/pg/client.go`
- Test: `internal/clients/pg/client_test.go`

- [ ] **Step 1: Write the failing tests**

```go
func TestCreatePhysicalSlot(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectExec("pg_create_physical_replication_slot").
		WithArgs("shadow_phys").
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	c := pgclient.NewFromPool(mock)
	require.NoError(t, c.CreatePhysicalSlot(context.Background(), "shadow_phys"))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCurrentWALLSN(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectQuery("pg_current_wal_lsn").
		WillReturnRows(pgxmock.NewRows([]string{"lsn"}).AddRow("0/3FA20000"))
	c := pgclient.NewFromPool(mock)
	lsn, err := c.CurrentWALLSN(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "0/3FA20000", lsn)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/clients/pg/ -run 'TestCreatePhysicalSlot|TestCurrentWALLSN' -v`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement**

Interface additions:

```go
	CreatePhysicalSlot(ctx context.Context, name string) error
	CurrentWALLSN(ctx context.Context) (string, error)
```

internalClient:

```go
// CreatePhysicalSlot creates a physical replication slot on the primary so it
// retains WAL for the shadow's stream. Idempotent: ignores "already exists".
func (c *internalClient) CreatePhysicalSlot(ctx context.Context, name string) error {
	_, err := c.q.Exec(ctx,
		`SELECT pg_create_physical_replication_slot($1)
		   WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, name)
	return err
}

func (c *internalClient) CurrentWALLSN(ctx context.Context) (string, error) {
	var lsn string
	err := c.q.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	return lsn, err
}
```

PoolClient delegations:

```go
func (p *PoolClient) CreatePhysicalSlot(ctx context.Context, name string) error {
	return p.ic().CreatePhysicalSlot(ctx, name)
}
func (p *PoolClient) CurrentWALLSN(ctx context.Context) (string, error) {
	return p.ic().CurrentWALLSN(ctx)
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/clients/pg/ -run 'TestCreatePhysicalSlot|TestCurrentWALLSN' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/clients/pg/client.go internal/clients/pg/client_test.go
git commit -m "feat(shadow): pg CreatePhysicalSlot + CurrentWALLSN"
```

---

### Task 4: `provision` phase

Three steps: apply standby_cluster on the shadow Patroni, create the physical slot on prod, wait until the shadow leader's replay is within a small lag of prod's current WAL and the shadow has its full node count.

**Files:**
- Create: `internal/phases/provision.go`
- Test: `internal/phases/provision_test.go`
- Modify: `internal/phases/deps.go` (add a `Shadow` pg-client provider + `ShadowPatroni` if not reusing `NewPatroni`)

This plan **reuses `Deps.NewPatroni`** as the shadow cluster's Patroni REST (the shadow becomes the new cluster) and adds `Deps.Shadow` for a pg client to the shadow leader.

- [ ] **Step 1: Add the Deps field**

In `internal/phases/deps.go`, add to the `Deps` struct:

```go
	// Shadow returns a pg client to the shadow cluster's leader (PG13 standby
	// during provision, PG17 after upgrade). Resolved lazily.
	Shadow func(ctx context.Context) (pg.Client, error)
```

- [ ] **Step 2: Write the failing test**

`internal/phases/provision_test.go`:

```go
package phases

import (
	"context"
	"testing"

	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvisionAppliesStandbyAndCreatesSlot(t *testing.T) {
	mgr := testMgr(t)
	require.NoError(t, mgr.SetPrimaryHost("prod-primary"))
	pat := &fakePatroni{}
	prod := &fakePG{}
	d := Deps{Mgr: mgr, NewPatroni: pat, Primary: func(context.Context) (pg.Client, error) { return prod, nil },
		Cfg: config.Config{Upgrade: config.UpgradeConfig{
			ShadowSourceHost: "prod-primary", ShadowSourcePort: 5432, PhysicalSlotName: "shadow_phys",
		}}}

	require.NoError(t, (&applyStandbyCluster{d}).Run(context.Background()))
	assert.True(t, pat.standbySet)
	require.NoError(t, (&createPhysicalSlot{d}).Run(context.Background()))
	assert.Equal(t, "shadow_phys", prod.physicalSlot)
}

func TestProvisionCaughtUpGate(t *testing.T) {
	prod := &fakePG{walCurrent: "0/100"}
	shadow := &fakePG{replayLSN: "0/100"}
	d := Deps{Cfg: config.Config{Upgrade: config.UpgradeConfig{ShadowNodeCount: 1}},
		Primary: func(context.Context) (pg.Client, error) { return prod, nil },
		Shadow:  func(context.Context) (pg.Client, error) { return shadow, nil },
		NewPatroni: &fakePatroni{cluster: clusterWithLeader(1)}}
	ok, err := (&waitShadowCaughtUp{d}).caughtUp(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
}
```

Add fakePG fields/methods (in `prepare_test.go`): `physicalSlot string`, `walCurrent string`; methods:

```go
func (f *fakePG) CreatePhysicalSlot(_ context.Context, name string) error { f.physicalSlot = name; return nil }
func (f *fakePG) CurrentWALLSN(context.Context) (string, error)           { return f.walCurrent, nil }
```

Add fakePatroni field `standbySet bool` + methods (in `prepare_test.go`):

```go
func (f *fakePatroni) SetStandbyCluster(context.Context, string, int, string) error { f.standbySet = true; return nil }
func (f *fakePatroni) ClearStandbyCluster(context.Context) error                     { f.standbySet = false; return nil }
```

Add a `clusterWithLeader(n int)` helper in `provision_test.go`:

```go
func clusterWithLeader(n int) *patroni.ClusterInfo {
	ms := []patroni.Member{{Name: "l", Host: "l", Role: "leader"}}
	for i := 1; i < n; i++ {
		ms = append(ms, patroni.Member{Name: "r", Host: "r", Role: "replica"})
	}
	return &patroni.ClusterInfo{Members: ms}
}
```

(import `patroni` in the test file.)

- [ ] **Step 3: Run to verify it fails**

Run: `go test ./internal/phases/ -run TestProvision -v`
Expected: FAIL — `applyStandbyCluster`, `createPhysicalSlot`, `waitShadowCaughtUp` undefined.

- [ ] **Step 4: Implement `provision.go`**

```go
package phases

import (
	"context"
	"fmt"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/jackc/pglogrepl"
)

// maxProvisionLagBytes is how close (in WAL bytes) the shadow leader's replay
// must be to prod's current WAL before provision is considered caught up.
const maxProvisionLagBytes = 16 * 1024 * 1024

var (
	provisionPollInterval = 5 * time.Second
	provisionTimeout      = 30 * time.Minute
)

// NewProvision builds Phase 0: turn the existing shadow cluster into a Patroni
// standby_cluster of prod, create the physical slot, and wait for it to sync.
func NewProvision(d Deps) runner.Phase {
	return &simplePhase{
		id: "provision",
		steps: []runner.Step{
			&createPhysicalSlot{d},
			&applyStandbyCluster{d},
			&waitShadowCaughtUp{d},
		},
		trans: []runner.Transition{{To: "prepare"}},
	}
}

type createPhysicalSlot struct{ d Deps }

func (s *createPhysicalSlot) ID() runner.StepID                   { return "CreatePhysicalSlot" }
func (s *createPhysicalSlot) Check(context.Context) (bool, error) { return false, nil } // idempotent SQL
func (s *createPhysicalSlot) Run(ctx context.Context) error {
	prod, err := s.d.Primary(ctx)
	if err != nil {
		return err
	}
	s.d.logf("создаю физический слот %q на проде для стрима шэдоу...", s.d.Cfg.Upgrade.PhysicalSlotName)
	return prod.CreatePhysicalSlot(ctx, s.d.Cfg.Upgrade.PhysicalSlotName)
}

type applyStandbyCluster struct{ d Deps }

func (s *applyStandbyCluster) ID() runner.StepID                   { return "ApplyStandbyCluster" }
func (s *applyStandbyCluster) Check(context.Context) (bool, error) { return false, nil }
func (s *applyStandbyCluster) Run(ctx context.Context) error {
	s.d.logf("навешиваю standby_cluster на шэдоу (источник %s:%d, слот %q)...",
		s.d.Cfg.Upgrade.ShadowSourceHost, s.d.Cfg.Upgrade.ShadowSourcePort, s.d.Cfg.Upgrade.PhysicalSlotName)
	return s.d.NewPatroni.SetStandbyCluster(ctx,
		s.d.Cfg.Upgrade.ShadowSourceHost, s.d.Cfg.Upgrade.ShadowSourcePort, s.d.Cfg.Upgrade.PhysicalSlotName)
}

type waitShadowCaughtUp struct{ d Deps }

func (s *waitShadowCaughtUp) ID() runner.StepID { return "WaitShadowCaughtUp" }
func (s *waitShadowCaughtUp) Check(ctx context.Context) (bool, error) { return s.caughtUp(ctx) }
func (s *waitShadowCaughtUp) Run(ctx context.Context) error {
	s.d.logf("жду, пока шэдоу догонит прод (lag < %d байт) и наберёт %d нод...",
		maxProvisionLagBytes, s.d.Cfg.Upgrade.ShadowNodeCount)
	wctx, cancel := context.WithTimeout(ctx, provisionTimeout)
	defer cancel()
	for {
		ok, err := s.caughtUp(wctx)
		if err == nil && ok {
			s.d.logf("шэдоу догнан и в полном составе")
			return nil
		}
		select {
		case <-wctx.Done():
			return fmt.Errorf("provision: shadow not caught up in time: %w", wctx.Err())
		case <-time.After(provisionPollInterval):
		}
	}
}
func (s *waitShadowCaughtUp) caughtUp(ctx context.Context) (bool, error) {
	cluster, err := s.d.NewPatroni.GetCluster(ctx)
	if err != nil {
		return false, err
	}
	if len(cluster.Members) < s.d.Cfg.Upgrade.ShadowNodeCount || cluster.Leader() == nil {
		return false, nil
	}
	prod, err := s.d.Primary(ctx)
	if err != nil {
		return false, err
	}
	shadow, err := s.d.Shadow(ctx)
	if err != nil {
		return false, err
	}
	cur, err := prod.CurrentWALLSN(ctx)
	if err != nil {
		return false, err
	}
	rep, err := shadow.GetLastWALReplayLSN(ctx)
	if err != nil || rep == "" {
		return false, err
	}
	curL, err := pglogrepl.ParseLSN(cur)
	if err != nil {
		return false, err
	}
	repL, err := pglogrepl.ParseLSN(rep)
	if err != nil {
		return false, err
	}
	return int64(curL)-int64(repL) <= maxProvisionLagBytes, nil
}

var (
	_ runner.Step = (*createPhysicalSlot)(nil)
	_ runner.Step = (*applyStandbyCluster)(nil)
	_ runner.Step = (*waitShadowCaughtUp)(nil)
)
```

- [ ] **Step 5: Run to verify it passes + full suite**

Run: `go test ./internal/phases/ -run TestProvision -v && go vet ./... && go test ./...`
Expected: PASS everywhere.

- [ ] **Step 6: Commit**

```bash
git add internal/phases/provision.go internal/phases/provision_test.go internal/phases/deps.go internal/phases/prepare_test.go
git commit -m "feat(shadow): provision phase (standby_cluster + physical slot + caught-up wait)"
```

---

## Notes for the implementer

- `provisionPollInterval`/`provisionTimeout` are package vars so tests can shrink them (mirror the isolate timing-var pattern already in the codebase).
- The catch-up gate compares prod `pg_current_wal_lsn()` to the shadow leader's `pg_last_wal_replay_lsn()` — valid because the shadow physically replicates prod (same LSN space). A 16 MB tolerance avoids flapping on a busy prod; tune as needed.
- `CreatePhysicalSlot` runs before `ApplyStandbyCluster` so prod retains WAL from slot creation; the standby then streams from a covered position. The physical slot is the first WAL-retention vector on prod (spec Disk Safety §2) — its breach during a long initial clone fails the clone (retryable), which is the intended safe failure.
