# Catchup (tail-only) + Rebuild-Replicas Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After the PG17 leader has caught the tail, rebuild the shadow replicas as PG17 standbys (Patroni `reinit` = basebackup from the leader) — as its own phase, with a disk-safety throttle so the basebackup load can't push the prod logical slot into invalidation.

**Architecture:** `catchup` stays tail-only (reuse existing steps + the DDL re-assert from Plan 1). A new `rebuild-replicas` phase iterates the shadow's non-leader members, triggers `reinit` on each (via a per-member Patroni client), and waits for it to stream — sampling `diskguard.Monitor` (Plan 2) before each reinit to pause on `Throttle` and abort on `Abort`.

**Tech Stack:** Go, the existing `internal/clients/patroni` + `internal/phases`, `internal/diskguard` (Plan 2). Depends on Plans 2 and 3.

Plan 5 of the shadow-cluster upgrade. **Precondition (platform-provided):** the shadow replica pods are already PG17-capable (image/`bin_dir` switched out-of-band) so a basebackup from the PG17 leader can start under PG17. The tool triggers `reinit` and verifies; it does not redeploy pods.

---

### Task 1: Per-member Patroni `Reinitialize`

`patronictl reinit` maps to `POST /reinitialize` on the **target member's** REST API. Add the method and a per-member client factory.

**Files:** Modify `internal/clients/patroni/client.go`; Test `internal/clients/patroni/client_test.go`.

- [ ] **Step 1: Failing test**

```go
func TestReinitialize(t *testing.T) {
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/reinitialize", r.URL.Path)
		hit = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := patroni.NewHTTPClient(srv.URL, "")
	require.NoError(t, c.Reinitialize(context.Background()))
	assert.True(t, hit)
}
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `go test ./internal/clients/patroni/ -run TestReinitialize -v`

- [ ] **Step 3: Implement** — interface line `Reinitialize(ctx context.Context) error`.

  **Type-consistency fix:** adding `Reinitialize` to the `patroni.Client` interface means EVERY fake implementing that interface needs it. Add to the shared `fakePatroni` in `internal/phases/prepare_test.go`:

  ```go
  func (f *fakePatroni) Reinitialize(context.Context) error { return nil }
  ```

  Then HTTPClient:

```go
// Reinitialize triggers `reinit` on the member this client points at: Patroni
// rebuilds its data dir from the current leader (basebackup). POST /reinitialize
// with {"force": true} so it proceeds even if the member looks healthy.
func (c *HTTPClient) Reinitialize(ctx context.Context) error {
	body, _ := json.Marshal(map[string]any{"force": true})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/reinitialize", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c.authorize(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("patroni POST /reinitialize: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("patroni POST /reinitialize: HTTP %d", resp.StatusCode)
	}
	return nil
}
```

(Match the existing `HTTPClient` field/helper names — `c.http`, `c.baseURL`, `c.authorize` — verify against the file; the test above only checks method+path so it passes regardless of header details.)

- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `git commit -am "feat(shadow): Patroni Reinitialize (per-member reinit)"`

---

### Task 2: `rebuild-replicas` phase with throttle

**Files:** Create `internal/phases/rebuild_replicas.go`; Test `internal/phases/rebuild_replicas_test.go`; Modify `internal/phases/deps.go`.

- [ ] **Step 1: Add Deps fields**

In `internal/phases/deps.go`:

```go
	// ShadowMember builds a Patroni client for a specific shadow member by its
	// API URL (so the tool can reinit each replica). Resolved from the member
	// host + the shadow Patroni API port.
	ShadowMember func(apiURL string) patroni.Client
	// DiskGuard samples the prod logical slot's WAL pressure (Plan 2). May be nil
	// in tests/topologies that don't need throttling.
	DiskGuard interface {
		Sample(ctx context.Context) (diskguard.Decision, int64, error)
	}
```

Add imports for `patroni` and `diskguard` to `deps.go`.

- [ ] **Step 2: Failing test**

`internal/phases/rebuild_replicas_test.go`:

```go
package phases

import (
	"context"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/diskguard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeMemberPatroni struct{ reinitialized bool }

func (f *fakeMemberPatroni) GetCluster(context.Context) (*patroni.ClusterInfo, error) { return nil, nil }
func (f *fakeMemberPatroni) NodePaused(context.Context) (bool, error)                 { return false, nil }
func (f *fakeMemberPatroni) Pause(context.Context) error                              { return nil }
func (f *fakeMemberPatroni) Resume(context.Context) error                             { return nil }
func (f *fakeMemberPatroni) SetStandbyCluster(context.Context, string, int, string) error { return nil }
func (f *fakeMemberPatroni) ClearStandbyCluster(context.Context) error                { return nil }
func (f *fakeMemberPatroni) Reinitialize(context.Context) error                       { f.reinitialized = true; return nil }

type fakeGuard struct{ d diskguard.Decision }

func (g fakeGuard) Sample(context.Context) (diskguard.Decision, int64, error) { return g.d, 0, nil }

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
```

- [ ] **Step 3: Run, expect FAIL.**

Run: `go test ./internal/phases/ -run TestRebuildReplicas -v`

- [ ] **Step 4: Implement `rebuild_replicas.go`**

```go
package phases

import (
	"context"
	"fmt"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/diskguard"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
)

var (
	rebuildPollInterval = 5 * time.Second
	rebuildTimeout      = 60 * time.Minute
)

// NewRebuildReplicas builds the phase that rebuilds the shadow replicas as PG17
// standbys of the upgraded leader, throttled by disk pressure on the prod slot.
func NewRebuildReplicas(d Deps) runner.Phase {
	return &simplePhase{
		id:    "rebuild-replicas",
		steps: []runner.Step{&reinitShadowReplicas{d}, &waitReplicasHealthy{d}},
		trans: []runner.Transition{{To: "switchover"}},
	}
}

type reinitShadowReplicas struct{ d Deps }

func (s *reinitShadowReplicas) ID() runner.StepID                   { return "ReinitShadowReplicas" }
func (s *reinitShadowReplicas) Check(context.Context) (bool, error) { return false, nil }
func (s *reinitShadowReplicas) Run(ctx context.Context) error {
	cluster, err := s.d.NewPatroni.GetCluster(ctx)
	if err != nil {
		return err
	}
	for _, m := range cluster.Members {
		if m.Role == "leader" {
			continue
		}
		if err := s.throttleBeforeLoad(ctx); err != nil {
			return err
		}
		s.d.logf("reinit реплики шэдоу %q (basebackup с PG17-лидера)...", m.Name)
		mc := s.d.ShadowMember(memberAPIURL(s.d.Cfg.Upgrade.ShadowPatroniURL, m))
		if err := mc.Reinitialize(ctx); err != nil {
			return fmt.Errorf("rebuild-replicas: reinit %s: %w", m.Name, err)
		}
	}
	return nil
}

// throttleBeforeLoad blocks while the prod slot is in the throttle band and
// aborts if it crosses the cap, so basebackup load can't invalidate the slot.
func (s *reinitShadowReplicas) throttleBeforeLoad(ctx context.Context) error {
	if s.d.DiskGuard == nil {
		return nil
	}
	wctx, cancel := context.WithTimeout(ctx, rebuildTimeout)
	defer cancel()
	for {
		dec, retained, err := s.d.DiskGuard.Sample(wctx)
		if err != nil {
			return err
		}
		switch dec {
		case diskguard.Abort:
			return fmt.Errorf("rebuild-replicas: prod slot disk pressure too high (retained=%d) — aborting before invalidation; raise max_slot_wal_keep_size or free disk, then re-run", retained)
		case diskguard.OK:
			return nil
		case diskguard.Throttle:
			s.d.logf("⏳ слот под давлением (retained=%d) — пауза перед следующим reinit...", retained)
		}
		select {
		case <-wctx.Done():
			return fmt.Errorf("rebuild-replicas: slot stayed under pressure too long: %w", wctx.Err())
		case <-time.After(rebuildPollInterval):
		}
	}
}

type waitReplicasHealthy struct{ d Deps }

func (s *waitReplicasHealthy) ID() runner.StepID { return "WaitReplicasHealthy" }
func (s *waitReplicasHealthy) Check(ctx context.Context) (bool, error) { return s.healthy(ctx) }
func (s *waitReplicasHealthy) Run(ctx context.Context) error {
	s.d.logf("жду, пока все реплики шэдоу станут running и наберут полный состав...")
	wctx, cancel := context.WithTimeout(ctx, rebuildTimeout)
	defer cancel()
	for {
		ok, err := s.healthy(wctx)
		if err == nil && ok {
			s.d.logf("реплики шэдоу здоровы — новый кластер в HA")
			return nil
		}
		select {
		case <-wctx.Done():
			return fmt.Errorf("rebuild-replicas: replicas not healthy in time: %w", wctx.Err())
		case <-time.After(rebuildPollInterval):
		}
	}
}
func (s *waitReplicasHealthy) healthy(ctx context.Context) (bool, error) {
	cluster, err := s.d.NewPatroni.GetCluster(ctx)
	if err != nil {
		return false, err
	}
	if cluster.Leader() == nil || len(cluster.Members) < s.d.Cfg.Upgrade.ShadowNodeCount {
		return false, nil
	}
	return true, nil
}

// memberAPIURL derives a member's Patroni REST URL from the shadow base URL's
// scheme/port and the member host.
func memberAPIURL(base string, m interface{ GetHost() string }) string { return "" } // see Step 5

var (
	_ runner.Step = (*reinitShadowReplicas)(nil)
	_ runner.Step = (*waitReplicasHealthy)(nil)
)
```

- [ ] **Step 5: Implement `memberAPIURL` concretely + the test timing helper**

Replace the stub `memberAPIURL` with a real one that swaps the host of the base URL for the member's host (members share the API port). Add to `rebuild_replicas.go`:

```go
import "net/url"

// memberAPIURL takes the shadow Patroni base URL and returns the same URL with
// its host replaced by the member's host (API port is the same across members).
func memberAPIURL(base string, m patroni.Member) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	_, port, _ := net.SplitHostPort(u.Host)
	if port != "" {
		u.Host = net.JoinHostPort(m.Host, port)
	} else {
		u.Host = m.Host
	}
	return u.String()
}
```

(Adjust imports: `net`, `net/url`, `patroni`. Update the call site `memberAPIURL(s.d.Cfg.Upgrade.ShadowPatroniURL, m)` to pass the `patroni.Member`.)

Add the timing helper to `rebuild_replicas_test.go`:

```go
func setRebuildTimingForTest(t *testing.T) func() {
	t.Helper()
	o1, o2 := rebuildPollInterval, rebuildTimeout
	rebuildPollInterval, rebuildTimeout = time.Millisecond, time.Second
	return func() { rebuildPollInterval, rebuildTimeout = o1, o2 }
}
```

(import `time` in the test.)

- [ ] **Step 6: Run, expect PASS + full suite**

Run: `go test ./internal/phases/ -run TestRebuildReplicas -v && go vet ./... && go test ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/phases/rebuild_replicas.go internal/phases/rebuild_replicas_test.go internal/phases/deps.go internal/clients/patroni/
git commit -m "feat(shadow): rebuild-replicas phase (throttled reinit of shadow replicas)"
```

---

## Notes for the implementer

- **PG17-capable replica pods are a precondition** (platform-provided). Patroni `reinit` basebackups the PG17 leader; the replica must start that data under PG17 binaries. The tool triggers reinit + verifies; it does not switch images. Document this in the runbook.
- **Throttle semantics:** `throttleBeforeLoad` runs before each replica's reinit (not mid-basebackup — Patroni owns that). Sequencing reinit one replica at a time keeps peak slot pressure down (spec Disk Safety §3). For very large DBs, consider reinit-one-then-wait-healthy before the next; this plan reinits all then waits — tighten to serial if peak pressure is a concern.
- **`Member.Host`** is read from `GetCluster`; confirm the field name in `patroni.ClusterInfo`/`Member` and adjust `memberAPIURL` accordingly.
