# PatchNewPatroniConfig Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the catchup step that *verifies* the new-cluster `patroni.yml` with one that *patches* it (scope + postgresql.data_dir/bin_dir/config_dir) to correct PG17 values before Patroni is started.

**Architecture:** A pure, byte-level `patchPatroniConfig` function rewrites the four keys via `yaml.Node` (preserving comments/order/other keys). The catchup step `patchNewPatroniConfig` reads `patroni_config_path`, writes a one-time `.bak`, and rewrites the file in place — the same file `patroni_start_command` launches. New config fields `new_scope` (default `<cluster_name>-17`) and `new_config_dir` (empty = leave config_dir untouched) drive the scope and config_dir values.

**Tech Stack:** Go, `gopkg.in/yaml.v3` (already a dependency), testify.

Spec: `docs/superpowers/specs/2026-06-03-patch-new-patroni-config-design.md`

**Context the implementer needs:**
- This is the `pg-upgrade` orchestrator. Phases live in `internal/phases/`; each phase is a list of `runner.Step` (interface: `ID() runner.StepID`, `Check(ctx) (bool, error)`, `Run(ctx) error`). `Check` returning `true` means "already done, skip Run". Steps re-run idempotently on resume.
- `Deps` (in `internal/phases/deps.go`) carries `Cfg config.Config`, `Mgr *state.Manager`, and a nil-safe logger via `d.logf(format, args...)`. Logs in this codebase are in Russian — match that.
- The catchup phase is `internal/phases/catchup.go`. The step being replaced is `verifyNewPatroniConfig` (ID `"VerifyNewPatroniConfig"`), positioned between `verifyOldClusterStopped` and `startPG17`. There is a `var _ runner.Step = (*verifyNewPatroniConfig)(nil)` assertion block at the bottom of the file.
- An existing helper `parsePatroniScopeDataDir(yamlBytes []byte) (scope, dataDir string, err error)` lives in `internal/phases/initdb.go`. We will add a sibling that also returns bin_dir/config_dir.
- Run the whole suite with `go test ./...`; a single package with `go test ./internal/phases/ -run TestName -v`. Always `gofmt -w <files>` and `go vet ./...` before committing.
- Commit messages in this repo end with a trailing line: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Work happens on a feature branch (already on `feat/patch-new-patroni-config`).

---

## Task 1: Config fields, default scope, and run validation

**Files:**
- Modify: `internal/config/config.go` (add fields to `UpgradeConfig` ~line 26-44; add `EffectiveNewScope` method; add guard in `ValidateForRun` before its final `return nil` at ~line 161)
- Test: `internal/config/config_test.go`

- [ ] **Step 1: Write failing tests for EffectiveNewScope and the validation guard**

Add to `internal/config/config_test.go` (create the file if it does not exist; if it exists, append these functions and reuse its existing imports — it needs `testing`, `github.com/stretchr/testify/assert`, `github.com/stretchr/testify/require`):

```go
func TestEffectiveNewScope_DefaultDerivesFromClusterName(t *testing.T) {
	c := Config{ClusterName: "prod-pg"}
	assert.Equal(t, "prod-pg-17", c.EffectiveNewScope())
}

func TestEffectiveNewScope_UsesConfiguredValue(t *testing.T) {
	c := Config{ClusterName: "prod-pg", Upgrade: UpgradeConfig{NewScope: "pg17-cluster"}}
	assert.Equal(t, "pg17-cluster", c.EffectiveNewScope())
}

func TestValidateForRun_RejectsNewScopeEqualToClusterName(t *testing.T) {
	c := validRunConfig()
	c.Upgrade.NewScope = c.ClusterName
	err := c.ValidateForRun()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "new_scope")
}
```

`validRunConfig()` is a helper that returns a `Config` passing `ValidateForRun`. If `internal/config/config_test.go` already has such a helper, reuse it. If not, add this one (fill every field `ValidateForRun` checks, so only the field under test triggers failure):

```go
func validRunConfig() Config {
	return Config{
		ClusterName: "prod-pg",
		Upgrade: UpgradeConfig{
			TargetNode: "n1", SlotName: "s", PublicationName: "p",
			NewPGBindir: "/new/bin", OldPGBindir: "/old/bin",
			DataDir: "/data/old", NewDataDir: "/data/new",
			PatroniConfigPath: "/etc/patroni.yml", SubscriptionName: "sub",
			ReversePubName: "rpub", ReverseSubName: "rsub", DBName: "appdb",
			PG17DSN: "host=h dbname=appdb", NewPatroniURL: "http://h:8008",
			DSNSwapSignalPath: "/run/swap.json", SequenceBuffer: 1000,
			PgUpgradeLogDir: "/var/log/pgu", LogArchiveDir: "/var/log/arch",
		},
		PG: PGConfig{SuperuserDSN: "host=h dbname=appdb"},
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/config/ -run 'TestEffectiveNewScope|TestValidateForRun_RejectsNewScope' -v`
Expected: compile error (`EffectiveNewScope` / `NewScope` undefined), or FAIL once those exist.

- [ ] **Step 3: Add the config fields**

In `internal/config/config.go`, inside `type UpgradeConfig struct { ... }`, after the `OldPatroniStopCommand` field (end of the struct), add:

```go
	// NewScope is the DCS scope (Patroni cluster name) the upgraded PG17 cluster
	// runs under. It MUST differ from cluster_name: the upgraded cluster has a new
	// system identifier, and reusing the old scope makes Patroni refuse to start
	// with a system-ID mismatch. Empty defaults to "<cluster_name>-17" (see
	// Config.EffectiveNewScope). The catchup PatchNewPatroniConfig step writes it
	// into the new cluster's patroni.yml.
	NewScope string `yaml:"new_scope"`
	// NewConfigDir is the PG17 config_dir written into the new cluster's
	// patroni.yml (postgresql.config_dir). Empty means leave config_dir untouched
	// (Patroni defaults config_dir to data_dir). There is no safe default path, so
	// it is only patched when set.
	NewConfigDir string `yaml:"new_config_dir"`
```

- [ ] **Step 4: Add the EffectiveNewScope method**

In `internal/config/config.go`, add after the `DefaultPatroniStart` const block (~line 78, before `Load`):

```go
// EffectiveNewScope is the DCS scope for the upgraded PG17 cluster: the
// explicit upgrade.new_scope when set, otherwise "<cluster_name>-17".
func (c Config) EffectiveNewScope() string {
	if c.Upgrade.NewScope != "" {
		return c.Upgrade.NewScope
	}
	return c.ClusterName + "-17"
}
```

- [ ] **Step 5: Add the ValidateForRun guard**

In `internal/config/config.go`, in `ValidateForRun`, immediately before the final `return nil` (currently line ~161, after the `SequenceBuffer` check), add:

```go
	if c.EffectiveNewScope() == c.ClusterName {
		return fmt.Errorf("config: upgrade.new_scope must differ from cluster_name %q (the upgraded cluster has a new system identifier; reusing the scope makes Patroni refuse to start)", c.ClusterName)
	}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/config/ -run 'TestEffectiveNewScope|TestValidateForRun_RejectsNewScope' -v`
Expected: PASS (3 tests).

- [ ] **Step 7: Format, vet, full config test, commit**

```bash
gofmt -w internal/config/config.go internal/config/config_test.go
go vet ./internal/config/ && go test ./internal/config/
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat(config): add upgrade.new_scope/new_config_dir + EffectiveNewScope

new_scope (default <cluster_name>-17) and new_config_dir feed the catchup
PatchNewPatroniConfig step. ValidateForRun rejects a new_scope equal to
cluster_name, which would reintroduce the Patroni system-ID mismatch.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Pure `patchPatroniConfig` function

**Files:**
- Create: `internal/phases/patroni_patch.go`
- Test: `internal/phases/patroni_patch_test.go`

This is a self-contained, file-system-free YAML transform. It is the riskiest
piece (yaml.Node manipulation), so it gets the most tests.

- [ ] **Step 1: Write the failing tests**

Create `internal/phases/patroni_patch_test.go`:

```go
package phases

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// scopeDataBinConfig decodes a patched document back into the four fields under
// test, so assertions don't depend on byte-for-byte formatting.
func scopeDataBinConfig(t *testing.T, b []byte) (scope, dataDir, binDir, configDir string) {
	t.Helper()
	var doc struct {
		Scope      string `yaml:"scope"`
		Postgresql struct {
			DataDir   string `yaml:"data_dir"`
			BinDir    string `yaml:"bin_dir"`
			ConfigDir string `yaml:"config_dir"`
		} `yaml:"postgresql"`
	}
	require.NoError(t, yaml.Unmarshal(b, &doc))
	return doc.Scope, doc.Postgresql.DataDir, doc.Postgresql.BinDir, doc.Postgresql.ConfigDir
}

func TestPatchPatroniConfig_RewritesExistingKeys(t *testing.T) {
	in := "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /usr/lib/postgresql/13/bin\n  config_dir: /etc/postgresql/13/main\n"
	out, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/usr/lib/postgresql/17/bin", "/etc/postgresql/17/main")
	require.NoError(t, err)
	scope, data, bin, cfg := scopeDataBinConfig(t, out)
	assert.Equal(t, "prod-17", scope)
	assert.Equal(t, "/data/pg17", data)
	assert.Equal(t, "/usr/lib/postgresql/17/bin", bin)
	assert.Equal(t, "/etc/postgresql/17/main", cfg)
}

func TestPatchPatroniConfig_LeavesConfigDirWhenEmpty(t *testing.T) {
	in := "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /b\n  config_dir: /etc/postgresql/13/main\n"
	out, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/usr/lib/postgresql/17/bin", "")
	require.NoError(t, err)
	_, _, _, cfg := scopeDataBinConfig(t, out)
	assert.Equal(t, "/etc/postgresql/13/main", cfg) // untouched
}

func TestPatchPatroniConfig_InsertsMissingKeys(t *testing.T) {
	in := "scope: prod\npostgresql:\n  data_dir: /data/pg13\n" // no bin_dir, no config_dir
	out, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/usr/lib/postgresql/17/bin", "/etc/postgresql/17/main")
	require.NoError(t, err)
	scope, data, bin, cfg := scopeDataBinConfig(t, out)
	assert.Equal(t, "prod-17", scope)
	assert.Equal(t, "/data/pg17", data)
	assert.Equal(t, "/usr/lib/postgresql/17/bin", bin)
	assert.Equal(t, "/etc/postgresql/17/main", cfg)
}

func TestPatchPatroniConfig_CreatesPostgresqlMappingWhenAbsent(t *testing.T) {
	in := "scope: prod\nrestapi:\n  listen: 0.0.0.0:8008\n" // no postgresql mapping at all
	out, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/b17", "")
	require.NoError(t, err)
	scope, data, bin, _ := scopeDataBinConfig(t, out)
	assert.Equal(t, "prod-17", scope)
	assert.Equal(t, "/data/pg17", data)
	assert.Equal(t, "/b17", bin)
}

func TestPatchPatroniConfig_PreservesCommentsAndOtherKeys(t *testing.T) {
	in := "# top comment\nscope: prod\nnamespace: /service/  # keep me\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /b13\n"
	out, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/b17", "")
	require.NoError(t, err)
	s := string(out)
	assert.Contains(t, s, "# top comment")
	assert.Contains(t, s, "keep me")
	assert.Contains(t, s, "namespace: /service/")
}

func TestPatchPatroniConfig_Idempotent(t *testing.T) {
	in := "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /b13\n"
	once, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/b17", "/c17")
	require.NoError(t, err)
	twice, err := patchPatroniConfig(once, "prod-17", "/data/pg17", "/b17", "/c17")
	require.NoError(t, err)
	s1, d1, b1, c1 := scopeDataBinConfig(t, once)
	s2, d2, b2, c2 := scopeDataBinConfig(t, twice)
	assert.Equal(t, []string{s1, d1, b1, c1}, []string{s2, d2, b2, c2})
}

func TestPatchPatroniConfig_RejectsMalformedYAML(t *testing.T) {
	_, err := patchPatroniConfig([]byte("scope: [unclosed\n"), "prod-17", "/d", "/b", "")
	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/phases/ -run TestPatchPatroniConfig -v`
Expected: compile error — `patchPatroniConfig` undefined.

- [ ] **Step 3: Implement `patchPatroniConfig`**

Create `internal/phases/patroni_patch.go`:

```go
package phases

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// patchPatroniConfig sets scope and postgresql.{data_dir,bin_dir} (and
// config_dir when configDir != "") in a Patroni YAML document, preserving
// comments, key order, and all other keys. Missing keys (or a missing
// postgresql mapping) are inserted. configDir == "" leaves config_dir untouched.
func patchPatroniConfig(in []byte, scope, dataDir, binDir, configDir string) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(in, &doc); err != nil {
		return nil, fmt.Errorf("catchup: parse patroni config: %w", err)
	}
	// A non-empty document has one child: the root mapping node.
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return nil, fmt.Errorf("catchup: patroni config is empty")
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("catchup: patroni config root is not a mapping")
	}

	setMapValue(root, "scope", scope)

	pg := mapValueNode(root, "postgresql")
	if pg == nil {
		pg = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		appendMapEntry(root, "postgresql", pg)
	}
	setMapValue(pg, "data_dir", dataDir)
	setMapValue(pg, "bin_dir", binDir)
	if configDir != "" {
		setMapValue(pg, "config_dir", configDir)
	}

	out, err := yaml.Marshal(&doc)
	if err != nil {
		return nil, fmt.Errorf("catchup: re-encode patroni config: %w", err)
	}
	return out, nil
}

// mapValueNode returns the value node for key in a mapping node, or nil. A
// mapping node's Content is [key0, val0, key1, val1, ...].
func mapValueNode(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// setMapValue updates key's scalar value in place when present (preserving any
// comment on the value node), otherwise appends a new scalar entry.
func setMapValue(m *yaml.Node, key, value string) {
	if v := mapValueNode(m, key); v != nil {
		v.Kind = yaml.ScalarNode
		v.Tag = "!!str"
		v.Value = value
		return
	}
	appendMapEntry(m, key, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value})
}

// appendMapEntry adds a key/value pair to the end of a mapping node.
func appendMapEntry(m *yaml.Node, key string, value *yaml.Node) {
	m.Content = append(m.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
		value)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/phases/ -run TestPatchPatroniConfig -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Format, commit**

```bash
gofmt -w internal/phases/patroni_patch.go internal/phases/patroni_patch_test.go
go vet ./internal/phases/
git add internal/phases/patroni_patch.go internal/phases/patroni_patch_test.go
git commit -m "feat(phases): pure patchPatroniConfig YAML transform

Sets scope and postgresql.{data_dir,bin_dir,config_dir} via yaml.Node,
preserving comments/order/other keys and inserting missing keys.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Extend the patroni parser to read bin_dir/config_dir

**Files:**
- Modify: `internal/phases/initdb.go` (the `parsePatroniScopeDataDir` function, ~line 58-69)
- Test: `internal/phases/initdb_test.go` (add a test) — or `patroni_patch_test.go`

We need `Check` (Task 4) to read scope/data_dir/bin_dir/config_dir from the file.
Add a sibling parser rather than changing the existing one's signature (it is
also used by other code paths that only want scope+data_dir).

- [ ] **Step 1: Write the failing test**

Add to `internal/phases/patroni_patch_test.go`:

```go
func TestParsePatroniManagedDirs(t *testing.T) {
	in := "scope: prod-17\npostgresql:\n  data_dir: /data/pg17\n  bin_dir: /b17\n  config_dir: /c17\n"
	p, err := parsePatroniManagedFields([]byte(in))
	require.NoError(t, err)
	assert.Equal(t, "prod-17", p.Scope)
	assert.Equal(t, "/data/pg17", p.DataDir)
	assert.Equal(t, "/b17", p.BinDir)
	assert.Equal(t, "/c17", p.ConfigDir)
}

func TestParsePatroniManagedDirs_MissingFieldsAreEmpty(t *testing.T) {
	p, err := parsePatroniManagedFields([]byte("scope: prod-17\npostgresql:\n  data_dir: /data/pg17\n"))
	require.NoError(t, err)
	assert.Equal(t, "", p.BinDir)
	assert.Equal(t, "", p.ConfigDir)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/phases/ -run TestParsePatroniManagedDirs -v`
Expected: compile error — `parsePatroniManagedFields` undefined.

- [ ] **Step 3: Implement the parser**

In `internal/phases/initdb.go`, add after `parsePatroniScopeDataDir`:

```go
// patroniManagedFields are the patroni.yml fields PatchNewPatroniConfig manages.
type patroniManagedFields struct {
	Scope     string
	DataDir   string
	BinDir    string
	ConfigDir string
}

// parsePatroniManagedFields reads scope and postgresql.{data_dir,bin_dir,
// config_dir} from a Patroni config. Any field may be empty if unset in the
// file (e.g. provided via environment instead); callers must tolerate that.
func parsePatroniManagedFields(patroniYAML []byte) (patroniManagedFields, error) {
	var doc struct {
		Scope      string `yaml:"scope"`
		Postgresql struct {
			DataDir   string `yaml:"data_dir"`
			BinDir    string `yaml:"bin_dir"`
			ConfigDir string `yaml:"config_dir"`
		} `yaml:"postgresql"`
	}
	if err := yaml.Unmarshal(patroniYAML, &doc); err != nil {
		return patroniManagedFields{}, fmt.Errorf("catchup: parse patroni config: %w", err)
	}
	return patroniManagedFields{
		Scope:     doc.Scope,
		DataDir:   doc.Postgresql.DataDir,
		BinDir:    doc.Postgresql.BinDir,
		ConfigDir: doc.Postgresql.ConfigDir,
	}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/phases/ -run TestParsePatroniManagedDirs -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Format, commit**

```bash
gofmt -w internal/phases/initdb.go internal/phases/patroni_patch_test.go
go vet ./internal/phases/
git add internal/phases/initdb.go internal/phases/patroni_patch_test.go
git commit -m "feat(phases): parsePatroniManagedFields reads scope+postgresql dirs

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Replace the catchup step with `patchNewPatroniConfig`

**Files:**
- Modify: `internal/phases/catchup.go` (replace `verifyNewPatroniConfig` struct/methods ~line 57-91; update `NewCatchup` steps slice ~line 21; update the `var _ runner.Step` block ~line 234)
- Test: `internal/phases/catchup_test.go` (replace the three `TestVerifyNewPatroniConfig_*` tests ~line 110-127 and their `verifyPatroniDeps` helper ~line 100-108)

- [ ] **Step 1: Replace the step's tests with patch-step tests**

In `internal/phases/catchup_test.go`, replace the helper `verifyPatroniDeps` and the three `TestVerifyNewPatroniConfig_*` functions (lines ~100-127) with:

```go
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

func TestPatchNewPatroniConfig_PatchesStaleFileAndBacksUp(t *testing.T) {
	d, p := patchPatroniDeps(t, "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /usr/lib/postgresql/13/bin\n")
	// not yet patched -> Check reports not done
	done, err := (&patchNewPatroniConfig{d}).Check(context.Background())
	require.NoError(t, err)
	assert.False(t, done)

	require.NoError(t, (&patchNewPatroniConfig{d}).Run(context.Background()))

	// file now patched
	fields, err := parsePatroniManagedFields(readFile(t, p))
	require.NoError(t, err)
	assert.Equal(t, "prod-17", fields.Scope) // EffectiveNewScope default
	assert.Equal(t, "/data/pg17", fields.DataDir)
	assert.Equal(t, "/usr/lib/postgresql/17/bin", fields.BinDir)

	// original preserved in .bak
	bak := readFile(t, p+".bak")
	assert.Contains(t, string(bak), "data_dir: /data/pg13")

	// Check now reports done
	done, err = (&patchNewPatroniConfig{d}).Check(context.Background())
	require.NoError(t, err)
	assert.True(t, done)
}

func TestPatchNewPatroniConfig_DoesNotOverwriteExistingBak(t *testing.T) {
	d, p := patchPatroniDeps(t, "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /b13\n")
	require.NoError(t, os.WriteFile(p+".bak", []byte("ORIGINAL-BAK"), 0o644))
	require.NoError(t, (&patchNewPatroniConfig{d}).Run(context.Background()))
	assert.Equal(t, "ORIGINAL-BAK", string(readFile(t, p+".bak"))) // untouched
}

func TestPatchNewPatroniConfig_PatchesConfigDirWhenSet(t *testing.T) {
	d, p := patchPatroniDeps(t, "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /b13\n  config_dir: /etc/postgresql/13/main\n")
	d.Cfg.Upgrade.NewConfigDir = "/etc/postgresql/17/main"
	require.NoError(t, (&patchNewPatroniConfig{d}).Run(context.Background()))
	fields, err := parsePatroniManagedFields(readFile(t, p))
	require.NoError(t, err)
	assert.Equal(t, "/etc/postgresql/17/main", fields.ConfigDir)
}
```

Add this small helper near the top of `catchup_test.go` (after the imports) if it is not already present:

```go
func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/phases/ -run TestPatchNewPatroniConfig -v`
Expected: compile error — `patchNewPatroniConfig` undefined.

- [ ] **Step 3: Replace the step implementation in catchup.go**

In `internal/phases/catchup.go`, replace the entire `verifyNewPatroniConfig` block (the doc comment starting "--- VerifyNewPatroniConfig ---" through the closing brace of its `Run`, lines ~57-91) with:

```go
// --- PatchNewPatroniConfig: rewrite the new-cluster patroni.yml for PG17 ---
//
// The upgraded PG17 cluster has a NEW system identifier and lives in a new data
// dir. Patroni will refuse to start ("system ID mismatch") under the old scope,
// and would manage the new data dir with the old binaries if bin_dir still
// points at the old major. So before starting Patroni we patch the new
// cluster's patroni.yml in place: scope -> EffectiveNewScope (default
// "<cluster_name>-17"), postgresql.data_dir -> new_data_dir, postgresql.bin_dir
// -> new_pg_bindir, and postgresql.config_dir -> new_config_dir when set. The
// operator's original is preserved once as <path>.bak.
type patchNewPatroniConfig struct{ d Deps }

func (s *patchNewPatroniConfig) ID() runner.StepID { return "PatchNewPatroniConfig" }
func (s *patchNewPatroniConfig) Check(_ context.Context) (bool, error) {
	data, err := os.ReadFile(s.d.Cfg.Upgrade.PatroniConfigPath)
	if err != nil {
		return false, fmt.Errorf("catchup: read new-cluster patroni config %s: %w", s.d.Cfg.Upgrade.PatroniConfigPath, err)
	}
	cur, err := parsePatroniManagedFields(data)
	if err != nil {
		return false, err
	}
	if cur.Scope != s.d.Cfg.EffectiveNewScope() {
		return false, nil
	}
	if cur.DataDir != s.d.Cfg.Upgrade.NewDataDir {
		return false, nil
	}
	if cur.BinDir != s.d.Cfg.Upgrade.NewPGBindir {
		return false, nil
	}
	if s.d.Cfg.Upgrade.NewConfigDir != "" && cur.ConfigDir != s.d.Cfg.Upgrade.NewConfigDir {
		return false, nil
	}
	return true, nil
}
func (s *patchNewPatroniConfig) Run(_ context.Context) error {
	path := s.d.Cfg.Upgrade.PatroniConfigPath
	scope := s.d.Cfg.EffectiveNewScope()
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("catchup: read new-cluster patroni config %s: %w", path, err)
	}
	// Preserve the operator's original once; never clobber an existing backup.
	bak := path + ".bak"
	if _, statErr := os.Stat(bak); os.IsNotExist(statErr) {
		if err := os.WriteFile(bak, data, 0o644); err != nil {
			return fmt.Errorf("catchup: write patroni config backup %s: %w", bak, err)
		}
	}
	s.d.logf("патчу новый patroni.yml %s: scope=%q, data_dir=%q, bin_dir=%q, config_dir=%q...",
		path, scope, s.d.Cfg.Upgrade.NewDataDir, s.d.Cfg.Upgrade.NewPGBindir, s.d.Cfg.Upgrade.NewConfigDir)
	patched, err := patchPatroniConfig(data, scope, s.d.Cfg.Upgrade.NewDataDir, s.d.Cfg.Upgrade.NewPGBindir, s.d.Cfg.Upgrade.NewConfigDir)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, patched, 0o644); err != nil {
		return fmt.Errorf("catchup: write patched patroni config %s: %w", path, err)
	}
	s.d.logf("новый patroni.yml приведён к PG17 (оригинал сохранён в %s)", bak)
	return nil
}
```

- [ ] **Step 4: Wire the new step into the phase and the interface assertions**

In `internal/phases/catchup.go`, in `NewCatchup`, change the step
`&verifyNewPatroniConfig{d},` to `&patchNewPatroniConfig{d},`.

In the same file, in the `var ( ... )` interface-assertion block at the bottom,
change `_ runner.Step = (*verifyNewPatroniConfig)(nil)` to
`_ runner.Step = (*patchNewPatroniConfig)(nil)`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/phases/ -run 'TestPatchNewPatroniConfig|TestCatchup' -v`
Expected: PASS (the 3 new patch tests + existing catchup tests).

- [ ] **Step 6: Run the whole phases package**

Run: `go test ./internal/phases/`
Expected: `ok` — confirms no other test referenced the removed `verifyNewPatroniConfig`. If any does, it was a stale reference to delete.

- [ ] **Step 7: Format, vet, commit**

```bash
gofmt -w internal/phases/catchup.go internal/phases/catchup_test.go
go vet ./internal/phases/
git add internal/phases/catchup.go internal/phases/catchup_test.go
git commit -m "feat(catchup): patch new-cluster patroni.yml instead of verifying it

Replaces VerifyNewPatroniConfig with PatchNewPatroniConfig: rewrites scope,
postgresql.data_dir/bin_dir (and config_dir when new_config_dir is set) in
place before starting PG17, backing up the operator's original to .bak.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Document the new config fields

**Files:**
- Modify: `pg-upgrade.example.yaml` (the `upgrade:` block; the `patroni_config_path` comment ~line 44-48)

- [ ] **Step 1: Update the example config**

In `pg-upgrade.example.yaml`, update the `patroni_config_path` comment to note the
binary now edits the file, and add the two new fields. Replace the existing
`patroni_config_path` comment+field (the block describing it) with:

```yaml
  # Path to the new PG17 cluster's patroni.yml on target_node. The catchup
  # PatchNewPatroniConfig step REWRITES this file in place before starting
  # Patroni — setting scope, postgresql.data_dir/bin_dir (and config_dir when
  # new_config_dir is set) — and saves the operator's original to
  # <path>.bak. The operator owns the rest of the file. It is also the default
  # source of bootstrap.initdb (see patroni_initdb_config). [run]
  patroni_config_path: /etc/patroni/patroni.yml

  # DCS scope (Patroni cluster name) for the upgraded PG17 cluster, written into
  # patroni_config_path. MUST differ from cluster_name: the upgraded cluster has
  # a new system identifier, so reusing the old scope makes Patroni refuse to
  # start (system ID mismatch). Empty defaults to "<cluster_name>-17". [optional]
  new_scope: ""

  # PG17 config_dir written into patroni_config_path (postgresql.config_dir).
  # Empty leaves config_dir untouched (Patroni defaults it to data_dir). Set it
  # when the new cluster's config_dir differs from its data_dir, e.g. a Debian
  # layout like /etc/postgresql/17/main. [optional]
  new_config_dir: ""
```

- [ ] **Step 2: Commit**

```bash
git add pg-upgrade.example.yaml
git commit -m "docs(config): document upgrade.new_scope and new_config_dir

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: Full-suite verification

**Files:** none (verification only)

- [ ] **Step 1: Run the entire suite, vet, and format check**

```bash
gofmt -l internal/ cmd/        # expect: no output
go vet ./...                   # expect: no output
go test ./...                  # expect: all ok
```

Expected: every package `ok`; `gofmt -l` and `go vet` print nothing.

- [ ] **Step 2: Confirm no dangling references to the old step**

Run: `grep -rn "verifyNewPatroniConfig\|VerifyNewPatroniConfig" internal/ cmd/`
Expected: no matches (the step is fully renamed). If any remain, fix them and re-run Task 6 Step 1.
