# PatchNewPatroniConfig Design

**Date:** 2026-06-03
**Status:** Approved, ready for implementation plan

## Problem

The catchup phase brings the upgraded PG17 cluster up under Patroni by running
`patroni_start_command` against the operator's new-cluster `patroni.yml`
(`upgrade.patroni_config_path`). Today the step `VerifyNewPatroniConfig` only
*verifies* that this file already declares a fresh DCS scope and the new
`data_dir`, and fails otherwise — leaving the operator to hand-edit the file.

In practice the operator copies the old cluster's `patroni.yml` and forgets to
update it, so it still carries PG13 values:

```yaml
scope: dbabuev-upgrade-test-2          # == old cluster name
postgresql:
  data_dir: /data/postgresql           # old data dir
  config_dir: /etc/postgresql/13/main  # PG13
  bin_dir: /usr/lib/postgresql/13/bin  # PG13
```

Two failure modes result:
- **scope == old cluster name** → Patroni refuses to start with a *system ID
  mismatch* (the DCS `/initialize` key holds the old cluster's sysid; the
  upgraded cluster has a new one).
- **bin_dir / data_dir still PG13** → Patroni manages the PG17 data dir with
  PG13 binaries → "database files are incompatible with server".

## Goal

Replace the verify-and-fail step with one that *patches* the new-cluster
`patroni.yml` to correct PG17 values before Patroni is started, so the operator
no longer hand-edits it.

## Decisions (settled during brainstorming)

1. **Fields patched:** `scope`, `postgresql.data_dir`, `postgresql.bin_dir`, and
   `postgresql.config_dir`. `config_dir` is patched only when a new value is
   configured (no safe default for its path); otherwise it is left untouched
   (Patroni defaults `config_dir` to `data_dir`).
2. **Scope value:** from a new config field `upgrade.new_scope`; when empty,
   derived as `<cluster_name>-17`.
3. **Write strategy:** in-place surgical edit of `patroni_config_path` via
   `yaml.Node` (preserving comments, key order, and untouched keys), writing a
   one-time `.bak` of the operator's original before the first edit. This is the
   same file `patroni_start_command` launches, so nothing else is rewired.

## Out of scope (YAGNI)

- Automating the finalize-phase DCS scope rename back to `<cluster_name>`.
  `VerifyRenamedCluster` stays operator-driven and unchanged — that is a
  different phase. This change only patches the file before PG17 starts.
- Patching any field other than the four above.

## Components

### 1. `internal/config/config.go`

- New `UpgradeConfig` fields:
  - `NewScope string` (`yaml:"new_scope"`) — DCS scope for the upgraded cluster.
  - `NewConfigDir string` (`yaml:"new_config_dir"`) — PG17 `config_dir`; empty
    means "do not patch config_dir".
- New helper `func (c Config) EffectiveNewScope() string` — returns
  `Upgrade.NewScope` if set, else `ClusterName + "-17"`.
- `ValidateForRun`: error if `EffectiveNewScope() == ClusterName` (guards against
  an explicit `new_scope` equal to the old name, which would reintroduce the
  system-ID-mismatch failure).

### 2. `internal/phases/patroni_patch.go` (new)

Pure, file-system-free function, unit-tested on bytes:

```go
// patchPatroniConfig sets scope and postgresql.{data_dir,bin_dir} (and
// config_dir when configDir != "") in a Patroni YAML document, preserving
// comments, key order, and all other keys. Missing keys are inserted.
func patchPatroniConfig(in []byte, scope, dataDir, binDir, configDir string) ([]byte, error)
```

Implementation: decode into a `yaml.Node` document; navigate the top-level
mapping; upsert the `scope` scalar and the `postgresql` sub-mapping's
`data_dir` / `bin_dir` / (`config_dir`) scalars via a small
`setMapValue(map *yaml.Node, key, value string)` helper that updates an existing
value node in place or appends a new key/value pair; re-encode.

### 3. `internal/phases/catchup.go`

Replace `verifyNewPatroniConfig` with `patchNewPatroniConfig` (step ID
`PatchNewPatroniConfig`), in the same position — immediately before
`StartPG17OnN1`.

- `Check(ctx)`: read `patroni_config_path`; report `done` when `scope`,
  `postgresql.data_dir`, and `postgresql.bin_dir` already equal the targets, and
  `config_dir` equals the target when `new_config_dir` is set. A missing/empty
  declared value (set via env, not the file) counts as not-yet-patched. Read
  error → return error.
- `Run(ctx)`: compute `scope = Cfg.EffectiveNewScope()`; read the file; if no
  `.bak` exists alongside it, write the original bytes to `<path>.bak`; produce
  patched bytes via `patchPatroniConfig`; write them back. Russian progress logs
  consistent with the rest of the phase (e.g. "патчу новый patroni.yml: scope=…,
  data_dir=…, bin_dir=…").

The existing helper `parsePatroniScopeDataDir` is extended (or a sibling added)
so `Check` can also read `bin_dir` and `config_dir`.

### 4. `pg-upgrade.example.yaml`

- Document `new_scope` (default `<cluster_name>-17`) and `new_config_dir`
  (empty = leave config_dir untouched).
- Update the `patroni_config_path` comment: the binary now *edits* this file
  (scope + postgresql data_dir/bin_dir/config_dir) and writes a `.bak`; the
  operator still owns the rest of the file.

## Data flow

```
catchup:
  VerifyOldClusterStopped
  PatchNewPatroniConfig   <- reads patroni_config_path, writes .bak once,
                             patches scope/data_dir/bin_dir/[config_dir]
  StartPG17OnN1           <- runs patroni_start_command on the corrected file
  CreateForwardSubscription
  WaitLagZero
  VerifyNewClusterHealthy
```

## Error handling

- File unreadable / unwritable → error including the path.
- Malformed YAML → error from `patchPatroniConfig`.
- `postgresql` key missing or not a mapping → the sub-mapping is created.
- `.bak` is written only if it does not already exist, so repeated runs never
  clobber the operator's original.

## Testing (TDD)

`patchPatroniConfig` (pure, byte-level):
- Patches `scope`, `data_dir`, `bin_dir` to targets.
- Patches `config_dir` when `configDir != ""`; leaves it untouched when `""`.
- Inserts missing keys (e.g. no `bin_dir` present, or no `postgresql` mapping).
- Preserves an unrelated key and a comment elsewhere in the document.
- Idempotent: patching already-patched bytes yields the same target values.

`patchNewPatroniConfig`:
- `Check` returns `true` when the file already holds all target values.
- `Check` returns `false` when any target differs.
- `Run` writes `<path>.bak` with the original on first run and patches the file.
- `Run` does not overwrite an existing `.bak` on a second run.

`config`:
- `EffectiveNewScope()` returns `<cluster_name>-17` by default and `NewScope`
  when set.
- `ValidateForRun` rejects `new_scope` equal to `cluster_name`.
