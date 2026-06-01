# pg-upgrade: Online PostgreSQL Major Version Upgrade

**Date:** 2026-06-01
**Scope:** Go binary for zero-downtime PostgreSQL upgrade (PG10+ → PG17) in a Patroni cluster

---

## Overview

A checkpoint-based orchestrator that upgrades a Patroni PostgreSQL cluster to a new major version with minimal downtime. The operator runs `pg-upgrade run` on the node designated as N1 (the upgrade target replica). The binary drives the process through ordered phases, pausing for operator confirmation at each phase boundary.

**Strategy:** Physical baseline + logical replication tail.
- N1 becomes a physical copy of the old primary via streaming replication
- N1 is isolated, pg_upgraded to PG17
- A logical subscription catches up the remaining delta
- A new Patroni cluster is formed from PG17 nodes; external tooling swaps the DSN

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    CLI (cobra)                       │
│   run │ status │ rollback │ [phase subcommands]      │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│                  Runner (FSM engine)                 │
│  executePhase → checkpoint → transition → persist    │
└──────┬─────────────────┬───────────────┬────────────┘
       │                 │               │
┌──────▼──────┐  ┌───────▼──────┐  ┌────▼───────────┐
│  Phase      │  │ StateManager │  │  Reporter      │
│  Registry   │  │  (JSON file) │  │  (live output) │
│  + Steps    │  └──────────────┘  └────────────────┘
└──────┬──────┘
       │
┌──────▼──────────────────────┐
│         Clients             │
│  PatroniClient  │ PGClient  │
│  (REST :8008)   │ (pgx)     │
└─────────────────────────────┘
```

**Separation of concerns:**
- `Runner` is the only place with transition logic. Phases do not import Runner.
- `Clients` are pure wrappers with no business logic.
- `StateManager` is the only place that writes to disk.
- `Reporter` receives events via channel and does not block Runner.

---

## Core Engine

### Interfaces

```go
type Step interface {
    ID()    StepID
    Check(ctx context.Context) (bool, error)  // true = already done, skip
    Run(ctx context.Context) error
}

type Transition struct {
    To        PhaseID
    Condition func(*State) bool  // nil = always; first match wins
}

type Phase interface {
    ID()          PhaseID
    Steps()       []Step
    Transitions() []Transition
}
```

### Runner loop

```go
func (r *Runner) Run(ctx context.Context) error {
    for {
        phase := r.phases[r.state.Current]
        if err := r.executePhase(ctx, phase); err != nil {
            return err
        }
        next, err := r.transition(phase)
        if err != nil || next == "" {
            return err
        }
        if err := r.checkpoint(phase, next); err != nil {
            return err  // operator aborted
        }
        r.state.Advance(next)
    }
}
```

`checkpoint()` is a no-op in `Headless` mode — the only change required to enable full automation.

---

## Configuration

```yaml
cluster_name: prod
upgrade:
  target_node: n1.internal      # must be the node running this binary
  slot_name: slot_upgrade
  publication_name: pub_upgrade
  new_pg_bindir: /usr/lib/postgresql/17/bin

pg:
  superuser_dsn: "host=... port=5432 dbname=postgres user=postgres password=..."
```

Cluster topology is discovered at runtime from `localhost:8008/cluster` (Patroni REST API, no auth). No static node list required.

---

## Phases

### Invariant notation
- **Check** column: condition that causes a step to be skipped (idempotency)
- All artifacts written to `state.Artifacts` are referenced by downstream steps

---

### Phase 1: Prepare

Creates the logical replication foundation on the primary **before** N1 disconnects from WAL. The slot's `confirmed_flush_lsn` must be ≤ the future `target_lsn`.

| Step | Check |
|------|-------|
| DiscoverTopology | — (always runs; writes primary host to Artifacts) |
| VerifyPrerequisites | wal_level=logical on primary; N1 is a replica, not primary |
| CreatePublication | `pg_publication` WHERE pubname = slot_name |
| CreateLogicalSlot | `pg_replication_slots` WHERE slot_name = slot_name |
| RecordSlotBaseline | `Artifacts.SlotBaseline != nil` |

**SlotBaseline** captures `restart_lsn` (WAL retention anchor) and `confirmed_flush_lsn` (subscription start point) from `pg_replication_slots` immediately after slot creation.

Checkpoint: *"Logical slot created. Proceed to isolate N1?"*

---

### Phase 2: Isolate

Disconnects N1 from WAL sources and records the physical boundary (`target_lsn`). `received_lsn` must be captured **before** disconnecting because `pg_stat_wal_receiver` becomes empty afterwards.

| Step | Check |
|------|-------|
| PausePatroni | Patroni `/cluster` → `paused=true` |
| CaptureReceivedLSN | `Artifacts.ReceivedLSN != ""` |
| DisconnectN1FromWAL | `pg_stat_wal_receiver` on N1 is empty |
| WaitReplayComplete | `pg_last_wal_replay_lsn() >= Artifacts.ReceivedLSN` |
| RecordTargetLSN | `Artifacts.TargetLSN != ""` |

**Disconnect mechanism:** `ALTER SYSTEM SET primary_conninfo = ''; SELECT pg_reload_conf();` on N1. Patroni is paused and will not override this.

**Post-phase invariant:**
```
Artifacts.SlotBaseline.ConfirmedFlushLSN <= Artifacts.TargetLSN
```
Violation is a fatal error: slot was created after N1 disconnected; changes would be lost.

Checkpoint: *"N1 isolated. target_lsn recorded. Run slot drain?"*

---

### Phase 3: Drain

Advances `confirmed_flush_lsn` to the last transaction commit ≤ `target_lsn`. The subscription on PG17 will start from this position, covering only changes absent from N1's physical copy.

**Why drain is required:** The logical slot contains changes from `SlotBaseline.ConfirmedFlushLSN` onward. N1's physical copy already contains all changes up to `target_lsn`. If the subscription started from `SlotBaseline.ConfirmedFlushLSN`, it would re-apply already-present data causing duplicate key violations.

**Protocol:** `slotdrain` uses the logical replication protocol (`START_REPLICATION SLOT ... LOGICAL`). For each transaction it reads `Begin → changes → Commit`. It sends `StandbyStatusUpdate(flush_lsn = commit_lsn)` only when `commit_lsn <= target_lsn`. On the first transaction with `commit_lsn > target_lsn` it disconnects without ACKing.

| Step | Check |
|------|-------|
| RunSlotDrain | `Artifacts.DrainReport != nil` |
| VerifySlotDrained | `confirmed_flush_lsn` from `pg_replication_slots` matches expected |

Checkpoint: *"Slot drained. Proceed to pg_upgrade?"*

---

### Phase 4: Upgrade

⚠️ **Point of no return: `pg_upgrade --link` hard-links the data directory, making the old PG10 data dir unusable.**

`pg_upgrade` requires a cleanly shut-down primary data directory. N1 is currently a standby, so it must be promoted first, then shut down.

**Shutdown procedure:**
1. Two `CHECKPOINT` calls to flush dirty pages
2. `pg_ctl stop -m smart` + `pg_terminate_backend` for client connections
3. Verify via `pg_controldata`: "Database cluster state: shut down"

**Patroni config** for the new cluster is finalized here because PG17's SYSID (required in the config) is only known after `pg_upgrade` completes: `pg_controldata | grep "Database system identifier"`.

| Step | Check |
|------|-------|
| PromoteN1 | `pg_controldata` → "in production"; `pg_is_in_recovery()` = false |
| ShutdownN1Clean | `pg_controldata` → "shut down" |
| RunPgUpgradeCheck | `Artifacts.PgUpgradeCheckPassed = true` |
| RunPgUpgrade | `Artifacts.PgUpgradeDone = true` |
| WriteFinalPatroniConfig | config file exists with correct PG17 SYSID |

Checkpoint: *"pg_upgrade complete. Create subscription and start catchup?"*

---

### Phase 5: Catchup

Starts PG17 on N1, creates the logical subscription, and builds the new Patroni cluster while N1 catches up. This is a **stable waiting state** — no time pressure, no write freeze.

**No conflict on subscription apply:** Uncommitted transactions physically present in N1's data dir (between `confirmed_flush_lsn` and `target_lsn`) are rolled back by PostgreSQL during PG17 startup. The subscription delivers them as new logical changes to a clean slate.

| Step | Check |
|------|-------|
| StartPG17OnN1 | PG17 accepts connections on port |
| CreateSubscription | `pg_subscription` WHERE subname = subscription_name |
| WaitLagZero | `pg_stat_subscription`: write_lag + flush_lag + replay_lag = 0 |
| InitNewPatroniCluster | Patroni on N1: `/cluster` shows leader |
| AddReplicas | N2/N3 join new cluster via physical replication from N1 |
| VerifyNewClusterHealthy | leader + ≥1 replica in new cluster |

Checkpoint: *"New cluster healthy, subscription at zero lag. Begin switchover?"*

---

### Phase 6: Switchover

The critical section. Duration should be seconds. Write freeze uses DML triggers so existing connections degrade gracefully to read-only rather than being disconnected.

**Freeze mechanism:** DML triggers with default `ENABLE` setting fire for application users but are **skipped** for the `sub_rollback` replication apply worker, which runs with `session_replication_role = 'replica'`.

```sql
CREATE OR REPLACE FUNCTION raise_upgrade_readonly() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'database is read-only during upgrade window'
        USING ERRCODE = 'read_only_sql_transaction';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Applied to all user tables via DO block
CREATE TRIGGER upgrade_freeze
    BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON <table>
    FOR EACH STATEMENT EXECUTE FUNCTION raise_upgrade_readonly();

-- Belt-and-suspenders for new connections
ALTER DATABASE mydb SET default_transaction_read_only = on;
```

**Sequence sync** is safe only after old primary is frozen. Sequences are not replicated by logical replication.

| Step | Check |
|------|-------|
| FreezeOldPrimary | DML triggers installed on all tables; `default_transaction_read_only` set |
| WaitFinalLagZero | subscription lag = 0 after freeze |
| SyncSequences | `Artifacts.SequencesSynced = true` |
| SetupReverseReplication | `pub_rollback` on PG17; `sub_rollback` on old primary (enabled, copy_data=false) |
| NotifyDSNSwap | `Artifacts.DSNSwapNotified = true`; writes signal file for external tooling |
| VerifyTrafficOnNew | connections arriving at new cluster primary |
| DisableForwardSubscription | `ALTER SUBSCRIPTION sub_upgrade DISABLE` |

**Rollback window after DSN swap:**
```
PG17 (writes) → pub_rollback → sub_rollback → old primary (current)
```
Window closes on: DDL change on PG17, or explicit operator decision to proceed to Finalize.

Checkpoint: *"DSN swapped. Monitor rollback window. Proceed to Finalize (no rollback after this)?"*

---

### Phase 7: Finalize

Commits the upgrade. Removes all upgrade artifacts from the live system.

**Patroni cluster rename** (prod-v17 → prod):
1. Stop old Patroni cluster (already paused)
2. `etcdctl del /service/prod/ --prefix` — remove stale old cluster DCS keys
3. Stop new Patroni cluster
4. Change `scope: prod-v17` → `scope: prod` in patroni.yml on N1/N2/N3
5. Start new Patroni → creates `/service/prod/initialize` with PG17 SYSID

| Step | Check |
|------|-------|
| DropReverseReplication | `DROP SUBSCRIPTION sub_rollback`; `DROP PUBLICATION pub_rollback` |
| DropForwardSubscription | `DROP SUBSCRIPTION sub_upgrade` (also drops slot on old primary) |
| UnfreezeOldPrimary | DROP upgrade_freeze triggers; `ALTER DATABASE mydb RESET default_transaction_read_only` |
| RenamePatroniCluster | new cluster responds as `prod` |
| VerifyRenamedCluster | `patronictl list prod` shows healthy cluster |

Checkpoint: *"Upgrade committed. Proceed to decommission old cluster?"*

---

### Phase 8: Cleanup

| Step | Check |
|------|-------|
| StopOldPostgres | old postmaster.pid absent |
| ArchivePgUpgradeLogs | `pg_upgrade_output.d/` copied to safe location |
| RemoveOldDCSKeys | `etcdctl del /service/prod/ --prefix` (safety; should be empty) |

---

## State

```go
type State struct {
    Version     string                 `json:"version"`
    ClusterName string                 `json:"cluster_name"`
    StartedAt   time.Time              `json:"started_at"`
    Current     PhaseID                `json:"current_phase"`
    Phases      map[PhaseID]PhaseState `json:"phases"`
    Artifacts   Artifacts              `json:"artifacts"`
    LastError   *StepError             `json:"last_error,omitempty"`
}

type Artifacts struct {
    PrimaryHost           string        `json:"primary_host,omitempty"`
    SlotBaseline          *SlotBaseline `json:"slot_baseline,omitempty"`
    ReceivedLSN           string        `json:"received_lsn,omitempty"`
    TargetLSN             string        `json:"target_lsn,omitempty"`
    DrainReport           *DrainReport  `json:"drain_report,omitempty"`
    PgUpgradeCheckPassed  bool          `json:"pg_upgrade_check_passed"`
    PgUpgradeDone         bool          `json:"pg_upgrade_done"`
    PG17SYSID             string        `json:"pg17_sysid,omitempty"`
    SequencesSynced       bool          `json:"sequences_synced"`
    DSNSwapNotified       bool          `json:"dsn_swap_notified"`
}
```

**Atomic write:** write to `<path>.tmp`, then `os.Rename()`. Safe against partial writes on Linux.

**Idempotency pattern:** each step's `Check()` reads from `Artifacts`. No separate "did this run?" flags — the artifact's presence is the proof.

---

## Reporter

Channel-based, non-blocking for Runner:

```go
type Reporter struct {
    events  chan Event
    metrics chan MetricSnapshot
}
```

**Two data sources:**
- Runner writes `Event{Type, Phase, Step, Message}` as steps execute
- A background goroutine polls PG + Patroni every 2 seconds, writes `MetricSnapshot`

**Phase-aware metrics:**
- Drain: `slot_lag_bytes = restart_lsn - confirmed_flush_lsn` from `pg_replication_slots`
- Catchup / Switchover: `sub_lag_ms` from `pg_stat_subscription`
- Always: Patroni cluster state from `/cluster`

**Terminal output:**

```
[pg-upgrade] prod  PG10→PG17  started: 2026-06-01 10:00:00

✓ PREPARE     10:00 → 10:04
▶ ISOLATE     10:04 → running
  ✓ pause_patroni
  ✓ capture_received_lsn    received_lsn=0/3FA20000
  ✓ disconnect_n1_from_wal
  ⟳ wait_replay_complete    lag: 24 kB remaining

Cluster: prod | primary: master.internal | replicas: 2/5
```

Steps are append-only lines. Metrics overwrite the last two lines via ANSI `\r\033[K`. In Headless mode: structured JSON to stdout.

---

## Directory Structure

```
online_upgrade/
├── cmd/
│   └── pg-upgrade/
│       └── main.go               # entry point; wires Runner from components
├── internal/
│   ├── config/
│   │   └── config.go
│   ├── runner/
│   │   ├── interfaces.go         # Phase, Step, Transition
│   │   ├── runner.go             # Run(), executePhase(), transition(), checkpoint()
│   │   └── types.go              # PhaseID, StepID, StepStatus, RunMode
│   ├── phases/
│   │   ├── prepare/
│   │   │   ├── phase.go
│   │   │   └── steps.go
│   │   ├── isolate/
│   │   ├── drain/
│   │   ├── upgrade/
│   │   ├── catchup/
│   │   ├── switchover/
│   │   ├── finalize/
│   │   └── cleanup/
│   ├── slotdrain/
│   │   └── drain.go              # pglogrepl-based logical slot reader
│   ├── state/
│   │   ├── manager.go
│   │   └── types.go
│   ├── reporter/
│   │   ├── reporter.go
│   │   └── types.go
│   └── clients/
│       ├── patroni/
│       │   └── client.go         # /cluster, /pause, /resume, /config
│       └── pg/
│           └── client.go         # pgx wrapper
├── docs/
│   └── superpowers/
│       └── specs/
├── go.mod
└── go.sum
```

**Dependency rule:**
```
phases/* → clients/*, state, runner/interfaces
runner   → runner/interfaces, state, reporter
cmd      → runner, phases/*, config
```

`phases` import only `runner/interfaces`, never `runner` itself. No circular dependencies.

**Key dependencies:**
```
github.com/jackc/pgx/v5       # PostgreSQL client
github.com/jackc/pglogrepl    # logical replication protocol (slotdrain)
github.com/spf13/cobra        # CLI
gopkg.in/yaml.v3              # config
```

---

## Known Limitations

- **Unlogged tables** are not replicated by logical replication. Must be handled separately (truncate + bulk copy, or accept loss).
- **Sequences** are not replicated. Synced manually in Switchover critical section with a safety buffer for cached values.
- **TRUNCATE** is covered by the `upgrade_freeze` trigger only if defined as `FOR EACH STATEMENT` — verify trigger fires on TRUNCATE across PostgreSQL versions.
- **Reverse replication rollback window** closes on first DDL change on PG17 after DSN swap.
- **pg_upgrade --link** is the point of no return. The old PG10 data directory becomes unusable. There is no filesystem-level rollback after this step.
