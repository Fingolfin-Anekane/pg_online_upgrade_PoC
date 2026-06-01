# loadtool: Load + Correctness Harness for Online Upgrade

**Date:** 2026-06-02
**Scope:** Go CLI companion to `pg-upgrade` that generates workload during an
online PostgreSQL major-version upgrade, simulates the client DSN swap on demand,
and renders an authoritative consistency verdict after the upgrade.

---

## Overview

`loadtool` is a test-bench companion to the `pg-upgrade` orchestrator. It drives
a continuous, verifiable workload against the cluster while an upgrade runs, then
proves (or disproves) that the upgrade preserved data integrity.

It is a **load generator + correctness oracle** (Jepsen-style, not a blind load
generator). Every committed write is recorded in a durable intent-log so that a
final reconciliation pass can decide, per operation, whether the database state
matches what the client believes it committed.

The headline feature is **in-flight DSN switching**: the tool holds two DSNs
(old primary `DSN-A` → new primary `DSN-B`) and, on an operator `SIGHUP`, moves
all workers from A to B. This imitates the external tooling that swaps the DSN
for real clients at switchover. The operator triggers the swap manually so the
switch can be pinned deterministically to a chosen upgrade phase.

**Design stance on self-observation:** the harness reads from the same system it
is testing, so live reads cannot be a hard oracle — replication lag or a stale
read would produce false "violation" reports. Therefore:

- **Continuous output during `run` = observation, not verdict.** Throughput,
  error classes, and the unavailability window are reported with timestamps so a
  violation can be correlated with an upgrade phase, but nothing is judged here.
- **Final reconciliation (`verify`) = the only authoritative oracle.** It runs
  after the workload stops and replication has converged, comparing the durable
  intent-log against the database. It is immune to lag because it runs at rest.

---

## Failure modes under test

Each case ties a workload to the specific bug it provokes in the upgrade
strategy (physical baseline + logical replication tail + critical-section DSN
swap).

| # | Case | Bug provoked | Workload | Oracle |
|---|------|--------------|----------|--------|
| 1 | Lost writes in switchover window | Committed on old primary, not yet applied to PG17 before DSN swap → write vanishes | `append` INSERTs with monotonic `client_seq` | acked `client_seq` present in DB exactly once |
| 2 | Sequence dup/gap after sync | `pg_upgrade` resets sequences from dump; logical replication does not replicate sequence advances; PG17 sequence lags during catchup; bad sync in critical section reissues a used id | `append` INSERTs into `bigserial` PK table | zero duplicate `id`; `nextval(PG17) ≥ max(id)` |
| 3 | Long transactions across `target_lsn` | Logical replication streams a txn at COMMIT (commit_lsn). A txn beginning before `target_lsn` but committing after is not physically on N1 and must arrive via subscription exactly once; a buggy drain (ACK by begin_lsn / wrong restart_lsn) loses or duplicates it | `long-txn` slow transactions tagged with `batch_id`, deliberately spanning the isolation point | every `batch_id` delivered whole, exactly once |
| 4 | Non-atomic apply (sum invariant) | Partial application of a transfer in catchup/reverse replication | `transfer` UPDATE of two rows in one txn | `SUM(balance)` constant |
| 5 | In-flight txn atomicity at swap | Multi-statement txn open when old primary goes read-only / DSN swaps must roll back wholly, not partially | `transfer` running continuously through the swap | all-or-nothing; no partial batch |
| 6 | Unavailability window | Duration the DB is unwritable/unreachable during the swap | continuous write probes with timestamps | length of the contiguous error window after `SIGHUP` |
| 7 | Read-your-writes across swap | Reads routed to PG17 before catchup completes / to a lagging replica miss a fresh commit | `ryw` write-then-read across the boundary | value read on DSN-B ≥ last pre-switch acked value |

**Oracle complementarity (important):** the sum invariant (case 4) does **not**
catch a cleanly lost or cleanly duplicated whole transfer — a transfer is
zero-sum, so losing both legs atomically, or applying both twice, leaves `SUM`
unchanged. The sum invariant only catches **non-atomic** application. Whole-txn
loss/duplication is caught by the append-only intent-log via `client_seq`. The
two oracles are complementary, not redundant.

---

## Architecture

```
┌──────────────────────────────────────────────┐
│                  CLI (cobra)                   │
│        init │ run │ verify                      │
└───────────────┬────────────────────────────────┘
                │
   ┌────────────┴───────────────┐
   │                            │
┌──▼─────────────┐      ┌───────▼──────────┐
│   loadgen       │      │     oracle        │
│  (run)          │      │  (verify)         │
│ append/long-txn │      │ intent-log reader │
│ transfer/ryw    │      │ + reconciliation  │
│ DSN switcher    │      └───────────────────┘
│ intent-log writer ─────────► JSONL on disk ◄───┘
└──┬──────────────┘
   │
┌──▼───────────────┐
│  Connections      │
│  pool A / pool B  │  (pgx/v5)
└───────────────────┘
```

**Separation of concerns:**

- `loadgen` owns workers, the live DSN selection (A vs B), and writing the
  intent-log. It produces observations to stdout but renders no verdict.
- `oracle` owns reading the intent-log and the database and producing the
  verdict. It is the only place a pass/fail decision is made.
- `loadcfg` owns flag + YAML config parsing (mirrors `pg-upgrade`'s config).
- Connection pools are dumb pgx wrappers; one pool per DSN.

---

## Schema

Created by `loadtool init`. Two tables.

```sql
-- Primary integrity oracle (cases 1, 2, 3, 7)
CREATE TABLE events (
    id         bigserial PRIMARY KEY,        -- from sequence → dup/advance check (case 2)
    writer_id  int        NOT NULL,
    client_seq bigint     NOT NULL,          -- monotonic per writer → loss/dup (case 1)
    batch_id   uuid       NULL,              -- groups rows of one long txn (case 3)
    payload    text       NOT NULL,
    ts         timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (writer_id, client_seq)
);

-- Atomicity oracle (cases 4, 5)
CREATE TABLE accounts (
    id      int    PRIMARY KEY,
    balance bigint NOT NULL
);
-- seeded with K accounts; SUM(balance) is a known constant
```

`init` is idempotent: `CREATE TABLE IF NOT EXISTS`; `accounts` is seeded only
when empty (or re-seeded with `--reset`, which TRUNCATEs both tables). The
`UNIQUE (writer_id, client_seq)` constraint turns a duplicated whole-txn into an
observable `unique_violation` as well as a reconciliation finding.

---

## Workers

Configured per-workload (worker count + rate). Sensible defaults `4 / 1 / 2 / 1`.

- **append** (N writers, rate): tight INSERT loop into `events`. Each writer owns
  a `writer_id` and an in-process monotonic `client_seq` counter (no gaps among
  acked). The bread-and-butter loss/dup/sequence workload.
- **long-txn** (M writers, txn-duration, batch-size): `BEGIN`; insert `batch-size`
  rows sharing a fresh `batch_id`; sleep `txn-duration`; `COMMIT`. Deliberately
  long-lived so commits straddle the N1 isolation point and exercise slot-drain.
- **transfer** (P writers, rate): `BEGIN; UPDATE accounts SET balance=balance-x
  WHERE id=a; UPDATE accounts SET balance=balance+x WHERE id=b; COMMIT`. Drives
  the sum invariant and in-flight atomicity.
- **ryw** (Q writers): INSERT (acked), then read back this writer's max
  `client_seq`. Across the swap, reads from DSN-B and asserts it observes its own
  pre-switch acked writes.

All workers select their pool through a shared, atomically-swappable `activeDSN`
selector. On `SIGHUP` the selector flips A→B; in-flight connections to A are
allowed to error and reconnect to B (that error window is case 6's measurement).

---

## Intent-log (durable, append-only JSONL)

Written by `loadgen` to a local file (`--intent-log`, default
`loadtool-intent.jsonl`) **and** streamed human-readably to stdout. Append-only:
two records per operation.

```jsonc
// attempt — written immediately before COMMIT
{"op_id":"...","kind":"append","writer_id":3,"client_seq":1041,
 "batch_id":null,"dsn":"a","phase":"pre-switch","status":"attempt",
 "ts":"2026-06-02T10:15:03.412Z"}

// result — written on response (or its absence)
{"op_id":"...","status":"acked","ts":"..."}            // got COMMIT ack
{"op_id":"...","status":"failed","error":"...","ts":"..."}   // explicit error before commit ack
{"op_id":"...","status":"indoubt","error":"...","ts":"..."}  // sent COMMIT, no response
```

**Status semantics** (the three-set classification):

- **acked** — received a clear COMMIT acknowledgement → must be in the DB
  **exactly once**.
- **failed** — received an explicit error before the commit was acknowledged →
  must be **absent**.
- **in-doubt** — sent COMMIT but the connection dropped before any response (the
  expected condition at the swap) → presence **or** absence is acceptable, but
  **never a duplicate**.

`phase` is `pre-switch` or `post-switch`, flipped by `SIGHUP`, so the log records
which side of the swap every operation landed on.

---

## `verify` — final reconciliation

Reads the intent-log, pairs `attempt`/result records by `op_id`, then queries the
database via DSN-B at rest (after the workload has stopped and replication has
converged). Findings:

**events — loss / duplication / phantom**
- acked: matched in DB by `(writer_id, client_seq)`. Missing → **LOST**.
  More than one → **DUP**.
- failed: must not be present. Present → **PHANTOM-COMMIT**.
- in-doubt: present-or-absent both fine; present more than once → **DUP**.

**events — sequence (case 2)**
- duplicate `id` count must be 0.
- `nextval` of the `events_id_seq` (read non-destructively from
  `pg_sequences.last_value`) must be `≥ max(id)`.
- **Gaps in `id` are NOT a finding** — sequence gaps are normal (rolled-back
  txns burn sequence values). The loss signal lives in `client_seq` continuity
  among acked operations, not in `id`.

**events — long-txn (case 3)**
- for every `batch_id` in an acked long-txn, all `batch-size` rows are present
  exactly once (no partial batch, no duplicate batch).

**accounts — atomicity (cases 4, 5)**
- `SUM(balance)` equals the seeded constant. Mismatch → **NON-ATOMIC**.

Output is a structured report (counts per finding class) plus a final
`PASS`/`FAIL`. Because `verify` is a separate command reading a durable log, it
can be re-run after a restart without losing ground-truth.

---

## Continuous observation (during `run`, non-authoritative)

Streamed to stdout, never a verdict:

- throughput per workload (commits/s);
- error counts classified: `read-only`, `conn-refused`, `timeout`, `other`;
- **unavailability window** (case 6): the contiguous interval from the first
  error after `SIGHUP` to the first success on DSN-B, reported with start/end
  timestamps and duration.

---

## CLI surface

- `loadtool init   [--dsn-a] [--accounts K] [--reset]` — create + seed schema.
- `loadtool run     --dsn-a --dsn-b [--duration] [worker knobs] [--intent-log]`
  — generate load; `SIGHUP` swaps A→B; `SIGINT`/`--duration` stops.
- `loadtool verify  --dsn-b --intent-log` — reconcile, print verdict.

The expected `accounts` sum is not a manual input: `init` records it (alongside
`K` and the seed value) in a small sidecar meta record so `verify` reads it back
rather than relying on the operator to re-supply it. The meta lives in the
intent-log header (first record) written by `run`, or, if `run` and `init` use
the same intent-log path, is carried forward; the concrete carrier is a
`{"kind":"meta","accounts_sum":...,"accounts_k":...}` record at the head of the
intent-log.

Config via flags **and** YAML (`--config`), same pattern as `pg-upgrade`.

---

## Directory structure

```
cmd/loadtool/main.go            # cobra root + init/run/verify subcommands
internal/loadcfg/config.go      # flags + YAML config, validation
internal/loadgen/
    runner.go                   # worker lifecycle, SIGHUP swap, shutdown
    workers.go                  # append / long-txn / transfer / ryw
    dsn.go                      # atomically-swappable active-DSN selector
    intentlog.go                # durable JSONL writer + stdout stream
internal/oracle/
    intentlog.go                # JSONL reader, attempt/result pairing
    reconcile.go                # the three-set + sequence + sum + batch checks
    report.go                   # findings + PASS/FAIL
```

---

## Out of scope (explicit)

- Does not directly validate reverse replication (PG17 → old primary); the
  rollback path is exercised by `pg-upgrade`, not asserted here.
- Single database / single publication.
- Does not validate Patroni topology or the DCS; it is a data-plane harness only.
- Does not auto-detect the swap moment; the operator triggers `SIGHUP`.
