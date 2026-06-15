# Shadow-Cluster Major Upgrade — Design

**Status:** design / approved skeleton, pending spec review
**Date:** 2026-06-15
**Supersedes (optionally):** the "cannibalize a live replica (N1)" topology of the current FSM. The phase machinery (prepare/isolate/drain/upgrade/catchup/switchover/finalize/cleanup) is largely reused; the topology changes.

## Goal

Perform an online PG13→PG17 major upgrade **without degrading the production cluster's HA or performance during the transition**, and **without the logical replication slot bloating pg_wal and filling the disk on the production primary**.

The current approach isolates a live replica (N1) out of the serving cluster, upgrades it, and promotes it as the new cluster — then replicas must be added back by cannibalizing the old cluster. That degrades HA/perf on the serving cluster during the window and performs its riskiest step (isolate) on the live production Patroni.

## Core idea

Run a **shadow cluster** on a full parallel set of nodes. It starts at the **old major (PG13)** as a Patroni `standby_cluster` that physically replicates the production cluster, then is upgraded in place to PG17 and finally receives traffic via a DSN swap. Production is never touched until cutover, so it keeps full HA the whole time, and rollback is trivial (don't swap).

**Key insight — LSN alignment.** Because the shadow physically replicated production, the shadow leader's freeze point is a valid LSN in production's WAL stream. The logical-replication seam (slot on production → tail delivered to the upgraded shadow leader) lands exactly on that LSN with no gap. Physical replication preserves LSNs; that is what makes the seam clean.

**Why physical replication can't be PG17←PG13.** Streaming replication requires identical majors (same WAL format / catalog layout). So the shadow must *start* as PG13 and the major jump still happens via `pg_upgrade` + a logical tail — the same machinery as today, but relocated onto a non-serving cluster.

## Decisions (locked during brainstorming)

- **Node budget:** full parallel cluster (M shadow nodes = production node count) for the migration window.
- **Replica upgrade method:** Patroni `reinit` (pg_basebackup from the upgraded PG17 leader). Not CSI volume-clone (not assumed available), not rsync-over-SSH (no SSH in k8s).
- **provision is tool-driven:** the tool itself applies the `standby_cluster` config and creates the physical slot on production (patch + wait), rather than only verifying operator setup.
- **Cutover posture:** **full HA before cutover.** Replicas are rebuilt to PG17 *before* the switch, so the new cluster serves with full HA from the first moment of cutover. This preserves the original HA goal.
- **Replica rebuild is its own phase** after catchup (not concurrent inside catchup), to lower peak WAL retention (see Disk Safety).

## Topology (during the migration window)

```
   PROD (PG13, untouched — full HA)                 SHADOW (separate nodes)
   ┌──────────────────────────────┐                ┌──────────────────────────────┐
   │ primary ─► replica ─► replica │ ──physical──►  │ standby-leader ─► repl ─► repl │
   └──────────────────────────────┘  (standby_     └──────────────────────────────┘
        ▲ publication + logical slot   cluster)            ▲ traffic moves here at cutover
        ▲ physical slot (shadow stream; dropped at shadow promote)
```

Two slots live on the production primary:
- **physical slot** — feeds the shadow's physical stream. Created in `provision`, dropped in `isolate` (after the shadow leader is promoted and no longer streams physically).
- **logical slot** — carries the post-freeze tail. Created in `prepare`, dropped in `finalize`.

## Phases

`provision → prepare → isolate → drain → upgrade → catchup → rebuild-replicas → switchover → finalize → cleanup`

| Phase | Action |
|---|---|
| **provision** *(new)* | Tool applies `standby_cluster` config to the existing shadow cluster (source = prod primary, `primary_slot_name` = physical slot) and creates the physical slot on prod. Patroni reinitializes the shadow nodes from prod. Gate: lag≈0 and full node set healthy. |
| **prepare** | On prod: ensure `wal_level=logical`, create publication + logical slot, record slot baseline. **Lock DDL on prod** (event trigger; last step, after pub+slot). (Otherwise same as today.) Plus verify the shadow is caught up. |
| **isolate** | On the **shadow leader**: remove `standby_cluster` → Patroni promotes it to a standalone PG13 primary; settle replay and capture `target_lsn` (the freeze point, in prod's LSN space); drop the physical slot on prod. **Production is not touched** — no prod-Patroni pause, no `primary_conninfo` races. |
| **drain** | Advance the prod logical slot's `confirmed_flush_lsn` to `target_lsn` (same drain machinery; LSNs align). Releases retained WAL up to `target_lsn`. |
| **upgrade** | `pg_upgrade --link` the **shadow leader only** → PG17 (frozen at `target_lsn`). |
| **catchup** | Bring up the PG17 shadow leader under Patroni; create the forward logical subscription prod→leader; wait until tail lag≈0. **Ensure the DDL lock is present on the new leader** (idempotent; inherited via physical repl + pg_upgrade, re-installed if missing). **No replicas here.** |
| **rebuild-replicas** *(new)* | For each shadow replica: switch its Patroni `bin_dir` to PG17 and `patronictl reinit` (pg_basebackup from the PG17 leader); wait until streaming and caught up → **full HA on the shadow**. The leader keeps applying the ongoing tail throughout (logical slot stays alive). Disk-safety throttle applies (below). |
| **switchover** | Freeze prod writes, drain the final lag, sync sequences, DSN swap → traffic to the PG17 shadow, verify traffic, disable the forward subscription. (Same as today.) |
| **finalize / cleanup** | **Release the DDL lock on the new (now-production) cluster** so the app can run migrations again. Drop the logical slot + publication on prod, drop any leftover slots, decommission the prod cluster after the rollback window. Prod is **not** unfrozen (split-brain protection); its DDL lock is dropped as part of teardown. |

### isolate — freeze + `target_lsn` capture (open mechanism)

The shadow leader must (a) stop replaying prod at a precise LSN, (b) expose that LSN as `target_lsn` in prod's LSN space, and (c) reach a datadir state `pg_upgrade` accepts. Two candidate mechanisms, to be nailed in the implementation plan:
1. **Patroni promote** (remove `standby_cluster`) → standalone primary; `target_lsn` = last replay LSN before the timeline switch. Reuse the existing settle-replay logic so `target_lsn` equals the true freeze point (see the resolved `received_lsn`-gap fix).
2. **Stop recovery at a chosen LSN** and cleanly shut the standby down; `pg_upgrade` over the stopped datadir.

The logical tail is timeline-agnostic (it ships row changes, not WAL), so the shadow's new timeline does not affect the seam.

## Disk safety / WAL retention (primary goal)

The failure to prevent: the logical slot's `restart_lsn` lags, pg_wal grows on the prod primary, and the disk fills → production outage. Strategy:

**1. Bound the slot, fail clean instead of filling disk.** Set `max_slot_wal_keep_size` on prod to a bounded value sized to free-disk headroom — **not `-1`**. If the slot ever lags past the cap, PostgreSQL invalidates it (`wal_status` → `unreserved`/`lost`) and our **hard guard** (`assertSlotReserved` in drain/catchup) turns that into a clean migration abort + retry, rather than a disk-full outage. For the stated goal, a clean abort is the desired failure mode.

**2. Where WAL actually accumulates** (slot `restart_lsn` not advancing):
   - *physical slot, during `provision`*: from slot creation until the shadow's initial basebackup completes and it streams. A long initial clone on a write-heavy prod accumulates WAL. Monitor; the bounded `max_slot_wal_keep_size` is the backstop (its breach fails the clone, which is retryable).
   - *logical slot, `prepare`→`drain`*: short — `drain` consumes up to `target_lsn` and releases it.
   - *logical slot, `drain`→`catchup` subscriber start*: idle window ≈ `pg_upgrade --link` duration (fast); retains prod writes during it.
   - *logical slot, `catchup`→`switchover`*: retention ≈ leader apply lag. The **main** window; spans `rebuild-replicas`.

**3. Lower the peak (the sequencing decision).** Catch the tail to lag≈0 *before* loading the leader with replica basebackups. During `rebuild-replicas` the leader then only tracks the ongoing prod write rate, not a backlog + basebackup at once → lower peak lag → lower peak retention. (This does not shorten the window — the slot lives until switchover regardless — it lowers the peak.)

**4. Active monitoring + adaptive throttle.** During the long poles (`provision` clone, `rebuild-replicas`), the tool monitors prod slot lag (`pg_current_wal_lsn() - restart_lsn`) and prod free disk. If lag approaches the cap, **pause the loading activity** (replica basebackup) to let the leader/standby catch up, then resume. Abort with a clear message if free disk crosses a hard threshold.

**5. Preflight.** The existing `prepare` preflight already warns on `max_slot_wal_keep_size` and long-running transactions (which pin `restart_lsn`/`catalog_xmin`). Keep it.

## DDL lockdown (accident prevention)

Logical replication does **not** carry DDL. A schema change on the old primary after the freeze (`target_lsn`) diverges the new cluster's schema from the incoming row stream → apply breaks or data corrupts. Same hazard on the new cluster if DDL hits it before the migration finishes. So DDL is locked on both clusters for the window, to stop an accidentally-run migration from breaking the upgrade.

**Mechanism — event trigger with a session-GUC bypass:**

```sql
CREATE OR REPLACE FUNCTION pg_upgrade_block_ddl() RETURNS event_trigger AS $$
BEGIN
  IF current_setting('pg_upgrade.allow_ddl', true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION 'DDL is locked during the online upgrade (command %)', tg_tag
      USING HINT = 'pg-upgrade safeguard: an app migration must not run now.';
  END IF;
END $$ LANGUAGE plpgsql;
CREATE EVENT TRIGGER pg_upgrade_ddl_lock ON ddl_command_start
  EXECUTE FUNCTION pg_upgrade_block_ddl();
```

The tool sets `SET pg_upgrade.allow_ddl = on` in its own admin sessions, so its legitimate DDL is not blocked — the freeze triggers (`CREATE TRIGGER` in switchover), `CREATE/ALTER/DROP SUBSCRIPTION`, `DROP PUBLICATION`, and the lock/unlock itself. Accidental app DDL (no GUC) is rejected with a clear error.

**Lifecycle:**
- `prepare` installs the lock on the **old primary** (last step, after the publication + slot exist, so those creates aren't blocked). It propagates to the shadow via physical replication and **survives `pg_upgrade`**, so the new cluster inherits it before any user access; `catchup` re-asserts it idempotently so the safeguard doesn't depend solely on inheritance.
- `finalize` releases it on the **new (now-production) cluster** (`DROP EVENT TRIGGER`) so the app can migrate normally. The old cluster stays DML-frozen and is decommissioned; its lock drops at teardown.

**Honest limitation:** this stops *accidents*, not intent — anyone who can `SET pg_upgrade.allow_ddl=on` or `DROP EVENT TRIGGER` overrides it. That matches the goal (don't let an accidentally-run migration break things), and mirrors the existing DML-freeze posture (also a safeguard, not a security boundary).

**Note on what is/isn't DDL here:** sequence sync in switchover uses `setval()` (a function call, DML) — not blocked. The forward-subscription apply worker applies row changes (DML) — not blocked. Only schema-changing statements trip the trigger.

## Division of labor

Same philosophy as the current tool: orchestrate + verify; delegate heavy infra to Patroni/the platform.

- **Platform/operator provides:** the existing second (shadow) cluster (pods + PVs), a `patroni.yml` template, PG17 binaries in the images, storage.
- **Tool drives:** applying `standby_cluster` + the physical slot, waiting for sync, isolate/drain/upgrade/catchup/rebuild-replicas/switchover orchestration, `patronictl reinit` of replicas, all verify gates, and the disk-safety monitor/throttle.

## New / changed components

- **config:** shadow cluster Patroni REST URL + scope, shadow node list, physical slot name, `standby_cluster` source (prod primary host/port), `max_slot_wal_keep_size` target + disk thresholds.
- **`provision` phase (new):** patch shadow `standby_cluster` + create physical slot on prod + wait for sync.
- **`isolate` (changed):** promote-via-Patroni (or stop-recovery) + `target_lsn` capture (reuse settle-replay) + drop physical slot. Operates on the shadow, not prod.
- **`catchup` (changed):** tail only (no replicas).
- **`rebuild-replicas` phase (new):** per-replica `bin_dir`→PG17 + `patronictl reinit` + wait, with the disk-safety throttle.
- **disk-safety monitor:** slot-lag + free-disk watcher used by the long-pole phases.
- **DDL lock:** `LockDDL`/`UnlockDDL` client methods (install/drop the `pg_upgrade_ddl_lock` event trigger) + the event-trigger SQL; the tool's admin connections always `SET pg_upgrade.allow_ddl = on`.

## What gets simpler vs the current approach

The riskiest current code disappears from the serving path. `isolate` no longer pauses the production Patroni, stops Patroni on N1, or fights `primary_conninfo` re-attach races — those are operations on a live cluster today. In the shadow approach the frozen node is the shadow leader, managed by its own Patroni; once promoted it cannot re-attach to prod. The session's hardest-won guards (`PatroniStoppedOnN1`, `VerifyN1Detached` re-attach, `NodePaused` wait) are unnecessary on the shadow path. The settle-replay/`target_lsn` logic and the slot-invalidation guards remain load-bearing.

## Carried-over caveats (not made worse)

- DDL is not carried by logical replication — no schema changes during the window. Now **enforced** by the DDL lock (see DDL lockdown), not just a manual caveat.
- Sequences are synced in `switchover` (as today).
- Rollback **after** the DSN swap loses writes made on the shadow post-cutover (standard cutover caveat). Before the swap, prod is intact.
- Prod is **not** unfrozen after cutover (split-brain protection).

## Open questions for the implementation plan

1. Exact `isolate` mechanism: Patroni promote vs stop-recovery, and the precise `target_lsn` capture across the timeline switch.
2. Disk-safety throttle: concrete thresholds and how the tool pauses/resumes `patronictl reinit`.
3. Whether the physical slot during `provision` should use a separate, more permissive `max_slot_wal_keep_size` than the logical slot (different risk/abort tradeoff for an interruptible clone).
4. Cost/time model: two full-DB copies (provision clone, replica reinit) — acceptable elapsed time for the target DB sizes.
