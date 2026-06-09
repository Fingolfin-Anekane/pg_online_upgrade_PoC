# pg-upgrade — последовательность шагов

Оркестратор zero-downtime мажорного апгрейда PostgreSQL (PG10+ → PG17) на кластере
Patroni. Это FSM из 8 фаз; каждый шаг **идемпотентен** — у него есть `Check()`
(переспрашивает реальное состояние системы) и `Run()`. Раннер выполняет `Run`
только там, где `Check` вернул «не сделано», поэтому повторный запуск безопасно
доезжает с места падения. Между фазами — интерактивный чекпойнт оператора.

## Терминология

| Термин | Значение |
|---|---|
| **N1 / target_node** | локальная реплика, которую промоутят и `pg_upgrade`'ят на месте в PG17-сид |
| **старый primary** | текущий лидер кластера (publisher логической репликации) |
| **target_lsn** | физический LSN, на котором N1 «заморожена» (её `pg_last_wal_replay_lsn()` после отключения от WAL) |
| **forward-репликация** | логическая репликация старый primary → PG17 (слот `upgrade_slot`, публикация `upgrade_pub`, подписка `upgrade_sub`) |

Идентификаторы (`upgrade_slot`, `upgrade_pub`, `upgrade_sub`, имена dir/scope) — из конфига.

---

## Фаза 1 — prepare (подготовка логической репликации)

| Шаг | Действие | SQL / команда |
|---|---|---|
| DiscoverTopology | найти лидера через Patroni REST, записать `PrimaryHost` | `GET /cluster` |
| VerifyPrerequisites | `wal_level=logical`; N1 — реплика; версия ≥ PG10 | `SHOW wal_level;` · `SELECT pg_is_in_recovery();` · `SHOW server_version_num;` |
| CreatePublication | публикация на старом primary | `CREATE PUBLICATION upgrade_pub FOR ALL TABLES;` |
| CreateLogicalSlot | логический слот (плагин `pgoutput`) на старом primary | `SELECT pg_create_logical_replication_slot('upgrade_slot', 'pgoutput');` |
| RecordSlotBaseline | зафиксировать стартовые LSN слота | `SELECT slot_name, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='upgrade_slot';` |

С момента создания слот **держит WAL** на старом primary и копит все изменения —
это нужно, чтобы ничего не потерять в окне между подготовкой и cutover.

---

## Фаза 2 — isolate (заморозка N1 на фиксированной точке WAL)

| Шаг | Действие | SQL / команда |
|---|---|---|
| PausePatroni | поставить кластер в maintenance и **дождаться применения** на ноде (иначе стоп Patroni грейсфул-погасит postgres) | `PATCH /config {"pause":true}` → поллинг `GET /patroni` до `pause:true` |
| StopPatroniOnN1 | остановить Patroni на N1, чтобы он не вернул `primary_conninfo` | `old_patroni_stop_command` (напр. `systemctl stop patroni`, postgres остаётся жив) |
| CaptureReceivedLSN | запомнить **нижнюю границу** принятого WAL (до отключения) | `SELECT COALESCE(flushed_lsn, received_lsn) FROM pg_stat_wal_receiver;` |
| DisconnectN1FromWAL | отцепить N1 от стрима (PG13+: без рестарта) | `ALTER SYSTEM SET primary_conninfo='';` · `SELECT pg_reload_conf();` |
| VerifyN1Detached | поллить, пока приёмник не станет пустым; не воскрес ли (Patroni не переподключил) | `SELECT count(*)>0 FROM pg_stat_wal_receiver;` → ждём `false` |
| WaitReplayDrained | дождаться, пока `replay_lsn` **перестанет расти** (3 равных отсчёта подряд) — это X', истинная точка заморозки; проверить `≥ received` | `SELECT pg_last_wal_replay_lsn();` до стабилизации |
| RecordTargetLSN | записать `target_lsn = pg_last_wal_replay_lsn()` (= X'); проверить инвариант | требует `slot.confirmed_flush_lsn ≤ target_lsn` |

После фазы N1 — standby без апстрима, replay стоит на `target_lsn`, таймлайн прежний.

> **Почему «дождаться стабилизации», а не `replay ≥ received`.** `received_lsn` читается *до* отключения, пока приёмник ещё стримит, поэтому это лишь **нижняя граница**: к моменту, когда `primary_conninfo` реально очищен, N1 принял больше — до X'. Если записать `target_lsn`, пока `replay` ещё догоняет X', то коммиты `(target, X']` уже физически в N1/PG17, но прямая подписка переотдаст их → дубликаты. Поэтому сначала `VerifyN1Detached` гарантирует, что новый WAL больше не придёт, затем `WaitReplayDrained` ждёт, пока `replay` упрётся в конец принятого WAL и замрёт — это и есть X'. (Ждать ровно `replay == received` нельзя: приёмник мог флашнуть середину записи, и `replay` никогда её не догонит.)

---

## Фаза 3 — drain (слив слота до target_lsn)

| Шаг | Действие |
|---|---|
| RunSlotDrain | прочитать слот через `pgoutput` и заACKать все коммиты `commit_lsn ≤ target_lsn`, затем подтвердить flush на `target_lsn`; первый коммит `> target_lsn` оставить в слоте |
| VerifySlotDrained | проверить `last_commit_lsn ≤ confirmed_flush_lsn ≤ target_lsn` |

Стрим: `START_REPLICATION SLOT upgrade_slot ... (proto_version '1', publication_names 'upgrade_pub')`,
обратная связь — Standby Status Update (`WALFlushPosition`).

👉 Детали и риски — раздел [«Про slot-drain»](#про-slot-drain) ниже.

---

## Фаза 4 — upgrade (промоут N1 + pg_upgrade в PG17)

| Шаг | Действие | Команда |
|---|---|---|
| PromoteN1 | промоут N1 из standby в primary (switchpoint на `target_lsn`, TLI+1) | `pg_ctl promote -w -D <old_datadir>` |
| ShutdownN1Clean | два чекпойнта + чистый стоп (нужно для `pg_upgrade`) | `CHECKPOINT;` ×2, затем `pg_ctl stop -m fast` |
| StopOldPatroniOnN1 | убедиться, что старый Patroni не воскресит старый сервер | `old_patroni_stop_command` + проверка недоступности REST |
| InitNewDataDir | `initdb` нового PG17-datadir (locale/encoding/checksums как у старого) | `initdb` с опциями из bootstrap |
| RunPgUpgradeCheck | предполётная проверка совместимости | `pg_upgrade --check` |
| RunPgUpgrade | сам апгрейд жёсткими ссылками | `pg_upgrade --link --old-bindir … --new-bindir …` |

`--link` хардлинкует файлы старого и нового кластера → **старый сервер после этого
запускать нельзя** (повредит новый). Поэтому в catchup есть guard на это.

---

## Фаза 5 — catchup (поднять PG17 под Patroni и догнать хвост)

| Шаг | Действие | SQL / команда |
|---|---|---|
| VerifyOldClusterStopped | отказать, если старый postmaster ещё жив | `pg_ctl status -D <old_datadir>` |
| PatchNewPatroniConfig | переписать новый `patroni.yml`: scope, data_dir, bin_dir, config_dir (бэкап в `.bak`) | — |
| StartPG17OnN1 | поднять PG17 под Patroni и **дождаться промоута в writable primary** | `systemctl start patroni`; поллинг `SELECT pg_is_in_recovery();` до `false` |
| CreateForwardSubscription | подписка на PG17, **переиспользует слитый слот** | `CREATE SUBSCRIPTION upgrade_sub CONNECTION '…' PUBLICATION upgrade_pub WITH (copy_data=false, create_slot=false, slot_name='upgrade_slot', enabled=true);` |
| WaitLagZero | дождаться `bytes_behind=0` (поллинг, не one-shot) | `SELECT pg_current_wal_lsn() - replay_lsn FROM pg_stat_replication;` на publisher |
| VerifyNewClusterHealthy | у нового кластера есть лидер | `GET /cluster` (NewPatroni) |

`copy_data=false` — начальный снапшот не нужен (данные ≤ target уже физически в PG17);
`create_slot=false` + `slot_name` — берём именно слитый слот, чтобы продолжить ровно с `target_lsn`.

---

## Фаза 6 — switchover (критическая секция, cutover)

| Шаг | Действие | SQL / команда |
|---|---|---|
| FreezeOldPrimary | заморозить запись на старом primary DML-триггерами | `CREATE TRIGGER upgrade_freeze BEFORE INSERT/UPDATE/DELETE/TRUNCATE … EXECUTE PROCEDURE raise_upgrade_readonly()` (бросает `ERRCODE read_only_sql_transaction`) |
| WaitFinalLagZero | дождаться слива финального хвоста на PG17 (поллинг) | `… pg_current_wal_lsn() - replay_lsn …` = 0 |
| SyncSequences | перенести `last_value` последовательностей на PG17 + буфер | `SELECT setval('schema.seq', $1);` |
| NotifyDSNSwap | записать сигнал-файл для смены DSN; **оператор/прокси переключает трафик на PG17** | пишет `dsn-swap.json` |
| VerifyTrafficOnNew | убедиться, что на PG17 есть клиентские backend'ы | `SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend';` |
| DisableForwardSubscription | отключить forward-подписку (запись уже на PG17) | `ALTER SUBSCRIPTION upgrade_sub DISABLE;` |

После freeze любая запись на старый primary падает с ошибкой — это и предотвращает
split-brain в окне переключения.

---

## Фаза 7 — finalize (фиксация апгрейда)

| Шаг | Действие | SQL |
|---|---|---|
| DropReverseReplication | снести артефакты отката (idempotent no-op) | `DROP SUBSCRIPTION IF EXISTS …` · `DROP PUBLICATION IF EXISTS …` |
| DropForwardSubscription | снести forward-подписку (заодно дропается её слот на старом primary) | `DROP SUBSCRIPTION IF EXISTS upgrade_sub;` |
| VerifyRenamedCluster | у переименованного PG17-кластера есть лидер | `GET /cluster` (NewPatroni) |

⚠️ Заморозку старого primary **намеренно НЕ снимаем** — он скоро выводится из
эксплуатации, а разморозка открыла бы окно split-brain, если кто-то ещё держит
старый DSN. Старый primary остаётся read-only до остановки.

---

## Фаза 8 — cleanup (терминальная)

| Шаг | Действие |
|---|---|
| ArchivePgUpgradeLogs | скопировать `pg_upgrade_output.d` в архив |
| VerifyOldPrimaryStopped | подтвердить, что оператор остановил старый primary |

---

## Про slot-drain

Здесь собраны ответы на вопросы «зачем вообще вычитывать слот», «при чём тут replay
и confirmed_flush» и «какие риски».

### Зачем вычитывать (drain) слот

Слот `upgrade_slot` создан в `prepare` и с этого момента копит **все** изменения
старого primary, начиная со своего baseline. Параллельно N1 физически заморожена на
`target_lsn` и после `pg_upgrade` становится сидом PG17 — то есть PG17 уже **физически
содержит** всё, что закоммичено `≤ target_lsn`.

Если отдать слот подписке PG17 «как есть» (с baseline), логическая репликация
переслала бы **заново** все изменения от baseline, включая те, что уже лежат в
физическом сиде → дубликаты/конфликты (`unique_violation`).

Поэтому мы сами **вычитываем и подтверждаем (ACK)** слот до `target_lsn`. Это сдвигает
`confirmed_flush_lsn` слота на границу, и forward-подписка PG17 получает **только
хвост** — коммиты `> target_lsn`, которых в физическом сиде ещё нет.

### replay_lsn vs confirmed_flush_lsn

- **`replay_lsn`** (`pg_last_wal_replay_lsn`) — докуда N1 **физически** проиграла WAL.
  После isolate это и есть `target_lsn` — точка заморозки.
- **`confirmed_flush_lsn`** (слота) — докуда **логический консьюмер** подтвердил приём;
  это позиция, **с которой логическая репликация продолжит** доставлять изменения.

Шов делается так, чтобы они сошлись на границе:

```
              target_lsn (replay_lsn N1)
                       │
 коммиты ≤ target  ────┤────  коммиты > target (хвост)
 уже в сиде PG17       │      доставит forward-подписка
 (физически)           │
              confirmed_flush_lsn слота → сюда сдвигаем drain'ом
```

### Почему проверяем окно, а не строгое равенство

Drain подтверждает `target_lsn`, но PostgreSQL **зажимает** `confirmed_flush_lsn` до
последней реально декодированной слотом записи. `target_lsn` — это физический replay
N1, и он может оказаться на несколько байт дальше последнего логического коммита
(в зазоре — недекодируемый WAL: коммит-записи, изменения непубликуемых таблиц).
Поэтому реальный инвариант проверки:

```
last_commit_lsn  ≤  confirmed_flush_lsn  ≤  target_lsn
```

Зазор `(confirmed_flush, target]` коммитов не содержит, так что слот возобновится, и
первый же доставленный коммит — это хвост (`> target`). Ни дублей, ни потерь.

### Риски

| Риск | Причина | Защита |
|---|---|---|
| **Потеря данных** | `confirmed_flush > target` → слот возобновится за границей, хвост `(target, …]` пропущен | `VerifySlotDrained` отклоняет overshoot |
| **Дубликаты/конфликты** | `confirmed_flush` ниже последнего слитого коммита → baseline-изменения пересылаются повторно | нижняя граница `last_commit ≤ confirmed_flush` |
| **Нарушение шва** | слот создан/слит уже **после** заморозки N1 → `confirmed_flush > target` | инвариант isolate: `baseline.confirmed_flush ≤ target_lsn` |
| **Раздувание WAL/диска** | неслитый/отстающий слот держит `restart_lsn` и пинит WAL на старом primary | drain двигает слот вперёд; слот дропается в finalize |
| **Инвалидация слота** | при `max_slot_wal_keep_size != -1` отставший слот превышает лимит → PG ставит `wal_status=lost` и сносит WAL → хвост потерян (самый опасный путь, усиливается долгой txn) | **жёсткий** `assertSlotReserved` в drain и catchup (падаем на `lost`/`unreserved`); **префлайт-предупреждение** в prepare про `max_slot_wal_keep_size` и долгие транзакции |
| **Длинные транзакции** | начались до `target`, но коммитят после | остаются в слоте (хвост) и корректно доставляются подпиской; начавшиеся **до создания слота** либо коммитят `≤ baseline ≤ target` (попадают в физический сид pg_upgrade), либо блокируют создание слота — потери нет |
| **Зависший drain** | сервер идёт на `target` без пост-target коммитов (idle/пустые txn/запись в непубликуемые таблицы) | стоп по `ServerWALEnd ≥ target`, а не по «первому коммиту > target» |
