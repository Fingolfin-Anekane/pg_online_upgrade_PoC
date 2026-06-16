# Онлайн-апгрейд PostgreSQL 13 → 17: два подхода

> **TL;DR.** У нас есть два рабочих пайплайна мажорного апгрейда без даунтайма:
> текущий **in-place** (каннибализируем реплику боевого кластера) и новый
> **shadow** (поднимаем параллельный кластер, апгрейдим его, переключаем DSN).
> Рекомендация — **shadow**: боевой кластер **сохраняет полный HA** на всё время
> работ, рисковые операции (`isolate`/freeze/`pg_upgrade`) уезжают с боевого
> кластера на параллельный, откат до cutover тривиален, а операторской возни с
> DCS меньше (не нужен rename scope). Цена — отдельный параллельный кластер и
> сейчас один TODO-шаг на репликах (`bin_dir`→PG17). Ниже — обе схемы по фазам,
> с реальной топологией Patroni, командами и SQL, и со всеми «закладками» (TODO /
> условия / внешние шаги), на которые мы заложились.

---

## 1. Задача и инварианты

Апгрейд мажорной версии PostgreSQL (13 → 17) под нагрузкой, без окна простоя.
Физическая репликация **не работает между мажорами** (PG17-standby не стримит с
PG13-primary), поэтому мажорный скачок неизбежно идёт через `pg_upgrade` +
логический «хвост» через слот. Инварианты, которые держим в обоих подходах:

- **Нулевой даунтайм** записи/чтения для приложения (переключение через DSN-swap).
- **Без потери данных**: логический слот удерживает WAL от точки заморозки до
  cutover; драйним слот ровно до `target_lsn`.
- **Откат** возможен до момента cutover.
- **Анти-split-brain**: старый primary замораживается на запись и не размораживается
  обратно (трафик уже на новом).
- **Запрет чужого DDL** на время окна (event-trigger «DDL-замок»), наши собственные
  DDL проходят через `pg_upgrade.allow_ddl`.

Обозначения в снимках топологии — как в реальном `patronictl list`: роли
**Leader / Sync Standby / Replica / Standby Leader** (для standby_cluster),
состояния `running/streaming/stopped`, `TL` — таймлайн, `Lag in MB`. DC-метки
z1/z2/z3.

---

## 2. Подход A — in-place (текущий baseline, ветка `master`)

**Идея.** Берём боевой кластер `pg-main`, ставим его Patroni на паузу,
**каннибализируем одну реплику** (`pg-main-6` = N1): отцепляем её от WAL в точке
`target_lsn`, апгрейдим `pg_upgrade --link` до PG17, поднимаем как новый кластер
`pg-main-17`, догоняем логическим «хвостом», **переносим в новый кластер 2 реплики**
из старого, замораживаем старый primary и переключаем DSN.

Стартовая топология (боевой кластер, 1 лидер + 5 реплик, 6 нод):

```
+ Cluster: pg-main (PG 13) -----------------------------------+
| Member    | Host | Role         | State     | TL | Lag in MB |
+-----------+------+--------------+-----------+----+-----------+
| pg-main-1 | z1   | Leader       | running   |  5 |           |
| pg-main-2 | z2   | Sync Standby | streaming |  5 |         0 |
| pg-main-3 | z3   | Replica      | streaming |  5 |         0 |
| pg-main-4 | z1   | Replica      | streaming |  5 |         0 |
| pg-main-5 | z2   | Replica      | streaming |  5 |         0 |
| pg-main-6 | z3   | Replica      | streaming |  5 |         0 |   ← N1 (target_node)
```

Фазы: `prepare → isolate → drain → upgrade → catchup → switchover → finalize → cleanup`.

### A.1 · prepare

Топология не меняется; на лидере появляются публикация, логический слот и DDL-замок.

**Действия** (на `pg-main-1`):
```sql
CREATE PUBLICATION pub_upgrade FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('slot_upgrade', 'pgoutput');
-- baseline слота: restart_lsn / confirmed_flush_lsn
-- DDL-замок (последним шагом):
CREATE OR REPLACE FUNCTION pg_upgrade_block_ddl() RETURNS event_trigger AS $$
BEGIN
  IF current_setting('pg_upgrade.allow_ddl', true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION 'DDL is locked during the online upgrade (command %)', tg_tag
      USING HINT = 'pg-upgrade safeguard: an app migration must not run now.';
  END IF;
END; $$ LANGUAGE plpgsql;
CREATE EVENT TRIGGER pg_upgrade_ddl_lock ON ddl_command_start
  EXECUTE FUNCTION pg_upgrade_block_ddl();
```

**Закладки:**
- ⚠ Предусловие `wal_level=logical` на боевом primary.
- Advisory-предупреждения (не фатальные): `max_slot_wal_keep_size != -1` и долгие
  транзакции — риск инвалидизации слота во время апгрейда.

### A.2 · isolate

```
+ Cluster: pg-main (PG 13) -----------------------------------+   Maintenance mode: on
| pg-main-1 | z1   | Leader       | running   |  5 |           |   ← pub + слот + DDL-замок
| pg-main-2 | z2   | Sync Standby | streaming |  5 |         0 |
| pg-main-3..5 ...  Replica       | streaming |  5 |         0 |
| pg-main-6 | z3   | Replica      | stopped   |  5 |           |   ← N1: отцеплена от WAL, заморожена на target_lsn
```

**Действия:**
1. `PATCH /config {"pause": true}` — весь кластер в maintenance (Patroni не делает
   failover **на всё время апгрейда**).
2. *(опц., `upgrade.old_patroni_stop_command`)* `systemctl stop patroni` на `pg-main-6`.
3. На `pg-main-6`: `ALTER SYSTEM SET primary_conninfo = ''; SELECT pg_reload_conf();`
4. Settle replay → `SELECT pg_last_wal_replay_lsn()` → запись `target_lsn`.

**Закладки:**
- ⚠ **Боевой кластер на ПАУЗЕ всю длину апгрейда** → деградированный HA прода
  (нет автофейловера, если упадёт `pg-main-1`).
- ⚠ Рисковая операция (отцепление от WAL, гонка с paused-Patroni, который может
  вернуть `primary_conninfo`) идёт **на боевом кластере**. Защита — guard
  `VerifyN1Detached` + повторные проверки walreceiver; если
  `old_patroni_stop_command` пуст, полагаемся только на guard.

### A.3 · drain

Топология как в isolate (N1 `stopped`). На `pg-main-1` драйним слот `slot_upgrade`
до `target_lsn` (pgoutput), затем `VerifySlotDrained` (проверка `wal_status != lost`
и окна `last_commit ≤ confirmed_flush ≤ target`).

**Закладки:** при инвалидизации слота (`max_slot_wal_keep_size`) — жёсткий стоп
с понятной ошибкой (потенциальная потеря хвоста).

### A.4 · upgrade

`pg-main-6` уходит из кластера `pg-main` и становится отдельным PG17.

```
+ Cluster: pg-main (PG 13) --- (5 нод, paused) ...               # pg-main-6 больше не член
( pg-main-6: PG13 → pg_upgrade --link → PG17, ещё не под Patroni )
```

**Действия** (на `pg-main-6`, тулза локально):
```
pg_ctl promote -w -D <datadir13>          # N1 → standalone primary
CHECKPOINT; CHECKPOINT                     # дважды, перед остановкой
pg_ctl stop -m fast -D <datadir13>
# остановить старый Patroni на ноде (иначе воскресит PG13 поверх --link)
initdb -D <datadir17> <opts из bootstrap.initdb>
pg_upgrade --old-bindir <bin13> --new-bindir <bin17> \
           --old-datadir <datadir13> --new-datadir <datadir17> --link --check
pg_upgrade --old-bindir <bin13> --new-bindir <bin17> \
           --old-datadir <datadir13> --new-datadir <datadir17> --link
```

### A.5 · catchup — и перенос реплик в новый кластер

`pg-main-6` поднимается под Patroni как **новый кластер `pg-main-17`** (scope
переименован), подписывается логически на старый primary и догоняет хвост.

```
+ Cluster: pg-main (PG 13) --- (5 нод, paused, всё ещё боевой)
+ Cluster: pg-main-17 (PG 17) -------------------------------+
| pg-main-6 | z3 | Leader | running | 1 |   |   ← подписка sub_upgrade на pg-main-1, lag→0
```

**Действия:**
1. Патч `patroni.yml`: `scope → pg-main-17`, `data_dir/bin_dir/config_dir → PG17`.
2. `systemctl start patroni` → Patroni поднимает PG17 и промоутит в primary.
3. `CREATE SUBSCRIPTION sub_upgrade CONNECTION '<dsn pg-main-1>' PUBLICATION pub_upgrade
   WITH (copy_data = false, create_slot = false, slot_name = 'slot_upgrade', enabled = true);`
4. Переустановить DDL-замок на PG17; ждать `bytes_behind = 0`.

Затем — **перенос 2 реплик в новый кластер** (TODO, сейчас операторский шаг): на
`pg-main-4` и `pg-main-5` ставим `bin_dir=PG17`, scope `pg-main-17`, и `reinit`
(basebackup с PG17-лидера):

```
+ Cluster: pg-main-17 (PG 17) -------------------------------+
| pg-main-6 | z3 | Leader       | running   | 1 |   |
| pg-main-4 | z1 | Sync Standby | streaming | 1 | 0 |   ← перенесена
| pg-main-5 | z2 | Replica      | streaming | 1 | 0 |   ← перенесена
```

**Закладки:**
- [TODO] **Формирование HA нового кластера — операторское**: тулза поднимает
  только лидера, перенос/добавление реплик (`pg-main-4`, `pg-main-5`) делает
  оператор; `VerifyNewClusterHealthy` лишь напоминает («есть лидер, реплик пока
  нет — добавь реплику перед switchover»).

### A.6 · switchover (критическая секция)

Замораживаем старый primary, синкаем последовательности, переключаем DSN на
`pg-main-17`.

**Действия:**
```sql
-- заморозка старого primary (DML-триггеры на всех таблицах):
CREATE OR REPLACE FUNCTION raise_upgrade_readonly() RETURNS trigger AS $$
BEGIN RAISE EXCEPTION 'database is read-only during upgrade window'
  USING ERRCODE = 'read_only_sql_transaction'; RETURN NULL; END; $$ LANGUAGE plpgsql;
-- CREATE TRIGGER upgrade_freeze BEFORE INSERT/UPDATE/DELETE/TRUNCATE ... на каждой таблице
```
Затем: ждать финальный `bytes_behind = 0` → `SELECT setval(...)` для всех
последовательностей (со страховочным буфером) → записать сигнал DSN-swap →
проверить трафик на PG17 (`pg_stat_activity`) → `ALTER SUBSCRIPTION sub_upgrade DISABLE`.

**Как считаем, что хвост проигран до конца.** Гейт cutover — нулевой **байтовый**
лаг walsender'а подписки на издателе (замороженном старом primary):

```
bytes_behind = pg_current_wal_lsn() − replay_lsn      из pg_stat_replication,
                                                      where application_name = 'sub_upgrade'
cutover-ready ⇔ bytes_behind = 0
```

После заморозки приложение не пишет, поэтому `pg_current_wal_lsn()` перестаёт расти
(кроме фонового WAL), а подписчик догоняет `replay_lsn` до этого LSN →
`bytes_behind → 0`. Берём именно байтовый лаг, а не временны́е `*_lag`-колонки:
фоновый WAL держит временны́е колонки около ненулевых значений, тогда как байтовый
честно садится в 0, когда подписчик проиграл WAL до текущего на издателе.

> **TODO.** Байтовый лаг — косвенный признак (фоновый WAL шумит, теоретически
> возможна гонка). Надёжнее — **позитивное подтверждение через контрольную таблицу
> с флагом**: после заморозки писать на проде строку-маркер, например
> `INSERT INTO _upgrade_cutover(marker_lsn, ready) VALUES (pg_current_wal_lsn(), true);`
> и ждать её появления на PG17. Приезд этой строки доказывает, что по логической
> репликации доехали **все** изменения вплоть до маркера, а не просто «лаг около нуля».

**Закладки:**
- [внешнее] Сам DSN-swap делает прокси/оператор; тулза только пишет сигнал и
  **верифицирует**, что трафик переехал (`VerifyTrafficOnNew`).
- Reverse-репликация (PG17→старый) осознанно выключена (создавала
  bidirectional-петлю); откат после cutover — недоступен.

### A.7 · finalize

`UnlockDDL` на новом; drop форвард-подписки (снимает слот на старом);
`VerifyRenamedCluster`.

**Закладки:**
- [TODO] **Rename кластера — операторский** (`etcdctl`): `pg-main-17` → боевое имя.
- Старый primary **остаётся замороженным** (анти-split-brain), не размораживается.

### A.8 · cleanup

Архив логов `pg_upgrade_output.d`; `VerifyOldPrimaryStopped`.

**Закладки:**
- [TODO] **Остановка старого primary — операторская** (тулза на удалённой ноде
  `pg_ctl` не делает; только проверяет, что он недоступен).
- [TODO] **Удаление старых DCS-ключей — операторский** follow-up (`etcdctl del`).
- Реплики `pg-main-1..3` (старый лидер + неперенесённые) — на вывод из эксплуатации.

---

## 3. Подход B — shadow (новый, ветка `feat/shadow-cluster-upgrade`)

**Идея.** Боевой кластер `pg-main` **не трогаем вообще**. Поднимаем параллельный
кластер `pg-main-shadow`, делаем его Patroni `standby_cluster` боевого (физическая
репликация), затем промоутим, апгрейдим `pg_upgrade --link` **с удалением DCS-ключа**
(чтобы новый sysid принялся под тем же scope), догоняем логическим хвостом,
доводим реплики reinit'ом до HA и переключаем DSN. Боевой кластер показан 3-нодовым
для краткости — его размер не важен, он не меняется.

Фазы: `provision → prepare → isolate → drain → upgrade → catchup → rebuild-replicas
→ switchover → finalize → cleanup`.

### B.0 · provision — перевод shadow в standby_cluster

**Было:** `pg-main-shadow` — самостоятельный PG13 Patroni-кластер со своим лидером,
ещё не связан с продом:

```
+ Cluster: pg-main-shadow (PG 13) — самостоятельный кластер ------+
| pg-shadow-1 | z1 | Leader       | running   |  1 |   |
| pg-shadow-2 | z2 | Sync Standby | streaming |  1 | 0 |
| pg-shadow-3 | z3 | Replica      | streaming |  1 | 0 |
```

**Как переводим в standby.** Создаём на проде физический слот и **патчим
динамический конфиг Patroni шэдоу блоком `standby_cluster`** (источник = боевой
primary, `primary_slot_name` = физический слот). Получив этот блок, Patroni шэдоу
переинициализирует свои ноды от прода (basebackup через физический слот), а его
лидер перестаёт быть самостоятельным primary и становится **Standby Leader** —
физически реплицирует боевой `pg-main`:

```sql
-- на проде: физический слот под стрим шэдоу
SELECT pg_create_physical_replication_slot('shadow_phys');
```
```
PATCH /config (Patroni шэдоу) — переводим весь кластер в standby:
{"standby_cluster": {"host": "<pg-main-1>", "port": 5432,
  "primary_slot_name": "shadow_phys", "create_replica_methods": ["basebackup"]}}
```

**Стало:** боевой нетронут (полный HA), shadow — standby_cluster, физически
догоняет прод (ждём lag < 16MB и полный набор нод):

```
+ Cluster: pg-main (PG 13) --- (боевой, полный HA, не трогаем) ---
+ Cluster: pg-main-shadow (PG 13) — standby_cluster --------------+
| pg-shadow-1 | z1 | Standby Leader | running   |  3 |   |
| pg-shadow-2 | z2 | Replica        | streaming |  3 | 0 |
| pg-shadow-3 | z3 | Replica        | streaming |  3 | 0 |
```

**Закладки:** нет операторских шагов (предполагаем, что сам shadow-кластер уже
существует как самостоятельный Patroni-кластер).

### B.1 · prepare

То же, что в A.1 (на боевом `pg-main-1`: `pub_upgrade`, `slot_upgrade`, DDL-замок).
Shadow не меняется.

### B.2 · isolate (на шэдоу — боевой не тронут)

```
+ Cluster: pg-main (PG 13) --- (боевой, полный HA, не тронут) ---
+ Cluster: pg-main-shadow (PG 13) -------------------------------+
| pg-shadow-1 | z1 | Leader       | running   |  4 |   |   ← promote, заморожен на target_lsn
| pg-shadow-2 | z2 | Sync Standby | streaming |  4 | 0 |
| pg-shadow-3 | z3 | Replica      | streaming |  4 | 0 |
```

**Действия:**
1. `PATCH /config {"standby_cluster": null}` → Patroni промоутит `pg-shadow-1` в
   обычный PG13-primary.
2. Ждать `SELECT pg_is_in_recovery()` = false; settle replay → `target_lsn`.
3. На проде: `SELECT pg_drop_replication_slot('shadow_phys');`

**Закладки:** ✅ **Прод не тронут — без паузы Patroni, без гонок `primary_conninfo`
на боевом.** Рисковый промоут/заморозка — на параллельном кластере.

### B.3 · drain

На боевом `pg-main-1` драйним `slot_upgrade` до `target_lsn` (как A.3).

**Закладки:**
- [TODO] Чекпойнт перед `upgrade` требует **оператору остановить Patroni на всех
  репликах шэдоу** (`pg-shadow-2`, `pg-shadow-3`) — чтобы реплика не сделала
  failover при остановке лидера и не помешала удалению DCS-ключа.

### B.4 · upgrade (на шэдоу)

```
( pg-main-shadow: Patroni остановлен на всех нодах; /service/pg-main-shadow удалён;
  pg-shadow-1: PG13 → pg_upgrade --link → PG17 )
+ Cluster: pg-main (PG 13) --- (боевой, полный HA, не тронут) ---
```

**Действия** (на `pg-shadow-1`, тулза локально):
```
systemctl stop patroni                     # StopShadowPatroni (shadow_patroni_stop_command, обязателен)
CHECKPOINT; CHECKPOINT; pg_ctl stop -m fast -D <datadir13>
DELETE /v2/keys/service/pg-main-shadow?recursive=true&dir=true   # ← удаление DCS-ключа (etcd v2, mTLS)
initdb -D <datadir17> <opts>
pg_upgrade ... --link --check
pg_upgrade ... --link
```

**Закладки:**
- [TODO] Реплики шэдоу остановлены оператором (см. B.3) **до** удаления ключа.
- Удаление `/service/pg-main-shadow` — это и есть «магия sysid»: после `pg_upgrade`
  у датадира новый system identifier; стерев ключ, мы позволяем Patroni при старте
  заново записать `/initialize` с новым sysid **под тем же scope** (без rename).

### B.5 · catchup (на шэдоу, scope сохранён)

`pg-shadow-1` поднимается под Patroni как PG17 **под тем же scope `pg-main-shadow`**,
подписывается на боевой primary, догоняет хвост.

```
+ Cluster: pg-main-shadow (PG 17) ---------------------------+
| pg-shadow-1 | z1 | Leader | running | 1 |   |   ← новый /initialize с новым sysid, scope тот же
```

**Действия:**
1. Патч `patroni.yml`: `data_dir/bin_dir → PG17` (**scope НЕ меняем**).
2. `systemctl start patroni` → Patroni адоптит PG17-датадир, пишет свежий
   `/initialize`, промоутит в primary.
3. `CREATE SUBSCRIPTION sub_upgrade CONNECTION '<dsn pg-main-1>' PUBLICATION pub_upgrade
   WITH (copy_data = false, create_slot = false, slot_name = 'slot_upgrade', enabled = true);`
4. Переустановить DDL-замок; ждать `bytes_behind = 0`.

### B.6 · rebuild-replicas

Реплики шэдоу доводятся до PG17 reinit'ом (basebackup с нового лидера) → полный HA.

```
+ Cluster: pg-main-shadow (PG 17) ---------------------------+
| pg-shadow-1 | z1 | Leader       | running   | 1 |   |
| pg-shadow-2 | z2 | Sync Standby | streaming | 1 | 0 |
| pg-shadow-3 | z3 | Replica      | streaming | 1 | 0 |
```

**Действия:** для каждой реплики `POST /reinitialize {"force": true}` (basebackup с
PG17-лидера) с disk-guard throttle/abort (чтобы конкурентные basebackup'ы не
распухли и не съели слот на проде).

**Закладки:**
- [TODO] **Оператор ставит `bin_dir=PG17` на репликах** (`pg-shadow-2/3`)
  и поднимает их Patroni перед фазой — scope у них уже верный (ключ-делит сохранил
  scope), `data_dir` тоже. Это единственный TODO-шаг по конфигам реплик.
- [TODO] verify-гейт PG17-готовности реплики перед reinit (быстрый фейл вместо
  немого таймаута).
- [TODO/бэклог] rsync-fast-path вместо basebackup для больших баз.
- [known minor] etcd-парсер читает `hosts:` (как на платформе), но не одиночный
  `host:`.

### B.7 · switchover

Критическая секция: замораживаем боевой `pg-main-1`, доигрываем хвост, синкаем
последовательности и переключаем DSN на shadow (PG17). После этого прод — только на
чтение и идёт на вывод.

```
+ Cluster: pg-main (PG 13) --- (заморожен на запись, на вывод) ---
+ Cluster: pg-main-shadow (PG 17) — принимает трафик -------------+
| pg-shadow-1 | z1 | Leader       | running   |  1 |   |   ← сюда переключён DSN
| pg-shadow-2 | z2 | Sync Standby | streaming |  1 | 0 |
| pg-shadow-3 | z3 | Replica      | streaming |  1 | 0 |
```

**Действия:**
1. **Заморозка прода** (`FreezeOldPrimary`): на `pg-main-1` ставим DML-триггеры
   `raise_upgrade_readonly` на все таблицы → INSERT/UPDATE/DELETE/TRUNCATE падают с
   `read_only_sql_transaction`. Запись на проде остановлена.
2. **Финальный догон** (`WaitFinalLagZero`): ждём `bytes_behind = 0` подписки
   `sub_upgrade` на издателе (математика — в A.6): весь хвост вплоть до точки
   заморозки доехал на shadow-лидер.
3. **Синк последовательностей** (`SyncSequences`): читаем `last_value` всех
   последовательностей с прода и `SELECT setval(...)` на shadow-лидере со
   страховочным буфером (`+sequence_buffer`) — чтобы новые id не пересеклись со
   старыми.
4. **Сигнал DSN-swap** (`NotifyDSNSwap`): пишем сигнал «новый primary =
   `pg-main-shadow` лидер»; сам swap выполняет прокси/оператор (тулза DSN не дёргает).
5. **Проверка трафика** (`VerifyTrafficOnNew`): на shadow-лидере считаем клиентские
   backend'ы (`pg_stat_activity`) — должны появиться, иначе фейл («сделай DSN-swap,
   потом re-run»).
6. **Отключение прямой подписки** (`DisableForwardSubscription`):
   `ALTER SUBSCRIPTION sub_upgrade DISABLE` на shadow — запись уже идёт сюда, хвост
   с прода больше не нужен.

**Закладки:**
- [внешнее] Сам DSN-swap делает прокси/оператор; тулза только пишет сигнал и
  **верифицирует**, что трафик переехал.
- Reverse-репликация (PG17→прод) осознанно выключена — откат после cutover недоступен.

### B.8 · finalize / cleanup

`UnlockDDL`; drop подписок; `VerifyOldPrimaryStopped`.

**Закладки:**
- ✅ **Rename scope НЕ нужен** (scope сохранён удалением ключа) — на один
  операторский `etcdctl`-шаг меньше, чем в in-place.
- [TODO] Остановка старого боевого primary + удаление его старых DCS-ключей —
  операторские (как и в A).

---

## 4. Сравнение

| Критерий | in-place (A) | shadow (B) |
|---|---|---|
| **HA боевого кластера во время апгрейда** | ⚠ Patroni на **паузе**, реплика забрана → деградированный HA | ✅ **Полный HA**, боевой не тронут |
| **Где рисковый `isolate`/freeze** | ⚠ на боевом кластере | ✅ на параллельном `pg-main-shadow` |
| **Откат до cutover** | ⚠ реплика уже каннибализирована, восстановление дороже | ✅ тривиально: снести shadow, прод как был |
| **Формирование HA нового кластера** | TODO: перенос 2 реплик (оператор) | фаза `rebuild-replicas` (тулза) + TODO: `bin_dir` на репликах |
| **DCS-операции оператора** | ⚠ `etcdctl` **rename scope** + удаление старых ключей | ✅ rename не нужен (ключ-делит тулзой); только удаление старых ключей || **Стоимость ресурсов** | не нужен параллельный кластер | ⚠ нужен полноразмерный параллельный кластер (2 полные копии БД: provision + reinit) |
| **Зависимость от ssh/доступов** | локальные операции на ноде N1 | тулза на shadow-лидере + reinit по REST |

---

## 5. Свод TODO и операторских ворот

**in-place (A):**
- [условие] остановить старый Patroni на N1 перед `pg_upgrade` (или `old_patroni_stop_command`);
- [TODO] перенос 2 реплик в новый кластер (`bin_dir`→PG17 + reinit);
- [TODO] `etcdctl` rename scope (`pg-main-17` → боевое имя);
- [TODO] остановка старого primary + удаление старых DCS-ключей;
- [TODO] DSN-swap (прокси/оператор);
- [TODO] узкое resume-окно `pg_upgrade`.

**shadow (B):**
- [TODO] остановить Patroni на репликах шэдоу перед `upgrade` (чекпойнт `drain`);
- [TODO] `bin_dir=PG17` на репликах + поднять их Patroni перед `rebuild-replicas`;
- [TODO] остановка старого primary + удаление старых DCS-ключей;
- [TODO] DSN-swap (прокси/оператор);
- [TODO] verify-гейт PG17-готовности реплик; [TODO] rsync-fast-path; [minor] `host:`-парсер;
- ✅ rename scope НЕ нужен.

**Операторские чекпойнты между фазами** (`DefaultPrompts`, оба подхода): после
`provision`, `prepare`, `isolate`, `drain`, `upgrade`, `catchup`,
`rebuild-replicas`, `switchover`, `finalize` — каждый требует явного «y» оператора.

---

## 6. Рекомендация

**Берём shadow (B).** Главное — боевой кластер **сохраняет полный HA** на всё время
работ, а все рисковые операции (отцепление от WAL, заморозка, `pg_upgrade`) уезжают
с боевого кластера на параллельный. Откат до cutover тривиален, операторской возни с
DCS меньше (не нужен rename scope). In-place же держит боевой кластер на паузе с
забранной репликой и требует переноса реплик + rename (оба шага — TODO) —
больше риска именно на проде.

**Когда оставить in-place:** если нет ресурсов на полноразмерный параллельный
кластер (две полные копии БД во время работ) или среда не позволяет поднять
параллельный Patroni-кластер нужного размера.

**Следующие шаги для shadow:** валидация связки на тест-стенде (etcd v2 mTLS +
delete-ключа → переинициализация Patroni с новым sysid под тем же scope), образец
конфига с shadow-полями, затем закрытие TODO (verify-гейт реплик, rsync-fast-path).
