# ETL `added_elements` — SQL-слой

Артефакты рефакторинга DAG-а `added_elements_etl` с in-memory pandas
на ELT в PostgreSQL.

## Структура

```
sql/added_elements/
├── ddl/                # Инициализация слоёв
│   ├── 00_schema.sql       # CREATE EXTENSION postgres_fdw + схемы
│   ├── 01_raw.sql          # datalake.raw_added_elements (RAW)
│   ├── 02_dim.sql          # dim_ad_users, dim_transactions, dim_bim_users (DIM)
│   ├── 03_stg.sql          # datalake.stg_added_elements (STG)
│   └── 04_marts.sql        # ext_added_elements_designers / _bim (MARTS)
├── functions/
│   └── parse_project.sql   # PL/pgSQL парсеры (порт common/utils.py)
├── seeds/
│   ├── setup_fdw.py        # CREATE SERVER / USER MAPPING / IMPORT FOREIGN SCHEMA
│   ├── load_dim_ad_users.py
│   ├── load_dim_transactions.py
│   └── load_dim_bim_users.py
└── transform/              # SQL шагов DAG
    ├── 01_extract_load_raw.sql
    ├── 02_transform_staging.sql
    └── 03_build_marts.sql
```

## Порядок инициализации (одноразово)

> Все шаги — внутри Airflow-контейнера (`airflow-cli` или любой `airflow-*` worker).
> Connection-ы: `tim_db_revit` (источник), `tim_db_postgres` (datalake),
> `tim_db_ad` (AD users) — должны быть настроены заранее.

1. **DDL: схема, расширение, таблицы, функции**

   ```bash
   # Под tim_db_postgres (через airflow connections psql-обёртку или напрямую psql):
   psql ... -f /opt/airflow/dags/etl_pipelines/sql/added_elements/ddl/00_schema.sql
   psql ... -f /opt/airflow/dags/etl_pipelines/sql/added_elements/ddl/01_raw.sql
   psql ... -f /opt/airflow/dags/etl_pipelines/sql/added_elements/ddl/02_dim.sql
   psql ... -f /opt/airflow/dags/etl_pipelines/sql/added_elements/ddl/03_stg.sql
   psql ... -f /opt/airflow/dags/etl_pipelines/sql/added_elements/ddl/04_marts.sql
   psql ... -f /opt/airflow/dags/etl_pipelines/sql/added_elements/functions/parse_project.sql
   ```

   Все скрипты идемпотентны (`IF NOT EXISTS`, `CREATE OR REPLACE`).

2. **FDW: подключение к источнику**

   ```bash
   python /opt/airflow/dags/etl_pipelines/sql/added_elements/seeds/setup_fdw.py
   ```

   Создаст в datalake foreign tables:
   - `revit_ext.added_element`, `revit_ext.modified_element`
   - `legacy_ext.added_element_legacy`

3. **Словари**

   ```bash
   python /opt/airflow/dags/etl_pipelines/sql/added_elements/seeds/load_dim_ad_users.py
   python /opt/airflow/dags/etl_pipelines/sql/added_elements/seeds/load_dim_transactions.py
   python /opt/airflow/dags/etl_pipelines/sql/added_elements/seeds/load_dim_bim_users.py
   ```

## Запуск трансформации

`transform/*.sql` параметризованы `%(last_date)s` / `%(run_date)s`. Запуск
из DAG-а `added_elements_etl` (каждые 2 часа в `:17`) или вручную через
`scripts/backfill_added_elements.py`.

**Окна:**
- RAW: `[last_date, run_date)`
- STG: `[last_date - 1 day, run_date)` — буфер для корректного `LAG`
- MARTS: `[last_date, run_date)`

**Бэкфилл:**

```bash
# план без выполнения
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/scripts/backfill_added_elements.py --dry-run

# полный бэкфилл (окна по 14 дней по умолчанию)
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/scripts/backfill_added_elements.py

# с конкретной даты (после прерывания / сбоя)
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/scripts/backfill_added_elements.py --start 2026-06-01
```

## Runbook: восстановление после сбоя

> **Важно:** DAG обрабатывает строго одно cron-окно `[data_interval_start, data_interval_end)`
> и **не догоняет** разрыв между последней записью в витрине и текущим моментом.
> При любом простое Airflow > 2 часов пропуск нужно закрывать вручную через
> `backfill_added_elements.py`.

### Симптомы, что что-то не так

- Дашборды не показывают свежие данные.
- `max(date)` в `ext_added_elements_designers` отстаёт от текущего времени более чем на 2.5 часа.
- В Airflow UI у `added_elements_etl` есть `failed` раны, или раны `success`, но витрина не растёт.

### Шаг 1. Диагностика

```bash
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/scripts/diagnose_added_elements.py
```

Скрипт ничего не пишет, только читает. Покажет:
- `count(*)` и `max(date)` на каждом слое за 36 часов + всего.
- Тип колонки `date` и таймзону БД.
- Реальные окна (`data_interval_start/end`) последних 30 ранов DAG.
- Видимость строк в источнике для последнего успешного окна.

### Шаг 2. Понять, где течёт

| Где пусто | Что чинить |
|-----------|-----------|
| `src.*` за 36 ч = 0, но `src.*` глобально растёт по `max(date)` | Источник перестал писать в нужном временном диапазоне — не наша зона. |
| `src.*` есть, но `raw` отстаёт | DAG не идёт / `extract_load_raw` падает / нулевое окно (см. ниже). |
| `raw` есть, `stg` отстаёт | `transform_staging` падает / новый user_id отсутствует в `dim_ad_users`. |
| `stg` есть, обе `mart.*` отстают | `build_marts` падает / фильтр `is_bim` неверный. |
| Все слои отстают, но раны DAG `success` с **нулевым окном** (`start == end`) | Тайт-таймтейбл (см. ниже). |

### Шаг 3. Типичные причины и фиксы

**(a) Нулевые окна `data_interval_start == data_interval_end` у scheduled-ранов**

В Airflow 3 дефолтным таймтейблом для строкового cron стал `CronTriggerTimetable`,
который даёт точечный интервал. Уже исправлено в `added_elements_etl_dag.py`
через явный `CronDataIntervalTimetable("17 */2 * * *", timezone="UTC")`. Если
кто-то откатит — окна снова станут нулевыми, и DAG будет молотить вхолостую.

Проверка: `diagnose` → таблица «Раны DAG»: `data_interval_end - data_interval_start`
должно быть **= 2 часа**.

**(b) FDW отвалился (источник недоступен)**

Шаг `extract_load_raw` упадёт с ошибкой подключения. Лечится перенастройкой
FDW: проверить `tim_db_revit` в Airflow Connections, при необходимости
пересоздать сервер:

```bash
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/dags/etl_pipelines/sql/added_elements/seeds/setup_fdw.py
```

**(c) Новый user_id в источнике, которого нет в `dim_ad_users`**

`transform_staging` использует `LEFT JOIN`, такие строки получат `user_name = NULL`.
В витрину они попадут, но без имени. Долить словарь:

```bash
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/dags/etl_pipelines/sql/added_elements/seeds/load_dim_ad_users.py
```

После этого — перезалить затронутые окна через бэкфилл (см. Шаг 4).

**(d) Airflow / scheduler / worker лежал N часов**

DAG не догонит сам. Идём к Шагу 4.

### Шаг 4. Долить пропуск через бэкфилл

Узнать дату, с которой доливать (см. вывод `diagnose`):

```sql
SELECT max(date) FROM datalake.ext_added_elements_designers;
SELECT max(date) FROM datalake.ext_added_elements_bim;
```

Берём **минимум** из двух — это нижняя граница пропуска. Округляем на день назад
(подстраховка):

```bash
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/scripts/backfill_added_elements.py --start 2026-06-04
```

Скрипт идемпотентен: бьёт диапазон на окна по 14 дней, в каждом окне
`DELETE + INSERT`. Пересечение с уже залитыми данными — безопасно, дубли не появятся.

Время работы: ≈ 1–2 минуты на сутки данных. Полный бэкфилл (10+ месяцев) — ~50 минут.

### Шаг 5. Убедиться, что DAG снова идёт

После следующего scheduled-рана (`:17` каждых чётных часов UTC):

```bash
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/scripts/diagnose_added_elements.py
```

Проверить:
- В таблице «Раны DAG» новый ран в `state=success`, окно = ровно 2 часа.
- `max(mart.designers)` подтянулся к `max(src.added)` с лагом ≤ 2 часа.

### Полный сброс (на крайний случай)

Если данные сильно расходятся и проще пересобрать с нуля:

```sql
-- На tim_db_postgres:
TRUNCATE datalake.raw_added_elements;
TRUNCATE datalake.stg_added_elements;
TRUNCATE datalake.ext_added_elements_designers;
TRUNCATE datalake.ext_added_elements_bim;
```

Затем полный бэкфилл:

```bash
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/scripts/backfill_added_elements.py
```

≈ 50 минут на ~12M строк.

## Статус этапов

| Этап | Что | Статус |
|------|-----|--------|
| 0+1 | DDL, функции, seeds | ✅ |
| 2 | transform-SQL | ✅ |
| 3 | Новый DAG `added_elements_etl` | ✅ работает |
| 4 | Backfill 12M строк (49 мин) | ✅ |
| 4.1 | Фикс таймтейбла (`CronDataIntervalTimetable`) | ✅ |
| 4.2 | Диагностический скрипт `diagnose_added_elements.py` | ✅ |
| 5 | Валидационные SQL в `tests/` | ⏳ |
| 6 | Удаление старого pandas-кода | ✅ |

Этап 6 — что удалено (от 2026-06-05):
- `dags/added_elements_etl_dag.py.bak` — старый pandas DAG
- `dags/etl_pipelines/transform/added_elements.py` — старая трансформация (631 строка)
- Функции в `dags/etl_pipelines/extract/pluginsdb.py`:
  `extract_added_incremental`, `extract_modified_incremental`,
  `_extract_element_table_incremental`, `extract_legacy_added_elements`
- Функции в `dags/common/utils.py`:
  `extract_short_project_name`, `extract_file_storage_name`, `get_object_name`
  (логика портирована в `functions/parse_project.sql`)

**Осталось сделать вручную на сервере** — удалить устаревшие Airflow Variables:

```bash
docker exec ask-apache-airflow-airflow-worker-1 \
  airflow variables delete added_elements_last_date
docker exec ask-apache-airflow-airflow-worker-1 \
  airflow variables delete modified_elements_last_date
```

## Соответствие Python-коду (история)

Таблица отражает, какие Python-функции были портированы в SQL. Часть Python-кода
**удалена в этапе 6** (отмечена ※), часть остаётся в `common/utils.py` потому что
используется `projectsync` / `scripts`.

| SQL                                              | Python-источник                                     |
|--------------------------------------------------|-----------------------------------------------------|
| `datalake.parse_short_project_name`              | `common/utils.py:extract_short_name`                |
| `datalake.parse_file_storage_name`               | `common/utils.py:extract_short_project_name` ※      |
| `datalake.parse_object_name`                     | `common/utils.py:get_object_name` ※                 |
| `datalake.parse_project_solution`                | `common/utils.py:get_project_solution` + `config.py` |
| `datalake.parse_project_stage`                   | `common/utils.py:get_project_stage` + `config.py`   |
| `datalake.parse_elements_count`                  | `transform/added_elements.py:count_elements` ※      |
| `datalake.classify_transaction_fallback`         | `transform/added_elements.py:FALLBACK_PATTERNS` ※   |
| `dim_transactions` (наполнение)                  | `mappings/transactions.csv`                         |
| `dim_bim_users` (наполнение)                     | `common/config.py:BIM_USERS`                        |

※ — Python-код удалён, остался только SQL-эквивалент.
