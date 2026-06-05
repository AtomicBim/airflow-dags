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

## Запуск трансформации (Этап 2 готов)

`transform/*.sql` параметризованы `%(last_date)s` / `%(run_date)s`. Запуск
из DAG-а `added_elements_etl` (каждые 2 часа) или вручную через backfill-скрипт.

**Окна:**
- RAW: `[last_date, run_date)`
- STG: `[last_date - 1 day, run_date)` — буфер для корректного `LAG`
- MARTS: `[last_date, run_date)`

**Бэкфилл (этап 4 — готов):**

```bash
# план без выполнения
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/scripts/backfill_added_elements.py --dry-run

# полный бэкфилл (окна по 14 дней по умолчанию)
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/scripts/backfill_added_elements.py

# с конкретной даты (после прерывания)
docker exec ask-apache-airflow-airflow-worker-1 \
  python /opt/airflow/scripts/backfill_added_elements.py --start 2025-06-01
```

## Статус этапов

| Этап | Что | Статус |
|------|-----|--------|
| 0+1 | DDL, функции, seeds | ✅ |
| 2 | transform-SQL | ✅ |
| 3 | Новый DAG `added_elements_etl` | ✅ создан, 🔍 отладка ручного триггера |
| 4 | Backfill 12M строк (49 мин) | ✅ |
| 5 | Валидационные SQL в `tests/` | ⏳ |
| 6 | Удаление старого кода (через 2 нед.) | ⏳ |

Старый DAG сохранён как `dags/added_elements_etl_dag.py.bak` — удалить после
2 недель стабильной работы нового.

Что удалить в этапе 6:
- `dags/added_elements_etl_dag.py.bak`
- `dags/etl_pipelines/transform/added_elements.py`
- функции `extract_added_incremental`, `extract_modified_incremental`,
  `extract_legacy_added_elements` из `etl_pipelines/extract/pluginsdb.py`
- Airflow Variables `added_elements_last_date`, `modified_elements_last_date`

## Соответствие Python-коду

| SQL                                              | Python-источник                                     |
|--------------------------------------------------|-----------------------------------------------------|
| `datalake.parse_short_project_name`              | `common/utils.py:extract_short_name`                |
| `datalake.parse_file_storage_name`               | `common/utils.py:extract_short_project_name`        |
| `datalake.parse_object_name`                     | `common/utils.py:get_object_name`                   |
| `datalake.parse_project_solution`                | `common/utils.py:get_project_solution` + `config.py` |
| `datalake.parse_project_stage`                   | `common/utils.py:get_project_stage` + `config.py`   |
| `datalake.parse_elements_count`                  | `transform/added_elements.py:count_elements`        |
| `datalake.classify_transaction_fallback`         | `transform/added_elements.py:FALLBACK_PATTERNS`     |
| `dim_transactions` (наполнение)                  | `mappings/transactions.csv`                         |
| `dim_bim_users` (наполнение)                     | `common/config.py:BIM_USERS`                        |
