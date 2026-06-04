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
└── transform/              # SQL шагов DAG (этап 2 рефакторинга — ещё не реализован)
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

## Что НЕ сделано (следующие этапы)

- `transform/01_extract_load_raw.sql` — инкрементальная загрузка RAW из FDW (этап 2)
- `transform/02_transform_staging.sql` — обогащение RAW → STG (этап 2)
- `transform/03_build_marts.sql` — STG → витрины (этап 2)
- Новый тонкий DAG `added_elements_etl_dag.py` (этап 3)
- Backfill-скрипт (этап 4)
- Валидационные SQL (этап 5)

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
