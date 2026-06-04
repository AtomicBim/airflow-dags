"""
DAG для Added/Modified Elements ETL pipeline (ELT-версия, без pandas).

Архитектура: Airflow — тонкий оркестратор. Вся логика — в PostgreSQL.

Источники (через postgres_fdw в datalake):
  - revit_ext.added_element       (date >= 2026-02-14)
  - revit_ext.modified_element    (date >= 2026-02-14)
  - legacy_ext.added_element_legacy (date <  2026-02-14)

Слои:
  RAW  -> datalake.raw_added_elements
  STG  -> datalake.stg_added_elements
  MARTS -> datalake.ext_added_elements_designers
           datalake.ext_added_elements_bim

SQL-файлы: dags/etl_pipelines/sql/added_elements/transform/

Инкрементальная стратегия: параметры %(last_date)s / %(run_date)s = Airflow
data_interval_start / data_interval_end (2-часовые окна).

Первоначальная историческая загрузка: scripts/backfill_added_elements.py
(запущен отдельно, данные уже в БД).

Идемпотентен: каждый шаг DELETE + INSERT в своём окне.
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Путь к SQL относительно DAGS_FOLDER (/opt/airflow/dags).
# SQLExecuteQueryOperator принимает путь к файлу — ищет его в DAGS_FOLDER.
_SQL = "etl_pipelines/sql/added_elements/transform"

# Параметры передаются в psycopg2 как %(name)s — Jinja рендерит строки,
# psycopg2 подставляет типизированные значения.
_PARAMS = {
    "last_date": "{{ data_interval_start }}",
    "run_date":  "{{ data_interval_end }}",
}

_DEFAULT_ARGS = {
    "retries":        2,
    "retry_delay":    timedelta(minutes=5),
    "retry_exponential_backoff": False,
}


@dag(
    dag_id="added_elements_etl",
    start_date=pendulum.datetime(2025, 7, 17, tz="UTC"),  # min(date) из источников
    schedule="17 */2 * * *",    # каждые 2 часа в :17 — как в старом DAG
    catchup=False,              # история залита через backfill_added_elements.py
    max_active_runs=1,          # не запускать параллельно (FDW + STG LAG)
    tags=["elements", "etl", "analytics", "incremental", "elt", "no-pandas"],
    default_args=_DEFAULT_ARGS,
    doc_md="""
    # ETL-пайплайн Added/Modified Elements (ELT, без pandas)

    Рефакторинг со старого in-memory pandas пайплайна на ELT в PostgreSQL.

    ## Схема выполнения
    ```
    extract_load_raw  ->  transform_staging  ->  build_marts
    ```

    ## Шаги
    1. **extract_load_raw** — инкрементальная загрузка из источников (через FDW)
       в `datalake.raw_added_elements`.
    2. **transform_staging** — обогащение raw → stg: JOIN с dim_ad_users /
       dim_transactions / dim_bim_users, парсинг проекта, расчёт сессий (LAG).
       Окно: `[data_interval_start - 1 day, data_interval_end)` для корректного
       LAG на границе сессий.
    3. **build_marts** — разделение stg → `ext_added_elements_designers` / `_bim`
       по is_bim. Окно: `[data_interval_start, data_interval_end)`.

    ## Параметры
    - `last_date` = `data_interval_start`
    - `run_date`  = `data_interval_end`

    ## Словари (обновляются вручную)
    - `datalake.dim_bim_users` — из `common/config.py:BIM_USERS`
    - `datalake.dim_transactions` — из `mappings/transactions.csv`
    - `datalake.dim_ad_users` — из `public.ad_user` (tim_db_ad)

    Скрипты обновления: `dags/etl_pipelines/sql/added_elements/seeds/`

    ## Сброс и перезагрузка истории
    ```bash
    python /opt/airflow/scripts/backfill_added_elements.py --start YYYY-MM-DD
    ```
    """,
)
def added_elements_etl() -> None:

    extract_load = SQLExecuteQueryOperator(
        task_id="extract_load_raw",
        conn_id="tim_db_postgres",
        sql=f"{_SQL}/01_extract_load_raw.sql",
        parameters=_PARAMS,
        # autocommit=True: файл сам управляет транзакцией (BEGIN/COMMIT).
        # Без этого psycopg2 откроет неявную внешнюю транзакцию и вложенный
        # BEGIN упадёт с "there is already a transaction in progress".
        autocommit=True,
    )

    transform_stg = SQLExecuteQueryOperator(
        task_id="transform_staging",
        conn_id="tim_db_postgres",
        sql=f"{_SQL}/02_transform_staging.sql",
        parameters=_PARAMS,
        autocommit=True,
    )

    build_marts = SQLExecuteQueryOperator(
        task_id="build_marts",
        conn_id="tim_db_postgres",
        sql=f"{_SQL}/03_build_marts.sql",
        parameters=_PARAMS,
        autocommit=True,
    )

    extract_load >> transform_stg >> build_marts


added_elements_etl()
