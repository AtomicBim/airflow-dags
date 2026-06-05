"""
scripts/diagnose_added_elements.py
==================================

Диагностический скрипт для added_elements ETL.

Что делает
----------
1. Считает count(*) и max(date) на каждом слое за последние 36 часов:
   - revit.added_element / revit.modified_element (через FDW: revit_ext.*)
   - datalake.raw_added_elements
   - datalake.stg_added_elements
   - datalake.ext_added_elements_designers / _bim
2. Показывает реальные окна, которые обработал DAG за последние 36 часов
   (из метабазы Airflow).
3. Печатает тип колонки `date` в источниках (с tz или без).
4. Печатает текущий час сервера БД (datalake), время сессии и таймзону —
   чтобы убедиться в расхождении UTC vs local.

Запуск
------
    docker exec ask-apache-airflow-airflow-worker-1 \\
        python /opt/airflow/scripts/diagnose_added_elements.py

Скрипт ничего не пишет, только читает. Безопасно запускать в любое время.
"""
from __future__ import annotations

import logging
import sys
from typing import Iterable, Sequence

from airflow import settings
from airflow.models import DagRun
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger("diagnose_added_elements")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DATALAKE_CONN = "tim_db_postgres"
DAG_ID = "added_elements_etl"


# ---------------------------------------------------------------------------
# Печать таблиц
# ---------------------------------------------------------------------------

def _print_table(title: str, headers: Sequence[str], rows: Iterable[Sequence]) -> None:
    rows = list(rows)
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)
    if not rows:
        print("(нет строк)")
        return
    # Преобразуем всё в str для измерения ширины
    str_rows = [[("" if v is None else str(v)) for v in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in str_rows)) for i, h in enumerate(headers)]
    fmt = " | ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("-+-".join("-" * w for w in widths))
    for r in str_rows:
        print(fmt.format(*r))


# ---------------------------------------------------------------------------
# Запросы к datalake
# ---------------------------------------------------------------------------

LAYERS_SQL = """
WITH layers AS (
    SELECT 'src.added'        AS layer, count(*)::bigint AS rows, max(date) AS max_date
      FROM revit_ext.added_element        WHERE date >= now() - interval '36 hours'
    UNION ALL
    SELECT 'src.modified',                 count(*)::bigint,        max(date)
      FROM revit_ext.modified_element     WHERE date >= now() - interval '36 hours'
    UNION ALL
    SELECT 'raw',                          count(*)::bigint,        max(date)
      FROM datalake.raw_added_elements    WHERE date >= now() - interval '36 hours'
    UNION ALL
    SELECT 'stg',                          count(*)::bigint,        max(date)
      FROM datalake.stg_added_elements    WHERE date >= now() - interval '36 hours'
    UNION ALL
    SELECT 'mart.designers',               count(*)::bigint,        max(date)
      FROM datalake.ext_added_elements_designers WHERE date >= now() - interval '36 hours'
    UNION ALL
    SELECT 'mart.bim',                     count(*)::bigint,        max(date)
      FROM datalake.ext_added_elements_bim       WHERE date >= now() - interval '36 hours'
)
SELECT layer, rows, max_date FROM layers;
"""

LAYERS_TOTAL_SQL = """
SELECT 'src.added'      AS layer, count(*)::bigint, min(date), max(date) FROM revit_ext.added_element
UNION ALL SELECT 'src.modified', count(*)::bigint, min(date), max(date) FROM revit_ext.modified_element
UNION ALL SELECT 'raw',          count(*)::bigint, min(date), max(date) FROM datalake.raw_added_elements
UNION ALL SELECT 'stg',          count(*)::bigint, min(date), max(date) FROM datalake.stg_added_elements
UNION ALL SELECT 'mart.designers', count(*)::bigint, min(date), max(date) FROM datalake.ext_added_elements_designers
UNION ALL SELECT 'mart.bim',     count(*)::bigint, min(date), max(date) FROM datalake.ext_added_elements_bim;
"""

DATE_TYPE_SQL = """
SELECT table_schema, table_name, column_name, data_type
  FROM information_schema.columns
 WHERE (table_schema = 'revit_ext' AND table_name IN ('added_element','modified_element'))
    OR (table_schema = 'datalake'  AND table_name IN ('raw_added_elements','stg_added_elements',
                                                       'ext_added_elements_designers',
                                                       'ext_added_elements_bim'))
   AND column_name = 'date'
 ORDER BY table_schema, table_name;
"""

TIMEZONE_SQL = """
SELECT
    current_setting('TimeZone')              AS session_tz,
    now()                                    AS now_local,
    (now() AT TIME ZONE 'UTC')               AS now_utc,
    current_setting('server_version')        AS pg_version;
"""


def diagnose_datalake() -> None:
    hook = PostgresHook(postgres_conn_id=DATALAKE_CONN)

    # 1) Слои за последние 36 часов
    rows = hook.get_records(LAYERS_SQL)
    _print_table(
        "СЛОИ за последние 36 часов (count + max(date))",
        ["layer", "rows_36h", "max_date_36h"],
        rows,
    )

    # 2) Слои всего (для общего понимания)
    rows = hook.get_records(LAYERS_TOTAL_SQL)
    _print_table(
        "СЛОИ всего (count + min/max date)",
        ["layer", "rows_total", "min_date", "max_date"],
        rows,
    )

    # 3) Типы колонок date
    rows = hook.get_records(DATE_TYPE_SQL)
    _print_table(
        "Тип колонки date в источниках и витринах",
        ["schema", "table", "column", "data_type"],
        rows,
    )

    # 4) Таймзона сервера БД
    rows = hook.get_records(TIMEZONE_SQL)
    _print_table(
        "Сессия PostgreSQL: таймзона и время",
        ["session_tz", "now_local", "now_utc", "pg_version"],
        rows,
    )


# ---------------------------------------------------------------------------
# Запросы к метабазе Airflow
# ---------------------------------------------------------------------------

def diagnose_airflow_runs() -> None:
    """Окна, которые реально прогонял DAG за последние 36 часов."""
    session = settings.Session()
    try:
        # Берём через ORM, чтобы не зависеть от структуры таблицы.
        from sqlalchemy import desc
        from datetime import datetime, timedelta, timezone

        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=36)

        runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == DAG_ID)
            .filter(DagRun.data_interval_start >= cutoff)
            .order_by(desc(DagRun.data_interval_start))
            .limit(30)
            .all()
        )

        rows = [
            (
                r.run_id,
                str(r.data_interval_start),
                str(r.data_interval_end),
                r.state,
                str(r.start_date) if r.start_date else "",
                str(r.end_date) if r.end_date else "",
            )
            for r in runs
        ]
        _print_table(
            f"Раны DAG {DAG_ID} за последние 36 часов",
            ["run_id", "data_interval_start", "data_interval_end", "state", "start_date", "end_date"],
            rows,
        )
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Точечная проверка одного конкретного окна
# ---------------------------------------------------------------------------

WINDOW_PROBE_SQL = """
-- Считаем, сколько строк ВИДЕН в источнике в окне последнего успешного рана.
-- Если 0 — DAG не виноват, окно пустое (таймзона / FDW / источник).
SELECT
    'src.added'                  AS layer,
    count(*) FILTER (WHERE source.date >= %(s)s AND source.date < %(e)s) AS in_window,
    count(*)                     AS total_36h
  FROM revit_ext.added_element AS source
 WHERE source.date >= now() - interval '36 hours'
UNION ALL
SELECT
    'src.modified',
    count(*) FILTER (WHERE source.date >= %(s)s AND source.date < %(e)s),
    count(*)
  FROM revit_ext.modified_element AS source
 WHERE source.date >= now() - interval '36 hours';
"""


def diagnose_last_window() -> None:
    """Проверяем последнее окно успешного рана: видны ли в нём строки в источнике."""
    session = settings.Session()
    try:
        from sqlalchemy import desc

        last_ok = (
            session.query(DagRun)
            .filter(DagRun.dag_id == DAG_ID, DagRun.state == "success")
            .order_by(desc(DagRun.data_interval_end))
            .first()
        )
        if not last_ok:
            print("\n(нет успешных ранов — пропускаем проверку окна)")
            return

        s, e = last_ok.data_interval_start, last_ok.data_interval_end
        print(f"\nПроверяем последнее успешное окно: [{s}, {e})  run_id={last_ok.run_id}")

        hook = PostgresHook(postgres_conn_id=DATALAKE_CONN)
        rows = hook.get_records(WINDOW_PROBE_SQL, parameters={"s": s, "e": e})
        _print_table(
            "Видимость строк в источнике для последнего окна",
            ["layer", "in_window", "total_last_36h"],
            rows,
        )
    finally:
        session.close()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    try:
        diagnose_datalake()
        diagnose_airflow_runs()
        diagnose_last_window()
        return 0
    except Exception:
        logger.exception("Ошибка диагностики")
        return 1


if __name__ == "__main__":
    sys.exit(main())
