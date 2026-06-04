"""
scripts/backfill_added_elements.py
==================================

Одноразовый бэкфилл исторических данных для нового ELT-пайплайна
``added_elements_etl``.

Запускается ВНУТРИ Airflow-контейнера (worker). Использует Airflow Connection
``tim_db_postgres`` через ``PostgresHook``.

Логика
------
1. Определяет диапазон [global_min_date, global_max_date) по источникам
   (revit_ext + legacy_ext) через FDW.
2. Бьёт диапазон на окна по 14 дней (BATCH_DAYS, конфигурируется).
3. Для каждого окна последовательно выполняет:
   - transform/01_extract_load_raw.sql
   - transform/02_transform_staging.sql
   - transform/03_build_marts.sql
4. Логирует прогресс, метрики, ошибки.

Идемпотентен: каждое окно — DELETE+INSERT. Можно прерывать и перезапускать
с конкретной даты (см. --start).

Запуск
------
Полный бэкфилл::

    docker exec ask-apache-airflow-airflow-worker-1 \\
      python /opt/airflow/scripts/backfill_added_elements.py

С конкретной даты (например, перезапустить после прерывания)::

    docker exec ask-apache-airflow-airflow-worker-1 \\
      python /opt/airflow/scripts/backfill_added_elements.py --start 2025-06-01

Dry-run (только показать план окон, ничего не запускать)::

    docker exec ask-apache-airflow-airflow-worker-1 \\
      python /opt/airflow/scripts/backfill_added_elements.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, date

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Конфигурация ----------------------------------------------------------

CONN_ID    = "tim_db_postgres"
BATCH_DAYS = 14  # размер окна в днях (по умолчанию 2 недели — компромисс
                 # между накладными расходами и нагрузкой на LAG в STG)

# Пути к SQL-файлам. В контейнере airflow-worker:
SQL_BASE = "/opt/airflow/dags/etl_pipelines/sql/added_elements/transform"
SQL_FILES = (
    os.path.join(SQL_BASE, "01_extract_load_raw.sql"),
    os.path.join(SQL_BASE, "02_transform_staging.sql"),
    os.path.join(SQL_BASE, "03_build_marts.sql"),
)

# Если запускаем вне контейнера — fallback на путь относительно файла.
if not os.path.exists(SQL_FILES[0]):
    _here = os.path.dirname(os.path.abspath(__file__))
    SQL_BASE = os.path.normpath(
        os.path.join(_here, "..", "dags", "etl_pipelines", "sql", "added_elements", "transform")
    )
    SQL_FILES = tuple(os.path.join(SQL_BASE, os.path.basename(p)) for p in SQL_FILES)


# --- Реализация ------------------------------------------------------------

def _detect_global_range(hook: PostgresHook) -> tuple[datetime, datetime]:
    """
    Минимум и максимум date по всем источникам через FDW.
    Возвращает (min_date, max_date_exclusive) — окно полуоткрытое.
    """
    sql = """
        SELECT
            LEAST(
                (SELECT min(date) FROM revit_ext.added_element),
                (SELECT min(date) FROM revit_ext.modified_element),
                (SELECT min(date) FROM legacy_ext.added_element_legacy)
            ) AS min_d,
            GREATEST(
                (SELECT max(date) FROM revit_ext.added_element),
                (SELECT max(date) FROM revit_ext.modified_element),
                (SELECT max(date) FROM legacy_ext.added_element_legacy)
            ) AS max_d
    """
    rows = hook.get_records(sql)
    if not rows or rows[0][0] is None:
        raise RuntimeError("Не удалось определить диапазон дат в источниках")
    min_d, max_d = rows[0]
    # Округляем до суток и делаем верх полуоткрытым (+1 день).
    min_d = datetime(min_d.year, min_d.month, min_d.day)
    max_d = datetime(max_d.year, max_d.month, max_d.day) + timedelta(days=1)
    return min_d, max_d


def _iter_windows(start: datetime, end: datetime, days: int):
    """Генератор окон [w_start, w_end) длиной `days`."""
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=days), end)
        yield cur, nxt
        cur = nxt


def _load_sql_files() -> list[tuple[str, str]]:
    """Читает SQL-файлы в память (один раз). Возвращает [(имя, текст), ...]."""
    result = []
    for path in SQL_FILES:
        if not os.path.exists(path):
            raise FileNotFoundError(f"SQL не найден: {path}")
        with open(path, "r", encoding="utf-8") as fh:
            result.append((os.path.basename(path), fh.read()))
    return result


def _run_window(
    hook: PostgresHook,
    sql_files: list[tuple[str, str]],
    last_date: datetime,
    run_date: datetime,
) -> None:
    """Прогоняет 3 SQL-файла последовательно в одной коннекции."""
    params = {"last_date": last_date, "run_date": run_date}
    conn = hook.get_conn()
    # Каждый файл сам управляет транзакцией через BEGIN/COMMIT, поэтому
    # включаем autocommit, чтобы psycopg2 не обернул всё в неявную транзакцию.
    conn.autocommit = True
    try:
        for name, sql in sql_files:
            t0 = datetime.now()
            with conn.cursor() as cur:
                cur.execute(sql, params)
            logger.info(
                "  %s: %.1fs (rowcount=%s)",
                name,
                (datetime.now() - t0).total_seconds(),
                cur.rowcount if hasattr(cur, "rowcount") else "n/a",
            )
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill datalake.ext_added_elements_*")
    ap.add_argument("--start", type=str, default=None,
                    help="Начальная дата YYYY-MM-DD (по умолчанию — min(date) из источников)")
    ap.add_argument("--end", type=str, default=None,
                    help="Конечная дата YYYY-MM-DD, исключительно (по умолчанию — max(date)+1)")
    ap.add_argument("--days", type=int, default=BATCH_DAYS,
                    help=f"Размер окна в днях (по умолчанию {BATCH_DAYS})")
    ap.add_argument("--dry-run", action="store_true",
                    help="Только показать план окон, не выполнять SQL")
    args = ap.parse_args()

    hook = PostgresHook(postgres_conn_id=CONN_ID)

    # Определяем диапазон.
    if args.start and args.end:
        global_start = datetime.fromisoformat(args.start)
        global_end   = datetime.fromisoformat(args.end)
    else:
        auto_start, auto_end = _detect_global_range(hook)
        global_start = datetime.fromisoformat(args.start) if args.start else auto_start
        global_end   = datetime.fromisoformat(args.end)   if args.end   else auto_end

    logger.info("Диапазон бэкфилла: [%s, %s), окно %s дней",
                global_start, global_end, args.days)

    windows = list(_iter_windows(global_start, global_end, args.days))
    logger.info("Всего окон: %s", len(windows))

    if args.dry_run:
        for i, (a, b) in enumerate(windows, 1):
            logger.info("  [%s/%s] %s -> %s", i, len(windows), a, b)
        logger.info("dry-run: SQL не выполнен")
        return

    sql_files = _load_sql_files()
    t_total = datetime.now()
    for i, (w_start, w_end) in enumerate(windows, 1):
        logger.info("[%s/%s] окно %s -> %s", i, len(windows), w_start, w_end)
        try:
            _run_window(hook, sql_files, w_start, w_end)
        except Exception:
            logger.exception("Ошибка в окне %s -> %s", w_start, w_end)
            logger.error(
                "Прерывание. Перезапуск:  --start %s",
                w_start.strftime("%Y-%m-%d"),
            )
            sys.exit(1)

    elapsed = (datetime.now() - t_total).total_seconds()
    logger.info("Бэкфилл завершён за %.1f сек (%.1f мин)", elapsed, elapsed / 60)


if __name__ == "__main__":
    main()
