"""
seeds/load_dim_ad_users.py
==========================

Синхронизация ``datalake.dim_ad_users`` из таблицы ``public.ad_user``
в инстансе AD (Airflow Connection ``tim_db_ad``).

Текущая реализация — TRUNCATE + COPY (полная перезагрузка). Объём небольшой
(несколько тысяч строк), время загрузки секунды. Если в будущем понадобится
инкремент по updated_at — заменить на UPSERT (см. ON CONFLICT).

Этот скрипт — временная мера в рамках рефакторинга added_elements_etl.
В дальнейшем планируется отдельный DAG ``ad_sync_dag`` со schedule=@daily,
который будет дёргать тот же код.

Запуск
------
Внутри Airflow-контейнера::

    python /opt/airflow/dags/etl_pipelines/sql/added_elements/seeds/load_dim_ad_users.py

Унификация полей
----------------
В public.ad_user может присутствовать ``project_section`` или ``project_doc_section``
(зависит от схемы исходника). Если есть ``project_doc_section`` и нет
``project_section`` — используем первое как источник для итоговой колонки
``project_section`` в dim_ad_users (соответствует transform/added_elements.py:412).
"""
from __future__ import annotations

import io
import csv
import logging
import sys

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SOURCE_CONN_ID = "tim_db_ad"
TARGET_CONN_ID = "tim_db_postgres"
TARGET_SCHEMA  = "datalake"
TARGET_TABLE   = "dim_ad_users"


def _detect_section_column(source_hook: PostgresHook) -> str:
    """Определяет, есть ли в public.ad_user колонка project_section или project_doc_section."""
    sql = """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name   = 'ad_user'
           AND column_name IN ('project_section', 'project_doc_section')
    """
    rows = source_hook.get_records(sql)
    cols = {r[0] for r in rows}
    if "project_section" in cols:
        return "project_section"
    if "project_doc_section" in cols:
        return "project_doc_section"
    # Нет ни одной — вернём NULL-выражение
    return "NULL::TEXT"


def main() -> None:
    src = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
    dst = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

    section_col = _detect_section_column(src)
    section_expr = section_col if section_col == "NULL::TEXT" else f'"{section_col}"'
    logger.info("Используем %s как источник для project_section", section_expr)

    # Достаём данные из источника. COALESCE даст пустые строки для NULL — это
    # важно для COPY (пустое поле = пустая строка).
    select_sql = f"""
        SELECT
            id::BIGINT                       AS ad_user_id,
            display_name                     AS user_name,
            department                       AS department,
            {section_expr}                   AS project_section,
            company                          AS company
          FROM public.ad_user
         WHERE id IS NOT NULL
    """
    rows = src.get_records(select_sql)
    logger.info("Получено %s строк из %s.public.ad_user", len(rows), SOURCE_CONN_ID)

    if not rows:
        logger.warning("Нет данных в public.ad_user — таблица %s.%s не будет тронута",
                       TARGET_SCHEMA, TARGET_TABLE)
        return

    # Буферим в CSV для COPY.
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    for ad_id, name, dept, section, company in rows:
        writer.writerow([
            ad_id,
            "" if name is None else name,
            "" if dept is None else dept,
            "" if section is None else section,
            "" if company is None else company,
        ])
    buffer.seek(0)

    full = f'"{TARGET_SCHEMA}"."{TARGET_TABLE}"'
    conn = dst.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {full}")
            copy_sql = (
                f"COPY {full} (ad_user_id, user_name, department, project_section, company) "
                f"FROM STDIN WITH (FORMAT CSV, NULL '')"
            )
            cur.copy_expert(copy_sql, buffer)
            cur.execute(f"SELECT count(*) FROM {full}")
            (loaded,) = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Загружено %s строк в %s.%s", loaded, TARGET_SCHEMA, TARGET_TABLE)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        logger.exception("Ошибка загрузки dim_ad_users: %s", exc)
        sys.exit(1)
