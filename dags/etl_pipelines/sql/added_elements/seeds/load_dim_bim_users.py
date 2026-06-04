"""
seeds/load_dim_bim_users.py
===========================

Загрузка списка BIM-специалистов из ``dags/common/config.py:BIM_USERS`` в
``datalake.dim_bim_users``.

Стратегия — TRUNCATE + INSERT. Список меняется редко, объём — ~20 строк.

Запуск
------
Внутри Airflow-контейнера::

    python /opt/airflow/dags/etl_pipelines/sql/added_elements/seeds/load_dim_bim_users.py
"""
from __future__ import annotations

import logging
import os
import sys

# Скрипт может запускаться вне Airflow-контекста (просто `python <file>`),
# тогда корень dags/ не попадает в sys.path и `from common.config` падает.
# Добавляем dags/ в sys.path явно (3 уровня вверх от seeds/).
_DAGS_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.config import BIM_USERS  # noqa: E402  (sys.path фиксится выше)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

TARGET_CONN_ID = "tim_db_postgres"
TARGET_SCHEMA  = "datalake"
TARGET_TABLE   = "dim_bim_users"


def main() -> None:
    users = sorted({u.strip() for u in BIM_USERS if u and u.strip()})
    if not users:
        logger.warning("BIM_USERS пуст — таблица %s.%s не будет тронута", TARGET_SCHEMA, TARGET_TABLE)
        return

    hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    full = f'"{TARGET_SCHEMA}"."{TARGET_TABLE}"'

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {full}")
            cur.executemany(
                f"INSERT INTO {full} (user_name) VALUES (%s)",
                [(u,) for u in users],
            )
            cur.execute(f"SELECT count(*) FROM {full}")
            (loaded,) = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Загружено %s пользователей в %s.%s", loaded, TARGET_SCHEMA, TARGET_TABLE)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        logger.exception("Ошибка загрузки dim_bim_users: %s", exc)
        sys.exit(1)
