"""
seeds/setup_fdw.py
==================

Одноразовый инициализатор postgres_fdw в datalake-инстансе (Airflow Connection
``tim_db_postgres``). Подключает источники из ``tim_db_revit`` как foreign tables
в схемах ``revit_ext`` и ``legacy_ext``.

Запуск
------
Внутри Airflow-контейнера (airflow-cli или worker)::

    python /opt/airflow/dags/etl_pipelines/sql/added_elements/seeds/setup_fdw.py

Или из обычного DAG-а (см. одноразовый dag etl_init_added_elements_fdw в
дальнейших этапах рефакторинга).

Что делает
----------
1. Читает реквизиты ``tim_db_revit`` из Airflow Connections.
2. В ``tim_db_postgres`` создаёт:
   - SERVER ``tim_db_revit_srv`` (postgres_fdw) — если ещё нет.
   - USER MAPPING для пользователя текущего соединения.
   - IMPORT FOREIGN SCHEMA для:
       * схема ``revit`` → ``datalake.revit_ext`` (только added_element / modified_element);
       * схема ``legacy`` → ``datalake.legacy_ext`` (только added_element_legacy).

Идемпотентен: повторный запуск — no-op (DROP перед IMPORT использует CASCADE
осторожно и только для foreign tables, не трогая локальные данные).
"""
from __future__ import annotations

import logging
import sys

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- Конфигурация ----------------------------------------------------------

# Connection ID источника (откуда тянем revit.* и legacy.*).
SOURCE_CONN_ID = "tim_db_revit"

# Connection ID целевого datalake (где создаём FDW server + foreign tables).
TARGET_CONN_ID = "tim_db_postgres"

# Имя FDW-сервера в datalake. Используется в transform/01_extract_load_raw.sql.
FDW_SERVER_NAME = "tim_db_revit_srv"

# Маппинг "удалённая схема -> локальная схема для foreign tables".
FDW_SCHEMA_MAP = {
    # схема в источнике : (схема в datalake, список таблиц)
    "revit":  ("revit_ext",  ["added_element", "modified_element"]),
    "legacy": ("legacy_ext", ["added_element_legacy"]),
}


# --- Реализация -------------------------------------------------------------

def _get_source_params() -> dict:
    """Достаёт реквизиты SOURCE_CONN_ID из Airflow Connections."""
    conn = BaseHook.get_connection(SOURCE_CONN_ID)
    if not conn.host or not conn.schema:
        raise RuntimeError(
            f"Connection {SOURCE_CONN_ID!r} не содержит host или schema (database). "
            "Проверьте настройку соединения в Airflow."
        )
    return {
        "host":     conn.host,
        "port":     str(conn.port or 5432),
        "dbname":   conn.schema,
        "user":     conn.login or "",
        "password": conn.password or "",
    }


def _execute_admin_sql(target_hook: PostgresHook, sql: str, params: tuple = ()) -> None:
    """Выполняет административный SQL с autocommit (CREATE SERVER, USER MAPPING и т.д.)."""
    conn = target_hook.get_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
    finally:
        conn.close()


def _server_exists(target_hook: PostgresHook, server_name: str) -> bool:
    sql = "SELECT 1 FROM pg_foreign_server WHERE srvname = %s"
    rows = target_hook.get_records(sql, parameters=(server_name,))
    return bool(rows)


def _user_mapping_exists(target_hook: PostgresHook, server_name: str) -> bool:
    sql = """
        SELECT 1
          FROM pg_user_mappings
         WHERE srvname = %s
           AND usename = current_user
    """
    rows = target_hook.get_records(sql, parameters=(server_name,))
    return bool(rows)


def setup_fdw() -> None:
    """Главная процедура. Идемпотентна."""
    src = _get_source_params()
    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

    # 1) Гарантируем наличие схемы и расширения (на случай, если 00_schema.sql
    # ещё не применяли).
    logger.info("Гарантируем CREATE EXTENSION postgres_fdw и схемы revit_ext/legacy_ext")
    _execute_admin_sql(target, "CREATE EXTENSION IF NOT EXISTS postgres_fdw")
    _execute_admin_sql(target, "CREATE SCHEMA IF NOT EXISTS revit_ext")
    _execute_admin_sql(target, "CREATE SCHEMA IF NOT EXISTS legacy_ext")

    # 2) CREATE SERVER. Идентификаторы и значения подставляются через format_ident /
    # SQL литералы — не через параметры (postgres_fdw OPTIONS не принимают $-плейсхолдеры).
    if _server_exists(target, FDW_SERVER_NAME):
        logger.info("SERVER %s уже существует — пропускаем CREATE", FDW_SERVER_NAME)
    else:
        logger.info("Создаём SERVER %s -> %s:%s/%s",
                    FDW_SERVER_NAME, src["host"], src["port"], src["dbname"])
        create_server_sql = (
            f"CREATE SERVER {FDW_SERVER_NAME} "
            f"FOREIGN DATA WRAPPER postgres_fdw "
            f"OPTIONS (host {_quote_lit(src['host'])}, "
            f"port {_quote_lit(src['port'])}, "
            f"dbname {_quote_lit(src['dbname'])})"
        )
        _execute_admin_sql(target, create_server_sql)

    # 3) USER MAPPING. Привязываем к current_user целевого соединения
    # (тот же пользователь, под которым Airflow ходит в datalake).
    if _user_mapping_exists(target, FDW_SERVER_NAME):
        logger.info("USER MAPPING уже существует — пропускаем")
    else:
        logger.info("Создаём USER MAPPING для CURRENT_USER -> %s", src["user"])
        create_mapping_sql = (
            f"CREATE USER MAPPING FOR CURRENT_USER SERVER {FDW_SERVER_NAME} "
            f"OPTIONS (user {_quote_lit(src['user'])}, "
            f"password {_quote_lit(src['password'])})"
        )
        _execute_admin_sql(target, create_mapping_sql)

    # 4) IMPORT FOREIGN SCHEMA для каждой пары "источник -> локальная схема".
    # Используем DROP FOREIGN TABLE IF EXISTS перед IMPORT, чтобы команда
    # была идемпотентной (IMPORT падает, если foreign table уже существует).
    for src_schema, (local_schema, tables) in FDW_SCHEMA_MAP.items():
        tables_list = ", ".join(tables)
        for tbl in tables:
            _execute_admin_sql(
                target,
                f'DROP FOREIGN TABLE IF EXISTS {local_schema}."{tbl}"',
            )
        logger.info(
            "IMPORT FOREIGN SCHEMA %s LIMIT TO (%s) -> %s",
            src_schema, tables_list, local_schema,
        )
        import_sql = (
            f"IMPORT FOREIGN SCHEMA {src_schema} LIMIT TO ({tables_list}) "
            f"FROM SERVER {FDW_SERVER_NAME} INTO {local_schema}"
        )
        _execute_admin_sql(target, import_sql)

    logger.info("FDW setup завершён успешно")


def _quote_lit(value: str) -> str:
    """Безопасно цитирует значение для SQL-литерала (одинарные кавычки)."""
    # Postgres OPTIONS принимают строковые литералы в одинарных кавычках.
    # Дублирование одинарных кавычек — стандартная защита от SQL-инъекций.
    return "'" + str(value).replace("'", "''") + "'"


if __name__ == "__main__":
    try:
        setup_fdw()
    except Exception as exc:  # pragma: no cover
        logger.exception("Ошибка настройки FDW: %s", exc)
        sys.exit(1)
