"""
Extractors для PluginsDB (PostgreSQL).
Извлекают данные из различных схем и таблиц pluginsdb.
"""
import os
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_ad_users(postgres_conn_id: str, output_path: str, **context) -> str:
    """Экспортирует таблицу users.ad_user из users_db."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM public.ad_user'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} пользователей AD в {output_path}")

    return output_path


def extract_legacy_monitoring(
    postgres_conn_id: str,
    output_path: str,
    force_reload: bool = False,
    **context,
) -> str:
    """Экспортирует старую таблицу legacy.monitoring_legacy."""
    if not force_reload and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        print(f"Legacy Monitoring CSV уже существует, пропускаем выгрузку: {output_path}")
        return output_path

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM legacy.monitoring_legacy'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} СТАРЫХ записей мониторинга в {output_path}")
    return output_path


def extract_plugins(postgres_conn_id: str, output_path: str, **context) -> str:
    """Экспортирует таблицу plugins.plugin из pluginsdb."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM plugins.plugin'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} плагинов в {output_path}")

    return output_path


def extract_monitoring(postgres_conn_id: str, output_path: str, **context) -> str:
    """Экспортирует новую таблицу revit.plugin_launch (после 2 марта 2026)."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM revit.plugin_launch'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} НОВЫХ записей мониторинга в {output_path}")

    return output_path


def extract_legacy_monitoring(
    postgres_conn_id: str,
    output_path: str,
    force_reload: bool = False,
    **context,
) -> str:
    """
    Экспортирует старую таблицу legacy.monitoring_legacy (до 2 марта 2026).

    Эта таблица содержит исторические данные и не пополняется новыми записями,
    поэтому extract идемпотентен: при повторных запусках возвращается путь к уже
    выгруженному CSV без обращения к БД.

    Args:
        postgres_conn_id: ID Airflow connection
        output_path: Путь к CSV
        force_reload: Если True — игнорировать существующий файл и выгрузить заново

    Returns:
        Путь к CSV файлу
    """
    if not force_reload and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        print(f"Legacy Monitoring CSV уже существует, пропускаем выгрузку: {output_path}")
        return output_path

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM legacy.monitoring_legacy'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} СТАРЫХ записей мониторинга в {output_path}")
    return output_path


def extract_development_stage(postgres_conn_id: str, output_path: str, **context) -> str:
    """Экспортирует таблицу plugins.plugin_development_stage из pluginsdb."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM plugins.plugin_development_stage'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} стадий разработки в {output_path}")

    return output_path


def extract_legacy_project_sync(
    postgres_conn_id: str,
    output_path: str,
    force_reload: bool = False,
    **context,
) -> str:
    """
    Экспортирует старую таблицу legacy.project_sync_legacy (до 2 марта 2026).

    Эта таблица содержит исторические данные и не пополняется новыми записями,
    поэтому extract идемпотентен: при повторных запусках возвращается путь к уже
    выгруженному CSV без обращения к БД.

    Args:
        postgres_conn_id: ID Airflow connection
        output_path: Путь к CSV
        force_reload: Если True — игнорировать существующий файл и выгрузить заново

    Returns:
        Путь к CSV файлу
    """
    if not force_reload and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        print(f"Legacy CSV уже существует, пропускаем выгрузку: {output_path}")
        return output_path

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM legacy.project_sync_legacy'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} СТАРЫХ записей project_sync в {output_path}")
    return output_path

def extract_new_project_sync(postgres_conn_id: str, output_path: str, **context) -> str:
    """Экспортирует новую таблицу revit.project_sync (после 2 марта 2026)."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM revit.project_sync'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} НОВЫХ записей project_sync в {output_path}")
    return output_path


def extract_logs(postgres_conn_id: str, output_path: str, **context) -> str:
    """Экспортирует таблицу plugins.log из pluginsdb."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM plugins.log'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} записей логов в {output_path}")

    return output_path


# NOTE: extract_added_incremental / extract_modified_incremental /
# extract_legacy_added_elements / _extract_element_table_incremental удалены
# вместе с переходом added_elements на ELT-архитектуру. Выгрузка из
# revit.added_element / revit.modified_element / legacy.added_element_legacy
# теперь идёт через postgres_fdw + SQL-файлы в
# dags/etl_pipelines/sql/added_elements/transform/.