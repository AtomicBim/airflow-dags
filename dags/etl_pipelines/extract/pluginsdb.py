"""
Extractors для PluginsDB (PostgreSQL).
Извлекают данные из различных схем и таблиц pluginsdb.
"""
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_ad_users(postgres_conn_id: str, output_path: str, **context) -> str:
    """Экспортирует таблицу users.ad_user из pluginsdb."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM users.ad_user'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} пользователей AD в {output_path}")

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
    """Экспортирует таблицу plugins.monitoring из pluginsdb."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM plugins.monitoring'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} записей мониторинга в {output_path}")

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


def extract_project_sync(postgres_conn_id: str, output_path: str, **context) -> str:
    """Экспортирует таблицу plugins.project_sync из pluginsdb."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM plugins.project_sync'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} записей project_sync в {output_path}")

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
