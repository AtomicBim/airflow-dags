import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path

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