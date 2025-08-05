import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

def load_data_to_postgres(transformed_data_path: str, postgres_conn_id: str, **context) -> None:
    """Загружает данные в staging, а затем обновляет основную таблицу."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_csv(transformed_data_path)
    
    staging_table = "stg_ext_sharepoint_coord"
    target_table = "ext_sharepoint_coord"
    schema = "datalake"

    # Шаг 1: Очистка и загрузка в staging-таблицу
    with engine.begin() as conn:
        print(f"Очистка staging-таблицы {schema}.{staging_table}...")
        conn.execute(f"TRUNCATE TABLE {schema}.{staging_table}")
        print("Загрузка данных в staging-таблицу...")
        df.to_sql(staging_table, conn, schema=schema, if_exists='append', index=False)
        print(f"Загружено {len(df)} строк.")

    # Шаг 2: Merge (Upsert) из staging в основную таблицу
    merge_sql = f"""
        INSERT INTO {schema}.{target_table} ({','.join(df.columns)})
        SELECT {','.join(df.columns)} FROM {schema}.{staging_table}
        ON CONFLICT (guid) DO UPDATE SET
          title = EXCLUDED.title,
          created = EXCLUDED.created,
          closing_date = EXCLUDED.closing_date,
          work_days_duration = EXCLUDED.work_days_duration,
          short_description = EXCLUDED.short_description,
          detailed_description = EXCLUDED.detailed_description,
          program = EXCLUDED.program,
          discipline = EXCLUDED.discipline,
          priority = EXCLUDED.priority,
          author = EXCLUDED.author,
          type_request = EXCLUDED.type_request,
          status = EXCLUDED.status,
          comment = EXCLUDED.comment,
          responsible = EXCLUDED.responsible,
          department = EXCLUDED.department,
          project_section = EXCLUDED.project_section,
          type_request_group = EXCLUDED.type_request_group,
          discipline_group = EXCLUDED.discipline_group;
    """
    
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            print("Обновление основной таблицы...")
            cursor.execute(merge_sql)
            conn.commit()
            print("Основная таблица успешно обновлена.")