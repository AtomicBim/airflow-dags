import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

def load_data_to_postgres(transformed_data_path: str, postgres_conn_id: str, **context) -> None:
    """
    Загружает данные в staging, а затем обновляет основную таблицу,
    используя SQLAlchemy engine.
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_csv(transformed_data_path)
    
    staging_table = "stg_ext_sharepoint_coord"
    target_table = "ext_sharepoint_coord"
    schema = "datalake"

    # Динамически создаем часть запроса для обновления
    update_cols = [col for col in df.columns if col != 'guid']
    set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    # Финальный SQL-запрос для слияния данных (UPSERT)
    merge_sql = f"""
        INSERT INTO {schema}.{target_table} ({','.join(f'"{col}"' for col in df.columns)})
        SELECT {','.join(f'"{col}"' for col in df.columns)} FROM {schema}.{staging_table}
        ON CONFLICT (guid) DO UPDATE SET
          {set_clause};
    """

    # Выполняем операции последовательно
    # Шаг 1: Очистка staging-таблицы
    print(f"Очистка staging-таблицы {schema}.{staging_table}...")
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{staging_table}"))
        conn.commit()
    
    # Шаг 2: Загрузка данных в staging-таблицу
    # pandas.to_sql требует engine, не connection
    print("Загрузка данных в staging-таблицу...")
    df.to_sql(
        staging_table, 
        engine,  # Используем engine напрямую
        schema=schema, 
        if_exists='append', 
        index=False,
        method='multi',  # Для более быстрой вставки
        chunksize=1000  # Загружаем порциями для больших данных
    )
    print(f"Загружено {len(df)} строк в staging.")

    # Шаг 3: Слияние данных из staging в основную таблицу
    print("Обновление основной таблицы...")
    with engine.connect() as conn:
        conn.execute(text(merge_sql))
        conn.commit()
    print("Основная таблица успешно обновлена.")