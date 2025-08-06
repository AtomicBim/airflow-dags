import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text # Важный импорт

def load_data_to_postgres(transformed_data_path: str, postgres_conn_id: str, **context) -> None:
    """
    Загружает данные в staging, а затем обновляет основную таблицу,
    используя единый SQLAlchemy engine.
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = hook.get_sqlalchemy_engine() # Получаем "движок" SQLAlchemy
    df = pd.read_csv(transformed_data_path)
    
    staging_table = "stg_ext_sharepoint_coord"
    target_table = "ext_sharepoint_coord"
    schema = "datalake"

    # Динамически создаем часть запроса для обновления
    # Это делает код более гибким, если в будущем изменятся колонки
    update_cols = [col for col in df.columns if col != 'guid']
    set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    # Финальный SQL-запрос для слияния данных (UPSERT)
    merge_sql = f"""
        INSERT INTO {schema}.{target_table} ({','.join(f'"{col}"' for col in df.columns)})
        SELECT {','.join(f'"{col}"' for col in df.columns)} FROM {schema}.{staging_table}
        ON CONFLICT (guid) DO UPDATE SET
          {set_clause};
    """

    # Используем одно соединение для всех операций
    with engine.connect() as conn:
        # Используем транзакцию: либо все выполнится, либо ничего
        with conn.begin():
            # Шаг 1: Очистка staging-таблицы
            print(f"Очистка staging-таблицы {schema}.{staging_table}...")
            conn.execute(text(f"TRUNCATE TABLE {schema}.{staging_table}"))
            
            # Шаг 2: Загрузка данных в staging-таблицу
            print("Загрузка данных в staging-таблицу...")
            # Теперь to_sql получает правильный объект 'conn'
            df.to_sql(staging_table, conn, schema=schema, if_exists='append', index=False)
            print(f"Загружено {len(df)} строк в staging.")

            # Шаг 3: Слияние данных из staging в основную таблицу
            print("Обновление основной таблицы...")
            # Выполняем наш SQL-запрос
            conn.execute(text(merge_sql))
            print("Основная таблица успешно обновлена.")