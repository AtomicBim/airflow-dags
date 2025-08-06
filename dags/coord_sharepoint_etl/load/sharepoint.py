import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

def load_data_to_postgres(df: pd.DataFrame, postgres_conn_id: str, **context):
    """
    Загружает данные в staging, а затем обновляет основную таблицу (UPSERT),
    используя SQLAlchemy engine и connection.
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = hook.get_sqlalchemy_engine()
    
    staging_table = "stg_ext_sharepoint_coord"
    target_table = "ext_sharepoint_coord"
    schema = "datalake"

    # Динамически создаем часть запроса для обновления (SET clause)
    # Исключаем первичный ключ 'guid' из списка обновляемых полей
    update_cols = [col for col in df.columns if col != 'guid']
    set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    # Финальный SQL-запрос для слияния данных (UPSERT)
    all_cols_quoted = ','.join(f'"{col}"' for col in df.columns)
    merge_sql = f"""
        INSERT INTO {schema}.{target_table} ({all_cols_quoted})
        SELECT {all_cols_quoted} FROM {schema}.{staging_table}
        ON CONFLICT (guid) DO UPDATE SET
          {set_clause};
    """

    # Используем engine для создания соединения, которое будет автоматически закрыто
    with engine.connect() as conn:
        # Начинаем транзакцию, которая будет автоматически закоммичена или отменена
        with conn.begin(): 
            # Шаг 1: Очистка staging-таблицы
            print(f"Очистка staging-таблицы {schema}.{staging_table}...")
            conn.execute(text(f"TRUNCATE TABLE {schema}.{staging_table}"))
            
            # Шаг 2: Загрузка данных в staging-таблицу
            print("Загрузка данных в staging-таблицу...")
            
            # ИСПРАВЛЕНИЕ: Передаем conn (Connection), а не engine (Engine)
            df.to_sql(
                staging_table, 
                conn,  # <--- КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ
                schema=schema, 
                if_exists='append', 
                index=False,
                method='multi'  # Для более быстрой вставки
            )
            print(f"Загружено {len(df)} строк в staging.")

            # Шаг 3: Слияние данных из staging в основную таблицу
            print("Обновление основной таблицы...")
            conn.execute(text(merge_sql))
            print("Основная таблица успешно обновлена.")