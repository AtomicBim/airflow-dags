import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import io

def load_data_to_postgres(df: pd.DataFrame, postgres_conn_id: str, **context):
    """
    Загружает данные в staging через COPY FROM, а затем обновляет основную таблицу (UPSERT).
    Этот метод более надежен и быстр, чем to_sql.
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    # Используем get_conn() для получения прямого psycopg2 соединения,
    # которое гарантированно имеет метод .cursor()
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    staging_table = "stg_ext_sharepoint_coord"
    target_table = "ext_sharepoint_coord"
    schema = "datalake"
    full_staging_table_name = f"{schema}.{staging_table}"

    # Динамически создаем часть запроса для обновления (SET clause)
    update_cols = [col for col in df.columns if col != 'guid']
    set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    # Финальный SQL-запрос для слияния данных (UPSERT)
    all_cols_quoted = ','.join(f'"{col}"' for col in df.columns)
    merge_sql = f"""
        INSERT INTO {schema}.{target_table} ({all_cols_quoted})
        SELECT {all_cols_quoted} FROM {full_staging_table_name}
        ON CONFLICT (guid) DO UPDATE SET
          {set_clause};
    """

    try:
        # Шаг 1: Очистка staging-таблицы
        print(f"Очистка staging-таблицы {full_staging_table_name}...")
        cursor.execute(f"TRUNCATE TABLE {full_staging_table_name}")
        print("Staging-таблица очищена.")

        # Шаг 2: Загрузка данных в staging-таблицу с помощью COPY
        print("Подготовка данных для загрузки через COPY...")
        # Создаем в памяти CSV файл без заголовка
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0) # Перемещаем курсор в начало файла
        
        print(f"Загрузка {len(df)} строк в {full_staging_table_name}...")
        # Используем copy_expert для указания формата CSV и списка колонок
        cursor.copy_expert(
            sql=f"COPY {full_staging_table_name} ({all_cols_quoted}) FROM STDIN WITH (FORMAT CSV)",
            file=buffer
        )
        print("Данные успешно загружены в staging-таблицу.")

        # Шаг 3: Слияние данных из staging в основную таблицу
        print("Обновление основной таблицы...")
        cursor.execute(merge_sql)
        print("Основная таблица успешно обновлена.")

        # Фиксируем транзакцию
        conn.commit()
        print("Транзакция успешно зафиксирована.")

    except Exception as e:
        print(f"Произошла ошибка: {e}")
        # Откатываем транзакцию в случае ошибки
        conn.rollback()
        raise
    finally:
        # Закрываем курсор и соединение, чтобы освободить ресурсы
        cursor.close()
        conn.close()
        print("Соединение с базой данных закрыто.")