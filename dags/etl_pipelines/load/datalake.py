"""
Load модуль для записи данных в PostgreSQL datalake схему.
"""
import pandas as pd
from io import StringIO
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_to_postgres(
    df: pd.DataFrame,
    postgres_conn_id: str,
    table_name: str,
    schema: str = "datalake",
    if_exists: str = "replace",
    **context
) -> int:
    """
    Загружает DataFrame в PostgreSQL таблицу используя нативные методы Airflow.

    Args:
        df: DataFrame для загрузки
        postgres_conn_id: ID Airflow connection для PostgreSQL
        table_name: Название таблицы
        schema: Название схемы (по умолчанию "datalake")
        if_exists: Стратегия загрузки ('fail', 'replace', 'append')

    Returns:
        Количество загруженных строк
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    full_table_name = f'"{schema}"."{table_name}"'
    
    print(f"Загрузка данных в {schema}.{table_name} (if_exists='{if_exists}')...")
    
    # Используем одно соединение для всех операций
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Создаем схему
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        conn.commit()
        
        # Обрабатываем стратегию if_exists
        if if_exists == "replace":
            cursor.execute(f'DROP TABLE IF EXISTS {full_table_name}')
            conn.commit()  # Коммитим удаление
            print(f"Таблица {full_table_name} удалена")
        elif if_exists == "fail":
            # Проверяем существование таблицы
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{schema}' 
                    AND table_name = '{table_name}'
                )
            """)
            exists = cursor.fetchone()[0]
            if exists:
                raise ValueError(f"Таблица {full_table_name} уже существует")
        
        # Создаем таблицу
        create_table_sql = _generate_create_table_sql(df, table_name, schema)
        cursor.execute(create_table_sql)
        conn.commit()  # Коммитим создание таблицы
        print(f"Таблица {full_table_name} создана")
        
        # Используем COPY для быстрой загрузки данных (проверенный метод из SharePoint ETL)
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)  # Обычный CSV формат
        buffer.seek(0)
        
        # Загружаем данные через COPY EXPERT
        columns_list = ', '.join([f'"{col}"' for col in df.columns])
        copy_sql = f"COPY {full_table_name} ({columns_list}) FROM STDIN WITH (FORMAT CSV)"
        print(f"Загрузка {len(df)} строк в {full_table_name}...")
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()
        
        print(f"Успешно загружено {len(df)} строк в {schema}.{table_name}")
        return len(df)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def _generate_create_table_sql(df: pd.DataFrame, table_name: str, schema: str) -> str:
    """Генерирует SQL для создания таблицы на основе типов DataFrame."""
    
    # Маппинг типов pandas -> PostgreSQL
    type_mapping = {
        'int64': 'BIGINT',
        'int32': 'INTEGER',
        'int16': 'SMALLINT',
        'float64': 'DOUBLE PRECISION',
        'float32': 'REAL',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'datetime64[ns, UTC]': 'TIMESTAMP WITH TIME ZONE',
    }
    
    columns_sql = []
    for col_name, dtype in df.dtypes.items():
        pg_type = type_mapping.get(str(dtype), 'TEXT')
        columns_sql.append(f'"{col_name}" {pg_type}')
    
    columns_str = ',\n    '.join(columns_sql)
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
        {columns_str}
    )
    """
    
    return sql


def load_incremental_to_postgres(
    df: pd.DataFrame,
    postgres_conn_id: str,
    table_name: str,
    date_column: str,
    schema: str = "datalake",
    **context
) -> int:
    """
    Загружает DataFrame в PostgreSQL с инкрементальной стратегией.
    Удаляет данные за текущую дату и загружает новые.

    Args:
        df: DataFrame для загрузки
        postgres_conn_id: ID Airflow connection для PostgreSQL
        table_name: Название таблицы
        date_column: Название колонки с датой для инкрементальной загрузки
        schema: Название схемы (по умолчанию "datalake")

    Returns:
        Количество загруженных строк
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    full_table_name = f'"{schema}"."{table_name}"'
    
    # Используем одно соединение для всех операций
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Создаем схему
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        conn.commit()
        
        # Создаем таблицу если её нет
        create_table_sql = _generate_create_table_sql(df, table_name, schema)
        cursor.execute(create_table_sql)
        conn.commit()
        
        # Получаем уникальные даты из DataFrame
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
            unique_dates = df[date_column].dropna().dt.date.unique()

            # Удаляем старые записи за эти даты
            if len(unique_dates) > 0:
                dates_str = ", ".join([f"'{d}'" for d in unique_dates])
                delete_sql = f"""
                    DELETE FROM {full_table_name}
                    WHERE DATE("{date_column}") IN ({dates_str})
                """
                print(f"Удаление старых данных за даты: {dates_str}")
                cursor.execute(delete_sql)
                conn.commit()  # Коммитим удаление
        
        # Используем COPY для быстрой загрузки данных (проверенный метод из SharePoint ETL)
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)  # Обычный CSV формат
        buffer.seek(0)
        
        # Загружаем данные через COPY EXPERT
        columns_list = ', '.join([f'"{col}"' for col in df.columns])
        copy_sql = f"COPY {full_table_name} ({columns_list}) FROM STDIN WITH (FORMAT CSV)"
        print(f"Загрузка {len(df)} строк в {full_table_name}...")
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()
        
        print(f"Инкрементально загружено {len(df)} строк в {schema}.{table_name}")
        return len(df)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()
