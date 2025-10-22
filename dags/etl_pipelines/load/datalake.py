"""
Load модуль для записи данных в PostgreSQL datalake схему.
"""
import pandas as pd
from sqlalchemy import create_engine, text
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
    Загружает DataFrame в PostgreSQL таблицу.

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
    engine = hook.get_sqlalchemy_engine()

    # Пересоздание структуры таблицы
    if if_exists == "replace":
        print(f"Пересоздание структуры таблицы {schema}.{table_name}...")
        df.head(0).to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="replace",
            index=False
        )
        print(f"Структура таблицы {schema}.{table_name} пересоздана.")

        # Загрузка данных
        print(f"Загрузка данных в {schema}.{table_name}...")
        df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="append",
            index=False
        )
    else:
        df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists=if_exists,
            index=False
        )

    print(f"Загружено {len(df)} строк в {schema}.{table_name}")
    return len(df)


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
    engine = hook.get_sqlalchemy_engine()

    # Получаем уникальные даты из DataFrame
    if date_column in df.columns:
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        unique_dates = df[date_column].dropna().dt.date.unique()

        # Удаляем старые записи за эти даты
        if len(unique_dates) > 0:
            dates_str = ", ".join([f"'{d}'" for d in unique_dates])
            delete_sql = f"""
                DELETE FROM {schema}.{table_name}
                WHERE DATE({date_column}) IN ({dates_str})
            """
            print(f"Удаление старых данных за даты: {dates_str}")
            with engine.begin() as conn:
                conn.execute(text(delete_sql))

    # Загрузка новых данных
    df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists="append",
        index=False
    )

    print(f"Инкрементально загружено {len(df)} строк в {schema}.{table_name}")
    return len(df)
