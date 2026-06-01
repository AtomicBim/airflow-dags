"""
Общие Airflow task функции, переиспользуемые в нескольких DAG-ах.
"""
from airflow.decorators import task
from etl_pipelines.extract import pluginsdb


@task
def extract_plugins_task(output_path: str, postgres_conn_id: str = "tim_db_pluginsdb") -> str:
    """
    Извлекает плагины из pluginsdb.

    Общая task-функция, используемая в нескольких DAG-ах (logs_etl_dag, scripts_etl_dag).

    Args:
        output_path: Путь для сохранения CSV файла с данными плагинов
        postgres_conn_id: ID Postgres connection в Airflow (по умолчанию "tim_db_pluginsdb")

    Returns:
        Путь к сохраненному CSV файлу
    """
    return pluginsdb.extract_plugins(
        postgres_conn_id=postgres_conn_id,
        output_path=output_path
    )

@task
def extract_ad_users_task(output_path: str) -> str:
    return pluginsdb.extract_ad_users(
        postgres_conn_id="tim_db_ad", # Подключение задается в одном месте
        output_path=output_path
    )