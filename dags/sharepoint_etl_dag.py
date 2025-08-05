from __future__ import annotations

import pendulum
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook

# Импорт наших модульных функций
from coord_sharepoint_etl.extract import sharepoint as extract_sp, tim_db
from coord_sharepoint_etl.transform import sharepoint as transform_sp
from coord_sharepoint_etl.load import sharepoint as load_sp

# Получаем путь для временных данных из переменных Airflow
# Убедитесь, что переменная ETL_DATA_ROOT_PATH задана в UI Airflow (/opt/airflow/data)
DATA_ROOT = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data"))
DATA_ROOT.mkdir(parents=True, exist_ok=True)

@dag(
    dag_id="sharepoint_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="*/15 * * * *",  # Каждые 15 минут
    catchup=False,
    tags=["sharepoint", "etl"],
    doc_md="""
    ETL-пайплайн для извлечения, обработки и загрузки данных из SharePoint и связанной БД.
    - Извлекает Задачи и Пользователей из SharePoint
    - Извлекает данные AD из БД pluginsdb
    - Трансформирует и объединяет данные
    - Загружает результат в DWH (datalake.ext_sharepoint_coord) через staging-таблицу
    """
)
def sharepoint_etl():
    
    # Получение данных для подключения из Airflow Connections
    sharepoint_conn = BaseHook.get_connection("askit_http_sharepoint_tim")
    
    @task
    def extract_sharepoint_tasks() -> str:
        output_path = str(DATA_ROOT / "sharepoint_export_tasks.csv")
        return extract_sp.extract_tasks(
            url=sharepoint_conn.host,
            username=sharepoint_conn.login,
            password=sharepoint_conn.get_password(),
            output_path=output_path
        )

    @task
    def extract_sharepoint_users() -> str:
        output_path = str(DATA_ROOT / "sharepoint_export_users.csv")
        return extract_sp.extract_users(
            url=sharepoint_conn.host,
            username=sharepoint_conn.login,
            password=sharepoint_conn.get_password(),
            output_path=output_path
        )

    @task
    def extract_ad_data() -> str:
        output_path = str(DATA_ROOT / "tim_export_ad_user.csv")
        # Используем Connection ID tim_db_pluginsdb из вашего скриншота
        return tim_db.extract_ad_users(postgres_conn_id="tim_db_pluginsdb", output_path=output_path)

    @task
    def transform_data(tasks_path: str, users_path: str, ad_path: str) -> str:
        output_path = str(DATA_ROOT / "sharepoint_transformed.csv")
        return transform_sp.transform_sharepoint_data(
            tasks_path=tasks_path,
            users_path=users_path,
            ad_path=ad_path,
            output_path=output_path
        )

    @task
    def load_data(transformed_path: str):
        # Используем Connection ID tim_db_postgres (DWH) из вашего скриншота
        load_sp.load_data_to_postgres(
            transformed_data_path=transformed_path,
            postgres_conn_id="tim_db_postgres"
        )
    
    # Определение зависимостей
    tasks_csv = extract_sharepoint_tasks()
    users_csv = extract_sharepoint_users()
    ad_csv = extract_ad_data()
    
    transformed_csv = transform_data(tasks_path=tasks_csv, users_path=users_csv, ad_path=ad_csv)
    
    load_data(transformed_path=transformed_csv)

sharepoint_etl()