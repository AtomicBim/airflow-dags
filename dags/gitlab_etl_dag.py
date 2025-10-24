"""
DAG для GitLab Analytics ETL pipeline.
Извлекает статистику LOC из GitLab, трансформирует данные и загружает в datalake.
"""
from __future__ import annotations

import pendulum
import pandas as pd
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

# Импорт модульных функций
from etl_pipelines.extract import pluginsdb, gitlab as extract_gitlab
from etl_pipelines.transform import gitlab as transform_gitlab
from etl_pipelines.load import datalake


# Переменные Airflow
DATA_ROOT = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "gitlab"
DATA_ROOT.mkdir(parents=True, exist_ok=True)


@dag(
    dag_id="gitlab_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 */4 * * *",  # Каждые 4 часа
    catchup=False,
    tags=["gitlab", "etl", "analytics", "loc"],
    doc_md="""
    # ETL-пайплайн для GitLab Analytics

    ## Процесс:
    - Извлекает статистику LOC (Lines of Code) из GitLab проектов
    - Извлекает плагины из базы данных pluginsdb
    - Трансформирует и объединяет данные
    - Загружает результат в datalake (ext_scripts_gitlab)

    ## Примечание:
    Выполняется еженедельно, так как GitLab extraction ресурсоемкий (клонирование репозиториев)
    """
)
def gitlab_etl():

    @task
    def extract_gitlab_loc() -> str:
        """Извлекает статистику LOC из GitLab проектов."""
        # Получаем connection внутри task
        gitlab_conn = BaseHook.get_connection("gitlab_api")

        output_path = str(DATA_ROOT / "gitlab_export_lines.json")
        return extract_gitlab.extract_gitlab_lines(
            gitlab_url=gitlab_conn.host,
            gitlab_token=gitlab_conn.password,
            output_path=output_path,
            max_workers=8
        )

    @task
    def extract_plugins() -> str:
        """Извлекает плагины из pluginsdb."""
        output_path = str(DATA_ROOT / "tim_export_plugin.csv")
        return pluginsdb.extract_plugins(
            postgres_conn_id="tim_db_pluginsdb",
            output_path=output_path
        )

    @task
    def transform_gitlab_data(
        gitlab_path: str,
        plugin_path: str
    ) -> str:
        """Трансформирует данные GitLab."""
        df_gitlab = transform_gitlab.transform_gitlab_analytics(
            gitlab_path=gitlab_path,
            plugin_path=plugin_path
        )

        # Сохраняем DataFrame
        gitlab_final_path = str(DATA_ROOT / "gitlab_transformed.csv")
        df_gitlab.to_csv(gitlab_final_path, index=False, encoding='utf-8')

        return gitlab_final_path

    @task
    def load_gitlab_data(gitlab_path: str) -> int:
        """Загружает данные GitLab в datalake."""
        df = pd.read_csv(gitlab_path)

        return datalake.load_to_postgres(
            df=df,
            postgres_conn_id="tim_db_postgres",
            table_name="ext_scripts_gitlab",
            schema="datalake",
            if_exists="replace"
        )

    # Определение зависимостей
    gitlab_json = extract_gitlab_loc()
    plugin_csv = extract_plugins()

    transformed_path = transform_gitlab_data(
        gitlab_path=gitlab_json,
        plugin_path=plugin_csv
    )

    load_gitlab_data(gitlab_path=transformed_path)


gitlab_etl()
