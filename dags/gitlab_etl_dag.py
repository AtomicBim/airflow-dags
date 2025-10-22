"""
DAG для GitLab Analytics ETL pipeline.
Извлекает статистику LOC из GitLab, трансформирует с маппингом плагинов,
обновляет Google Sheets и загружает в datalake.
"""
from __future__ import annotations

import pendulum
import pandas as pd
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

# Импорт модульных функций
from etl_pipelines.extract import pluginsdb, gitlab as extract_gitlab, gsheet
from etl_pipelines.transform import gitlab as transform_gitlab
from etl_pipelines.load import datalake


# Переменные Airflow
DATA_ROOT = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "gitlab"
DATA_ROOT.mkdir(parents=True, exist_ok=True)


@dag(
    dag_id="gitlab_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 5 * * 0",  # Еженедельно по воскресеньям в 5:00 UTC
    catchup=False,
    tags=["gitlab", "etl", "analytics", "loc"],
    doc_md="""
    # ETL-пайплайн для GitLab Analytics

    ## Процесс:
    - Извлекает статистику LOC (Lines of Code) из GitLab проектов
    - Извлекает плагины и маппинг gitlab-plugins из Google Sheets
    - Трансформирует данные с подсчетом новых проектов
    - Обновляет Google Sheets с новыми маппингами gitlab-plugins
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
    def extract_gitlab_mapping() -> str:
        """Извлекает маппинг gitlab-plugins из Google Sheets."""
        # Получаем variable внутри task
        gsheet_config = Variable.get("gsheet_config", deserialize_json=True)

        output_path = str(DATA_ROOT / "gitlab-plugins_mapping.csv")
        return gsheet.extract_gitlab_mapping(
            service_account_path=gsheet_config["service_account_path"],
            spreadsheet_key=gsheet_config["spreadsheet_key"],
            worksheet_name="gitlab-plugins",
            output_path=output_path
        )

    @task
    def transform_gitlab_data(
        gitlab_path: str,
        plugin_path: str,
        mapping_path: str
    ) -> dict:
        """Трансформирует данные GitLab."""
        df_gitlab, df_new_mappings = transform_gitlab.transform_gitlab_analytics(
            gitlab_path=gitlab_path,
            plugin_path=plugin_path,
            mapping_path=mapping_path
        )

        # Сохраняем DataFrames
        gitlab_final_path = str(DATA_ROOT / "gitlab_transformed.csv")
        new_mappings_path = str(DATA_ROOT / "gitlab_new_mappings.csv")

        df_gitlab.to_csv(gitlab_final_path, index=False, encoding='utf-8')
        df_new_mappings.to_csv(new_mappings_path, index=False, encoding='utf-8')

        return {
            "gitlab_path": gitlab_final_path,
            "new_mappings_path": new_mappings_path,
            "has_new_mappings": len(df_new_mappings) > 0
        }

    @task
    def update_gsheet_mappings(paths: dict) -> int:
        """Обновляет Google Sheets с новыми маппингами."""
        if not paths["has_new_mappings"]:
            print("Нет новых GitLab проектов для маппинга")
            return 0

        # Получаем variable внутри task
        gsheet_config = Variable.get("gsheet_config", deserialize_json=True)
        df_new_mappings = pd.read_csv(paths["new_mappings_path"])

        return gsheet.append_new_gitlab_mappings(
            service_account_path=gsheet_config["service_account_path"],
            spreadsheet_key=gsheet_config["spreadsheet_key"],
            worksheet_name="gitlab-plugins",
            new_mappings_df=df_new_mappings
        )

    @task
    def load_gitlab_data(paths: dict) -> int:
        """Загружает данные GitLab в datalake."""
        df = pd.read_csv(paths["gitlab_path"])

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
    mapping_csv = extract_gitlab_mapping()

    transformed_paths = transform_gitlab_data(
        gitlab_path=gitlab_json,
        plugin_path=plugin_csv,
        mapping_path=mapping_csv
    )

    update_gsheet_mappings(paths=transformed_paths)
    load_gitlab_data(paths=transformed_paths)


gitlab_etl()
