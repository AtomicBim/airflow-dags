"""
DAG для Scripts Analytics ETL pipeline.
Комплексный pipeline: объединяет данные из monitoring, plugins, gitlab.
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
from etl_pipelines.transform import scripts as transform_scripts
from etl_pipelines.load import datalake


# Переменные Airflow
DATA_ROOT = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "scripts"
DATA_ROOT.mkdir(parents=True, exist_ok=True)


@dag(
    dag_id="scripts_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    tags=["scripts", "etl", "analytics", "main"],
    doc_md="""
    # ETL-пайплайн для Scripts Analytics

    **Основной комплексный pipeline**, объединяющий данные из множества источников.

    ## Процесс:
    - Извлекает monitoring, plugins, development_stage, AD users из pluginsdb
    - Извлекает статистику LOC из GitLab
    - Трансформирует и объединяет все данные
    - Загружает результат в datalake:
      - ext_scripts_analytics_designers
      - ext_scripts_analytics_bim
      - ext_scripts_plugin

    ## Зависимости:
    Все extract задачи выполняются параллельно, transform ждет завершения всех экстракций.
    """
)
def scripts_etl():

    # === Extract tasks (параллельно) ===

    @task
    def extract_ad_users() -> str:
        """Извлекает AD users из pluginsdb."""
        output_path = str(DATA_ROOT / "tim_export_ad_user.csv")
        return pluginsdb.extract_ad_users(
            postgres_conn_id="tim_db_pluginsdb",
            output_path=output_path
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
    def extract_monitoring() -> str:
        """Извлекает мониторинг из pluginsdb."""
        output_path = str(DATA_ROOT / "tim_export_monitoring.csv")
        return pluginsdb.extract_monitoring(
            postgres_conn_id="tim_db_pluginsdb",
            output_path=output_path
        )

    @task
    def extract_development_stage() -> str:
        """Извлекает стадии разработки из pluginsdb."""
        output_path = str(DATA_ROOT / "tim_export_plugin_development_stage.csv")
        return pluginsdb.extract_development_stage(
            postgres_conn_id="tim_db_pluginsdb",
            output_path=output_path
        )

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

    # === Transform task (ждет все extract) ===

    @task
    def transform_scripts_data(
        ad_path: str,
        plugin_path: str,
        monitoring_path: str,
        plugin_development_stage_path: str,
        gitlab_path: str
    ) -> dict:
        """Трансформирует все данные для scripts analytics."""
        df_designers, df_bim, df_plugin = transform_scripts.transform_scripts_analytics(
            ad_path=ad_path,
            plugin_path=plugin_path,
            monitoring_path=monitoring_path,
            plugin_development_stage_path=plugin_development_stage_path,
            gitlab_path=gitlab_path
        )

        # Сохраняем DataFrames во временные файлы
        designers_path = str(DATA_ROOT / "scripts_designers_transformed.csv")
        bim_path = str(DATA_ROOT / "scripts_bim_transformed.csv")
        plugin_path = str(DATA_ROOT / "scripts_plugin_transformed.csv")

        df_designers.to_csv(designers_path, index=False, encoding='utf-8')
        df_bim.to_csv(bim_path, index=False, encoding='utf-8')
        df_plugin.to_csv(plugin_path, index=False, encoding='utf-8')

        return {
            "designers_path": designers_path,
            "bim_path": bim_path,
            "plugin_path": plugin_path
        }

    # === Load tasks (параллельно после transform) ===

    @task
    def load_designers_data(paths: dict) -> int:
        """Загружает данные designers в datalake."""
        df = pd.read_csv(paths["designers_path"])

        return datalake.load_to_postgres(
            df=df,
            postgres_conn_id="tim_db_postgres",
            table_name="ext_scripts_analytics_designers",
            schema="datalake",
            if_exists="replace"
        )

    @task
    def load_bim_data(paths: dict) -> int:
        """Загружает данные BIM в datalake."""
        df = pd.read_csv(paths["bim_path"])

        return datalake.load_to_postgres(
            df=df,
            postgres_conn_id="tim_db_postgres",
            table_name="ext_scripts_analytics_bim",
            schema="datalake",
            if_exists="replace"
        )

    @task
    def load_plugin_data(paths: dict) -> int:
        """Загружает данные plugins в datalake."""
        df = pd.read_csv(paths["plugin_path"])

        return datalake.load_to_postgres(
            df=df,
            postgres_conn_id="tim_db_postgres",
            table_name="ext_scripts_plugin",
            schema="datalake",
            if_exists="replace"
        )

    # === Определение зависимостей ===

    # Все extract задачи запускаются параллельно
    ad_csv = extract_ad_users()
    plugin_csv = extract_plugins()
    monitoring_csv = extract_monitoring()
    dev_stage_csv = extract_development_stage()
    gitlab_json = extract_gitlab_loc()

    # Transform ждет все extract
    transformed_paths = transform_scripts_data(
        ad_path=ad_csv,
        plugin_path=plugin_csv,
        monitoring_path=monitoring_csv,
        plugin_development_stage_path=dev_stage_csv,
        gitlab_path=gitlab_json
    )

    # Load задачи запускаются параллельно после transform
    load_designers_data(paths=transformed_paths)
    load_bim_data(paths=transformed_paths)
    load_plugin_data(paths=transformed_paths)


scripts_etl()
