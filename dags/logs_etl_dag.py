"""
DAG для Logs Analytics ETL pipeline.
Извлекает логи из pluginsdb, трансформирует и загружает в datalake.
"""
from __future__ import annotations

import pendulum
import pandas as pd
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.variable import Variable

# Импорт модульных функций
from etl_pipelines.extract import pluginsdb
from etl_pipelines.transform import logs as transform_logs
from etl_pipelines.load import datalake
from common_tasks import extract_plugins_task


# Переменные Airflow
DATA_ROOT = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "logs"
DATA_ROOT.mkdir(parents=True, exist_ok=True)


@dag(
    dag_id="logs_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="*/18 * * * *",  # Каждые 18 минут
    catchup=False,
    tags=["logs", "etl", "analytics"],
    doc_md="""
    # ETL-пайплайн для Logs Analytics

    ## Процесс:
    - Извлекает логи и плагины из pluginsdb
    - Трансформирует данные с разделением на BIM/designers
    - Загружает результат в datalake (ext_logs_analytics_designers, ext_logs_analytics_bim)
    """
)
def logs_etl():

    @task
    def extract_logs() -> str:
        """Извлекает логи из pluginsdb."""
        output_path = str(DATA_ROOT / "tim_export_log.csv")
        return pluginsdb.extract_logs(
            postgres_conn_id="tim_db_pluginsdb",
            output_path=output_path
        )

    @task
    def transform_logs_data(logs_path: str, plugin_path: str) -> dict:
        """Трансформирует данные логов."""
        df_designers, df_bim = transform_logs.transform_logs_analytics(
            logs_path=logs_path,
            plugin_path=plugin_path
        )

        # Сохраняем DataFrames во временные файлы
        designers_path = str(DATA_ROOT / "logs_designers_transformed.csv")
        bim_path = str(DATA_ROOT / "logs_bim_transformed.csv")

        df_designers.to_csv(designers_path, index=False, encoding='utf-8')
        df_bim.to_csv(bim_path, index=False, encoding='utf-8')

        return {
            "designers_path": designers_path,
            "bim_path": bim_path
        }

    @task
    def load_designers_data(paths: dict) -> int:
        """Загружает данные designers в datalake."""
        df = pd.read_csv(paths["designers_path"])

        return datalake.load_to_postgres(
            df=df,
            postgres_conn_id="tim_db_postgres",
            table_name="ext_logs_analytics_designers",
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
            table_name="ext_logs_analytics_bim",
            schema="datalake",
            if_exists="replace"
        )

    # Определение зависимостей
    logs_csv = extract_logs()
    plugin_csv = extract_plugins_task(output_path=str(DATA_ROOT / "tim_export_plugin.csv"))

    transformed_paths = transform_logs_data(logs_path=logs_csv, plugin_path=plugin_csv)

    load_designers_data(paths=transformed_paths)
    load_bim_data(paths=transformed_paths)


logs_etl()
