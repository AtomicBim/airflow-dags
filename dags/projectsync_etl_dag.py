"""
DAG для ProjectSync ETL pipeline.
Извлекает данные синхронизации проектов, трансформирует и загружает инкрементально в datalake.
"""
from __future__ import annotations

import pendulum
import pandas as pd
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.variable import Variable

# Импорт модульных функций
from etl_pipelines.extract import pluginsdb
from etl_pipelines.transform import projectsync as transform_projectsync
from etl_pipelines.load import datalake

# Коммент проверочный

# Переменные Airflow
DATA_ROOT = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "projectsync"
DATA_ROOT.mkdir(parents=True, exist_ok=True)


@dag(
    dag_id="projectsync_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@hourly",  # Ежечасно
    catchup=False,
    tags=["projectsync", "etl", "analytics"],
    doc_md="""
    # ETL-пайплайн для ProjectSync Analytics

    ## Процесс:
    - Извлекает project_sync и AD users из pluginsdb
    - Трансформирует данные с классификацией проектов и разделением на BIM/designers
    - Загружает результат инкрементально в datalake по дате
      (ext_project_sync_designers, ext_project_sync_bim)
    """
)
def projectsync_etl():

    @task
    def extract_project_sync() -> str:
        """Извлекает project_sync из pluginsdb."""
        output_path = str(DATA_ROOT / "tim_export_project_sync.csv")
        return pluginsdb.extract_project_sync(
            postgres_conn_id="tim_db_pluginsdb",
            output_path=output_path
        )

    @task
    def extract_ad_users() -> str:
        """Извлекает AD users из pluginsdb."""
        output_path = str(DATA_ROOT / "tim_export_ad_user.csv")
        return pluginsdb.extract_ad_users(
            postgres_conn_id="tim_db_pluginsdb",
            output_path=output_path
        )

    @task
    def transform_projectsync_data(ad_path: str, sync_path: str) -> dict:
        """Трансформирует данные project_sync."""
        df_designers, df_bim = transform_projectsync.transform_projectsync_analytics(
            ad_path=ad_path,
            sync_path=sync_path
        )

        # Сохраняем DataFrames во временные файлы
        designers_path = str(DATA_ROOT / "projectsync_designers_transformed.csv")
        bim_path = str(DATA_ROOT / "projectsync_bim_transformed.csv")

        df_designers.to_csv(designers_path, index=False, encoding='utf-8')
        df_bim.to_csv(bim_path, index=False, encoding='utf-8')

        return {
            "designers_path": designers_path,
            "bim_path": bim_path
        }

    @task
    def load_designers_data(paths: dict) -> int:
        """Загружает данные designers инкрементально в datalake."""
        df = pd.read_csv(paths["designers_path"])

        return datalake.load_incremental_to_postgres(
            df=df,
            postgres_conn_id="tim_db_postgres",
            table_name="ext_project_sync_designers",
            date_column="date",
            schema="datalake"
        )

    @task
    def load_bim_data(paths: dict) -> int:
        """Загружает данные BIM инкрементально в datalake."""
        df = pd.read_csv(paths["bim_path"])

        return datalake.load_incremental_to_postgres(
            df=df,
            postgres_conn_id="tim_db_postgres",
            table_name="ext_project_sync_bim",
            date_column="date",
            schema="datalake"
        )

    # Определение зависимостей
    sync_csv = extract_project_sync()
    ad_csv = extract_ad_users()

    transformed_paths = transform_projectsync_data(ad_path=ad_csv, sync_path=sync_csv)

    load_designers_data(paths=transformed_paths)
    load_bim_data(paths=transformed_paths)


projectsync_etl()
