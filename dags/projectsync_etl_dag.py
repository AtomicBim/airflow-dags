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

from etl_pipelines.extract import pluginsdb
from etl_pipelines.transform import projectsync as transform_projectsync
from etl_pipelines.load import datalake

DATA_ROOT = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "projectsync"
DATA_ROOT.mkdir(parents=True, exist_ok=True)

@dag(
    dag_id="projectsync_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="10 * * * *",
    catchup=False,
    tags=["projectsync", "etl", "analytics"]
)
def projectsync_etl():

    # Укажите здесь ваше название подключения к node3
    NODE3_REVIT_ID = "tim_db_revit"
    NODE3_AD_ID = "tim_db_ad"


    @task
    def extract_legacy_sync() -> str:
        """Извлекает старые данные из legacy.project_sync_legacy."""
        output_path = str(DATA_ROOT / "tim_export_project_sync_legacy.csv")
        return pluginsdb.extract_legacy_project_sync(
            postgres_conn_id=NODE3_REVIT_ID,
            output_path=output_path
        )

    @task
    def extract_new_sync() -> str:
        """Извлекает новые данные из revit.project_sync."""
        output_path = str(DATA_ROOT / "tim_export_project_sync_new.csv")
        return pluginsdb.extract_new_project_sync(
            postgres_conn_id=NODE3_REVIT_ID,
            output_path=output_path
        )

    @task
    def extract_ad_users() -> str:
        """Извлекает AD users из новой БД на node3."""
        output_path = str(DATA_ROOT / "tim_export_ad_user.csv")
        return pluginsdb.extract_ad_users(
            postgres_conn_id=NODE3_AD_ID, 
            output_path=output_path
        )

    @task
    def transform_projectsync_data(ad_path: str, legacy_path: str, new_path: str) -> dict:
        """Трансформирует данные project_sync."""
        df_designers, df_bim = transform_projectsync.transform_projectsync_analytics(
            ad_path=ad_path,
            legacy_sync_path=legacy_path,
            new_sync_path=new_path
        )

        designers_path = str(DATA_ROOT / "projectsync_designers_transformed.csv")
        bim_path = str(DATA_ROOT / "projectsync_bim_transformed.csv")

        df_designers.to_csv(designers_path, index=False, encoding='utf-8')
        df_bim.to_csv(bim_path, index=False, encoding='utf-8')

        return {"designers_path": designers_path, "bim_path": bim_path}

    @task
    def load_designers_data(paths: dict) -> int:
        df = pd.read_csv(paths["designers_path"])
        return datalake.load_incremental_to_postgres(
            df=df, postgres_conn_id="tim_db_postgres",
            table_name="ext_project_sync_designers", date_column="date", schema="datalake"
        )

    @task
    def load_bim_data(paths: dict) -> int:
        df = pd.read_csv(paths["bim_path"])
        return datalake.load_incremental_to_postgres(
            df=df, postgres_conn_id="tim_db_postgres",
            table_name="ext_project_sync_bim", date_column="date", schema="datalake"
        )

    # Выполнение
    ad_csv = extract_ad_users()
    legacy_csv = extract_legacy_sync()
    new_csv = extract_new_sync()

    transformed_paths = transform_projectsync_data(
        ad_path=ad_csv, legacy_path=legacy_csv, new_path=new_csv
    )

    load_designers_data(paths=transformed_paths)
    load_bim_data(paths=transformed_paths)

projectsync_etl()