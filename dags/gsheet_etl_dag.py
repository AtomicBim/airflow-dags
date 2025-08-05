from __future__ import annotations

import pendulum
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.variable import Variable

from coord_sharepoint_etl.extract import gsheet
from coord_sharepoint_etl.transform import gsheet as transform_gs
from coord_sharepoint_etl.load import sharepoint as load_sp # Переиспользуем тот же загрузчик!

DATA_ROOT = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data"))
DATA_ROOT.mkdir(parents=True, exist_ok=True)

# Ключи и пути лучше хранить в Airflow Variables
GSHEET_KEY = Variable.get("gsheet_families_key", "1C3AJ-0uzoIr97ZaVqeOsi-JbqkYKhsirsoSRhOc1Xnw")
GSHEET_WORKSHEET = Variable.get("gsheet_families_worksheet", "DB_Семейства")
GSHEET_KEYFILE_PATH = Variable.get("gsheet_service_account_path", "/opt/airflow/config/revitmaterials-d96ae3c7a1d1.json")

@dag(
    dag_id="gsheet_families_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval="0 */2 * * *",  # Каждые 2 часа
    catchup=False,
    tags=["gsheet", "etl"],
    doc_md="""
    ETL-пайплайн для данных о семействах из Google Sheets.
    Загружает данные в ту же таблицу, что и SharePoint, используя общую модель.
    """
)
def gsheet_families_etl():

    @task
    def extract_data_from_gsheet() -> str:
        output_path = str(DATA_ROOT / "gsheet_export_families.csv")
        return gsheet.extract_families_from_gsheet(
            keyfile_path=GSHEET_KEYFILE_PATH,
            sheet_key=GSHEET_KEY,
            worksheet_name=GSHEET_WORKSHEET,
            output_path=output_path
        )

    @task
    def transform_gsheet_data(raw_data_path: str) -> str:
        output_path = str(DATA_ROOT / "gsheet_transformed.csv")
        return transform_gs.transform_gsheet_data(
            families_path=raw_data_path,
            output_path=output_path
        )
    
    @task
    def load_gsheet_data(transformed_path: str):
        load_sp.load_data_to_postgres(
            transformed_data_path=transformed_path,
            postgres_conn_id="tim_db_postgres"
        )
    
    raw_csv = extract_data_from_gsheet()
    transformed_csv = transform_gsheet_data(raw_data_path=raw_csv)
    load_gsheet_data(transformed_path=transformed_csv)

gsheet_families_etl()