from __future__ import annotations

import pendulum
import pandas as pd
import io
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.variable import Variable

# Важно: Убедитесь, что ваш проект 'coord_sharepoint_etl' находится в PYTHONPATH Airflow
from coord_sharepoint_etl.extract import gsheet as extract_gs
from coord_sharepoint_etl.transform import gsheet as transform_gs
from coord_sharepoint_etl.load import sharepoint as load_sp

# --- Переменные Airflow ---
# Ключ Google Sheet'а
GSHEET_KEY = Variable.get("gsheet_families_key", "1C3AJ-0uzoIr97ZaVqeOsi-JbqkYKhsirsoSRhOc1Xnw")
# Имя листа в Google Sheet
GSHEET_WORKSHEET = Variable.get("gsheet_families_worksheet", "DB_Семейства")
# Корневая папка для временных данных. Создаст подпапку 'gsheet'.
DATA_ROOT = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "gsheet"
DATA_ROOT.mkdir(parents=True, exist_ok=True)


@dag(
    dag_id="gsheet_families_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 */2 * * *",  # Каждые 2 часа
    catchup=False,
    tags=["gsheet", "etl", "xcom"],
    doc_md="""
    # ETL-пайплайн для данных о семействах из Google Sheets c XCom
    
    Передает данные между задачами через XCom, но сохраняет резервные CSV на каждом шаге.
    
    ## Требования:
    1. Создайте Airflow Variable gsheet_service_account_json с JSON-кредами сервисного аккаунта.
    2. Установите Airflow Variable ETL_DATA_ROOT_PATH (например, /opt/airflow/data).
    
    ## Процесс:
    - Extract: Извлекает данные из Google Sheets, сохраняет в CSV и передает в XCom.
    - Transform: Получает данные из XCom, трансформирует, сохраняет результат в CSV и передает дальше.
    - Load: Получает финальные данные из XCom и загружает их в базу данных.
    """
)
def gsheet_families_etl_xcom():

    @task
    def extract_data_from_gsheet() -> str:
        """Извлекает данные из Google Sheets, сохраняет в CSV и возвращает CSV-строку для XCom."""
        backup_path = str(DATA_ROOT / "gsheet_extracted_families.csv")
        
        # Эта функция теперь возвращает DataFrame
        df = extract_gs.extract_families_as_dataframe(
            sheet_key=GSHEET_KEY,
            worksheet_name=GSHEET_WORKSHEET
        )
        
        # Сохраняем резервную копию
        df.to_csv(backup_path, index=False, encoding='utf-8')
        print(f"Резервная копия извлеченных данных сохранена: {backup_path}")
        
        # Возвращаем CSV-строку для передачи через XCom
        return df.to_csv(index=False, encoding='utf-8')

    @task
    def transform_gsheet_data(raw_csv_data: str) -> str:
        """Трансформирует данные из XCom, сохраняет результат и возвращает его для XCom."""
        df_families = pd.read_csv(io.StringIO(raw_csv_data))
        backup_path = str(DATA_ROOT / "gsheet_transformed_families.csv")
        
        # Эта функция принимает и возвращает DataFrame
        transformed_df = transform_gs.transform_gsheet_data_df(df_families)
        
        # Сохраняем резервную копию
        transformed_df.to_csv(backup_path, index=False, encoding='utf-8')
        print(f"Резервная копия трансформированных данных сохранена: {backup_path}")

        # Возвращаем CSV-строку для следующего таска
        return transformed_df.to_csv(index=False, encoding='utf-8')
    
    @task
    def load_gsheet_data_to_db(transformed_csv_data: str):
        """Загружает трансформированные данные из XCom в базу данных."""
        df_to_load = pd.read_csv(io.StringIO(transformed_csv_data))
        
        # Используем функцию загрузки, которая принимает DataFrame
        load_sp.load_data_to_postgres(
            df=df_to_load,
            postgres_conn_id="tim_db_postgres" # Убедитесь, что это ваш Connection ID для DWH
        )
    
    # Определение последовательности выполнения задач
    raw_data_xcom = extract_data_from_gsheet()
    transformed_data_xcom = transform_gsheet_data(raw_data_xcom)
    load_gsheet_data_to_db(transformed_data_xcom)

gsheet_families_etl_xcom()