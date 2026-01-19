"""
DAG для Added Elements ETL pipeline.
Инкрементальная загрузка данных о добавленных элементах Revit.
"""
from __future__ import annotations

import pendulum
import pandas as pd
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.variable import Variable

from etl_pipelines.extract import pluginsdb
from etl_pipelines.transform import added_elements as transform_added
from etl_pipelines.load import datalake


@dag(
    dag_id="added_elements_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="17 */2 * * *",  # каждые 2 часа в :17
    catchup=False,
    tags=["elements", "etl", "analytics", "incremental"],
    doc_md="""
    # ETL-пайплайн для Added Elements (Инкрементальный)
    
    **Анализ добавленных элементов в Revit** с классификацией транзакций.
    
    ## Особенности:
    - **Инкрементальная загрузка**: первый запуск - полная загрузка, далее только новые записи
    - **2 таблицы**: designers и bim (по полю is_bim)
    - **Эффективность**: миллионы записей не перезагружаются каждый раз
    
    ## Источник:
    - `elements.added_element` из pluginsdb
    
    ## Таблицы результата:
    - `datalake.ext_added_elements_designers` - проектировщики
    - `datalake.ext_added_elements_bim` - BIM-специалисты
    
    ## Инкрементальная стратегия:
    - Airflow Variable `added_elements_last_date` хранит дату последней загрузки
    - Extract: `WHERE date > last_date`
    - Load: удаляет записи за загружаемые даты и добавляет новые (защита от дубликатов)
    
    ## Сброс и перезагрузка:
    1. Удалить Variable `added_elements_last_date` в Airflow UI
    2. Удалить таблицы в datalake
    3. Запустить DAG
    """
)
def added_elements_etl():
    
    data_root = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "added_elements"
    data_root.mkdir(parents=True, exist_ok=True)
    
    # === Extract tasks ===
    
    @task
    def extract_ad_users() -> str:
        """Извлекает AD users из pluginsdb."""
        output_path = str(data_root / "ad_users.csv")
        return pluginsdb.extract_ad_users(
            postgres_conn_id="tim_db_pluginsdb",
            output_path=output_path
        )
    
    @task
    def extract_added_elements() -> str:
        """
        Извлекает added_element с инкрементальной стратегией по дате.
        Первый запуск - полная выгрузка, далее - только новые записи.
        """
        output_path = str(data_root / "added_elements.csv")
        
        # Получаем last_date из Variable (None если первый запуск)
        last_date = Variable.get("added_elements_last_date", default_var=None)
        
        return pluginsdb.extract_added_incremental(
            postgres_conn_id="tim_db_pluginsdb",
            output_path=output_path,
            last_date=last_date
        )
    
    # === Transform task ===
    
    @task
    def transform_data(ad_path: str, added_path: str) -> dict:
        """
        Трансформирует данные и разделяет на designers/bim.
        Возвращает пути к CSV и max_date для обновления Variable.
        """
        # Читаем исходные данные для проверки
        df_added_raw = pd.read_csv(added_path, encoding='utf-8')
        
        if df_added_raw.empty:
            print("Нет новых данных для обработки")
            return {
                "designers_path": None,
                "bim_path": None,
                "max_date": None,
                "is_empty": True
            }
        
        # Трансформация
        df_transformed = transform_added.transform_added_elements(
            ad_path=ad_path,
            added_path=added_path
        )
        
        # Получаем max_date из трансформированных данных
        max_date = df_transformed['date'].max()
        max_date_str = max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else None
        print(f"Max date в выгрузке: {max_date_str}")
        
        # Разделение на designers и bim
        df_designers = df_transformed[df_transformed['is_bim'] == False].copy()
        df_bim = df_transformed[df_transformed['is_bim'] == True].copy()
        
        # Удаляем колонку is_bim (уже не нужна после разделения)
        df_designers.drop(columns=['is_bim'], inplace=True, errors='ignore')
        df_bim.drop(columns=['is_bim'], inplace=True, errors='ignore')
        
        # Сохраняем
        designers_path = str(data_root / "added_elements_designers.csv")
        bim_path = str(data_root / "added_elements_bim.csv")
        
        df_designers.to_csv(designers_path, index=False, encoding='utf-8')
        df_bim.to_csv(bim_path, index=False, encoding='utf-8')
        
        print(f"Designers: {len(df_designers)} записей")
        print(f"BIM: {len(df_bim)} записей")
        
        return {
            "designers_path": designers_path,
            "bim_path": bim_path,
            "max_date": max_date_str,
            "is_empty": False
        }
    
    # === Load tasks ===
    
    @task
    def load_designers(paths: dict) -> int:
        """Загружает данные designers в datalake (инкрементально)."""
        if paths.get("is_empty") or paths.get("designers_path") is None:
            print("Нет данных designers для загрузки")
            return 0
        
        df = pd.read_csv(paths["designers_path"], encoding='utf-8')
        
        if df.empty:
            print("DataFrame designers пустой")
            return 0
        
        return datalake.load_incremental_to_postgres(
            df=df,
            postgres_conn_id="tim_db_postgres",
            table_name="ext_added_elements_designers",
            date_column="date",
            schema="datalake"
        )
    
    @task
    def load_bim(paths: dict) -> int:
        """Загружает данные BIM в datalake (инкрементально)."""
        if paths.get("is_empty") or paths.get("bim_path") is None:
            print("Нет данных BIM для загрузки")
            return 0
        
        df = pd.read_csv(paths["bim_path"], encoding='utf-8')
        
        if df.empty:
            print("DataFrame BIM пустой")
            return 0
        
        return datalake.load_incremental_to_postgres(
            df=df,
            postgres_conn_id="tim_db_postgres",
            table_name="ext_added_elements_bim",
            date_column="date",
            schema="datalake"
        )
    
    @task
    def update_last_date(paths: dict, designers_loaded: int, bim_loaded: int) -> None:
        """Обновляет Variable с последней загруженной датой."""
        if paths.get("is_empty") or not paths.get("max_date"):
            print("Нет данных, Variable не обновляется")
            return
        
        max_date = paths["max_date"]
        Variable.set("added_elements_last_date", max_date)
        print(f"Variable 'added_elements_last_date' обновлена: {max_date}")
        print(f"Загружено: designers={designers_loaded}, bim={bim_loaded}")
    
    # === Определение зависимостей ===
    
    # Extract параллельно
    ad_csv = extract_ad_users()
    added_csv = extract_added_elements()
    
    # Transform ждёт оба extract
    transformed_paths = transform_data(
        ad_path=ad_csv,
        added_path=added_csv
    )
    
    # Load параллельно после transform
    designers_count = load_designers(paths=transformed_paths)
    bim_count = load_bim(paths=transformed_paths)
    
    # Обновление state после успешной загрузки обеих таблиц
    update_last_date(
        paths=transformed_paths,
        designers_loaded=designers_count,
        bim_loaded=bim_count
    )


added_elements_etl()
