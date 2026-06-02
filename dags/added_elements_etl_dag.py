"""
DAG для Added/Modified Elements ETL pipeline.
Инкрементальная загрузка данных о добавленных и модифицированных элементах Revit.

Источники:
- legacy.added_element_legacy (до 2 марта 2026) — идемпотентная полная выгрузка
- revit.added_element (после 2 марта 2026) — инкрементально по дате
- revit.modified_element (после 2 марта 2026) — инкрементально по дате
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

from common.common_tasks import extract_ad_users_task

@dag(
    dag_id="added_elements_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="17 */2 * * *",  # каждые 2 часа в :17
    catchup=False,
    tags=["elements", "etl", "analytics", "incremental"],
    doc_md="""
    # ETL-пайплайн для Added/Modified Elements (Инкрементальный)

    **Анализ добавленных и модифицированных элементов в Revit** с классификацией транзакций.

    ## Особенности:
    - **Инкрементальная загрузка** для новых таблиц: первый запуск — полная загрузка, далее только новые записи
    - **Legacy** загружается идемпотентно (полная выгрузка, кешируется в CSV)
    - **2 таблицы результата**: designers и bim (по полю is_bim)
    - **action_type**: колонка различает 'added' и 'modified' записи

    ## Источники:
    - `legacy.added_element_legacy` (tim_db_revit) — до 2 марта 2026
    - `revit.added_element` (tim_db_revit) — после 2 марта 2026
    - `revit.modified_element` (tim_db_revit) — после 2 марта 2026

    ## Таблицы результата:
    - `datalake.ext_added_elements_designers` — проектировщики
    - `datalake.ext_added_elements_bim` — BIM-специалисты

    ## Инкрементальная стратегия:
    - `added_elements_last_date` — дата последней загрузки из revit.added_element
    - `modified_elements_last_date` — дата последней загрузки из revit.modified_element
    - Extract: `WHERE date > last_date` отдельно для каждой таблицы
    - Load: удаляет записи за загружаемые даты и добавляет новые (защита от дубликатов)

    ## Сброс и перезагрузка:
    1. Удалить Variables `added_elements_last_date` и `modified_elements_last_date` в Airflow UI
    2. Удалить legacy CSV-файл (или установить Variable `FORCE_RELOAD_LEGACY_ADDED=true`)
    3. Удалить таблицы в datalake (важно: новая схема несовместима со старой!)
    4. Запустить DAG

    ## ⚠️ Миграция со старой версии DAG:
    Перед первым запуском новой версии DAG **необходимо**:
    - DROP TABLE datalake.ext_added_elements_designers;
    - DROP TABLE datalake.ext_added_elements_bim;
    Иначе COPY упадёт из-за несовместимости схемы (новые колонки).
    """
)
def added_elements_etl():

    # Connection IDs для node3 (новая инфраструктура после 2 марта 2026)
    NODE3_REVIT_ID = "tim_db_revit"

    data_root = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "added_elements"
    data_root.mkdir(parents=True, exist_ok=True)

    # === Extract tasks ===

    @task
    def extract_legacy_added() -> str:
        """
        Извлекает СТАРЫЕ данные из legacy.added_element_legacy.

        Идемпотентен: если CSV уже выгружен — повторно к БД не обращаемся.
        Принудительная перевыгрузка: Airflow Variable FORCE_RELOAD_LEGACY_ADDED=true.
        """
        output_path = str(data_root / "added_elements_legacy.csv")
        force_reload = Variable.get(
            "FORCE_RELOAD_LEGACY_ADDED", default_var="false"
        ).strip().lower() == "true"
        return pluginsdb.extract_legacy_added_elements(
            postgres_conn_id=NODE3_REVIT_ID,
            output_path=output_path,
            force_reload=force_reload,
        )

    @task
    def extract_added_elements() -> str:
        """
        Извлекает revit.added_element с инкрементальной стратегией по дате.
        Первый запуск — полная выгрузка, далее — только новые записи.
        """
        output_path = str(data_root / "added_elements.csv")
        last_date = Variable.get("added_elements_last_date", default_var=None)

        return pluginsdb.extract_added_incremental(
            postgres_conn_id=NODE3_REVIT_ID,
            output_path=output_path,
            last_date=last_date,
        )

    @task
    def extract_modified_elements() -> str:
        """
        Извлекает revit.modified_element с инкрементальной стратегией по дате.
        Первый запуск — полная выгрузка, далее — только новые записи.
        """
        output_path = str(data_root / "modified_elements.csv")
        last_date = Variable.get("modified_elements_last_date", default_var=None)

        return pluginsdb.extract_modified_incremental(
            postgres_conn_id=NODE3_REVIT_ID,
            output_path=output_path,
            last_date=last_date,
        )

    # === Transform task ===

    @task
    def transform_data(
        ad_path: str,
        legacy_path: str,
        added_path: str,
        modified_path: str,
    ) -> dict:
        """
        Трансформирует данные и разделяет на designers/bim.
        Возвращает пути к CSV и max_date'ы для обновления Variables.
        """
        # Читаем сырые данные для проверки и определения max_dates
        df_legacy_raw = pd.read_csv(legacy_path, encoding='utf-8')
        df_added_raw = pd.read_csv(added_path, encoding='utf-8')
        df_modified_raw = pd.read_csv(modified_path, encoding='utf-8')

        # Если ВСЕ источники пустые — нечего обрабатывать
        if df_legacy_raw.empty and df_added_raw.empty and df_modified_raw.empty:
            print("Все источники пусты — нет данных для обработки")
            return {
                "designers_path": None,
                "bim_path": None,
                "max_added_date": None,
                "max_modified_date": None,
                "is_empty": True,
            }

        # Вычисляем max_date отдельно для added и modified (из сырых данных),
        # чтобы корректно обновить инкрементальные Variables независимо друг от друга.
        max_added = None
        if not df_added_raw.empty and 'date' in df_added_raw.columns:
            dates = pd.to_datetime(df_added_raw['date'], errors='coerce')
            max_added = dates.max()

        max_modified = None
        if not df_modified_raw.empty and 'date' in df_modified_raw.columns:
            dates = pd.to_datetime(df_modified_raw['date'], errors='coerce')
            max_modified = dates.max()

        print(f"Max date added: {max_added}")
        print(f"Max date modified: {max_modified}")

        # Трансформация с подгрузкой предыдущих транзакций из datalake
        # для корректного расчёта time_since_prev_sec на границе инкрементальных порций
        df_transformed = transform_added.transform_added_elements(
            ad_path=ad_path,
            legacy_path=legacy_path,
            added_path=added_path,
            modified_path=modified_path,
            postgres_conn_id="tim_db_postgres",
        )

        if df_transformed.empty:
            print("Transform вернул пустой DataFrame")
            return {
                "designers_path": None,
                "bim_path": None,
                "max_added_date": None,
                "max_modified_date": None,
                "is_empty": True,
            }

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
            "max_added_date": max_added.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(max_added) else None,
            "max_modified_date": max_modified.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(max_modified) else None,
            "is_empty": False,
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
            schema="datalake",
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
            schema="datalake",
        )

    @task
    def update_last_dates(paths: dict, designers_loaded: int, bim_loaded: int) -> None:
        """
        Обновляет Variables с последней загруженной датой.
        Отдельные Variables для added и modified, чтобы корректно отслеживать прогресс
        каждой таблицы независимо.
        """
        if paths.get("is_empty"):
            print("Нет данных, Variables не обновляются")
            return

        if paths.get("max_added_date"):
            Variable.set("added_elements_last_date", paths["max_added_date"])
            print(f"Variable 'added_elements_last_date' обновлена: {paths['max_added_date']}")
        else:
            print("Нет новых added — Variable 'added_elements_last_date' не обновляется")

        if paths.get("max_modified_date"):
            Variable.set("modified_elements_last_date", paths["max_modified_date"])
            print(f"Variable 'modified_elements_last_date' обновлена: {paths['max_modified_date']}")
        else:
            print("Нет новых modified — Variable 'modified_elements_last_date' не обновляется")

        print(f"Загружено: designers={designers_loaded}, bim={bim_loaded}")

    # === Определение зависимостей ===

    # Extract параллельно
    ad_csv = extract_ad_users_task(output_path=str(data_root / "ad_users.csv"))
    legacy_csv = extract_legacy_added()
    added_csv = extract_added_elements()
    modified_csv = extract_modified_elements()

    # Transform ждёт все extract
    transformed_paths = transform_data(
        ad_path=ad_csv,
        legacy_path=legacy_csv,
        added_path=added_csv,
        modified_path=modified_csv,
    )

    # Load параллельно после transform
    designers_count = load_designers(paths=transformed_paths)
    bim_count = load_bim(paths=transformed_paths)

    # Обновление state после успешной загрузки обеих таблиц
    update_last_dates(
        paths=transformed_paths,
        designers_loaded=designers_count,
        bim_loaded=bim_count,
    )


added_elements_etl()
