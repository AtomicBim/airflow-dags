"""
DAG для расчета Plugin Engagement Score проектировщиков.
Оценивает использование плагинов сотрудниками (не BIM-пользователями).
"""
from __future__ import annotations

import pendulum
import pandas as pd
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.variable import Variable

# Импорт модульных функций
from etl_pipelines.extract import pluginsdb
from etl_pipelines.transform import plugin_engagement as transform_engagement
from etl_pipelines.load import datalake

# Импорт конфигурации
from common.config import BIM_USERS
from common.common_tasks import extract_ad_users_task

# Весовые коэффициенты для расчета метрики Plugin Engagement Score.
# w1 — вес для количества уникальных плагинов в окне (разнообразие инструментария).
# w2 — вес для количества запусков в окне (интенсивность использования).
# Сумма должна быть равна 1.0.
#
# Текущий баланс смещён в сторону "разнообразия", чтобы один фоновый плагин
# с auto-trigger не доминировал в score через total_launches.
WEIGHT_UNIQUE_PLUGINS = 0.7
WEIGHT_TOTAL_LAUNCHES = 0.3


@dag(
    dag_id="plugin_engagement_etl_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="7 * * * *", # каждый час в :07
    catchup=False,
    tags=["plugins", "etl", "analytics", "engagement"],
    doc_md="""
    # ETL-пайплайн для Plugin Engagement Score

    **Оценка использования плагинов проектировщиками** на основе rolling-метрики
    за последние 30 дней.

    ## Цель:
    Объективная оценка ТЕКУЩЕЙ вовлечённости проектировщиков (не BIM-пользователей)
    в использование плагинов для выявления сотрудников, нуждающихся в
    дополнительном обучении или мотивации.

    ## Методология:

    ### 1. Данные
    - **AD Users**: справочник пользователей (tim_db_ad)
    - **Monitoring (new)**: новые логи запуска плагинов из plugins.monitoring (после 2 марта 2026)
    - **Monitoring (legacy)**: исторические логи из legacy.monitoring_legacy (до 2 марта 2026)

    ### 2. Фильтрация
    Исключаются BIM-пользователи (из config.BIM_USERS).

    ### 3. Метрики (в окне 30 дней)
    Для каждого проектировщика на каждый день истории:
    - **Unique Plugins**: количество уникальных плагинов в окне `(day - 30d, day]`
    - **Total Launches**: общее количество запусков в окне `(day - 30d, day]`

    Пользователь, у которого нет активности за последние 30 дней, **в результат не попадает**.

    ### 4. Нормализация (Min-Max)
    Приведение метрик к диапазону [0, 1] **в рамках одного дня**:
    ```
    normalized = (value - min) / (max - min)
    ```
    Если у всех в этот день одинаковые значения (`min == max`) — `NULL`,
    метрика в этом случае неопределена.

    ### 5. Plugin Engagement Score
    Взвешенное среднее нормализованных метрик:
    ```
    Score = w1 * UniquePlugins_norm + w2 * TotalLaunches_norm
    ```
    **Текущие веса: `w1 = 0.7, w2 = 0.3`** (см. константы `WEIGHT_*` в начале файла).
    Смещены в сторону разнообразия инструментария, чтобы один фоновый плагин
    с auto-trigger не доминировал через `total_launches`.

    ## Результат:
    Таблица `datalake.ext_plugin_engagement` с колонками:
    - `day`: Дата
    - `user_name`: ФИО проектировщика
    - `email`, `company`, `department`, `project_section`: Доп. поля из AD
    - `unique_plugins`: Количество уникальных плагинов в окне `(day - 30d, day]`
    - `total_launches`: Количество запусков в окне `(day - 30d, day]`
    - `unique_plugins_norm`: Нормализованное (0-1) в рамках дня; `NULL` для вырожденных групп
    - `total_launches_norm`: Нормализованное (0-1) в рамках дня; `NULL` для вырожденных групп
    - `plugin_engagement_score`: w1*plugins_norm + w2*launches_norm; `NULL` если одна из норм `NULL`

    ## Семантика NULL
    `NULL` в `*_norm` / `plugin_engagement_score` означает **неопределённую** метрику
    (все пользователи в этот день имели идентичные сырые значения — нечего нормировать).
    На дашбордах такие записи стоит либо фильтровать, либо показывать отдельно
    (НЕ интерпретировать как 0 — это другое).

    ## Интерпретация:
    - **Высокая оценка (0.7-1.0)**: активное использование плагинов
    - **Средняя оценка (0.4-0.7)**: умеренное использование
    - **Низкая оценка (0.0-0.4)**: требуется внимание
    - **NULL**: метрика неопределена для этого дня (вырожденная группа)

    ## Запуск:
    Каждый час в `:07`. Полная перезаливка таблицы (`if_exists="replace"`).
    """
)
def plugin_engagement_etl():

    # Connection IDs для node3 (новая инфраструктура после 2 марта 2026)
    NODE3_REVIT_ID = "tim_db_revit"

    # Инициализация путей внутри DAG
    data_root = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "plugin_engagement"
    data_root.mkdir(parents=True, exist_ok=True)

    # === Extract tasks (параллельно) ===

    @task
    def extract_monitoring() -> str:
        """Извлекает НОВЫЙ мониторинг из plugins.monitoring (после 2 марта 2026)."""
        output_path = str(data_root / "monitoring.csv")
        return pluginsdb.extract_monitoring(
            postgres_conn_id=NODE3_REVIT_ID,
            output_path=output_path
        )

    @task
    def extract_legacy_monitoring() -> str:
        """
        Извлекает СТАРЫЙ мониторинг из legacy.monitoring_legacy (до 2 марта 2026).

        Идемпотентен: если CSV уже выгружен — повторно к БД не обращаемся.
        Принудительная перевыгрузка: Airflow Variable FORCE_RELOAD_LEGACY_MONITORING=true.
        """
        output_path = str(data_root / "monitoring_legacy.csv")
        force_reload = Variable.get(
            "FORCE_RELOAD_LEGACY_MONITORING", default_var="false"
        ).strip().lower() == "true"
        return pluginsdb.extract_legacy_monitoring(
            postgres_conn_id=NODE3_REVIT_ID,
            output_path=output_path,
            force_reload=force_reload,
        )

    # === Transform task (ждет все extract) ===

    @task
    def transform_plugin_engagement_data(
        ad_path: str,
        monitoring_path: str,
        legacy_monitoring_path: str
    ) -> str:
        """
        Вычисляет Plugin Engagement Score для проектировщиков с разбивкой по дням.
        
        Применяет методологию:
        1. Фильтрация не-BIM пользователей
        2. Извлечение всех доступных дат из мониторинга
        3. ДЛЯ КАЖДОГО ДНЯ:
           - Кумулятивная агрегация (данные до конца дня)
           - Нормализация Min-Max (в рамках дня)
           - Расчет взвешенного среднего
        
        Результат: временные ряды с метрикой на конец каждого дня.
        """
        df_engagement = transform_engagement.transform_plugin_engagement(
            ad_path=ad_path,
            monitoring_path=monitoring_path,
            legacy_monitoring_path=legacy_monitoring_path,
            bim_users=BIM_USERS,
            w1=WEIGHT_UNIQUE_PLUGINS,
            w2=WEIGHT_TOTAL_LAUNCHES
        )
        
        # Сохраняем результат во временный файл
        output_path = str(data_root / "plugin_engagement_transformed.csv")
        df_engagement.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"\nТрансформация завершена. Результат сохранен: {output_path}")
        print(f"Всего проектировщиков: {len(df_engagement)}")
        
        return output_path
    
    # === Load task ===
    
    @task
    def load_plugin_engagement_data(engagement_path: str) -> int:
        """
        Загружает результаты расчета Plugin Engagement Score в datalake.
        
        Таблица: datalake.ext_plugin_engagement
        Стратегия: replace (полная замена данных)
        """
        df = pd.read_csv(engagement_path, encoding='utf-8')
        
        rows_loaded = datalake.load_to_postgres(
            df=df,
            postgres_conn_id="tim_db_postgres",
            table_name="ext_plugin_engagement",
            schema="datalake",
            if_exists="replace"
        )
        
        print("\n" + "=" * 80)
        print("ЗАГРУЗКА В DATALAKE ЗАВЕРШЕНА")
        print("=" * 80)
        print(f"Таблица: datalake.ext_plugin_engagement")
        print(f"Загружено строк: {rows_loaded}")
        print(f"Уникальных дней: {df['day'].nunique()}")
        print(f"Уникальных проектировщиков: {df['user_name'].nunique()}")
        print(f"Диапазон дат: {df['day'].min()} - {df['day'].max()}")
        print("=" * 80)
        
        return rows_loaded
    
    # === Определение зависимостей ===
    
    # Extract задачи запускаются параллельно
    ad_csv = extract_ad_users_task(output_path=str(data_root / "ad_users.csv"))
    monitoring_csv = extract_monitoring()
    legacy_monitoring_csv = extract_legacy_monitoring()

    # Transform ждет завершения extract
    engagement_csv = transform_plugin_engagement_data(
        ad_path=ad_csv,
        monitoring_path=monitoring_csv,
        legacy_monitoring_path=legacy_monitoring_csv
    )
    
    # Load загружает результат в datalake
    load_plugin_engagement_data(engagement_path=engagement_csv)


plugin_engagement_etl()

