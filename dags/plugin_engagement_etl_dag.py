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

# Весовые коэффициенты для расчета метрики
# w1 - вес для количества уникальных плагинов
# w2 - вес для количества запусков
# Сумма должна быть равна 1.0
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
    
    **Оценка использования плагинов проектировщиками** на основе комплексной метрики.
    
    ## Цель:
    Объективная оценка вовлеченности проектировщиков (не BIM-пользователей) 
    в использование плагинов для выявления сотрудников, нуждающихся в 
    дополнительном обучении или мотивации.
    
    ## Методология:
    
    ### 1. Данные
    - **AD Users**: справочник пользователей
    - **Monitoring**: логи запуска плагинов
    
    ### 2. Фильтрация
    Исключаются BIM-пользователи (из config.BIM_USERS)
    
    ### 3. Метрики
    Для каждого проектировщика:
    - **Unique Plugins**: количество уникальных плагинов
    - **Total Launches**: общее количество запусков
    
    ### 4. Нормализация (Min-Max)
    Приведение метрик к диапазону [0, 1]:
    ```
    normalized = (value - min) / (max - min)
    ```
    
    ### 5. Plugin Engagement Score
    Взвешенное среднее нормализованных метрик:
    ```
    Score = w1 * UniquePlugins_norm + w2 * TotalLaunches_norm
    ```
    
    По умолчанию: w1 = 0.5, w2 = 0.5 (сбалансированная оценка)
    
    ## Результат:
    Таблица `datalake.ext_plugin_engagement` с колонками:
    - `day`: Дата (конец дня)
    - `user_name`: ФИО проектировщика
    - `email`: Email пользователя (из AD)
    - `company`: Компания (из AD)
    - `department`: Отдел (из AD)
    - `project_section`: Раздел проекта (из AD)
    - `unique_plugins`: Кумулятивное количество уникальных плагинов
    - `total_launches`: Кумулятивное количество запусков
    - `unique_plugins_norm`: Нормализованное значение (0-1) в рамках дня
    - `total_launches_norm`: Нормализованное значение (0-1) в рамках дня
    - `plugin_engagement_score`: Итоговая оценка (0-1) в рамках дня
    
    ## Особенности:
    - **Кумулятивный расчет**: для каждого дня учитываются ВСЕ данные до конца этого дня
    - **Динамика**: отслеживание изменения вовлеченности во времени
    - **Исторические данные**: все доступные дни из источника
    
    ## Интерпретация:
    - **Высокая оценка (0.7-1.0)**: активное использование плагинов
    - **Средняя оценка (0.4-0.7)**: умеренное использование
    - **Низкая оценка (0.0-0.4)**: требуется внимание
    
    ## Запуск:
    Одновременно с `scripts_etl_dag` (@hourly)
    """
)
def plugin_engagement_etl():
    
    # Инициализация путей внутри DAG
    data_root = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "plugin_engagement"
    data_root.mkdir(parents=True, exist_ok=True)
    
    # === Extract tasks (параллельно) ===
    
    @task
    def extract_ad_users() -> str:
        """Извлекает AD users из pluginsdb."""
        output_path = str(data_root / "ad_users.csv")
        return pluginsdb.extract_ad_users(
            postgres_conn_id="tim_db_pluginsdb",
            output_path=output_path
        )
    
    @task
    def extract_monitoring() -> str:
        """Извлекает данные мониторинга плагинов из pluginsdb."""
        output_path = str(data_root / "monitoring.csv")
        return pluginsdb.extract_monitoring(
            postgres_conn_id="tim_db_pluginsdb",
            output_path=output_path
        )
    
    # === Transform task (ждет все extract) ===
    
    @task
    def transform_plugin_engagement_data(
        ad_path: str,
        monitoring_path: str
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
    ad_csv = extract_ad_users()
    monitoring_csv = extract_monitoring()
    
    # Transform ждет завершения extract
    engagement_csv = transform_plugin_engagement_data(
        ad_path=ad_csv,
        monitoring_path=monitoring_csv
    )
    
    # Load загружает результат в datalake
    load_plugin_engagement_data(engagement_path=engagement_csv)


plugin_engagement_etl()

