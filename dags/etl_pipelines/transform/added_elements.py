"""
Transform модуль для Added Elements pipeline.
Обрабатывает данные о добавленных элементах с классификацией транзакций Revit.
"""
import pandas as pd
import re
import os
from typing import Tuple, Optional, Dict
from functools import lru_cache

# Импорт из централизованной конфигурации и утилит
from common.config import BIM_USERS
from common.utils import (
    get_object_name, 
    get_project_solution, 
    get_project_stage, 
    extract_short_name,
    extract_short_project_name
)


# Порог неактивности для определения новой сессии (в секундах)
# Если разрыв между транзакциями > SESSION_GAP_SECONDS — считаем началом новой сессии
SESSION_GAP_SECONDS = 900  # 15 минут

# Путь к CSV файлу с маппингом транзакций (относительно корня проекта)
MAPPING_CSV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    "mappings", "transactions.csv"
)

# ============================================================================
# ПАТТЕРНЫ ДЛЯ КЛАССИФИКАЦИИ
# ============================================================================

# Паттерн для определения плагинов
PLUGIN_PATTERN = r'^аск:|^ack:|microdesk|mpr[A-Z]|modplus|квартирография'

# Fallback-паттерны для классификации НОВЫХ транзакций (не найденных в CSV)
# Порядок важен: проверяются сверху вниз
FALLBACK_PATTERNS = {
    "Удаление": r'удал|delete|remove|purge',
    
    "Печать и экспорт": r'печать|экспорт|export|pdf|dwg|ifc',
    
    "Материалы": r'материал',
    
    "Оформление": r'вид|размер|марк|лист|спецификац|легенд|разрез|фасад|'
                  r'план|текст|аннотац|нумерац|фрагмент|фильтр|шаблон|tag|dim',
    
    "Управление моделью": r'параметр|связь|семейств|группа|загруз|сохран|'
                         r'рабоч|уровень|штрихов|стил|настро|выгруз|блокир|'
                         r'workset|setting|parameter',
    
    "Построение (моделирование)": r'стена|труба|перекрыт|крыш|дверь|окно|'
                                   r'колонн|балк|потолок|армир|арматур|'
                                   r'воздуховод|фитинг|проем|отверст|лестниц|'
                                   r'ограждение|компонент|эскиз|копир|вставк|'
                                   r'соедин|создать|редактир|wall|pipe|duct|'
                                   r'floor|roof|door|window|create|bend',
}

# ============================================================================
# ОПИСАНИЕ КЛАССОВ ТРАНЗАКЦИЙ
# ============================================================================
#
# 1. ПЕЧАТЬ И ЭКСПОРТ - вывод данных: печать, экспорт PDF/DWG/IFC
# 2. УДАЛЕНИЕ - удаление элементов из модели
# 3. ОФОРМЛЕНИЕ - 2D: виды, размеры, марки, спецификации, листы
# 4. ПОСТРОЕНИЕ (МОДЕЛИРОВАНИЕ) - 3D: стены, трубы, арматура, MEP
# 5. МАТЕРИАЛЫ - работа с материалами и текстурами
# 6. УПРАВЛЕНИЕ МОДЕЛЬЮ - параметры, связи, группы, синхронизация
# 7. СИСТЕМНЫЕ REVIT - внутренние операции (по умолчанию)
# ============================================================================


@lru_cache(maxsize=1)
def _load_transaction_mapping() -> Dict[str, Tuple[str, bool]]:
    """
    Загружает маппинг транзакций из CSV файла.
    Кэширует результат для повторного использования.
    
    Returns:
        Словарь {название_транзакции: (класс, is_plugin)}
    """
    mapping = {}
    
    # Пробуем разные пути к файлу
    possible_paths = [
        MAPPING_CSV_PATH,
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "mappings", "transactions.csv"),
        "/opt/airflow/mappings/transactions.csv",  # для Docker
    ]
    
    csv_path = None
    for path in possible_paths:
        if os.path.exists(path):
            csv_path = path
            break
    
    if csv_path is None:
        print(f"⚠️ ВНИМАНИЕ: CSV файл маппинга не найден. Используются только fallback-паттерны.")
        return mapping
    
    try:
        # Пробуем разные кодировки
        for encoding in ['utf-8', 'utf-8-sig', 'cp1251']:
            try:
                df = pd.read_csv(csv_path, sep=';', encoding=encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            print(f"⚠️ Не удалось прочитать CSV с известными кодировками")
            return mapping
        
        # Проверяем наличие нужных колонок
        required_cols = ['transaction_name', 'class', 'is_plugin']
        if not all(col in df.columns for col in required_cols):
            print(f"⚠️ CSV файл не содержит нужных колонок: {required_cols}")
            return mapping
        
        # Заполняем словарь
        for _, row in df.iterrows():
            name = str(row['transaction_name']).strip()
            if not name or name == 'nan':
                continue
            
            trans_class = str(row['class']).strip()
            is_plugin_str = str(row['is_plugin']).strip().upper()
            is_plugin = is_plugin_str in ('ИСТИНА', 'TRUE', '1', 'ДА', 'YES')
            
            mapping[name] = (trans_class, is_plugin)
        
        print(f"✓ Загружено {len(mapping)} транзакций из CSV")
        
    except Exception as e:
        print(f"⚠️ Ошибка загрузки CSV маппинга: {e}")
    
    return mapping


def classify_transaction(name: str) -> Tuple[str, bool]:
    """
    Гибридная классификация транзакции Revit.
    
    Логика:
    1. Точное совпадение в CSV маппинге (приоритет)
    2. Fallback на regex-паттерны для новых транзакций
    3. По умолчанию: "Системные Revit"
    
    Args:
        name: Название транзакции
    
    Returns:
        Tuple (category, is_plugin)
    """
    name_str = str(name).strip()
    
    # 1. Загружаем маппинг из CSV (кэшируется)
    mapping = _load_transaction_mapping()
    
    # 2. Проверяем точное совпадение
    if name_str in mapping:
        return mapping[name_str]
    
    # 3. Определяем, является ли это плагином
    name_lower = name_str.lower()
    is_plugin = bool(re.search(PLUGIN_PATTERN, name_lower))
    
    # 4. Fallback на паттерны для новых транзакций
    for category, pattern in FALLBACK_PATTERNS.items():
        if re.search(pattern, name_lower):
            return category, is_plugin
    
    # 5. По умолчанию - системные
    return "Системные Revit", is_plugin


def count_elements(element_ids: str) -> int:
    """
    Подсчитывает количество элементов в строке формата {id1,id2,id3}.
    
    Args:
        element_ids: Строка с ID элементов (например, '{14476419,14476420}')
    
    Returns:
        Количество элементов
    """
    if pd.isna(element_ids) or not element_ids:
        return 0
    
    # Убираем фигурные скобки и считаем элементы
    cleaned = str(element_ids).strip('{}')
    if not cleaned:
        return 0
    
    elements = cleaned.split(',')
    return len([e for e in elements if e.strip()])


def _fetch_last_user_transactions(
    postgres_conn_id: str,
    min_date: pd.Timestamp,
    users: list
) -> Optional[pd.DataFrame]:
    """
    Подгружает последнюю транзакцию каждого пользователя из datalake.
    Используется для корректного расчёта time_since_prev_sec на границе инкрементальных порций.
    
    Args:
        postgres_conn_id: ID Airflow connection для PostgreSQL
        min_date: Минимальная дата из новой порции данных
        users: Список пользователей из новой порции
    
    Returns:
        DataFrame с последними транзакциями или None если ошибка/пусто
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    try:
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = hook.get_conn()
        
        # Форматируем дату для SQL
        min_date_str = min_date.strftime('%Y-%m-%d %H:%M:%S')
        
        # Форматируем список пользователей для SQL IN clause
        users_escaped = [u.replace("'", "''") for u in users if u]
        users_str = "', '".join(users_escaped)
        
        # Подгружаем последнюю транзакцию каждого пользователя ДО min_date
        # Объединяем обе таблицы (designers и bim)
        sql = f"""
            WITH all_data AS (
                SELECT "date", "user_name"
                FROM datalake.ext_added_elements_designers
                WHERE "date" < '{min_date_str}'
                  AND "user_name" IN ('{users_str}')
                UNION ALL
                SELECT "date", "user_name"
                FROM datalake.ext_added_elements_bim
                WHERE "date" < '{min_date_str}'
                  AND "user_name" IN ('{users_str}')
            ),
            ranked AS (
                SELECT "date", "user_name",
                       ROW_NUMBER() OVER (PARTITION BY "user_name" ORDER BY "date" DESC) as rn
                FROM all_data
            )
            SELECT "date" as prev_date, "user_name"
            FROM ranked 
            WHERE rn = 1
        """
        
        df_prev = pd.read_sql(sql, conn)
        conn.close()
        
        if df_prev.empty:
            return None
        
        df_prev['prev_date'] = pd.to_datetime(df_prev['prev_date'])
        return df_prev
        
    except Exception as e:
        print(f"   - Не удалось подгрузить предыдущие транзакции: {e}")
        return None


def transform_added_elements(
    ad_path: str,
    added_path: str,
    postgres_conn_id: Optional[str] = None,
    **context
) -> pd.DataFrame:
    """
    Трансформирует данные о добавленных элементах.
    
    Args:
        ad_path: Путь к CSV с данными AD пользователей
        added_path: Путь к CSV с данными added_element
        postgres_conn_id: ID Airflow connection для подгрузки предыдущих транзакций из datalake.
                         Если указан, интервалы времени рассчитываются с учётом данных из datalake
                         (для корректной работы при инкрементальной загрузке).
    
    Returns:
        DataFrame с трансформированными данными
    """
    print("=" * 80)
    print("НАЧАЛО ТРАНСФОРМАЦИИ: Added Elements")
    print("=" * 80)
    
    # 1. Загрузка данных
    print("\n1. Загрузка данных...")
    df_ad = pd.read_csv(ad_path, encoding='utf-8')
    df_added = pd.read_csv(added_path, encoding='utf-8')
    
    print(f"   - AD пользователей: {len(df_ad)}")
    print(f"   - Записей added_element: {len(df_added)}")
    
    # 2. Подтягиваем имена пользователей из AD
    print("\n2. Join с AD users по user_id...")
    df = df_added.merge(
        df_ad[['id', 'display_name']],
        left_on='user_id',
        right_on='id',
        how='left'
    ).drop(columns=['id'])
    
    df.rename(columns={'display_name': 'user_name'}, inplace=True)
    
    matched = df['user_name'].notna().sum()
    print(f"   - Совпало: {matched}/{len(df)} ({matched/len(df)*100:.1f}%)")
    
    # 3. Названия проекта: короткое и file_storage
    print("\n3. Создание short_project_name и file_storage_name...")
    df['short_project_name'] = df['project_name'].apply(extract_short_name)  # K01_AR (первые 2 части)
    df['file_storage_name'] = df['project_name'].apply(extract_short_project_name)  # K01_AR_2024 (без последней части)
    
    # 4. Определение объекта, раздела и стадии проекта
    print("\n4. Определение объекта, раздела и стадии...")
    df['object_name'] = df['project_name'].apply(get_object_name)
    df['project_solution_name'] = df.apply(
        lambda row: get_project_solution(row['project_name'], row['object_name']), axis=1
    )
    df['project_stage_name'] = df.apply(
        lambda row: get_project_stage(row['project_name'], row['object_name']), axis=1
    )
    
    print(f"   - Объекты: {df['object_name'].value_counts().to_dict()}")
    print(f"   - Разделы: {df['project_solution_name'].value_counts().head(5).to_dict()}")
    print(f"   - Стадии: {df['project_stage_name'].value_counts().to_dict()}")
    
    # 5. Обработка даты
    print("\n5. Обработка даты...")
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    print(f"   - Диапазон дат: {df['date'].min()} - {df['date'].max()}")
    
    # 6. Классификация транзакций (гибридный подход)
    print("\n6. Классификация транзакций (CSV + fallback)...")
    classifications = df['transaction_name'].apply(classify_transaction)
    df['class'] = classifications.apply(lambda x: x[0])
    df['is_plugin'] = classifications.apply(lambda x: x[1])
    
    print(f"   - Категории: {df['class'].value_counts().to_dict()}")
    print(f"   - Плагины: {df['is_plugin'].sum()} записей")
    
    # 7. Подсчёт количества элементов
    print("\n7. Подсчёт элементов...")
    df['elements_count'] = df['element_ids'].apply(count_elements)
    
    print(f"   - Всего элементов: {df['elements_count'].sum()}")
    print(f"   - Среднее на запись: {df['elements_count'].mean():.2f}")
    
    # 8. Классификация BIM/designers
    print("\n8. Классификация пользователей...")
    df['is_bim'] = df['user_name'].isin(BIM_USERS)
    
    # 9. Расчёт времени между транзакциями (в пределах пользователя и дня)
    print("\n9. Расчёт времени между транзакциями...")
    
    # Сортируем по пользователю и времени
    df = df.sort_values(['user_name', 'date'])
    
    # Добавляем колонку с датой (без времени) для группировки по дню
    df['date_only'] = df['date'].dt.date
    
    # Время до предыдущей транзакции в секундах (в пределах пользователя и дня)
    df['time_since_prev_sec'] = df.groupby(['user_name', 'date_only'])['date'].diff().dt.total_seconds()
    
    # Подгружаем последние транзакции из datalake для корректного расчёта на границе порций
    if postgres_conn_id:
        min_date = df['date'].min()
        users = df['user_name'].dropna().unique().tolist()
        
        print(f"   - Подгрузка предыдущих транзакций из datalake (до {min_date})...")
        df_prev = _fetch_last_user_transactions(postgres_conn_id, min_date, users)
        
        if df_prev is not None and not df_prev.empty:
            print(f"   - Найдено {len(df_prev)} предыдущих транзакций")
            
            # Находим первые транзакции каждого пользователя за каждый день (где time_since_prev_sec = NaN)
            first_trans_mask = df['time_since_prev_sec'].isna()
            
            if first_trans_mask.any():
                # Для первых транзакций дня подставляем интервал от последней транзакции из datalake
                df_first = df[first_trans_mask][['user_name', 'date', 'date_only']].copy()
                df_first = df_first.merge(df_prev, on='user_name', how='left')
                
                # Рассчитываем интервал от предыдущей транзакции из datalake
                # Только если prev_date в тот же день
                df_first['prev_date_only'] = df_first['prev_date'].dt.date
                same_day_mask = df_first['date_only'] == df_first['prev_date_only']
                
                df_first.loc[same_day_mask, 'corrected_interval'] = (
                    df_first.loc[same_day_mask, 'date'] - df_first.loc[same_day_mask, 'prev_date']
                ).dt.total_seconds()
                
                # Обновляем интервалы в основном DataFrame
                corrected_count = 0
                for idx, row in df_first.iterrows():
                    if pd.notna(row.get('corrected_interval')):
                        df.loc[idx, 'time_since_prev_sec'] = row['corrected_interval']
                        corrected_count += 1
                
                print(f"   - Скорректировано интервалов: {corrected_count}")
    
    # Первая транзакция дня (без предыдущей в datalake) = NaN, заполняем 0
    df['time_since_prev_sec'] = df['time_since_prev_sec'].fillna(0)
    
    # Флаг "начало сессии" если разрыв > SESSION_GAP_SECONDS или первая транзакция дня
    df['is_session_start'] = (df['time_since_prev_sec'] > SESSION_GAP_SECONDS) | (df['time_since_prev_sec'] == 0)
    
    # Статистика по интервалам (исключая нулевые - первые транзакции дня)
    active_intervals = df[df['time_since_prev_sec'] > 0]['time_since_prev_sec']
    if len(active_intervals) > 0:
        print(f"   - Средний интервал: {active_intervals.mean():.1f} сек")
        print(f"   - Медианный интервал: {active_intervals.median():.1f} сек")
    print(f"   - Сессий (разрыв > {SESSION_GAP_SECONDS} сек): {df['is_session_start'].sum()}")
    
    # Убираем вспомогательную колонку
    df.drop(columns=['date_only'], inplace=True)
    
    # 10. Формируем итоговый DataFrame
    result_columns = [
        'date',
        'user_name',
        'short_project_name',
        'file_storage_name',
        'object_name',
        'project_solution_name',
        'project_stage_name',
        'program_version',
        'transaction_name',
        'class',
        'is_plugin',
        'elements_count',
        'is_bim',
        'time_since_prev_sec',
        'is_session_start'
    ]
    
    # Берём только существующие колонки
    result_columns = [col for col in result_columns if col in df.columns]
    df_result = df[result_columns].copy()
    
    print("\n" + "=" * 80)
    print("ТРАНСФОРМАЦИЯ ЗАВЕРШЕНА")
    print(f"   - Записей: {len(df_result)}")
    print(f"   - Уникальных пользователей: {df_result['user_name'].nunique()}")
    print(f"   - Уникальных дней: {df_result['date'].dt.date.nunique()}")
    print("=" * 80)
    
    return df_result
