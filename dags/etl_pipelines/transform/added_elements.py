"""
Transform модуль для Added Elements pipeline.
Обрабатывает данные о добавленных и модифицированных элементах с классификацией транзакций Revit.
Объединяет данные из 3 источников: legacy.added_element_legacy, revit.added_element, revit.modified_element.
"""
import json
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

# Минимальное время на транзакцию (для первых транзакций сессий)
# Компенсирует потерю времени ДО первого действия
MIN_TRANSACTION_SEC = 30  # 30 секунд

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


def count_elements(element_ids) -> int:
    """
    Подсчитывает количество элементов в строке/массиве.

    Поддерживает 2 формата:
    - Legacy (varchar[]): '{14476419,14476420}' — postgres array literal
    - New (jsonb): '["17279361", "17279362"]' — JSON array

    Args:
        element_ids: Строка с ID элементов в одном из форматов выше

    Returns:
        Количество элементов
    """
    if pd.isna(element_ids):
        return 0

    s = str(element_ids).strip()
    if not s or s in ('nan', '[]', '{}'):
        return 0

    # New jsonb формат: пробуем сначала распарсить как JSON массив
    if s.startswith('['):
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return len(parsed)
        except (json.JSONDecodeError, ValueError):
            pass  # Fallback к ручному парсингу ниже

    # Legacy varchar[] формат: '{id1,id2,id3}'
    cleaned = s.strip('{}[]')
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
    legacy_path: str,
    added_path: str,
    modified_path: str,
    postgres_conn_id: Optional[str] = None,
    **context
) -> pd.DataFrame:
    """
    Трансформирует данные о добавленных и модифицированных элементах.

    Объединяет 3 источника:
    - legacy.added_element_legacy (до 2 марта 2026) — action_type='added'
    - revit.added_element (после 2 марта 2026) — action_type='added'
    - revit.modified_element (после 2 марта 2026) — action_type='modified'

    Args:
        ad_path: Путь к CSV с данными AD пользователей
        legacy_path: Путь к CSV со СТАРЫМИ данными (legacy.added_element_legacy)
        added_path: Путь к CSV с НОВЫМИ added данными (revit.added_element)
        modified_path: Путь к CSV с НОВЫМИ modified данными (revit.modified_element)
        postgres_conn_id: ID Airflow connection для подгрузки предыдущих транзакций из datalake.
                         Если указан, интервалы времени рассчитываются с учётом данных из datalake
                         (для корректной работы при инкрементальной загрузке).

    Returns:
        DataFrame с трансформированными данными
    """
    print("=" * 80)
    print("НАЧАЛО ТРАНСФОРМАЦИИ: Added/Modified Elements")
    print("=" * 80)

    # 1. Загрузка данных
    print("\n1. Загрузка данных...")
    df_ad = pd.read_csv(ad_path, encoding='utf-8')
    df_legacy = pd.read_csv(legacy_path, encoding='utf-8')
    df_added = pd.read_csv(added_path, encoding='utf-8')
    df_modified = pd.read_csv(modified_path, encoding='utf-8')

    print(f"   - AD пользователей: {len(df_ad)}")
    print(f"   - Legacy added_element: {len(df_legacy)}")
    print(f"   - New added_element: {len(df_added)}")
    print(f"   - New modified_element: {len(df_modified)}")

    # 2. Подготовка СТАРЫХ данных (legacy):
    #    - rename project_name -> project_title к общему стандарту
    #    - action_type = 'added' (legacy была только для добавленных элементов)
    if not df_legacy.empty:
        df_legacy = df_legacy.rename(columns={"project_name": "project_title"})
        df_legacy["action_type"] = "added"
        # program_name всегда "Revit" — отбрасываем
        df_legacy = df_legacy.drop(columns=["program_name"], errors="ignore")

    # 3. Подготовка НОВЫХ added данных:
    #    - rename cad_program_version -> program_version (общий стандарт)
    #    - action_type = 'added'
    if not df_added.empty:
        df_added = df_added.rename(columns={"cad_program_version": "program_version"})
        df_added["action_type"] = "added"
        df_added = df_added.drop(columns=["cad_program_id"], errors="ignore")

    # 4. Подготовка НОВЫХ modified данных (структура идентична added):
    if not df_modified.empty:
        df_modified = df_modified.rename(columns={"cad_program_version": "program_version"})
        df_modified["action_type"] = "modified"
        df_modified = df_modified.drop(columns=["cad_program_id"], errors="ignore")

    # 5. Объединение всех источников
    df_combined = pd.concat([df_legacy, df_added, df_modified], ignore_index=True)
    print(f"   - Итого записей после concat: {len(df_combined)}")

    if df_combined.empty:
        print("Нет данных для обработки — возвращаем пустой DataFrame")
        return pd.DataFrame()

    # 6. Подтягиваем данные пользователей из AD (имя, отдел, раздел)
    print("\n2. Join с AD users по user_id...")
    df = df_combined.merge(
        df_ad[['id', 'display_name', 'department', 'project_section']],
        left_on='user_id',
        right_on='id',
        how='left'
    ).drop(columns=['id'], errors='ignore')

    df.rename(columns={'display_name': 'user_name'}, inplace=True)

    matched = df['user_name'].notna().sum()
    print(f"   - Совпало: {matched}/{len(df)} ({matched/len(df)*100:.1f}%)")
    if 'department' in df.columns:
        print(f"   - Отделов: {df['department'].nunique()}")

    # 7. Названия проекта: короткое и file_storage (из project_title)
    print("\n3. Создание short_project_name и file_storage_name...")
    df['short_project_name'] = df['project_title'].apply(extract_short_name)
    df['file_storage_name'] = df['project_title'].apply(extract_short_project_name)

    # 8. Определение объекта, раздела и стадии проекта (из project_title)
    print("\n4. Определение объекта, раздела и стадии...")
    df['object_name'] = df['project_title'].apply(get_object_name)
    df['project_solution_name'] = df.apply(
        lambda row: get_project_solution(row['project_title'], row['object_name']), axis=1
    )
    df['project_stage_name'] = df.apply(
        lambda row: get_project_stage(row['project_title'], row['object_name']), axis=1
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
    
    # 9. Расчёт времени между транзакциями (в пределах пользователя)
    # Без группировки по дню - корректно считает ночную работу через полночь
    print("\n9. Расчёт времени между транзакциями...")
    
    # Сортируем по пользователю и времени
    df = df.sort_values(['user_name', 'date'])
    
    # Время до предыдущей транзакции в секундах (в пределах пользователя, БЕЗ группировки по дню)
    df['time_since_prev_sec'] = df.groupby(['user_name'])['date'].diff().dt.total_seconds()
    
    # Подгружаем последние транзакции из datalake для корректного расчёта на границе порций
    if postgres_conn_id:
        min_date = df['date'].min()
        users = df['user_name'].dropna().unique().tolist()
        
        print(f"   - Подгрузка предыдущих транзакций из datalake (до {min_date})...")
        df_prev = _fetch_last_user_transactions(postgres_conn_id, min_date, users)
        
        if df_prev is not None and not df_prev.empty:
            print(f"   - Найдено {len(df_prev)} предыдущих транзакций")
            
            # Находим первые транзакции каждого пользователя в порции (где time_since_prev_sec = NaN)
            first_trans_mask = df['time_since_prev_sec'].isna()
            
            if first_trans_mask.any():
                # Для первых транзакций подставляем интервал от последней транзакции из datalake
                df_first = df[first_trans_mask][['user_name', 'date']].copy()
                df_first = df_first.merge(df_prev, on='user_name', how='left')
                
                # Рассчитываем интервал от предыдущей транзакции из datalake
                df_first['corrected_interval'] = (
                    df_first['date'] - df_first['prev_date']
                ).dt.total_seconds()
                
                # Обновляем интервалы в основном DataFrame (только если <= SESSION_GAP_SECONDS)
                corrected_count = 0
                for idx, row in df_first.iterrows():
                    interval = row.get('corrected_interval')
                    if pd.notna(interval) and interval <= SESSION_GAP_SECONDS:
                        df.loc[idx, 'time_since_prev_sec'] = interval
                        corrected_count += 1
                
                print(f"   - Скорректировано интервалов: {corrected_count}")
    
    # Первая транзакция пользователя (без предыдущей в datalake) = NaN, заполняем 0
    df['time_since_prev_sec'] = df['time_since_prev_sec'].fillna(0)
    
    # Флаг "начало сессии" если разрыв > SESSION_GAP_SECONDS или первая транзакция
    df['is_session_start'] = (df['time_since_prev_sec'] > SESSION_GAP_SECONDS) | (df['time_since_prev_sec'] == 0)
    
    # Статистика по интервалам ДО обнуления пауз
    active_intervals = df[(df['time_since_prev_sec'] > 0) & (df['time_since_prev_sec'] <= SESSION_GAP_SECONDS)]['time_since_prev_sec']
    if len(active_intervals) > 0:
        print(f"   - Средний активный интервал: {active_intervals.mean():.1f} сек")
        print(f"   - Медианный активный интервал: {active_intervals.median():.1f} сек")
    print(f"   - Сессий (разрыв > {SESSION_GAP_SECONDS} сек): {df['is_session_start'].sum()}")
    
    # Обнуляем большие интервалы (паузы > SESSION_GAP_SECONDS = не работа)
    # Это позволяет в DataLens просто суммировать time_since_prev_sec для получения времени работы
    pause_count = (df['time_since_prev_sec'] > SESSION_GAP_SECONDS).sum()
    df.loc[df['time_since_prev_sec'] > SESSION_GAP_SECONDS, 'time_since_prev_sec'] = 0
    print(f"   - Обнулено пауз (> {SESSION_GAP_SECONDS} сек): {pause_count}")
    
    # Устанавливаем минимальное время для первых транзакций сессий
    # Компенсирует потерю времени ДО первого действия в сессии
    session_start_count = df['is_session_start'].sum()
    df.loc[df['is_session_start'], 'time_since_prev_sec'] = MIN_TRANSACTION_SEC
    print(f"   - Установлено мин. время ({MIN_TRANSACTION_SEC} сек) для {session_start_count} начал сессий")
    
    # 10. Формируем итоговый DataFrame
    # Базовые колонки (есть и в legacy, и в new) + новые из revit-таблиц.
    # Колонки из revit, которых нет в legacy, будут NaN для legacy-строк.
    result_columns = [
        'date',
        'action_type',           # NEW: added/modified
        'user_name',
        'department',
        'project_section',
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
        'is_session_start',
        # NEW поля из revit.added_element / revit.modified_element (NaN для legacy)
        'project_path',
        'group_model',
        'description',
        'builtin_category',
        'family_name',
        'floor',
        'element_type_name',
        'trace_id',
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
