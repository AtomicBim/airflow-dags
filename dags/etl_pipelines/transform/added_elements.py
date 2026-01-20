"""
Transform модуль для Added Elements pipeline.
Обрабатывает данные о добавленных элементах с классификацией транзакций Revit.
"""
import pandas as pd
import re
from typing import Tuple, Optional

# Импорт из централизованной конфигурации и утилит
from common.config import BIM_USERS
from common.utils import get_object_name, get_project_solution, get_project_stage


# Маски для классификации транзакций Revit
PLUGIN_PATTERN = r'modplus|mpd|mpr|mp:|аск:|ack:|diroots|paramanager|microdesk|dynamo|bim:|pris'

# Порог неактивности для определения новой сессии (в секундах)
# Если разрыв между транзакциями > SESSION_GAP_SECONDS — считаем началом новой сессии
SESSION_GAP_SECONDS = 900  # 15 минут

TRANSACTION_CATEGORIES = {
    # Вывод данных из модели
    "Печать и экспорт": r'печать|экспорт|pdf|dwg|ifc|print|export',
    
    # Удаление элементов
    "Удаление": r'удален|удалить|delete|remove|purge|очист',
    
    # Оформление чертежей и документации
    "Оформление": r'вид|размер|текст|марк|бирка|аннотац|лист|спецификац|легенд|цветов|view|dimension|dim|text|tag|annotation|sheet|schedule|legend|color|elevation|section|разрез|фасад|узел|маркер|выноска|график|фильтр|стиль|штамп|рамка|подложка|граница|область|маскировка|символ|outline|detail|детализац|план',
    
    # Создание и редактирование 3D-элементов модели
    "Построение (моделирование)": r'стена|труба|перекрыти|крыш|лестниц|окно|дверь|армиров|арматур|колонн|балок|потолок|пол|оборудован|воздуховод|лоток|фитинг|мебель|семейств|компонент|create|draw|sketch|place|insert|bend|wall|pipe|duct|floor|roof|stair|window|door|column|beam|reinforcement|rebar|part|topography|эскиз|отделк|проем|отверсти|спринклер|тройник|отвод|переход|кабель|mep|соединение|panel|корпуса|коробка|потребител|траектория|ограждение|топо-поверхность|точка|плоскость|workplane|элемент|построен|размещен|вставка|каркас|торцеобразовател|гидроизоляц|радиатор|решетк|гильз|муфт|цепь|поток|topology|cable|fitting|fixture|вложение|воздухораспределитель|заглушка|балка|витраж|пространств|отрисовка|вытягивание|вырезание|редактиров|изменение типа|copy|копиров|вариант|вариаци|duplicate|дублиров',
    
    # Работа с материалами и текстурами
    "Материалы": r'материал|material|текстур|texture|покрыти|краск|цвет поверхност',
    
    # Управление структурой и настройками модели
    "Управление моделью": r'изменен|модифик|параметр|настройк|связь|уровен|ось|групп|категор|стадия|parameter|setting|link|level|grid|group|category|phase|data storage|synchronize|save|сохранить|переименов|move|rotate|mirror|перенос|поворот|зеркал|split|разделить|соединить|join|cut|изменить|загрузка|обновить|disconnect|отсоединить|смещение|alignment|выравниван|проверк|check|update|modify|process|rule|selection|выделение|property|свойств|статус|status|синхрониз|скрытие|изоляц|сборк|буфер|выбор|рабочая плоскость|тест|согласование|guidstorage'
}

# ============================================================================
# ОПИСАНИЕ КЛАССОВ ТРАНЗАКЦИЙ
# ============================================================================
#
# 1. ПЕЧАТЬ И ЭКСПОРТ
#    Операции вывода данных из модели: печать чертежей, экспорт в форматы
#    PDF, DWG, IFC и другие. Используется для передачи документации.
#
# 2. УДАЛЕНИЕ
#    Удаление элементов из модели: delete, remove, purge (очистка неиспользуемых).
#    Важно для отслеживания потерь данных и оптимизации модели.
#
# 3. ОФОРМЛЕНИЕ
#    Работа с 2D-оформлением: виды, размеры, текст, марки, спецификации,
#    листы, легенды, разрезы, фасады, детали. Подготовка документации.
#
# 4. ПОСТРОЕНИЕ (МОДЕЛИРОВАНИЕ)
#    Создание и редактирование 3D-элементов: стены, перекрытия, крыши,
#    окна, двери, трубы, воздуховоды, арматура, оборудование, MEP-системы.
#    Включает копирование, редактирование, изменение типа, вариации элементов.
#
# 5. МАТЕРИАЛЫ
#    Работа с материалами и текстурами: назначение, редактирование,
#    создание новых материалов. Визуальное оформление модели.
#
# 6. УПРАВЛЕНИЕ МОДЕЛЬЮ
#    Настройки и структура модели: параметры, уровни, оси, группы,
#    категории, стадии, связи, синхронизация, сохранение.
#    Не включает непосредственное редактирование геометрии.
#
# 7. СИСТЕМНЫЕ REVIT (по умолчанию)
#    Все остальные транзакции, не попавшие в категории выше.
#    Внутренние операции Revit, служебные процессы.
# ============================================================================


def extract_short_project_name(name: str) -> str:
    """
    Извлекает короткое название проекта БЕЗ последнего блока по '_'.
    
    Args:
        name: Полное название проекта (например, 'K01_AR_2024_vaskov')
    
    Returns:
        Название без последней части (например, 'K01_AR_2024')
    """
    if not isinstance(name, str) or not name:
        return name
    parts = name.split('_')
    if len(parts) <= 1:
        return name
    return '_'.join(parts[:-1])


def classify_transaction(name: str) -> Tuple[str, bool]:
    """
    Классифицирует транзакцию Revit по категории и определяет плагин.
    
    Args:
        name: Название транзакции
    
    Returns:
        Tuple (category, is_plugin)
    """
    name_lower = str(name).lower()
    is_plugin = bool(re.search(PLUGIN_PATTERN, name_lower))
    
    for category, pattern in TRANSACTION_CATEGORIES.items():
        if re.search(pattern, name_lower):
            return category, is_plugin
    
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
    
    # 3. Короткое название проекта (без последнего блока)
    print("\n3. Создание short_project_name...")
    df['short_project_name'] = df['project_name'].apply(extract_short_project_name)
    
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
    
    # 6. Классификация транзакций
    print("\n6. Классификация транзакций...")
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
