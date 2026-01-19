"""
Transform модуль для Added Elements pipeline.
Обрабатывает данные о добавленных элементах с классификацией транзакций Revit.
"""
import pandas as pd
import re
from typing import Tuple

# Импорт из централизованной конфигурации
from common.config import BIM_USERS


# Маски для классификации транзакций Revit
PLUGIN_PATTERN = r'modplus|mpd|mpr|mp:|аск:|ack:|diroots|paramanager|microdesk|dynamo|bim:|pris'

TRANSACTION_CATEGORIES = {
    "Печать и экспорт": r'печать|экспорт|pdf|dwg|ifc|print|export',
    "Удаление": r'удален|удалить|delete|remove|purge|очист',
    "Оформление": r'вид|размер|текст|марк|бирка|аннотац|лист|спецификац|легенд|цветов|view|dimension|dim|text|tag|annotation|sheet|schedule|legend|color|elevation|section|разрез|фасад|узел|маркер|выноска|график|фильтр|стиль|штамп|рамка|подложка|граница|область|маскировка|символ|outline|detail|детализац|план',
    "Построение (моделирование)": r'стена|труба|перекрыти|крыш|лестниц|окно|дверь|армиров|арматур|колонн|балок|потолок|пол|оборудован|воздуховод|лоток|фитинг|мебель|семейств|компонент|create|draw|sketch|place|insert|bend|wall|pipe|duct|floor|roof|stair|window|door|column|beam|reinforcement|rebar|part|topography|эскиз|отделк|проем|отверсти|спринклер|тройник|отвод|переход|кабель|mep|соединение|panel|корпуса|коробка|потребител|траектория|ограждение|топо-поверхность|точка|плоскость|workplane|элемент|построен|размещен|вставка|каркас|торцеобразовател|гидроизоляц|радиатор|решетк|гильз|муфт|цепь|поток|topology|cable|fitting|fixture|вложение|воздухораспределитель|заглушка|балка|витраж|пространств|отрисовка|вытягивание|вырезание',
    "Управление моделью": r'изменен|модифик|параметр|настройк|связь|уровен|ось|групп|материал|категор|стадия|parameter|setting|link|level|grid|group|material|category|phase|data storage|synchronize|save|сохранить|переименов|move|rotate|copy|mirror|перенос|поворот|копиров|зеркал|split|разделить|соединить|join|cut|изменить|загрузка|обновить|duplicate|дублировать|disconnect|отсоединить|смещение|alignment|выравниван|проверк|check|update|modify|process|rule|selection|выделение|property|свойств|статус|status|синхрониз|скрытие|изоляц|вариант|сборк|буфер|выбор|рабочая плоскость|тест|согласование|guidstorage'
}


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


def transform_added_elements(
    ad_path: str,
    added_path: str,
    **context
) -> pd.DataFrame:
    """
    Трансформирует данные о добавленных элементах.
    
    Args:
        ad_path: Путь к CSV с данными AD пользователей
        added_path: Путь к CSV с данными added_element
    
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
    
    # 4. Обработка даты
    print("\n4. Обработка даты...")
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    print(f"   - Диапазон дат: {df['date'].min()} - {df['date'].max()}")
    
    # 5. Классификация транзакций
    print("\n5. Классификация транзакций...")
    classifications = df['transaction_name'].apply(classify_transaction)
    df['class'] = classifications.apply(lambda x: x[0])
    df['is_plugin'] = classifications.apply(lambda x: x[1])
    
    print(f"   - Категории: {df['class'].value_counts().to_dict()}")
    print(f"   - Плагины: {df['is_plugin'].sum()} записей")
    
    # 6. Подсчёт количества элементов
    print("\n6. Подсчёт элементов...")
    df['elements_count'] = df['element_ids'].apply(count_elements)
    
    print(f"   - Всего элементов: {df['elements_count'].sum()}")
    print(f"   - Среднее на запись: {df['elements_count'].mean():.2f}")
    
    # 7. Классификация BIM/designers
    print("\n7. Классификация пользователей...")
    df['is_bim'] = df['user_name'].isin(BIM_USERS)
    
    # 8. Формируем итоговый DataFrame
    result_columns = [
        'date',
        'user_name',
        'short_project_name',
        'program_version',
        'transaction_name',
        'class',
        'is_plugin',
        'elements_count',
        'is_bim'
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
