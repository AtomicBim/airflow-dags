import pandas as pd
import uuid

def check_responsible(val):
    """Проверяет, входит ли ответственный в список запрещенных."""
    forbidden = ['овсянкин', 'кузовлева', 'кичигин', 'андреев']
    if not isinstance(val, str):
        return False
    first = val.split(",")[0].strip().lower()
    return any(fam in first for fam in forbidden)

def generate_deterministic_guid(row: pd.Series) -> str:
    """
    Создает детерминированный GUID на основе содержимого строки.
    Это гарантирует, что для одной и той же записи всегда будет один и тот же ID.
    """
    # Используем ключевые поля, которые определяют уникальность записи.
    # Преобразуем дату в строку, чтобы избежать проблем с форматами.
    closing_date_str = str(row.get('closing_date', ''))
    
    key_string = (
        f"{row.get('Семейство', '')}"
        f"{closing_date_str}"
        f"{row.get('Описание изменения', '')}"
        f"{row.get('Ответственный', '')}" # Добавляем ответственного для большей уникальности
    )
    
    # Генерируем UUIDv5, который основан на SHA-1 хеше имени и неймспейса.
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, key_string))


def transform_gsheet_data_df(df_families: pd.DataFrame, **context) -> pd.DataFrame:
    """
    Полный цикл трансформации данных из Google Sheets.
    Принимает и возвращает pandas DataFrame.
    """
    if df_families.empty:
        print("Входной DataFrame пуст, трансформация не требуется.")
        return df_families

    # --- Начало вашей логики трансформации ---
    # Безопасное переименование столбца с датой, если он существует
    if 'Unnamed: 0' in df_families.columns:
        df_families.rename(columns={'Unnamed: 0': 'closing_date'}, inplace=True)

    # Удалить строки без даты закрытия
    if 'closing_date' in df_families.columns:
        df_families.dropna(subset=['closing_date'], inplace=True)
        df_families = df_families[df_families['closing_date'].astype(str).str.strip() != '']

    # Очистка от пустых строк и столбцов
    df_families.dropna(how='all', inplace=True)
    df_families.dropna(axis=1, how='all', inplace=True)

    # Удаление лишних Unnamed-столбцов
    df_families = df_families.loc[:, ~df_families.columns.str.contains('^Unnamed', na=False)]

    print("Генерация детерминированных GUID...")
    df_families['guid'] = df_families.apply(generate_deterministic_guid, axis=1)
    print("GUID сгенерированы.")

    # Удаление дубликатов и сброс индекса
    df_families.drop_duplicates(inplace=True)
    df_families.reset_index(drop=True, inplace=True)

    # Добавление новых столбцов
    df_families['title'] = 'BIM_Семейства'
    df_families['created'] = df_families['closing_date'] if 'closing_date' in df_families.columns else pd.NaT
    df_families['short_description'] = df_families['Семейство'] if 'Семейство' in df_families.columns else ''
    df_families['program'] = 'Revit'

    # Преобразование дат
    default_date = pd.to_datetime("2024-03-01")
    for date_col in ['closing_date', 'created']:
        if date_col in df_families.columns:
            df_families[date_col] = pd.to_datetime(df_families[date_col], format="%d.%m.%Y", errors='coerce')
            df_families[date_col] = df_families[date_col].fillna(default_date)
        else:
            df_families[date_col] = default_date

    # Считаем продолжительность задач
    df_families["work_days_duration"] = 1.0

    # Безопасное переименование 'Раздел' → 'discipline'
    if 'Раздел' in df_families.columns:
        df_families.rename(columns={'Раздел': 'discipline'}, inplace=True)
    elif 'discipline' not in df_families.columns:
        df_families['discipline'] = 'Не указано'
        # Сопоставление групп дисциплин
    discipline_mapping = {
        'АР': 'Архитектура', 'ВК': 'Инженерные системы', 'ОВ': 'Инженерные системы',
        'КЖ': 'Конструкции', 'ТХ': 'Инженерные системы', 'ЭЛ': 'Электросети и связь',
        'КМ': 'Конструкции',
    }
    df_families['discipline_group'] = df_families['discipline'].map(lambda x: discipline_mapping.get(x, 'Другое'))

    # Остальные поля
    df_families['priority'] = 'Обычный'
    df_families['author'] = 'Нет данных'
    df_families['type_request'] = 'Семейства'
    df_families['type_request_group'] = 'Семейства'
    df_families['status'] = 'Закрыта'
    df_families['comment'] = df_families['Описание изменения'] if 'Описание изменения' in df_families.columns else ''
    df_families['responsible'] = df_families['Ответственный'] if 'Ответственный' in df_families.columns else 'Нет данных'

    # Итоговая структура
    columns_order = [
        'title', 'guid', 'created', 'closing_date', 'work_days_duration', 'short_description', 'program',
        'discipline', 'discipline_group', 'priority', 'author', 'type_request', 
        'type_request_group', 'status', 'comment', 'responsible'
    ]
    
    # Добавляем department и project_section в final_order
    final_order = columns_order + ['department', 'project_section']
    
    # Добавляем недостающие столбцы перед reindex
    df_families['department'] = 'Отдел ТИМ'
    df_families['project_section'] = 'ТИМ'

    df_families = df_families.reindex(columns=final_order)

    # Убираем разработчиков
    df_families = df_families[~df_families["responsible"].apply(check_responsible)]

    # Заполнение NaN
    df_families['department'] = df_families['department'].fillna('Нет данных')
    df_families['project_section'] = df_families['project_section'].fillna('Нет данных')

    # Фильтровать по дате (оставить только >= 01.01.2022)
    if 'created' in df_families.columns:
        cutoff_date = pd.Timestamp("2022-01-01")
        df_families = df_families[pd.to_datetime(df_families["created"]) >= cutoff_date]
    
    print(f"Трансформация GSheet завершена. Обработано {len(df_families)} строк.")
    
    return df_families