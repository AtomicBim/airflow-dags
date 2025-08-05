import pandas as pd
import uuid

forbidden = ['овсянкин', 'кузовлева', 'кичигин', 'андреев']
def check_responsible(val):
    if not isinstance(val, str):
        return False
    first = val.split(",")[0].strip().lower()
    return any(fam in first for fam in forbidden)


def transform_gsheet_data(families_path: str, output_path: str, **context) -> str:
    """Полный цикл трансформации данных из Google Sheets."""
    df_families = pd.read_csv(families_path)
    
    # Безопасное переименование столбца с датой, если он существует
    if 'Unnamed: 0' in df_families.columns:
        df_families.rename(columns={'Unnamed: 0': 'closing_date'}, inplace=True)

    # Удалить строки без даты закрытия
    df_families.dropna(subset=['closing_date'], inplace=True)
    df_families = df_families[df_families['closing_date'].astype(str).str.strip() != '']

    # Очистка от пустых строк и столбцов
    df_families.dropna(how='all', inplace=True)
    df_families.dropna(axis=1, how='all', inplace=True)

    # Удаление лишних Unnamed-столбцов (кроме Unnamed: 0)
    df_families = df_families.loc[:, ~df_families.columns.str.contains(r'^Unnamed(?!(\: 0))', na=False)]

    # Удаление дубликатов и сброс индекса
    df_families.drop_duplicates(inplace=True)
    df_families.reset_index(drop=True, inplace=True)

    # Добавление новых столбцов
    df_families['guid'] = [uuid.uuid4() for _ in range(len(df_families))]
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

    # считаем продолжительность задач
    df_families["work_days_duration"] = 1.0

    # Безопасное переименование 'Раздел' → 'discipline'
    if 'Раздел' in df_families.columns:
        df_families.rename(columns={'Раздел': 'discipline'}, inplace=True)
    elif 'discipline' not in df_families.columns:
        df_families['discipline'] = 'Не указано'

    # Сопоставление групп дисциплин
    discipline_mapping = {
        'АР': 'Архитектура',
        'ВК': 'Инженерные системы',
        'ОВ': 'Инженерные системы',
        'КЖ': 'Конструкции',
        'ТХ': 'Инженерные системы',
        'ЭЛ': 'Электросети и связь',
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
        'discipline', 'discipline_group', 'priority', 'author',
        'type_request', 'type_request_group', 'status', 'comment',
        'responsible'
    ]
    df_families = df_families.reindex(columns=columns_order)

    # Убираем разработчиков
    df_families = df_families[~df_families["responsible"].apply(check_responsible)]

    # Правим ответственных
    df_families['department'] = 'Отдел ТИМ'
    df_families['project_section'] = 'ТИМ'

    # Заполнение NaN
    df_families['department'] = df_families['department'].fillna('Нет данных')
    df_families['project_section'] = df_families['project_section'].fillna('Нет данных')

    # Фильтровать по дате (оставить только >= 01.01.2022)
    cutoff_date = pd.Timestamp("2022-01-01")
    df_families = df_families[df_families["created"] >= cutoff_date]

    df_families.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Трансформация GSheet завершена. Сохранено в {output_path}")

    return output_path