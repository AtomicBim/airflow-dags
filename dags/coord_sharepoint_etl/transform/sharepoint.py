import pandas as pd
import numpy as np
from workalendar.europe import Russia

# Импорт из централизованной конфигурации и утилит
from config import (
    BIM_USERS, TO_REMOVE, FORBIDDEN_USERS,
    DISCIPLINE_MAPPING, TYPE_REQUEST_MAPPING,
    LOCAL_TIMEZONE, WORKDAY_START_HOUR, WORKDAY_END_HOUR
)
from utils import (
    parse_responsible_ids, to_local, clean_html_safe,
    extract_number, remove_specific, check_responsible, clean_type_request
)

cal = Russia()


def transform_sharepoint_data(tasks_path: str, users_path: str, ad_path: str, output_path: str, **context) -> str:
    """Полный цикл трансформации данных SharePoint."""
    df_tasks = pd.read_csv(tasks_path)
    df_users = pd.read_csv(users_path)
    df_ad = pd.read_csv(ad_path)

    # Разворачиваем responsibleId
    df_tasks['responsibleId'] = df_tasks['responsibleId'].apply(parse_responsible_ids)

    # Заменяем ID на имена
    id_to_title = dict(zip(df_users['Id'], df_users['Title']))
    df_tasks['responsibleId'] = df_tasks['responsibleId'].apply(
        lambda ids: '; '.join([id_to_title.get(i, '') for i in ids if id_to_title.get(i, '')])
    )
    df_tasks['applicantId'] = df_tasks['applicantId'].map(id_to_title).fillna('Нет данных')

    # Меняем порядок столбцов и их имена
    df_tasks = df_tasks.rename(columns={
        'Title': 'title',
        'GUID': 'guid',
        'Created': 'created',
        'closing_date': 'closing_date',
        'short_description': 'short_description',
        'detailed_description': 'detailed_description',
        'PO_ESP': 'program',
        'discipline': 'discipline',
        'Priority': 'priority',
        'applicantId': 'author',
        'Type_request': 'type_request',
        'Status': 'status',
        'comment': 'comment',
        'responsibleId': 'responsible'
    })

    # Приводим типы данных дат
    for col in ["created", "closing_date"]:
        df_tasks[col] = pd.to_datetime(df_tasks[col].replace("Нет данных", pd.NaT), errors='coerce', utc=True)
    
    # Считаем рабочие дни
    from utils import workdays_diff as calc_workdays
    df_tasks["work_days_duration"] = df_tasks.apply(
        lambda row: calc_workdays(
            row["created"], row["closing_date"],
            workday_start=WORKDAY_START_HOUR,
            workday_end=WORKDAY_END_HOUR,
            calendar=cal
        ), axis=1
    )  

    # Обновляем порядок с новыми именами
    desired_order = [
        'title', 'guid',
        'created', 'closing_date',
        'work_days_duration', 'short_description',
        'detailed_description', 'program',
        'discipline', 'priority',
        'author','type_request',
        'status', 'comment',
        'responsible'
    ]
    remaining_cols = [col for col in df_tasks.columns if col not in desired_order]
    final_order = desired_order + remaining_cols
    df_tasks = df_tasks[final_order]

    # Извлекаем текст детального описания заявки detailed_descriptio
    if 'detailed_description' in df_tasks.columns:
        print("Очистка HTML в поле detailed_description...")
        df_tasks['detailed_description'] = df_tasks['detailed_description'].apply(clean_html_safe)
        print("Готово!")
    else:
        print("Поле 'detailed_description' не найдено в DataFrame")
    
    # Очищаем title
    df_tasks["title"] = df_tasks["title"].apply(extract_number)

    # Очищаем и маппим type_request
    df_tasks['type_request'] = df_tasks['type_request'].apply(clean_type_request)
    df_tasks['type_request_group'] = df_tasks['type_request'].map(TYPE_REQUEST_MAPPING).fillna('Другое')
    cols = df_tasks.columns.tolist()
    cols.remove('type_request_group')
    insert_pos = cols.index('type_request') + 1
    cols.insert(insert_pos, 'type_request_group')
    df_tasks = df_tasks[cols]

    # Очищаем discipline и применяем маппинг
    df_tasks['discipline_group'] = df_tasks['discipline'].map(DISCIPLINE_MAPPING).fillna('Другое')

    # Вставка discipline_group после discipline
    cols = df_tasks.columns.tolist()
    if 'discipline_group' in cols:
        cols.remove('discipline_group')
    insert_pos = cols.index('discipline') + 1
    cols.insert(insert_pos, 'discipline_group')
    df_tasks = df_tasks[cols]

    # Удаляем уволенных и тестовых
    df_tasks = df_tasks[~df_tasks["responsible"].apply(remove_specific)]

    # Мерджим AD в tasks
    df_tasks = df_tasks.merge(
        df_ad[['display_name', 'department', 'project_section']],
        how='left',
        left_on='author',
        right_on='display_name'
    )
    df_tasks.drop(columns=['display_name'], inplace=True)

    # Настраиваем ответственных и убираем удаленных
    df_tasks = df_tasks[~df_tasks["responsible"].apply(check_responsible)]

    to_delete = [
        "6ce81543-eedb-4048-b22c-f0d5771d2d2d",
        "749b69f9-45db-49ca-93ef-9591fa2ce042",
        "cf68a4c4-471c-4c59-b67e-01ccacf0b536",
        "0f315791-cfbb-4af9-86a0-2d7c6fb77d47",
        "7b390bca-e355-4438-a4fe-4f65a7b21ec7",
        "8a95f7cb-fc19-45e6-ad33-7da08411acb6",
    ]
    df_tasks = df_tasks[~df_tasks['guid'].isin(to_delete)]

    # Правим статусы
    df_tasks.loc[
        (df_tasks["status"] == "Назначение ответственного") | (df_tasks["status"] == "Отменена"),
        "responsible"
    ] = "Не назначен"

    # Приводим даты
    for col in ["created", "closing_date"]:
        df_tasks[col] = df_tasks[col].apply(
            lambda x: pd.to_datetime(x, errors='coerce') if x != "Нет данных" else pd.NaT
        )

    # Удаляем дубликаты
    df_tasks = df_tasks.drop_duplicates(subset=["guid"], keep="first")

    # Приводим guid к строке
    df_tasks["guid"] = df_tasks["guid"].astype(str)
    
    df_tasks.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Трансформация SharePoint завершена. Сохранено в {output_path}")

    return output_path