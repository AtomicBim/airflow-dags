import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import ast
import re
from workalendar.europe import Russia

LOCAL_TZ = 'Asia/Yekaterinburg'  # Екатеринбург, UTC+5
cal = Russia()

# Функции
def parse_responsible_ids(val):
    try:
        obj = ast.literal_eval(val)
        return obj.get('results', [])
    except Exception:
        return []


def to_local(dt):
    """Переводит UTC datetime в локальное время Екатеринбурга, убирает tzinfo для совместимости с workalendar."""
    # Если уже pd.Timestamp и без tzinfo, считаем что это UTC
    dt = pd.to_datetime(dt)
    if dt.tzinfo is None:
        dt = dt.tz_localize('UTC')
    return dt.tz_convert(LOCAL_TZ).replace(tzinfo=None)     
   

def workdays_diff(row):
    start = row["created"]
    end = row["closing_date"]
    if pd.isnull(start) or pd.isnull(end):
        return np.nan

    # Переводим в локальное время Екатеринбурга
    start = to_local(start)
    end = to_local(end)

    # Настройки рабочего дня
    workday_start = 8  # 8:00
    workday_end = 17   # 17:00
    work_hours = workday_end - workday_start

    if start.date() < end.date():
        days_between = cal.get_working_days_delta(start.date(), end.date()) - 1
        days_between = max(0, days_between)

        # Доля первого дня
        if cal.is_working_day(start.date()):
            first_day_hours = workday_end - max(start.hour + start.minute/60, workday_start)
            first_day_hours = np.clip(first_day_hours, 0, work_hours)
            first_day_part = first_day_hours / work_hours
        else:
            first_day_part = 0

        # Доля последнего дня
        if cal.is_working_day(end.date()):
            last_day_hours = min(end.hour + end.minute/60, workday_end) - workday_start
            last_day_hours = np.clip(last_day_hours, 0, work_hours)
            last_day_part = last_day_hours / work_hours
        else:
            last_day_part = 0

        total = days_between + first_day_part + last_day_part
    else:
        if cal.is_working_day(start.date()):
            t1 = max(start.hour + start.minute/60, workday_start)
            t2 = min(end.hour + end.minute/60, workday_end)
            hours = np.clip(t2 - t1, 0, work_hours)
            total = hours / work_hours
        else:
            total = 0

    return round(total, 2)


def clean_html_safe(html_string):
    """Безопасно извлекает текст из HTML"""
    # Проверяем на None, NaN, пустую строку
    if pd.isna(html_string) or html_string == '' or html_string is None:
        return ''
    
    try:
        # Преобразуем в строку на всякий случай
        html_str = str(html_string)
        
        # Если это не HTML (нет тегов), возвращаем как есть
        if '<' not in html_str or '>' not in html_str:
            return html_str.strip()
        
        # Парсим HTML
        soup = BeautifulSoup(html_str, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)
        
        # Убираем лишние пробелы
        text = ' '.join(text.split())
        return text
        
    except Exception as e:
        # Если что-то пошло не так, возвращаем исходную строку
        print(f"Ошибка обработки HTML: {e}")
        return str(html_string) if html_string is not None else ''


def extract_number(title):
    try:
        parts = title.split("от")
        if len(parts) < 2:
            return title
        number_part = parts[0].replace("№", "").strip()
        return number_part
    except Exception:
        return title


to_remove = ['мельникова', 'шишляева']
def remove_specific(val):
    if not isinstance(val, str):
        return False
    first = val.strip().lower()
    return any(first.startswith(fam) for fam in to_remove)


forbidden = ['овсянкин', 'кузовлева', 'кичигин', 'андреев', 'романова', 'урманчеев']
def check_responsible(val):
    if not isinstance(val, str):
        return False
    first = val.split(",")[0].strip().lower()
    return any(fam in first for fam in forbidden)


def clean_type_request(value):
    """
    Удаляет ведущие цифры и точку из строки.
    Пример: '1. Семейства' → 'Семейства'
    """
    if isinstance(value, str):
        return re.sub(r'^\d+\.\s*', '', value).strip()
    return value


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
    df_tasks["work_days_duration"] = df_tasks.apply(workdays_diff, axis=1)  

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
    type_request_mapping = {
        'Совместная работа': 'Консультирование',
        'Семейства': 'Семейства',
        'BIM поддержка': 'Консультирование',
        'Проверка ИМ': 'Аудит модели',
        'Выгрузка из BIM Tangl': 'Tangl',
        'Другое': 'Другое',
        'Виды, листы, спецификации': 'Консультирование',
        'Аудит модели': 'Аудит модели',
        'Обучение': 'Обучение',
        'Корректировка модели': 'Корректировка модели',
        'Экспорт данных': 'Аудит модели',
        'Нет данных': 'Другое',
        'Работа с объектами Civil': 'Civil',
        'Задачи BIM Tangl': 'Tangl',
        'Блоки': 'Другое',
        'Проверка файлов': 'Аудит модели'
    }

    df_tasks['type_request_group'] = df_tasks['type_request'].map(type_request_mapping).fillna('Другое')
    cols = df_tasks.columns.tolist()
    cols.remove('type_request_group')
    insert_pos = cols.index('type_request') + 1
    cols.insert(insert_pos, 'type_request_group')
    df_tasks = df_tasks[cols]

    # Очищаем discipline
    discipline_mapping = {
        'КР/КЖ': 'Конструкции',
        'АР/ЭП': 'Архитектура',
        'ОВ/ВК/ТХ': 'Инженерные системы',
        'Другое': 'Другое',
        'Сметы': 'Сметы',
        'ГП/ПОС': 'Генплан и ПОС',
        'ПТО': 'ПТО',
        'Внутренняя BIM': 'Внутренняя BIM',
        'КАСК': 'ПТО',
        'ЭЛ/СС/АК': 'Электросети и связь',
        'ТС/НВК/НСС': 'Наружные сети',
        'Распределять по площадям': 'Другое',
        'Распределять вручную': 'Другое'
    }

    # Применение маппинга
    df_tasks['discipline_group'] = df_tasks['discipline'].map(discipline_mapping).fillna('Другое')

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