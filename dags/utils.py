"""
Общие утилиты для ETL pipelines.

Содержит переиспользуемые функции для обработки данных:
- Извлечение имен проектов
- Фильтрация пользователей
- Обработка HTML
- Работа с датами
"""
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import ast
import re
from typing import Optional
from config import FORBIDDEN_USERS, TO_REMOVE


# ============================================================================
# Функции обработки имен проектов
# ============================================================================

def extract_short_name(name: str) -> str:
    """
    Извлекает короткое название проекта из полного.

    Берет первые 2 части названия, разделенные '_'.

    Args:
        name: Полное название проекта (например, 'K01_AR_2024_vaskov')

    Returns:
        Короткое название (например, 'K01_AR')

    Examples:
        >>> extract_short_name('K01_AR_2024_vaskov')
        'K01_AR'
        >>> extract_short_name('simple')
        'simple'
    """
    if not isinstance(name, str):
        return name
    parts = name.split('_')
    return '_'.join(parts[:2]) if len(parts) >= 2 else name


def extract_file_storage_name(row: pd.Series) -> str:
    """
    Извлекает название файлового хранилища из project_name.

    Удаляет имя пользователя из конца названия проекта, если оно там есть.

    Args:
        row: Pandas Series с полями 'project_name' и 'username'

    Returns:
        Название файлового хранилища

    Examples:
        >>> row = pd.Series({'project_name': 'K01_AR_2024_vaskov', 'username': 'vaskov'})
        >>> extract_file_storage_name(row)
        'K01_AR_2024'
    """
    project = row.get("project_name")
    username = row.get("username")

    if pd.isna(project) or pd.isna(username):
        return project

    parts = str(project).split("_")
    if len(parts) < 2:
        return project

    last_part = parts[-1].strip().lower()
    user_name = str(username).strip().lower()

    if last_part == user_name:
        return "_".join(parts[:-1])
    else:
        return project


# ============================================================================
# Функции фильтрации пользователей
# ============================================================================

def check_responsible(val: str) -> bool:
    """
    Проверяет, входит ли ответственный в список запрещенных.

    Используется для фильтрации уволенных/удаленных сотрудников.

    Args:
        val: ФИО ответственного (может быть список через запятую)

    Returns:
        True если пользователь в списке запрещенных, False иначе

    Examples:
        >>> check_responsible('Овсянкин Роман')
        True
        >>> check_responsible('Иванов Иван')
        False
    """
    if not isinstance(val, str):
        return False
    first = val.split(",")[0].strip().lower()
    return any(fam in first for fam in FORBIDDEN_USERS)


def remove_specific(val: str) -> bool:
    """
    Проверяет, нужно ли удалить конкретного пользователя.

    Используется для фильтрации тестовых/временных пользователей.

    Args:
        val: ФИО пользователя

    Returns:
        True если пользователя нужно удалить, False иначе

    Examples:
        >>> remove_specific('Мельникова Анна')
        True
        >>> remove_specific('Иванов Иван')
        False
    """
    if not isinstance(val, str):
        return False
    first = val.strip().lower()
    return any(first.startswith(fam) for fam in TO_REMOVE)


# ============================================================================
# Функции обработки HTML и текста
# ============================================================================

def clean_html_safe(html_string: Optional[str]) -> str:
    """
    Безопасно извлекает текст из HTML.

    Обрабатывает None, NaN, пустые строки и обычный текст без HTML тегов.

    Args:
        html_string: HTML строка или None

    Returns:
        Чистый текст без HTML тегов

    Examples:
        >>> clean_html_safe('<p>Hello <b>World</b></p>')
        'Hello World'
        >>> clean_html_safe(None)
        ''
        >>> clean_html_safe('Simple text')
        'Simple text'
    """
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


def extract_number(title: str) -> str:
    """
    Извлекает номер заявки из заголовка.

    Убирает все после 'от' и символ '№'.

    Args:
        title: Заголовок заявки (например, '№123 от 01.01.2024')

    Returns:
        Номер заявки (например, '123')

    Examples:
        >>> extract_number('№123 от 01.01.2024')
        '123'
        >>> extract_number('Simple title')
        'Simple title'
    """
    try:
        parts = title.split("от")
        if len(parts) < 2:
            return title
        number_part = parts[0].replace("№", "").strip()
        return number_part
    except Exception:
        return title


def clean_type_request(value: str) -> str:
    """
    Удаляет ведущие цифры и точку из строки типа запроса.

    Args:
        value: Тип запроса (например, '1. Семейства')

    Returns:
        Очищенный тип (например, 'Семейства')

    Examples:
        >>> clean_type_request('1. Семейства')
        'Семейства'
        >>> clean_type_request('Семейства')
        'Семейства'
    """
    if isinstance(value, str):
        return re.sub(r'^\d+\.\s*', '', value).strip()
    return value


# ============================================================================
# Функции работы с датами и временем
# ============================================================================

def to_local(dt: pd.Timestamp, timezone: str = 'Asia/Yekaterinburg') -> pd.Timestamp:
    """
    Переводит UTC datetime в локальное время.

    Убирает tzinfo для совместимости с workalendar.

    Args:
        dt: Timestamp в UTC
        timezone: Целевая таймзона (по умолчанию Asia/Yekaterinburg)

    Returns:
        Timestamp в локальной таймзоне без tzinfo

    Examples:
        >>> dt = pd.Timestamp('2024-01-01 12:00:00', tz='UTC')
        >>> to_local(dt)
        Timestamp('2024-01-01 17:00:00')
    """
    dt = pd.to_datetime(dt)
    if dt.tzinfo is None:
        dt = dt.tz_localize('UTC')
    return dt.tz_convert(timezone).replace(tzinfo=None)


def workdays_diff(
    start: pd.Timestamp,
    end: pd.Timestamp,
    workday_start: int = 8,
    workday_end: int = 17,
    calendar=None
) -> float:
    """
    Подсчитывает количество рабочих дней между двумя датами.

    Учитывает:
    - Рабочее время (по умолчанию 8:00-17:00)
    - Выходные дни
    - Праздники (через calendar)

    Args:
        start: Начальная дата
        end: Конечная дата
        workday_start: Час начала рабочего дня (по умолчанию 8)
        workday_end: Час окончания рабочего дня (по умолчанию 17)
        calendar: Calendar объект для определения выходных (workalendar)

    Returns:
        Количество рабочих дней (может быть дробным)

    Examples:
        >>> from workalendar.europe import Russia
        >>> cal = Russia()
        >>> start = pd.Timestamp('2024-01-15 09:00:00')
        >>> end = pd.Timestamp('2024-01-16 14:00:00')
        >>> workdays_diff(start, end, calendar=cal)
        1.56
    """
    if pd.isnull(start) or pd.isnull(end):
        return np.nan

    if calendar is None:
        from workalendar.europe import Russia
        calendar = Russia()

    # Конвертируем в локальное время
    start = to_local(start)
    end = to_local(end)

    work_hours = workday_end - workday_start

    if start.date() < end.date():
        # Разные дни
        days_between = calendar.get_working_days_delta(start.date(), end.date()) - 1
        days_between = max(0, days_between)

        # Доля первого дня
        if calendar.is_working_day(start.date()):
            first_day_hours = workday_end - max(start.hour + start.minute/60, workday_start)
            first_day_hours = np.clip(first_day_hours, 0, work_hours)
            first_day_part = first_day_hours / work_hours
        else:
            first_day_part = 0

        # Доля последнего дня
        if calendar.is_working_day(end.date()):
            last_day_hours = min(end.hour + end.minute/60, workday_end) - workday_start
            last_day_hours = np.clip(last_day_hours, 0, work_hours)
            last_day_part = last_day_hours / work_hours
        else:
            last_day_part = 0

        total = days_between + first_day_part + last_day_part
    else:
        # Один день
        if calendar.is_working_day(start.date()):
            t1 = max(start.hour + start.minute/60, workday_start)
            t2 = min(end.hour + end.minute/60, workday_end)
            hours = np.clip(t2 - t1, 0, work_hours)
            total = hours / work_hours
        else:
            total = 0

    return round(total, 2)


# ============================================================================
# Функции парсинга SharePoint данных
# ============================================================================

def parse_responsible_ids(val: str) -> list:
    """
    Парсит строку с ID ответственных из SharePoint.

    Args:
        val: Строка с результатами SharePoint (обычно dict в виде строки)

    Returns:
        Список ID ответственных

    Examples:
        >>> parse_responsible_ids("{'results': [1, 2, 3]}")
        [1, 2, 3]
        >>> parse_responsible_ids("invalid")
        []
    """
    try:
        obj = ast.literal_eval(val)
        return obj.get('results', [])
    except Exception:
        return []


# ============================================================================
# Функции определения разделов и стадий проектов
# ============================================================================

def get_project_solution(project_name: str, object_name: str) -> str:
    """
    Определяет раздел проекта (АР, КР и т.д.).

    Args:
        project_name: Название проекта
        object_name: Название объекта (Кортрос, ИНПРО и т.д.)

    Returns:
        Код раздела ('АР', 'КР', 'ВК' и т.д.) или 'НД' если не определен

    Examples:
        >>> get_project_solution('K01_AR_2024', 'Кортрос')
        'АР'
        >>> get_project_solution('Проект_КР_2024', 'ИНПРО')
        'КР'
    """
    from config import SECTION_MAP_KORTROS, SECTION_MAP_RUS

    name = str(project_name)

    if object_name == "Кортрос":
        for pattern, section in SECTION_MAP_KORTROS.items():
            if pattern in name:
                return section
        return "НД"
    else:
        for pattern, section in SECTION_MAP_RUS.items():
            if pattern in name:
                return section
        return "НД"


def get_project_stage(project_name: str, object_name: str) -> str:
    """
    Определяет стадию проекта (П, Р, ЭП и т.д.).

    Args:
        project_name: Название проекта
        object_name: Название объекта (Кортрос, ИНПРО и т.д.)

    Returns:
        Код стадии ('П', 'Р', 'ЭП' и т.д.) или 'НД' если не определена

    Examples:
        >>> get_project_stage('K01_AR_P_2024', 'Кортрос')
        'П'
        >>> get_project_stage('Проект_АР_Р_2024', 'ИНПРО')
        'Р'
    """
    from config import STAGE_MAP_KORTROS, STAGE_MAP_RUS

    name = str(project_name)

    if object_name == "Кортрос":
        for (mode, pattern), stage in STAGE_MAP_KORTROS.items():
            if (mode == "contains" and pattern in name) or \
               (mode == "endswith" and name.endswith(pattern)):
                return stage
        return "НД"
    else:
        for (mode, pattern), stage in STAGE_MAP_RUS.items():
            if (mode == "contains" and pattern in name) or \
               (mode == "endswith" and name.endswith(pattern)):
                return stage
        return "НД"
