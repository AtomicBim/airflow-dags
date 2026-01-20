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
# Функции определения объектов, разделов и стадий проектов
# ============================================================================

def get_object_name(project_name: str) -> str:
    """
    Определяет название объекта по имени проекта.

    Args:
        project_name: Название проекта

    Returns:
        Название объекта ('АТОМ', 'Кортрос', 'ИНПРО', 'Ялта' и т.д.)

    Examples:
        >>> get_object_name('K01_AR_2024')
        'Кортрос'
        >>> get_object_name('АТОМ_КР_П_2024')
        'АТОМ'
    """
    name = str(project_name)
    
    if re.search(r"СП\.ЛЛУ|стандарт|узлы|узел|библиотека", name, re.IGNORECASE):
        return "Узлы и стандарты"
    elif re.search(r"АТОМ|ДОУ|08-12|ИКП|ATOM|АПУ", name, re.IGNORECASE):
        return "АТОМ"
    elif re.search(r"K01", name, re.IGNORECASE):
        return "Кортрос"
    elif re.search(r"ИНПРО", name, re.IGNORECASE):
        return "ИНПРО"
    elif re.search(r"Ялта", name, re.IGNORECASE):
        return "Ялта"
    else:
        return "Неизвестные проекты"


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
    from common.config import SECTION_MAP_KORTROS, SECTION_MAP_RUS

    name = str(project_name)

    if object_name == "Кортрос":
        for pattern, section in SECTION_MAP_KORTROS.items():
            if pattern in name:
                return section
        return "Нет данных"
    else:
        for pattern, section in SECTION_MAP_RUS.items():
            if pattern in name:
                return section
        return "Нет данных"


def get_project_stage(project_name: str, object_name: str) -> str:
    """
    Определяет стадию проекта (П, Р, ЭП и т.д.).

    Args:
        project_name: Название проекта
        object_name: Название объекта (Кортрос, ИНПРО и т.д.)

    Returns:
        Код стадии ('П', 'Р', 'ЭП' и т.д.) или 'Нет данных' если не определена

    Examples:
        >>> get_project_stage('K01_AR_P_2024', 'Кортрос')
        'П'
        >>> get_project_stage('Проект_АР_Р_2024', 'ИНПРО')
        'Р'
    """
    from common.config import STAGE_MAP_KORTROS, STAGE_MAP_RUS

    name = str(project_name)

    if object_name == "Кортрос":
        for (mode, pattern), stage in STAGE_MAP_KORTROS.items():
            if (mode == "contains" and pattern in name) or \
               (mode == "endswith" and name.endswith(pattern)):
                return stage
        return "Нет данных"
    else:
        for (mode, pattern), stage in STAGE_MAP_RUS.items():
            if (mode == "contains" and pattern in name) or \
               (mode == "endswith" and name.endswith(pattern)):
                return stage
        return "Нет данных"
