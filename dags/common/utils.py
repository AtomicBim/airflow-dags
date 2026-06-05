"""
Общие утилиты для ETL pipelines.

Содержит переиспользуемые функции для обработки имён проектов
(используются projectsync и scripts).

Логика added_elements портирована в PL/pgSQL — см.
dags/etl_pipelines/sql/added_elements/functions/parse_project.sql
"""


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


# NOTE: extract_short_project_name / extract_file_storage_name / get_object_name
# удалены вместе с переходом added_elements на ELT-архитектуру. Их Python-логика
# портирована в PL/pgSQL: dags/etl_pipelines/sql/added_elements/functions/parse_project.sql
#   - parse_file_storage_name(text) — порт extract_short_project_name
#   - parse_object_name(text)       — порт get_object_name


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
