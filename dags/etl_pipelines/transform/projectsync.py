"""
Transform модуль для ProjectSync pipeline.
Обрабатывает данные синхронизации проектов с классификацией по BIM/дизайнерам.
"""
import pandas as pd
import numpy as np
from typing import Tuple


# Константы
BIM_USERS = {
    'Колпаков Семен Дмитриевич', 'Пятков Роман Анатольевич',
    'Андреев Александр Константинович', 'Кичигин Андрей Владимирович',
    'Панов Антон Владимирович', 'Васьков Денис Игоревич', 'Попов Антон Михайлович',
    'Кузовлева Ольга Сергеевна', 'Калачев Даниил Артемович',
    'Григорьев Роман Николаевич', 'Красильников Дмитрий Сергеевич',
    'Литуева Юлия Дмитриевна', 'Жук Виталий Томашевич', 'Овсянкин Роман Николаевич',
    'Романова Анна Вячеславовна', 'Коновалов Василий Сергеевич',
    'Урманчеев Роман Дамирович', 'Докладчик 708'
}


def extract_short_name(name: str) -> str:
    """Извлекает короткое название проекта."""
    parts = name.split('_')
    return '_'.join(parts[:2]) if len(parts) >= 2 else name


def extract_file_storage_name(row):
    """Извлекает название файлового хранилища."""
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


def get_project_solution(row):
    """Определяет раздел проекта (АР, КР и т.д.)."""
    name = str(row["project_name"])
    obj = row["object_name"]

    if obj == "Кортрос":
        section_map_kortros = {
            "_AR": "АР", "_AI": "АИ",
            "_KR": "КР", "_AGK": "АГК",
            "_VK": "ВК", "_EL": "ЭЛ",
            "_OV": "ОВ", "_AK": "АК",
            "_SS": "СС", "_P": "П",
            "_R": "Р", "_TS": "ТС",
            "_AP": "АП"
        }

        for pattern, section in section_map_kortros.items():
            if pattern in name:
                return section
        return "НД"

    else:
        section_map_rus = {
            "_АР": "АР", "_Форэскиз": "АР",
            "_АИ": "АИ", "_КЖ": "КЖ",
            "_ВК": "ВК", "_ЭЛ": "ЭЛ",
            "_ТС": "ТС", "_ТХ": "ТХ",
            "_ОВ": "ОВ", "_КР": "КР",
            "_КМ": "КМ", "_АП": "АП",
            "_ПТ": "ПТ", "_СС": "СС",
            "_ПБ": "ПБ", "_ЭГ": "ЭГ",
            "_АП": "АП"
        }

        for pattern, section in section_map_rus.items():
            if pattern in name:
                return section
        return "НД"


def get_project_stage(row):
    """Определяет стадию проекта (П, Р, ЭП и т.д.)."""
    name = str(row["project_name"])
    obj = row["object_name"]

    if obj == "Кортрос":
        stage_map_kortros = {
            ("contains", "_P_"): "П",
            ("contains", "_R_"): "Р",
            ("contains", "_AGK_"): "ГК",
            ("endswith", "_P"): "П",
            ("endswith", "_R"): "Р",
            ("endswith", "_AGK"): "ГК"
        }

        for (mode, pattern), stage in stage_map_kortros.items():
            if (mode == "contains" and pattern in name) or \
               (mode == "endswith" and name.endswith(pattern)):
                return stage

        return "НД"

    else:
        stage_map = {
            ("contains", "_П_"): "П",
            ("contains", "_Р_"): "Р",
            ("contains", "_РД_"): "Р",
            ("contains", "_ЭП_"): "ЭП",
            ("contains", "_Форэскиз_"): "ЭП",
            ("contains", "_Эскиз_"): "ЭП",
            ("contains", "_ФЭ_"): "ЭП",
            ("endswith", "_П"): "П",
            ("endswith", "_Р"): "Р",
            ("endswith", "_РД"): "Р",
            ("endswith", "_ЭП"): "ЭП",
            ("endswith", "_Форэскиз"): "ЭП",
            ("endswith", "_Эскиз"): "ЭП",
            ("endswith", "_ФЭ"): "ЭП"
        }

        for (mode, pattern), stage in stage_map.items():
            if (mode == "contains" and pattern in name) or \
               (mode == "endswith" and name.endswith(pattern)):
                return stage

        return "НД"


def transform_projectsync_analytics(
    ad_path: str,
    sync_path: str,
    **context
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Трансформирует данные для projectsync pipeline.

    Args:
        ad_path: Путь к CSV с данными AD пользователей
        sync_path: Путь к CSV с данными project_sync

    Returns:
        Tuple: (df_designers, df_bim)
    """
    # Чтение данных
    df_ad = pd.read_csv(ad_path)
    df_sync = pd.read_csv(sync_path)

    # === Слияние с AD ===
    df_sync = df_sync.merge(
        df_ad[["display_name", "department", "project_section"]],
        how="left",
        left_on="user_display_name",
        right_on="display_name"
    ).drop(columns="display_name")

    # === Классификация пользователей ===
    df_sync["is_bim"] = df_sync["user_display_name"].isin(BIM_USERS)

    # === Создание короткого названия ===
    df_sync['short_project_name'] = df_sync['project_name'].astype(str).apply(extract_short_name)

    df_sync = df_sync.drop(columns=[
        'program_name',
        'program_version',
    ])

    # === Определение объекта ===
    mask_atom = df_sync["project_name"].str.contains(
        "АТОМ|ДОУ|08-12|ИКП|ATOM|АПУ", case=False, na=False
    )

    df_sync["object_name"] = np.select(
        [
            df_sync["project_name"].str.contains("СП.ЛЛУ|стандарт|узлы|узел|библиотека", case=False, na=False),
            mask_atom,
            df_sync["project_name"].str.contains("K01", case=False, na=False),
            df_sync["project_name"].str.contains("ИНПРО", case=False, na=False),
            df_sync["project_name"].str.contains("Ялта", case=False, na=False)
        ],
        [
            "Узлы и стандарты",
            "АТОМ",
            "Кортрос",
            "ИНПРО",
            "Ялта"
        ],
        default="Неизвестные проекты"
    )

    # === Флаг отсоединенных проектов ===
    df_sync["is_detached"] = df_sync["project_name"].str.contains("отсоединено", case=False, na=False).astype(int)

    # === Извлечение имени файлового хранилища ===
    df_sync["file_storage_name"] = df_sync.apply(extract_file_storage_name, axis=1)

    # === Определение раздела и стадии проекта ===
    df_sync["project_solution_name"] = df_sync.apply(get_project_solution, axis=1)
    df_sync["project_stage_name"] = df_sync.apply(get_project_stage, axis=1)

    # === Заполнение пропусков ===
    str_cols = df_sync.select_dtypes(include='object').columns
    df_sync[str_cols] = df_sync[str_cols].fillna("Нет данных")

    num_cols = df_sync.select_dtypes(include=['number', 'Int64']).columns
    df_sync[num_cols] = df_sync[num_cols].fillna(0)

    date_cols = df_sync.select_dtypes(include='datetime').columns
    df_sync[date_cols] = df_sync[date_cols].fillna(pd.NaT)

    # === Разделение на BIM и designers (только не отсоединенные) ===
    df_sync_bim = df_sync[(df_sync['is_bim'] == True) & (df_sync['is_detached'] == 0)].copy()
    df_sync_designers = df_sync[(df_sync['is_bim'] == False) & (df_sync['is_detached'] == 0)].copy()

    print(f"Трансформация ProjectSync завершена:")
    print(f"  - Designers: {len(df_sync_designers)} строк")
    print(f"  - BIM: {len(df_sync_bim)} строк")

    return df_sync_designers, df_sync_bim
