"""
Transform модуль для ProjectSync pipeline.
Обрабатывает старые и новые данные синхронизации проектов.
"""
import pandas as pd
import numpy as np
from typing import Tuple

from common.config import BIM_USERS
from common.utils import extract_short_name, extract_file_storage_name, get_project_solution, get_project_stage

def transform_projectsync_analytics(
    ad_path: str,
    legacy_sync_path: str,
    new_sync_path: str,
    **context
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    
    # 1. Чтение данных
    df_ad = pd.read_csv(ad_path, low_memory=False)
    df_legacy = pd.read_csv(legacy_sync_path, low_memory=False)
    df_new = pd.read_csv(new_sync_path, low_memory=False)

    # Подготовка справочника AD
    df_ad_clean = df_ad[["id", "display_name", "email", "department", "project_doc_section", "company", "enabled"]].copy()
    # Вытаскиваем логин до @ для стыковки со старой таблицей
    df_ad_clean["ad_username"] = df_ad_clean["email"].astype(str).str.split("@").str[0].str.lower().str.strip()
    df_ad_unique = df_ad_clean.drop_duplicates(subset=["ad_username"])

    # 2. Обработка СТАРЫХ данных (legacy)
    # Приводим к общему формату
    df_legacy = df_legacy.rename(columns={"project_name": "project_title"})
    df_legacy["username_lower"] = df_legacy["username"].astype(str).str.lower().str.strip()
    
    # Соединяем с AD по username == ad_username
    df_legacy = df_legacy.merge(
        df_ad_unique,
        how="left",
        left_on="username_lower",
        right_on="ad_username"
    )
    # Убираем лишние колонки
    df_legacy = df_legacy.drop(columns=[
        "username_lower", "ad_username", "user_display_name", "program_name", "program_version"
    ], errors="ignore")

    # 3. Обработка НОВЫХ данных (revit)
    # Соединяем с AD по GUID
    df_new = df_new.merge(
        df_ad_clean,
        how="left",
        left_on="user_id",
        right_on="id"
    )
    # Убираем лишние колонки
    df_new = df_new.drop(columns=["cad_program_id", "cad_program_version", "ad_username"], errors="ignore")
    # Достаем username из email для алгоритма обрезки файлов
    df_new["username"] = df_new["email"].astype(str).str.split("@").str[0]

    # 4. ОБЪЕДИНЕНИЕ СТАРОЙ И НОВОЙ БАЗЫ
    df_sync = pd.concat([df_legacy, df_new], ignore_index=True)
    if "id" in df_sync.columns:
        df_sync = df_sync.drop(columns=["id"])

    # 5. БИЗНЕС-ЛОГИКА (применяется ко всем данным)
    
    # Защита от пустых email/username
    df_sync.loc[df_sync["email"].isna(), "username"] = np.nan

    # Флаг BIM
    df_sync["is_bim"] = df_sync["display_name"].isin(BIM_USERS)

    # Короткое название
    df_sync['short_project_name'] = df_sync['project_title'].astype(str).apply(extract_short_name)

    # Объект
    mask_atom = df_sync["project_title"].str.contains("АТОМ|ДОУ|08-12|ИКП|ATOM|АПУ", case=False, na=False)
    df_sync["object_name"] = np.select(
        [
            df_sync["project_title"].str.contains("СП.ЛЛУ|стандарт|узлы|узел|библиотека", case=False, na=False),
            mask_atom,
            df_sync["project_title"].str.contains("K01", case=False, na=False),
            df_sync["project_title"].str.contains("ИНПРО", case=False, na=False),
            df_sync["project_title"].str.contains("Ялта", case=False, na=False)
        ],
        ["Узлы и стандарты", "АТОМ", "Кортрос", "ИНПРО", "Ялта"],
        default="Неизвестные проекты"
    )

    # Отсоединено
    df_sync["is_detached"] = df_sync["project_title"].str.contains("отсоединено", case=False, na=False).astype(int)

    # File storage name
    project_parts = df_sync["project_title"].astype(str).str.split("_")
    last_part = project_parts.str[-1].str.strip().str.lower()
    username_lower = df_sync["username"].astype(str).str.strip().str.lower()
    
    mask_match = (last_part == username_lower) & (project_parts.str.len() >= 2) & (~df_sync["username"].isna())
    df_sync["file_storage_name"] = df_sync["project_title"].copy()
    df_sync.loc[mask_match, "file_storage_name"] = project_parts[mask_match].str[:-1].str.join("_")

    # Раздел и стадия
    df_sync["project_solution_name"] = df_sync.apply(
        lambda row: get_project_solution(row["project_title"], row["object_name"]), axis=1
    )
    df_sync["project_stage_name"] = df_sync.apply(
        lambda row: get_project_stage(row["project_title"], row["object_name"]), axis=1
    )

    # Заполнение пустых
    fill_values = {}
    for col in df_sync.columns:
        if df_sync[col].dtype == 'object':
            fill_values[col] = "Нет данных"
        elif pd.api.types.is_numeric_dtype(df_sync[col]):
            fill_values[col] = 0
        elif pd.api.types.is_datetime64_any_dtype(df_sync[col]):
            fill_values[col] = pd.NaT

    df_sync.fillna(fill_values, inplace=True)

    # Разделение
    df_sync_bim = df_sync[(df_sync['is_bim'] == True) & (df_sync['is_detached'] == 0)].copy()
    df_sync_designers = df_sync[(df_sync['is_bim'] == False) & (df_sync['is_detached'] == 0)].copy()

    print(f"Трансформация ProjectSync завершена:")
    print(f"  - Designers: {len(df_sync_designers)} строк")
    print(f"  - BIM: {len(df_sync_bim)} строк")

    return df_sync_designers, df_sync_bim