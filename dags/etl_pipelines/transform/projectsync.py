"""
Transform модуль для ProjectSync pipeline.
Обрабатывает данные синхронизации проектов с классификацией по BIM/дизайнерам.
"""
import pandas as pd
import numpy as np
from typing import Tuple

# Импорт из централизованной конфигурации и утилит
from common.config import BIM_USERS
from common.utils import extract_short_name, extract_file_storage_name, get_project_solution, get_project_stage


def transform_projectsync_analytics(
    ad_path: str,
    sync_path: str,
    **context
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Трансформирует данные для projectsync pipeline с гибридным слиянием.
    До 01.03.2026: по логину в названии проекта (хвост).
    После 01.03.2026: по user_id (GUID).
    """
    # Чтение данных
    df_ad = pd.read_csv(ad_path)
    df_sync = pd.read_csv(sync_path)

    # === Гибридное слияние с AD (по дате 01.03.2026) ===
    
    # Переводим дату в формат datetime для безопасного сравнения
    df_sync["date_parsed"] = pd.to_datetime(df_sync["date"], errors="coerce").dt.tz_localize(None)
    cutoff_date = pd.to_datetime("2026-03-01")
    
    # Разделяем на новые и старые записи
    mask_new = df_sync["date_parsed"] >= cutoff_date
    df_sync_new = df_sync[mask_new].copy()
    df_sync_old = df_sync[~mask_new].copy()

    # --- 1. Слияние для НОВЫХ записей (>= 01.03.2026) по GUID ---
    df_sync_new = df_sync_new.merge(
        df_ad[["id", "display_name", "email", "department", "project_section", "company", "enabled"]],
        how="left",
        left_on="user_id",
        right_on="id"
    )
    if "id" in df_sync_new.columns:
        df_sync_new = df_sync_new.drop(columns=["id"])

    # --- 2. Слияние для СТАРЫХ записей (< 01.03.2026) по хвосту ---
    # Готовим справочник AD: вытаскиваем логин (до @)
    df_ad_old = df_ad[["display_name", "email", "department", "project_section", "company", "enabled"]].copy()
    df_ad_old["ad_username"] = df_ad_old["email"].astype(str).str.split("@").str[0].str.lower().str.strip()
    df_ad_old = df_ad_old.drop_duplicates(subset=["ad_username"]) # Удаляем дубли, чтобы избежать размножения строк

    # Извлекаем "хвост" из project_title в старых записях (все, что после последнего "_")
    df_sync_old["project_title_tail"] = df_sync_old["project_title"].astype(str).str.split("_").str[-1].str.lower().str.strip()

    df_sync_old = df_sync_old.merge(
        df_ad_old,
        how="left",
        left_on="project_title_tail",
        right_on="ad_username"
    )
    # Чистим временные колонки слияния
    df_sync_old = df_sync_old.drop(columns=["ad_username", "project_title_tail"], errors="ignore")

    # --- Склеиваем датафреймы обратно ---
    df_sync = pd.concat([df_sync_new, df_sync_old], ignore_index=True)
    df_sync = df_sync.drop(columns=["date_parsed"]) # Удаляем техническую колонку с датой


    # === Извлечение username из email (для всех склеенных данных) ===
    df_sync["username"] = df_sync["email"].astype(str).str.split("@").str[0]
    # Защита: если email не нашелся, ставим честный NaN (чтобы не было текста "nan")
    df_sync.loc[df_sync["email"].isna(), "username"] = np.nan

    # === Классификация пользователей ===
    df_sync["is_bim"] = df_sync["display_name"].isin(BIM_USERS)

    # === Создание короткого названия ===
    df_sync['short_project_name'] = df_sync['project_title'].astype(str).apply(extract_short_name)

    # Удаляем неиспользуемые технические колонки из новой таблицы
    cols_to_drop = [col for col in ['cad_program_id', 'cad_program_version'] if col in df_sync.columns]
    if cols_to_drop:
        df_sync = df_sync.drop(columns=cols_to_drop)

    # === Определение объекта ===
    mask_atom = df_sync["project_title"].str.contains(
        "АТОМ|ДОУ|08-12|ИКП|ATOM|АПУ", case=False, na=False
    )

    df_sync["object_name"] = np.select(
        [
            df_sync["project_title"].str.contains("СП.ЛЛУ|стандарт|узлы|узел|библиотека", case=False, na=False),
            mask_atom,
            df_sync["project_title"].str.contains("K01", case=False, na=False),
            df_sync["project_title"].str.contains("ИНПРО", case=False, na=False),
            df_sync["project_title"].str.contains("Ялта", case=False, na=False)
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
    df_sync["is_detached"] = df_sync["project_title"].str.contains("отсоединено", case=False, na=False).astype(int)

    # === Извлечение имени файлового хранилища (векторизованная версия) ===
    project_parts = df_sync["project_title"].astype(str).str.split("_")
    last_part = project_parts.str[-1].str.strip().str.lower()
    username_lower = df_sync["username"].astype(str).str.strip().str.lower()
    
    # Добавлено условие: ~df_sync["username"].isna(), чтобы пустые не матчились с пустыми
    mask_match = (last_part == username_lower) & (project_parts.str.len() >= 2) & (~df_sync["username"].isna())
    
    df_sync["file_storage_name"] = df_sync["project_title"].copy()
    df_sync.loc[mask_match, "file_storage_name"] = project_parts[mask_match].str[:-1].str.join("_")

    # === Определение раздела и стадии проекта ===
    df_sync["project_solution_name"] = df_sync.apply(
        lambda row: get_project_solution(row["project_title"], row["object_name"]), axis=1
    )
    df_sync["project_stage_name"] = df_sync.apply(
        lambda row: get_project_stage(row["project_title"], row["object_name"]), axis=1
    )

    # === Заполнение пропусков ===
    fill_values = {}
    for col in df_sync.columns:
        if df_sync[col].dtype == 'object':
            fill_values[col] = "Нет данных"
        elif pd.api.types.is_numeric_dtype(df_sync[col]):
            fill_values[col] = 0
        elif pd.api.types.is_datetime64_any_dtype(df_sync[col]):
            fill_values[col] = pd.NaT

    df_sync.fillna(fill_values, inplace=True)

    # === Разделение на BIM и designers (только не отсоединенные) ===
    df_sync_bim = df_sync[(df_sync['is_bim'] == True) & (df_sync['is_detached'] == 0)].copy()
    df_sync_designers = df_sync[(df_sync['is_bim'] == False) & (df_sync['is_detached'] == 0)].copy()

    print(f"Трансформация ProjectSync завершена:")
    print(f"  - Designers: {len(df_sync_designers)} строк")
    print(f"  - BIM: {len(df_sync_bim)} строк")

    return df_sync_designers, df_sync_bim