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
    # Соединяем по UUID (user_id в таблице sync и id в таблице AD)
    df_sync = df_sync.merge(
        df_ad[["id", "display_name", "email", "department", "project_section", "company", "enabled"]],
        how="left",
        left_on="user_id",
        right_on="id"
    )
    
    # Удаляем дублирующийся столбец id после слияния, если он есть
    if "id" in df_sync.columns:
        df_sync = df_sync.drop(columns=["id"])

    # === Извлечение username из email ===
    # Если email ivanov_i@atomsk.ru, то username будет ivanov_i
    df_sync["username"] = df_sync["email"].astype(str).str.split("@").str[0]

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
    # Разбиваем project_title на части
    project_parts = df_sync["project_title"].astype(str).str.split("_")
    # Получаем последнюю часть
    last_part = project_parts.str[-1].str.strip().str.lower()
    # Получаем username в нижнем регистре (тот, что вытащили из email)
    username_lower = df_sync["username"].str.strip().str.lower()
    
    # Проверяем совпадение
    mask_match = (last_part == username_lower) & (project_parts.str.len() >= 2)
    # Создаем file_storage_name: если совпадает - убираем последнюю часть, иначе оставляем как есть
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
    # Определяем значения для заполнения в один проход
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