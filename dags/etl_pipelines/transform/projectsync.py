"""
Transform модуль для ProjectSync pipeline.
Обрабатывает старые и новые данные синхронизации проектов.
"""
import pandas as pd
import numpy as np
from difflib import SequenceMatcher
from typing import Tuple

from common.config import BIM_USERS
from common.utils import get_project_solution, get_project_stage

# Порог похожести фамилии в хвосте project_title и username (0..1).
# 0.7 покрывает опечатки вроде "baraz" vs "barats" (ratio=0.727)
# и "ivanov" vs "ivanofff" (ratio=0.714), но отсекает реально разные фамилии
# (ivanov vs petrov = 0.333). Можно подкрутить при необходимости.
USERNAME_SIMILARITY_THRESHOLD = 0.7

# Колонки, которые НЕ должны попасть в итоговую таблицу.
DROP_COLUMNS_FINAL = ["enabled", "project_doc_section", "company"]


def _strip_username_tail(project_title: str, username: str) -> str:
    """
    Отбрасывает хвост project_title после последнего '_', если он похож на username.
    Использует SequenceMatcher для устойчивости к опечаткам (например, baraz vs barats).

    Возвращает оригинальный project_title, если:
    - меньше двух частей в названии;
    - username пустой/NaN;
    - похожесть хвоста и username ниже порога.
    """
    if not isinstance(project_title, str) or not project_title:
        return project_title
    if not isinstance(username, str) or not username or username.lower() == "nan":
        return project_title

    parts = project_title.split("_")
    if len(parts) < 2:
        return project_title

    tail = parts[-1].strip().lower()
    user = username.strip().lower()
    if not tail or not user:
        return project_title

    # Точное совпадение — быстрый путь
    if tail == user:
        return "_".join(parts[:-1])

    # Fuzzy: похожесть по Левенштейну (через SequenceMatcher)
    ratio = SequenceMatcher(None, tail, user).ratio()
    if ratio >= USERNAME_SIMILARITY_THRESHOLD:
        return "_".join(parts[:-1])

    return project_title


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

    # Подготовка справочника AD (берем только нужные поля)
    df_ad_clean = df_ad[["id", "display_name", "email", "department", "project_doc_section", "company", "enabled"]].copy()

    # 2. Обработка СТАРЫХ данных (legacy)
    # В старых данных уже есть username и user_display_name, поэтому AD не подключаем.
    # Просто приводим названия колонок к общему стандарту.
    df_legacy = df_legacy.rename(columns={
        "project_name": "project_title",
        "user_display_name": "display_name"
    })
    # Убираем лишние колонки (если они есть)
    df_legacy = df_legacy.drop(columns=["program_name", "program_version"], errors="ignore")


    # 3. Обработка НОВЫХ данных (revit)
    # В новых данных нет имен, только user_id, поэтому джоиним с AD по GUID
    df_new = df_new.merge(
        df_ad_clean,
        how="left",
        left_on="user_id",
        right_on="id"
    )

    # Убираем технические колонки
    df_new = df_new.drop(columns=["cad_program_id", "cad_program_version", "id"], errors="ignore")

    # Достаем username из email для алгоритма обрезки файлов
    df_new["username"] = df_new["email"].astype(str).str.split("@").str[0]
    # Защита: если email пустой, ставим NaN, чтобы не получить текст "nan"
    df_new.loc[df_new["email"].isna(), "username"] = np.nan


    # 3.5 Фильтрация: legacy строго до 15 февраля, new начиная с 15 февраля
    if "date" in df_legacy.columns:
        df_legacy["date"] = pd.to_datetime(df_legacy["date"], errors="coerce")
        df_legacy = df_legacy[df_legacy["date"] < "2026-02-15"]

    if "date" in df_new.columns:
        df_new["date"] = pd.to_datetime(df_new["date"], errors="coerce")
        df_new = df_new[df_new["date"] >= "2026-02-15"]


    # 4. ОБЪЕДИНЕНИЕ СТАРОЙ И НОВОЙ БАЗЫ
    df_sync = pd.concat([df_legacy, df_new], ignore_index=True)

    # 4.1 Приводим date к datetime ДО fillna, чтобы fillna не превратил NaT в строку.
    # Некорректные значения станут NaT и будут отфильтрованы ниже.
    if "date" in df_sync.columns:
        df_sync["date"] = pd.to_datetime(df_sync["date"], errors="coerce")
        rows_before = len(df_sync)
        df_sync = df_sync[df_sync["date"].notna()].copy()
        rows_dropped = rows_before - len(df_sync)
        if rows_dropped > 0:
            print(f"Отброшено {rows_dropped} строк с некорректной/пустой date")
    else:
        print("ВНИМАНИЕ: колонка 'date' отсутствует в данных")


    # 5. БИЗНЕС-ЛОГИКА (применяется ко всем данным сразу)

    # Флаг BIM (проверяется по ФИО, которое есть и там, и там)
    df_sync["is_bim"] = df_sync["display_name"].isin(BIM_USERS)

    # Короткое название
    df_sync['short_project_name'] = df_sync['project_title'].astype(str).str.split('_').str[:2].str.join('_')

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

    # Source: RSN в начале project_path -> Revit Server, иначе Locale
    if "project_path" in df_sync.columns:
        path_str = df_sync["project_path"].astype(str).str.strip()
        is_rsn = path_str.str.startswith("RSN", na=False)
        df_sync["source"] = np.where(is_rsn, "Revit Server", "Locale")
    else:
        df_sync["source"] = "Locale"
        print("ВНИМАНИЕ: колонка 'project_path' отсутствует — source проставлен 'Locale' для всех строк")

    # File storage name: отбрасываем фамилию пользователя с хвоста project_title.
    # Используем fuzzy match (SequenceMatcher), чтобы покрывать опечатки типа baraz/barats.
    df_sync["file_storage_name"] = df_sync.apply(
        lambda row: _strip_username_tail(row.get("project_title"), row.get("username")),
        axis=1
    )

    # Раздел и стадия
    df_sync["project_solution_name"] = df_sync.apply(
        lambda row: get_project_solution(row["project_title"], row["object_name"]), axis=1
    )
    df_sync["project_stage_name"] = df_sync.apply(
        lambda row: get_project_stage(row["project_title"], row["object_name"]), axis=1
    )

    # 6. Удаляем колонки, которые не нужны в итоговой таблице
    df_sync = df_sync.drop(columns=DROP_COLUMNS_FINAL, errors="ignore")

    # 7. Заполнение пустых значений.
    # ВАЖНО: datetime-колонки (включая date) уже очищены выше и заполнять их нельзя.
    fill_values = {}
    for col in df_sync.columns:
        if pd.api.types.is_datetime64_any_dtype(df_sync[col]):
            continue  # date уже без NaT, остальные datetime пропускаем
        if df_sync[col].dtype == 'object':
            fill_values[col] = "Нет данных"
        elif pd.api.types.is_numeric_dtype(df_sync[col]):
            fill_values[col] = 0

    df_sync.fillna(fill_values, inplace=True)

    # 8. Финальная фильтрация: убираем строки без идентифицированного пользователя.
    # После fillna username для строк без email/match с AD = "Нет данных".
    rows_before = len(df_sync)
    df_sync = df_sync[df_sync["username"] != "Нет данных"].copy()
    rows_dropped = rows_before - len(df_sync)
    if rows_dropped > 0:
        print(f"Отброшено {rows_dropped} строк с username='Нет данных'")

    # 9. Разделение на BIM и Designers
    df_sync_bim = df_sync[(df_sync['is_bim'] == True) & (df_sync['is_detached'] == 0)].copy()
    df_sync_designers = df_sync[(df_sync['is_bim'] == False) & (df_sync['is_detached'] == 0)].copy()

    print(f"Трансформация ProjectSync завершена:")
    print(f"  - Designers: {len(df_sync_designers)} строк")
    print(f"  - BIM: {len(df_sync_bim)} строк")

    return df_sync_designers, df_sync_bim
