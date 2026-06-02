"""
Transform модуль для Scripts Analytics pipeline.
Объединяет данные из monitoring (legacy + new) и plugins для аналитики скриптов.
"""
import pandas as pd
import numpy as np
from typing import Tuple

# Импорт из централизованной конфигурации и утилит
from common.config import BIM_USERS
from common.utils import extract_short_name


def transform_scripts_analytics(
    ad_path: str,
    plugin_path: str,
    monitoring_path: str,
    legacy_monitoring_path: str,
    plugin_development_stage_path: str,
    **context
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Трансформирует данные для scripts analytics pipeline.

    Args:
        ad_path: Путь к CSV с данными AD пользователей
        plugin_path: Путь к CSV с данными плагинов
        monitoring_path: Путь к CSV с НОВЫМИ данными мониторинга (plugins.monitoring)
        legacy_monitoring_path: Путь к CSV со СТАРЫМИ данными мониторинга (legacy.monitoring_legacy)
        plugin_development_stage_path: Путь к CSV со стадиями разработки

    Returns:
        Tuple из трех DataFrames: (designers, bim, plugin)
    """
    # 1. Чтение данных
    df_ad = pd.read_csv(ad_path)
    df_plugin = pd.read_csv(plugin_path)
    df_plugin_development_stage = pd.read_csv(plugin_development_stage_path)
    df_monitoring_legacy = pd.read_csv(legacy_monitoring_path)
    df_monitoring_new = pd.read_csv(monitoring_path)

    # 2. Подготовка СТАРЫХ данных мониторинга
    # В старой таблице уже есть user_display_name и username — JOIN с AD не нужен.
    # Приводим названия колонок к общему стандарту.
    df_monitoring_legacy = df_monitoring_legacy.rename(columns={
        "project_name": "project_title",
        "user_display_name": "display_name"
    })

    # 3. Подготовка НОВЫХ данных мониторинга (JOIN с AD по user_id)
    df_ad_clean = df_ad[["id", "display_name", "email", "department", "company"]].copy()
    df_monitoring_new = df_monitoring_new.merge(
        df_ad_clean,
        how="left",
        left_on="user_id",
        right_on="id"
    )
    # Генерируем username из email (для совместимости со старой структурой)
    df_monitoring_new["username"] = df_monitoring_new["email"].astype(str).str.split("@").str[0]
    df_monitoring_new.loc[df_monitoring_new["email"].isna(), "username"] = np.nan
    # Убираем технические колонки из AD
    df_monitoring_new = df_monitoring_new.drop(columns=["id"], errors="ignore")

    # 4. Объединение СТАРЫХ и НОВЫХ данных
    df_monitoring = pd.concat([df_monitoring_legacy, df_monitoring_new], ignore_index=True)

    # === Трансформация объединенного мониторинга ===
    df_monitoring['short_project_name'] = df_monitoring['project_title'].astype(str).apply(extract_short_name)

    # Удаляем технические/неинформативные колонки.
    # errors='ignore' защищает от падений, если колонки нет в одной из частей (legacy/new).
    df_monitoring = df_monitoring.drop(columns=[
        'plugin_version',
        'username',
        'program_name',          # только legacy
        'program_version',       # только legacy
        'project_name',          # legacy (на случай если rename не сработал)
        'project_title',         # уже преобразован в short_project_name
        'project_path',          # только new
        'cad_program_id',        # только new
        'cad_program_version',   # только new
        'user_id',               # только new (уже отработал в merge)
    ], errors='ignore')

    # Флаг BIM проверяем по единому полю display_name
    df_monitoring["is_bim"] = df_monitoring["display_name"].isin(BIM_USERS)

    # === Трансформация plugin ===
    df_plugin = df_plugin.merge(
        df_plugin_development_stage[['id', 'description']].rename(columns={'id': 'development_stage_id_ref'}),
        left_on='development_stage_id',
        right_on='development_stage_id_ref',
        how='left'
    ).drop(columns=['development_stage_id_ref'])

    df_plugin = df_plugin.drop(columns=[
        'development_stage_id',
        'long_description',
        'instruction_link',
        'video_link',
        'technical_specification'
    ], errors='ignore')

    # === Merge monitoring + plugin ===
    df_monitoring = df_monitoring.merge(
        df_plugin,
        left_on='plugin_id',
        right_on='id',
        how='left'
    ).drop(columns=['id'], errors='ignore')

    # === Заполнение пропусков ===
    fill_values = {}
    for col in df_monitoring.columns:
        if df_monitoring[col].dtype == 'object':
            fill_values[col] = "Нет данных"
        elif pd.api.types.is_numeric_dtype(df_monitoring[col]):
            fill_values[col] = 0
        elif pd.api.types.is_datetime64_any_dtype(df_monitoring[col]):
            fill_values[col] = pd.NaT

    df_monitoring.fillna(fill_values, inplace=True)

    # === Разделение на BIM и designers ===
    df_monitoring_bim = df_monitoring[df_monitoring['is_bim'] == True].copy()
    df_monitoring_designers = df_monitoring[df_monitoring['is_bim'] == False].copy()

    print(f"Трансформация завершена:")
    print(f"  - Legacy записей мониторинга: {len(df_monitoring_legacy)}")
    print(f"  - New записей мониторинга: {len(df_monitoring_new)}")
    print(f"  - Designers: {len(df_monitoring_designers)} строк")
    print(f"  - BIM: {len(df_monitoring_bim)} строк")
    print(f"  - Plugins: {len(df_plugin)} строк")

    return df_monitoring_designers, df_monitoring_bim, df_plugin
