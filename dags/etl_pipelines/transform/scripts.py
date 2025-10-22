"""
Transform модуль для Scripts Analytics pipeline.
Объединяет данные из monitoring, plugins, gitlab для аналитики скриптов.
"""
import json
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
    'Урманчеев Роман Дамирович'
}


def extract_short_name(name: str) -> str:
    """Извлекает короткое название проекта из полного."""
    parts = name.split('_')
    return '_'.join(parts[:2]) if len(parts) >= 2 else name


def transform_scripts_analytics(
    ad_path: str,
    plugin_path: str,
    monitoring_path: str,
    plugin_development_stage_path: str,
    gitlab_path: str,
    **context
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Трансформирует данные для scripts analytics pipeline.

    Args:
        ad_path: Путь к CSV с данными AD пользователей
        plugin_path: Путь к CSV с данными плагинов
        monitoring_path: Путь к CSV с данными мониторинга
        plugin_development_stage_path: Путь к CSV со стадиями разработки
        gitlab_path: Путь к JSON с данными GitLab

    Returns:
        Tuple из трех DataFrames: (designers, bim, plugin)
    """
    # Чтение данных
    df_ad = pd.read_csv(ad_path)
    df_plugin = pd.read_csv(plugin_path)
    df_plugin_development_stage = pd.read_csv(plugin_development_stage_path)
    df_monitoring = pd.read_csv(monitoring_path)

    with open(gitlab_path, encoding='utf-8') as f:
        gitlab = json.load(f)
    df_gitlab = pd.json_normalize(gitlab)

    # === Трансформация monitoring ===
    df_monitoring['short_project_name'] = df_monitoring['project_name'].astype(str).apply(extract_short_name)

    df_monitoring = df_monitoring.drop(columns=[
        'plugin_version',
        'username',
        'program_name',
        'program_version',
        'project_name'
    ])

    df_monitoring["is_bim"] = df_monitoring["user_display_name"].isin(BIM_USERS)

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
    ])

    # === Merge monitoring + plugin ===
    df_monitoring = df_monitoring.merge(
        df_plugin,
        left_on='plugin_id',
        right_on='id',
        how='left'
    ).drop(columns=['id'])

    # === Заполнение пропусков ===
    str_cols = df_monitoring.select_dtypes(include='object').columns
    df_monitoring[str_cols] = df_monitoring[str_cols].fillna("Нет данных")

    num_cols = df_monitoring.select_dtypes(include=['number', 'Int64']).columns
    df_monitoring[num_cols] = df_monitoring[num_cols].fillna(0)

    date_cols = df_monitoring.select_dtypes(include='datetime').columns
    df_monitoring[date_cols] = df_monitoring[date_cols].fillna(pd.NaT)

    # === Разделение на BIM и designers ===
    df_monitoring_bim = df_monitoring[df_monitoring['is_bim'] == True].copy()
    df_monitoring_designers = df_monitoring[df_monitoring['is_bim'] == False].copy()

    print(f"Трансформация завершена:")
    print(f"  - Designers: {len(df_monitoring_designers)} строк")
    print(f"  - BIM: {len(df_monitoring_bim)} строк")
    print(f"  - Plugins: {len(df_plugin)} строк")

    return df_monitoring_designers, df_monitoring_bim, df_plugin
