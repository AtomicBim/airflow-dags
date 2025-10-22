"""
Transform модуль для GitLab Analytics pipeline.
Трансформирует данные GitLab LOC для загрузки в datalake.
"""
import json
import pandas as pd


def transform_gitlab_analytics(
    gitlab_path: str,
    plugin_path: str,
    **context
) -> pd.DataFrame:
    """
    Трансформирует данные для gitlab analytics pipeline.

    Args:
        gitlab_path: Путь к JSON с данными GitLab
        plugin_path: Путь к CSV с данными плагинов (не используется, оставлен для совместимости)

    Returns:
        pd.DataFrame: Итоговый DataFrame с данными GitLab
    """
    # Чтение данных
    with open(gitlab_path, encoding='utf-8') as f:
        gitlab = json.load(f)
    df_gitlab = pd.json_normalize(gitlab)

    # === Переименование колонок ===
    df_gitlab.rename(columns={
        "id": "gitlab_id",
        "name": "gitlab_name",
        "chosen_branch": "gitlab_branch"
    }, inplace=True)

    print(f"Трансформация GitLab завершена. Строк: {len(df_gitlab)}")

    return df_gitlab
