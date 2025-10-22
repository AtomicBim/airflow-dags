"""
Transform модуль для GitLab Analytics pipeline.
Объединяет данные GitLab LOC с плагинами и обновляет маппинг.
"""
import json
import pandas as pd
from typing import Tuple


def transform_gitlab_analytics(
    gitlab_path: str,
    plugin_path: str,
    mapping_path: str,
    **context
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Трансформирует данные для gitlab analytics pipeline.

    Args:
        gitlab_path: Путь к JSON с данными GitLab
        plugin_path: Путь к CSV с данными плагинов
        mapping_path: Путь к CSV с маппингом gitlab-plugins

    Returns:
        Tuple: (df_gitlab - итоговый DataFrame, df_new_mappings - новые маппинги для добавления в GSheet)
    """
    # Чтение данных
    df_mapping = pd.read_csv(mapping_path, encoding='utf-8', sep=',')
    df_mapping.columns = df_mapping.columns.str.strip().str.replace('\ufeff', '', regex=False)

    with open(gitlab_path, encoding='utf-8') as f:
        gitlab = json.load(f)
    df_gitlab = pd.json_normalize(gitlab)

    df_plugin = pd.read_csv(plugin_path)

    # === Проверка маппинга и поиск новых проектов ===
    # 1. Фильтрация по префиксу
    df_revit = df_gitlab[df_gitlab["name"].str.startswith("plugins/revit/")].copy()

    # 2. Извлечение имени плагина
    df_revit["plugin_name"] = df_revit["name"].str.replace("^plugins/revit/", "", regex=True)

    # 3. Точное сравнение с учётом регистра
    known_plugins = df_mapping["gitlab_name"].tolist()
    df_new = df_revit[~df_revit["plugin_name"].isin(known_plugins)].copy()

    # 4. Подготовка к вставке
    df_new_to_add = df_new[["plugin_name", "id"]].rename(columns={
        "plugin_name": "gitlab_name",
        "id": "gitlab_id"
    })
    df_new_to_add["gitlab_id"] = df_new_to_add["gitlab_id"].apply(lambda x: f"{int(x)}.0")

    print(f"Найдено новых GitLab проектов для маппинга: {len(df_new_to_add)}")

    # === Merge с маппингом ===
    df_gitlab = df_gitlab.merge(
        df_mapping[["gitlab_id", "tim_guid"]],
        how="left",
        left_on="id",
        right_on="gitlab_id"
    )
    df_gitlab.drop(columns=["gitlab_id"], inplace=True)

    # === Merge с plugin ===
    df_gitlab = df_gitlab.merge(
        df_plugin[["id", "display_name"]],
        how="left",
        left_on="tim_guid",
        right_on="id"
    )

    df_gitlab.drop(columns=["id_y"], inplace=True)
    df_gitlab.rename(columns={"id_x": "id"}, inplace=True)

    df_gitlab = df_gitlab.merge(
        df_plugin[['id', 'developer']],
        how='left',
        left_on='tim_guid',
        right_on='id'
    )

    df_gitlab.drop(columns=["id_y"], inplace=True)
    df_gitlab.rename(columns={"id_x": "id"}, inplace=True)

    # === Переименование колонок ===
    df_gitlab.rename(columns={
        "id": "gitlab_id",
        "name": "gitlab_name",
        "chosen_branch": "gitlab_branch"
    }, inplace=True)

    print(f"Трансформация GitLab завершена. Строк: {len(df_gitlab)}")

    return df_gitlab, df_new_to_add
