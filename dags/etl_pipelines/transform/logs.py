"""
Transform модуль для Logs Analytics pipeline.
Обрабатывает логи с классификацией по BIM/дизайнерам.
"""
import pandas as pd
from typing import Tuple

# Импорт из централизованной конфигурации
from config import BIM_USERS


def transform_logs_analytics(
    logs_path: str,
    plugin_path: str,
    **context
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Трансформирует данные для logs analytics pipeline.

    Args:
        logs_path: Путь к CSV с логами
        plugin_path: Путь к CSV с данными плагинов

    Returns:
        Tuple: (df_designers, df_bim)
    """
    # Чтение данных
    df_plugin = pd.read_csv(plugin_path)
    df_logs = pd.read_csv(logs_path)

    # === Удаление ненужных столбцов ===
    columns_to_drop = [
        'plugin_version', 'username',
        'project_name', 'additional_message',
        'program_name', 'program_version'
    ]
    df_logs.drop(columns=[col for col in columns_to_drop if col in df_logs.columns], inplace=True)

    # === Слияние с плагинами ===
    df_logs = df_logs.merge(
        df_plugin[["id", "display_name", "developer"]],
        how="left",
        left_on="plugin_id",
        right_on="id"
    )
    df_logs.drop(columns=['id'], inplace=True)

    # === Классификация пользователей ===
    df_logs["is_bim"] = df_logs["user_display_name"].isin(BIM_USERS)

    # === Разделение на BIM и designers ===
    df_logs_bim = df_logs[df_logs['is_bim'] == True].copy()
    df_logs_designers = df_logs[df_logs['is_bim'] == False].copy()

    print(f"Трансформация Logs завершена:")
    print(f"  - Designers: {len(df_logs_designers)} строк")
    print(f"  - BIM: {len(df_logs_bim)} строк")

    return df_logs_designers, df_logs_bim
