"""
Extractors для Google Sheets.
Извлекают маппинги из Google Sheets таблиц.
"""
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from pathlib import Path


def extract_gitlab_mapping(
    service_account_path: str,
    spreadsheet_key: str,
    worksheet_name: str,
    output_path: str,
    **context
) -> str:
    """
    Экспортирует маппинг GitLab-плагинов из Google Sheets.

    Args:
        service_account_path: Путь к JSON файлу service account
        spreadsheet_key: ID Google Spreadsheet
        worksheet_name: Название листа (обычно 'gitlab-plugins')
        output_path: Путь для сохранения CSV
    """
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    creds = Credentials.from_service_account_file(service_account_path, scopes=SCOPES)
    ws = (
        gspread.authorize(creds)
        .open_by_key(spreadsheet_key)
        .worksheet(worksheet_name)
    )

    print(f"Загрузка данных из таблицы '{worksheet_name}'...")
    values = ws.get_all_values()
    header = values[0]
    data_rows = values[1:]

    print(f"Получено строк данных: {len(data_rows)}")

    # Подчищаем данные
    cleaned = []
    for r in data_rows:
        r = r + [''] * (len(header) - len(r))
        r = [c.replace('\r', ' ').replace('\n', ' ') for c in r]
        cleaned.append(r)

    # Сохраняем данные
    df = pd.DataFrame(cleaned, columns=header)
    df.to_csv(output_path, index=False, header=True, encoding='utf-8')

    print(f"Экспорт маппинга GitLab завершен. Сохранено {len(df)} записей в {output_path}")
    return output_path


def extract_instructions(
    service_account_path: str,
    spreadsheet_key: str,
    worksheet_name: str,
    output_path: str,
    **context
) -> str:
    """
    Экспортирует инструкции из Google Sheets.

    Args:
        service_account_path: Путь к JSON файлу service account
        spreadsheet_key: ID Google Spreadsheet
        worksheet_name: Название листа
        output_path: Путь для сохранения CSV
    """
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    creds = Credentials.from_service_account_file(service_account_path, scopes=SCOPES)
    ws = (
        gspread.authorize(creds)
        .open_by_key(spreadsheet_key)
        .worksheet(worksheet_name)
    )

    print(f"Загрузка данных из таблицы '{worksheet_name}'...")
    values = ws.get_all_values()
    header = values[0]
    data_rows = values[1:]

    print(f"Получено строк данных: {len(data_rows)}")

    # Подчищаем данные
    cleaned = []
    for r in data_rows:
        r = r + [''] * (len(header) - len(r))
        r = [c.replace('\r', ' ').replace('\n', ' ') for c in r]
        cleaned.append(r)

    # Сохраняем данные
    df = pd.DataFrame(cleaned, columns=header)
    df.to_csv(output_path, index=False, header=True, encoding='utf-8')

    print(f"Экспорт инструкций завершен. Сохранено {len(df)} записей в {output_path}")
    return output_path


def append_new_gitlab_mappings(
    service_account_path: str,
    spreadsheet_key: str,
    worksheet_name: str,
    new_mappings_df: pd.DataFrame,
    **context
) -> int:
    """
    Добавляет новые маппинги GitLab в Google Sheets.
    Используется в transform pipeline для обновления таблицы.

    Args:
        service_account_path: Путь к JSON файлу service account
        spreadsheet_key: ID Google Spreadsheet
        worksheet_name: Название листа (обычно 'gitlab-plugins')
        new_mappings_df: DataFrame с новыми маппингами для добавления

    Returns:
        Количество добавленных строк
    """
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    creds = Credentials.from_service_account_file(service_account_path, scopes=SCOPES)
    ws = (
        gspread.authorize(creds)
        .open_by_key(spreadsheet_key)
        .worksheet(worksheet_name)
    )

    rows_to_append = new_mappings_df.astype(str).values.tolist()

    for row in rows_to_append:
        ws.append_row(row, value_input_option='USER_ENTERED')

    print(f"Добавлено {len(rows_to_append)} новых маппингов в Google Sheets")
    return len(rows_to_append)
