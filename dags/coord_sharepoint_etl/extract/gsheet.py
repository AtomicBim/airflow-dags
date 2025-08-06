import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from pathlib import Path
import csv
import json
from airflow.models import Variable

def extract_families_from_gsheet(sheet_key: str, worksheet_name: str, output_path: str, keyfile_path: str = None, **context) -> str:
    """
    Извлекает и очищает данные по семействам из Google Sheets.
    Приоритет: 
    1. Airflow Variable 'gsheet_service_account_json'
    2. Файл по пути keyfile_path (если указан)
    """
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    # Попробуем получить credentials из Airflow Variable
    try:
        # Получаем JSON строку из Variable
        service_account_json = Variable.get("gsheet_service_account_json")
        
        # Парсим JSON строку в словарь
        if isinstance(service_account_json, str):
            service_account_info = json.loads(service_account_json)
        else:
            service_account_info = service_account_json
            
        creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        print("✓ Используются credentials из Airflow Variable 'gsheet_service_account_json'")
        
    except Exception as e:
        print(f"⚠ Не удалось получить credentials из Variable: {e}")
        
        # Пробуем использовать файл
        if keyfile_path:
            try:
                # Проверяем существование файла
                if not Path(keyfile_path).exists():
                    raise FileNotFoundError(f"Файл не найден: {keyfile_path}")
                    
                creds = Credentials.from_service_account_file(keyfile_path, scopes=SCOPES)
                print(f"✓ Используется файл credentials: {keyfile_path}")
                
            except Exception as file_error:
                raise Exception(
                    f"Не удалось загрузить credentials:\n"
                    f"1. Variable 'gsheet_service_account_json' не настроена или содержит некорректный JSON\n"
                    f"2. Файл '{keyfile_path}' не найден или недоступен\n"
                    f"Ошибка: {file_error}"
                )
        else:
            raise Exception(
                "Не удалось получить credentials для Google Sheets:\n"
                "1. Создайте Airflow Variable 'gsheet_service_account_json' с JSON credentials\n"
                "2. Или укажите путь к файлу с credentials"
            )
    
    # Подключаемся к Google Sheets
    try:
        client = gspread.authorize(creds)
        worksheet = client.open_by_key(sheet_key).worksheet(worksheet_name)
        print(f"✓ Подключено к Google Sheet: {sheet_key}, лист: {worksheet_name}")
    except Exception as e:
        raise Exception(f"Ошибка подключения к Google Sheets: {e}")
    
    # Получаем данные
    values = worksheet.get_all_values()
    if not values:
        raise ValueError("Google Sheet пустой или недоступен")
    
    header = values[0]
    data_rows = values[1:]
    
    # Создаем DataFrame
    df = pd.DataFrame(data_rows, columns=header)
    
    # Сохраняем в CSV
    df.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')
    print(f"✓ Сохранено {len(df)} строк из Google Sheet в {output_path}")
    
    return output_path