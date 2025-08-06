import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import json
from airflow.models import Variable

def extract_families_as_dataframe(sheet_key: str, worksheet_name: str, **context) -> pd.DataFrame:
    """
    Извлекает данные из Google Sheets и возвращает их в виде pandas DataFrame.
    Аутентификация происходит ИСКЛЮЧИТЕЛЬНО через Airflow Variable 'gsheet_service_account_json'.
    """
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    try:
        # Шаг 1: Получаем JSON-строку из Airflow Variable
        service_account_json_str = Variable.get("gsheet_service_account_json")
        
        # Шаг 2: Парсим JSON-строку в словарь
        service_account_info = json.loads(service_account_json_str)
            
        # Шаг 3: Создаем креды
        creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        print("✓ Аутентификация в Google API через Airflow Variable 'gsheet_service_account_json' прошла успешно.")
        
    except Exception as e:
        print(f"⚠️ КРИТИЧЕСКАЯ ОШИБКА: Не удалось получить или распарсить креды из Variable 'gsheet_service_account_json'.")
        # Прерываем выполнение, если креды не получены
        raise e
    
    # Подключаемся к Google Sheets
    try:
        client = gspread.authorize(creds)
        worksheet = client.open_by_key(sheet_key).worksheet(worksheet_name)
        print(f"✓ Успешное подключение к Google Sheet: {worksheet.title}")
    except Exception as e:
        raise Exception(f"Ошибка подключения к Google Sheets: {e}")
    
    # Получаем все данные из листа
    values = worksheet.get_all_values()
    if not values:
        print("⚠️ Предупреждение: Лист Google Sheet пуст или недоступен. Возвращается пустой DataFrame.")
        return pd.DataFrame()
    
    header = values[0]
    data_rows = values[1:]
    
    # Создаем DataFrame
    df = pd.DataFrame(data_rows, columns=header)
    print(f"✓ Извлечено {len(df)} строк из Google Sheet.")
    
    return df