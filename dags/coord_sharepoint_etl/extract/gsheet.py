import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from pathlib import Path
import csv

def extract_families_from_gsheet(keyfile_path: str, sheet_key: str, worksheet_name: str, output_path: str, **context) -> str:
    """Извлекает и очищает данные по семействам из Google Sheets."""
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(keyfile_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    worksheet = client.open_by_key(sheet_key).worksheet(worksheet_name)
    
    values = worksheet.get_all_values()
    header = values[0]
    data_rows = values[1:]
    
    df = pd.DataFrame(data_rows, columns=header)
    df.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')
    print(f"Сохранено {len(df)} строк из Google Sheet в {output_path}")
    return output_path