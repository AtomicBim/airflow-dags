import csv
import requests
from pathlib import Path
from requests_ntlm import HttpNtlmAuth
from airflow.hooks.base import BaseHook

def extract_sharepoint_data(conn_id: str, list_guid: str, output_path: Path, select_fields: list):
    """Извлекает данные из списка SharePoint и сохраняет в CSV."""
    connection = BaseHook.get_connection(conn_id) [cite: 284]
    base_url = connection.host
    username = connection.login
    password = connection.password
    
    select_query = ",".join(select_fields)
    url = f"{base_url}_api/web/lists(guid'{list_guid}')/items?$select={select_query}"
    headers = {"Accept": "application/json; odata=verbose"} [cite: 374]
    
    session = requests.Session()
    session.auth = HttpNtlmAuth(username, password) [cite: 377]
    
    all_items = []
    while url:
        response = session.get(url, headers=headers, verify=False)
        response.raise_for_status()
        data = response.json()
        items = data.get('d', {}).get('results', [])
        all_items.extend(items)
        url = data.get('d', {}).get('__next') [cite: 390]

    if not all_items:
        print(f"No items found for list {list_guid}.")
        # Создаем пустой файл с заголовками, чтобы следующие шаги не упали
        with open(output_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=select_fields)
            writer.writeheader()
        return

    # Удаляем метаданные из ключей для чистоты CSV
    clean_items = []
    for item in all_items:
        clean_item = {k: v for k, v in item.items() if k != '__metadata'}
        clean_items.append(clean_item)

    fieldnames = clean_items[0].keys()
    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_items) [cite: 399]
    
    print(f"SharePoint: выгружено {len(clean_items)} записей, сохранено в {output_path}")