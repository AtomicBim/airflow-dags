import pandas as pd
import requests
from requests_ntlm import HttpNtlmAuth
import urllib3
from pathlib import Path

# Убираем предупреждения о небезопасном соединении, если это приемлемо
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def extract_tasks(url: str, username: str, password: str, output_path: str, **context) -> str:
    """Извлекает задачи из SharePoint и сохраняет в CSV."""
    select_fields = ",".join([
        "PO_ESP", "discipline", "comment", "Title", "GUID", "Priority",
        "Created", "responsibleId", "closing_date", "short_description",
        "Type_request", "Status", "applicantId", "detailed_description"
    ])
    full_url = f"{url}/lists(guid'e3b526d1-7461-454b-92d9-1d9ea33e5098')/items?$select={select_fields}"
    headers = {"Accept": "application/json;odata=verbose"}
    
    all_items = []
    while full_url:
        response = requests.get(
            full_url, auth=HttpNtlmAuth(username, password), headers=headers, verify=False
        )
        response.raise_for_status() # Вызовет исключение при плохом статусе
        data = response.json()
        all_items.extend(data['d']['results'])
        full_url = data['d'].get('__next', None)
    
    df = pd.DataFrame(all_items)
    # Оставляем только нужные колонки, чтобы избежать проблем при их отсутствии
    df = df[[key for key in select_fields.split(',') if key in df.columns]]
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Сохранено {len(df)} строк задач в {output_path}")
    return output_path

def extract_users(url: str, username: str, password: str, output_path: str, **context) -> str:
    """Извлекает пользователей из SharePoint."""
    full_url = f"{url}/siteusers"
    headers = {"Accept": "application/json;odata=verbose"}
    response = requests.get(
        full_url, auth=HttpNtlmAuth(username, password), headers=headers, verify=False
    )
    response.raise_for_status()
    users = response.json()['d']['results']
    df_users = pd.DataFrame(users)
    df_users = df_users[['Id', 'Title', 'Email', 'LoginName']]
    df_users.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Сохранено {len(df_users)} пользователей в {output_path}")
    return output_path