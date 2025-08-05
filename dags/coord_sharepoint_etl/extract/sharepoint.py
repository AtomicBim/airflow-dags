import csv
import json
import logging
import pandas as pd

from pathlib import Path
from typing import Dict, List
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException

from ..config import (
    RAW_DIR, 
    SHAREPOINT_CONFIG, 
    SHAREPOINT_TASK_FIELDS
)

logger = logging.getLogger(__name__)

class SharePointExtractor:
    """Класс для извлечения данных из SharePoint"""
    
    def __init__(self, conn_id: str = None):
        self.conn_id = conn_id or SHAREPOINT_CONFIG['conn_id']
        self.http_hook = HttpHook(method='GET', http_conn_id=self.conn_id)
        
    def extract_tasks(self, output_path: Path = None) -> Path:
        """
        Извлекает задачи из SharePoint списка
        
        Args:
            output_path: Путь для сохранения CSV файла
            
        Returns:
            Path: Путь к сохраненному файлу
        """
        if output_path is None:
            output_path = RAW_DIR / 'sharepoint_export_tasks.csv'
            
        logger.info(f"Начало извлечения задач SharePoint в {output_path}")
        
        # Формируем URL с выбранными полями
        select_fields = ",".join(SHAREPOINT_TASK_FIELDS)
        endpoint = (
            f"/_api/web/lists(guid'{SHAREPOINT_CONFIG['tasks_list_guid']}')"
            f"/items?$select={select_fields}"
        )
        
        all_items = []
        page_count = 0
        
        # Пагинация через __next
        while endpoint:
            try:
                response = self.http_hook.run(
                    endpoint=endpoint,
                    headers={"Accept": "application/json;odata=verbose"}
                )
                
                data = json.loads(response.text)
                items = data['d']['results']
                all_items.extend(items)
                
                # Получаем URL следующей страницы
                endpoint = data['d'].get('__next')
                if endpoint:
                    # Извлекаем только путь после базового URL
                    endpoint = endpoint.replace(SHAREPOINT_CONFIG['base_url'], '')
                
                page_count += 1
                logger.info(f"Обработана страница {page_count}, получено {len(items)} записей")
                
            except Exception as e:
                logger.error(f"Ошибка при извлечении данных: {str(e)}")
                raise AirflowException(f"Не удалось извлечь данные из SharePoint: {str(e)}")
        
        # Сохраняем в CSV
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=SHAREPOINT_TASK_FIELDS)
            writer.writeheader()
            for item in all_items:
                writer.writerow({key: item.get(key, '') for key in SHAREPOINT_TASK_FIELDS})
        
        logger.info(f"Сохранено {len(all_items)} задач в {output_path}")
        return output_path
    
    def extract_users(self, output_path: Path = None) -> Path:
        """
        Извлекает пользователей SharePoint
        
        Args:
            output_path: Путь для сохранения CSV файла
            
        Returns:
            Path: Путь к сохраненному файлу
        """
        if output_path is None:
            output_path = RAW_DIR / 'sharepoint_export_users.csv'
            
        logger.info(f"Начало извлечения пользователей SharePoint в {output_path}")
        
        try:
            response = self.http_hook.run(
                endpoint="/_api/web/siteusers",
                headers={"Accept": "application/json;odata=verbose"}
            )
            
            data = json.loads(response.text)
            users = data['d']['results']
            
            # Создаем DataFrame и сохраняем только нужные поля
            df_users = pd.DataFrame(users)
            df_users = df_users[['Id', 'Title', 'Email', 'LoginName']]
            
            df_users.to_csv(output_path, index=False, encoding='utf-8')
            
            logger.info(f"Сохранено {len(df_users)} пользователей в {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении пользователей: {str(e)}")
            raise AirflowException(f"Не удалось извлечь пользователей из SharePoint: {str(e)}")

def extract_sharepoint_tasks(**context) -> Dict[str, str]:
    """Airflow task для извлечения задач SharePoint"""
    extractor = SharePointExtractor()
    tasks_path = extractor.extract_tasks()
    
    # Сохраняем путь в XCom для следующих задач
    return {'tasks_path': str(tasks_path)}

def extract_sharepoint_users(**context) -> Dict[str, str]:
    """Airflow task для извлечения пользователей SharePoint"""
    extractor = SharePointExtractor()
    users_path = extractor.extract_users()
    
    # Сохраняем путь в XCom для следующих задач
    return {'users_path': str(users_path)}