import logging
import pandas as pd

from pathlib import Path
from typing import Dict
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

from ..config import RAW_DIR, DB_CONNECTIONS

logger = logging.getLogger(__name__)

class TimDbExtractor:
    """Класс для извлечения данных из баз данных TIM"""
    
    def __init__(self):
        self.postgres_hook = PostgresHook(postgres_conn_id=DB_CONNECTIONS['postgres']['conn_id'])
        self.pluginsdb_hook = PostgresHook(postgres_conn_id=DB_CONNECTIONS['pluginsdb']['conn_id'])
    
    def extract_ad_users(self, output_path: Path = None) -> Path:
        """
        Извлекает данные пользователей AD из pluginsdb
        
        Args:
            output_path: Путь для сохранения CSV файла
            
        Returns:
            Path: Путь к сохраненному файлу
        """
        if output_path is None:
            output_path = RAW_DIR / 'tim_export_ad_user.csv'
            
        logger.info(f"Начало извлечения AD пользователей в {output_path}")
        
        try:
            # Используем COPY для эффективного экспорта
            schema = DB_CONNECTIONS['pluginsdb']['schema']
            table = DB_CONNECTIONS['pluginsdb']['table']
            
            copy_sql = f"""
                COPY (SELECT * FROM "{schema}"."{table}")
                TO STDOUT WITH CSV HEADER ENCODING 'UTF8'
            """
            
            conn = self.pluginsdb_hook.get_conn()
            cursor = conn.cursor()
            
            with open(output_path, 'w', encoding='utf-8') as f:
                cursor.copy_expert(copy_sql, f)
                
            cursor.close()
            conn.close()
            
            # Проверяем количество записей
            df = pd.read_csv(output_path)
            logger.info(f"Извлечено {len(df)} пользователей AD в {output_path}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении AD пользователей: {str(e)}")
            raise AirflowException(f"Не удалось извлечь AD пользователей: {str(e)}")
    
    def extract_gsheet_families(self, output_path: Path = None) -> Path:
        """
        Извлекает данные о семействах из Google Sheets
        Это заглушка - в реальности нужно использовать Google Sheets API
        
        Args:
            output_path: Путь для сохранения CSV файла
            
        Returns:
            Path: Путь к сохраненному файлу
        """
        if output_path is None:
            output_path = RAW_DIR / 'gsheet_export_families.csv'
            
        logger.info(f"Извлечение данных семейств Google Sheets в {output_path}")
        
        # TODO: Реализовать извлечение из Google Sheets API
        # Сейчас предполагаем, что файл уже существует
        if not output_path.exists():
            logger.warning(f"Файл {output_path} не найден. Создаем пустой файл.")
            pd.DataFrame().to_csv(output_path, index=False)
            
        return output_path

def extract_ad_users(**context) -> Dict[str, str]:
    """Airflow task для извлечения AD пользователей"""
    extractor = TimDbExtractor()
    ad_path = extractor.extract_ad_users()
    
    return {'ad_users_path': str(ad_path)}

def extract_gsheet_families(**context) -> Dict[str, str]:
    """Airflow task для извлечения данных о семействах"""
    extractor = TimDbExtractor()
    families_path = extractor.extract_gsheet_families()
    
    return {'families_path': str(families_path)}