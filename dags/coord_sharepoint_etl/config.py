import os
from pathlib import Path

# Пути
ETL_DATA_ROOT_PATH = Path(os.environ.get('ETL_DATA_ROOT_PATH', '/opt/airflow/data'))
RAW_DIR = ETL_DATA_ROOT_PATH / 'raw_data'
PROCESSED_DIR = ETL_DATA_ROOT_PATH / 'processed'

# Создание директорий если не существуют
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Подключения к базам данных
DB_CONNECTIONS = {
    'postgres': {
        'conn_id': 'tim_db_postgres',
        'schema': 'datalake',
        'table': 'ext_sharepoint_coord'
    },
    'pluginsdb': {
        'conn_id': 'tim_db_pluginsdb',
        'schema': 'users',
        'table': 'ad_user'
    }
}

# sharepoint настройки
SHAREPOINT_CONFIG = {
    'base_url': 'https://life.atomsk.ru/BIM',
    'tasks_list_guid': 'e3b526d1-7461-454b-92d9-1d9ea33e5098',
    'conn_id': 'askit_http_sharepoint_tim'
}

# Поля для извлечения из sharepoint
SHAREPOINT_TASK_FIELDS = [
    "PO_ESP", "discipline", "comment", "Title",
    "GUID", "Priority", "Created", "responsibleId",
    "closing_date", "short_description", "Type_request", 
    "Status", "applicantId", "detailed_description"
]

# Маппинги для трансформации
DISCIPLINE_MAPPING = {
    'КР/КЖ': 'Конструкции',
    'АР/ЭП': 'Архитектура',
    'ОВ/ВК/ТХ': 'Инженерные системы',
    'Другое': 'Другое',
    'Сметы': 'Сметы',
    'ГП/ПОС': 'Генплан и ПОС',
    'ПТО': 'ПТО',
    'Внутренняя BIM': 'Внутренняя BIM',
    'КАСК': 'ПТО',
    'ЭЛ/СС/АК': 'Электросети и связь',
    'ТС/НВК/НСС': 'Наружные сети',
    'Распределять по площадям': 'Другое',
    'Распределять вручную': 'Другое',
    # Для Google Sheets
    'АР': 'Архитектура',
    'ВК': 'Инженерные системы',
    'ОВ': 'Инженерные системы',
    'КЖ': 'Конструкции',
    'ТХ': 'Инженерные системы',
    'ЭЛ': 'Электросети и связь',
    'КМ': 'Конструкции',
}

# Маппинг групп внутри sharepoint к адекватным
TYPE_REQUEST_MAPPING = {
    'Совместная работа': 'Консультирование',
    'Семейства': 'Семейства',
    'BIM поддержка': 'Консультирование',
    'Проверка ИМ': 'Аудит модели',
    'Выгрузка из BIM Tangl': 'Tangl',
    'Другое': 'Другое',
    'Виды, листы, спецификации': 'Консультирование',
    'Аудит модели': 'Аудит модели',
    'Обучение': 'Обучение',
    'Корректировка модели': 'Корректировка модели',
    'Экспорт данных': 'Аудит модели',
    'Нет данных': 'Другое',
    'Работа с объектами Civil': 'Civil',
    'Задачи BIM Tangl': 'Tangl',
    'Блоки': 'Другое',
    'Проверка файлов': 'Аудит модели'
}

# Список для фильтрации тех, кто не работает по заявкам
FORBIDDEN_RESPONSIBLE = ['овсянкин', 'кузовлева', 'кичигин', 'андреев', 'романова', 'урманчеев']

# Список для фильтрации уволенных
REMOVE_RESPONSIBLE = ['мельникова', 'шишляева']

# GUIDs заявок для удаления - всякие мусорные заявки с ошибками в sharepoint
TASKS_TO_DELETE = [
    "6ce81543-eedb-4048-b22c-f0d5771d2d2d",
    "749b69f9-45db-49ca-93ef-9591fa2ce042",
    "cf68a4c4-471c-4c59-b67e-01ccacf0b536",
    "0f315791-cfbb-4af9-86a0-2d7c6fb77d47",
    "7b390bca-e355-4438-a4fe-4f65a7b21ec7",
    "8a95f7cb-fc19-45e6-ad33-7da08411acb6",
]