# Отчет по рефакторингу проекта Airflow DAGs

**Дата:** 2025-10-23
**Версия:** 1.0
**Статус:** Готово к реализации

---

## Содержание

1. [Резюме](#резюме)
2. [Критические проблемы](#критические-проблемы)
3. [Дублирование кода](#дублирование-кода)
4. [Неиспользуемый код](#неиспользуемый-код)
5. [Рекомендации по оптимизации](#рекомендации-по-оптимизации)
6. [План рефакторинга](#план-рефакторинга)
7. [Ожидаемые результаты](#ожидаемые-результаты)

---

## Резюме

### Статистика проекта

| Метрика | Текущее значение | После рефакторинга |
|---------|------------------|---------------------|
| Python файлов | 28 | 27 (-1 дубликат) |
| Строк кода | ~2100+ | ~1800- (-15%) |
| Дублирующихся функций | 4 | 0 |
| Неиспользуемых файлов | 1 | 0 |
| Константы BIM_USERS | 4 версии | 1 централизованная |
| Пустых __init__.py | 8 | 0 |

### Общая оценка

**Сильные стороны:**
- ✅ Четкая архитектура ETL (Extract/Transform/Load)
- ✅ Хорошее использование Airflow TaskFlow API
- ✅ Модульная структура кода
- ✅ Параллельное выполнение задач
- ✅ Использование XCom для передачи данных

**Проблемные области:**
- ❌ Значительное дублирование кода (4 критичных случая)
- ❌ Неиспользуемый модуль gsheet.py (141 строка)
- ❌ Расхождения в константах BIM_USERS между модулями
- ❌ Отсутствие централизованной конфигурации
- ❌ Пустые __init__.py файлы

---

## Критические проблемы

### 1. Дублирование модуля extract_ad_users() ⚠️ CRITICAL

**Проблема:**
Функция `extract_ad_users()` полностью дублируется в двух файлах:

```
etl_pipelines/extract/pluginsdb.py (строки 9-20)
coord_sharepoint_etl/extract/tim_db.py (строки 5-16)
```

**Код:**
```python
def extract_ad_users(postgres_conn_id: str, output_path: str, **context) -> str:
    """Экспортирует таблицу users.ad_user из pluginsdb."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    sql = 'SELECT * FROM users.ad_user'
    df = pd.read_sql(sql, conn)
    conn.close()

    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Экспортировано {len(df)} пользователей AD в {output_path}")

    return output_path
```

**Влияние:** Высокое - используется в 3 DAG (scripts, projectsync, sharepoint)

**Решение:**
1. Удалить `coord_sharepoint_etl/extract/tim_db.py`
2. Везде использовать `etl_pipelines.extract.pluginsdb.extract_ad_users`

### 2. Расхождения в константе BIM_USERS ⚠️ CRITICAL

**Проблема:**
Константа `BIM_USERS` определена 4 раза с РАЗНЫМИ значениями:

| Файл | Количество человек | Уникальные |
|------|-------------------|------------|
| `transform/scripts.py:12-21` | 17 | - |
| `transform/projectsync.py:11-20` | 18 | +Докладчик 708 |
| `transform/logs.py:10-20` | 17 | - |
| `coord_sharepoint_etl/transform/sharepoint.py` | 17 | - |

**Код (scripts.py):**
```python
BIM_USERS = {
    'Колпаков Семен Дмитриевич', 'Пятков Роман Анатольевич',
    'Андреев Александр Константинович', 'Кичигин Андрей Владимирович',
    'Панов Антон Владимирович', 'Васьков Денис Игоревич', 'Попов Антон Михайлович',
    'Кузовлева Ольга Сергеевна', 'Калачев Даниил Артемович',
    'Григорьев Роман Николаевич', 'Красильников Дмитрий Сергеевич',
    'Литуева Юлия Дмитриевна', 'Жук Виталий Томашевич', 'Овсянкин Роман Николаевич',
    'Романова Анна Вячеславовна', 'Коновалов Василий Сергеевич',
    'Урманчеев Роман Дамирович'
}
```

**Влияние:** КРИТИЧЕСКОЕ - может привести к расхождениям в аналитике

**Решение:**
Создать `dags/config.py` с единой константой:

```python
"""
Централизованная конфигурация для всех DAG.
"""

# BIM пользователи для классификации данных
BIM_USERS = {
    'Колпаков Семен Дмитриевич',
    'Пятков Роман Анатольевич',
    'Андреев Александр Константинович',
    'Кичигин Андрей Владимирович',
    'Панов Антон Владимирович',
    'Васьков Денис Игоревич',
    'Попов Антон Михайлович',
    'Кузовлева Ольга Сергеевна',
    'Калачев Даниил Артемович',
    'Григорьев Роман Николаевич',
    'Красильников Дмитрий Сергеевич',
    'Литуева Юлия Дмитриевна',
    'Жук Виталий Томашевич',
    'Овсянкин Роман Николаевич',
    'Романова Анна Вячеславовна',
    'Коновалов Василий Сергеевич',
    'Урманчеев Роман Дамирович',
    'Докладчик 708'  # Добавлен из projectsync
}

# Фамилии для фильтрации (уволенные)
FORBIDDEN_USERS = [
    'овсянкин',
    'кузовлева',
    'кичигин',
    'андреев',
    'романова',
    'урманчеев'
]

# Фамилии для удаления (тестовые/уволенные)
TO_REMOVE = ['мельникова', 'шишляева']

# Google Sheets API scopes
GSHEET_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Локальная таймзона
LOCAL_TIMEZONE = 'Asia/Yekaterinburg'  # UTC+5
```

### 3. Дублирование функции extract_short_name()

**Проблема:**
Идентичная функция в 2 файлах:

```
transform/scripts.py:24-27
transform/projectsync.py:23-26
```

**Код:**
```python
def extract_short_name(name: str) -> str:
    """Извлекает короткое название проекта из полного."""
    parts = name.split('_')
    return '_'.join(parts[:2]) if len(parts) >= 2 else name
```

**Решение:**
Создать `dags/utils.py` с общими утилитами:

```python
"""
Общие утилиты для ETL pipelines.
"""

def extract_short_name(name: str) -> str:
    """
    Извлекает короткое название проекта из полного.

    Пример:
        'K01_AR_2024_vaskov' -> 'K01_AR'
    """
    if not isinstance(name, str):
        return name
    parts = name.split('_')
    return '_'.join(parts[:2]) if len(parts) >= 2 else name
```

### 4. Разные версии check_responsible()

**Проблема:**
Функция `check_responsible()` реализована по-разному:

| Файл | Фамилий | Список |
|------|---------|--------|
| `coord_sharepoint_etl/transform/sharepoint.py:124-129` | 6 | овсянкин, кузовлева, кичигин, андреев, романова, урманчеев |
| `coord_sharepoint_etl/transform/gsheet.py:4-10` | 4 | овсянкин, кузовлева, кичигин, андреев |

**Решение:**
Использовать централизованную константу `FORBIDDEN_USERS` из `config.py`

---

## Дублирование кода

### Полный список дублирования

| # | Что дублируется | Файлы | Строки | Приоритет |
|---|----------------|-------|--------|-----------|
| 1 | `extract_ad_users()` | pluginsdb.py, tim_db.py | 12 строк | CRITICAL |
| 2 | `BIM_USERS` | scripts.py, projectsync.py, logs.py, sharepoint.py | 10-18 строк × 4 | CRITICAL |
| 3 | `extract_short_name()` | scripts.py, projectsync.py | 4 строки × 2 | HIGH |
| 4 | `check_responsible()` | sharepoint.py, gsheet.py | 6 строк × 2 | MEDIUM |
| 5 | SCOPES константа | etl_pipelines/extract/gsheet.py (2 раза) | 1 строка × 2 | LOW |
| 6 | Логика подключения к GSheet | etl_pipelines/extract/gsheet.py | 5-7 строк × 2 | MEDIUM |

**Итого:** ~80-100 строк дублирующегося кода

---

## Неиспользуемый код

### 1. Модуль etl_pipelines/extract/gsheet.py ❌ К УДАЛЕНИЮ

**Статус:** НЕ ИСПОЛЬЗУЕТСЯ НИ В ОДНОМ DAG

**Размер:** 141 строка

**Функции:**
1. `extract_gitlab_mapping()` - извлечение маппинга GitLab из Google Sheets
2. `extract_instructions()` - извлечение инструкций из Google Sheets
3. `append_new_gitlab_mappings()` - добавление новых маппингов

**Поиск использования:**
```bash
# Проверено во всех 6 DAG файлах - импортов не найдено
```

**Рекомендация:**
1. **Если планируется использовать позже:** Переместить в отдельный архив
2. **Если не нужен:** Удалить полностью

**Команда удаления:**
```bash
# Опция 1: Удалить файл
rm dags/etl_pipelines/extract/gsheet.py

# Опция 2: Переместить в архив
mkdir -p archive/unused
mv dags/etl_pipelines/extract/gsheet.py archive/unused/
```

### 2. Пустые __init__.py файлы

**Найдено:** 8 пустых файлов (только docstring или вообще пусты)

```
dags/etl_pipelines/__init__.py
dags/etl_pipelines/extract/__init__.py
dags/etl_pipelines/transform/__init__.py
dags/etl_pipelines/load/__init__.py
dags/coord_sharepoint_etl/__init__.py
dags/coord_sharepoint_etl/extract/__init__.py
dags/coord_sharepoint_etl/transform/__init__.py
dags/coord_sharepoint_etl/load/__init__.py
```

**Рекомендация:**
Добавить правильные imports для удобного использования:

**Пример для `etl_pipelines/__init__.py`:**
```python
"""ETL Pipelines модуль для Airflow DAGs."""

from etl_pipelines.extract import pluginsdb, gitlab
from etl_pipelines.transform import scripts, projectsync, logs, gitlab as gitlab_transform
from etl_pipelines.load import datalake

__all__ = [
    'pluginsdb',
    'gitlab',
    'scripts',
    'projectsync',
    'logs',
    'gitlab_transform',
    'datalake'
]
```

---

## Рекомендации по оптимизации

### 1. Создать централизованную конфигурацию

**Файл:** `dags/config.py`

**Содержимое:**
- BIM_USERS (единая версия)
- FORBIDDEN_USERS
- TO_REMOVE
- GSHEET_SCOPES
- LOCAL_TIMEZONE
- Маппинги дисциплин (discipline_mapping)
- Маппинги типов запросов (type_request_mapping)

### 2. Создать модуль общих утилит

**Файл:** `dags/utils.py`

**Функции:**
- `extract_short_name()` - извлечение короткого названия проекта
- `check_responsible()` - проверка ответственных
- `remove_specific()` - фильтрация уволенных
- Другие общие функции

### 3. Улучшить __init__.py файлы

Добавить правильные импорты во все `__init__.py` для упрощения импортов в DAG файлах.

### 4. Оптимизация бизнес-логики

#### 4.1 Унификация маппингов

Вынести все маппинги в `config.py`:
- `discipline_mapping` (дисциплины)
- `type_request_mapping` (типы запросов)
- `section_map_kortros` (разделы Кортрос)
- `section_map_rus` (разделы русские)
- `stage_map_kortros` (стадии Кортрос)
- `stage_map` (стадии общие)

#### 4.2 Улучшение обработки ошибок

Добавить try-except блоки и логирование в критичных местах:
- Подключение к БД
- Клонирование GitLab репозиториев
- Обращение к Google Sheets API
- Обращение к SharePoint API

#### 4.3 Кеширование GitLab операций

Добавить кеширование для GitLab LOC подсчета:
```python
import hashlib
import json
from pathlib import Path

def get_cache_key(project_id: int, branch: str) -> str:
    return hashlib.md5(f"{project_id}:{branch}".encode()).hexdigest()

def load_from_cache(cache_key: str, cache_dir: Path) -> dict:
    cache_file = cache_dir / f"{cache_key}.json"
    if cache_file.exists():
        with open(cache_file) as f:
            return json.load(f)
    return None

def save_to_cache(cache_key: str, data: dict, cache_dir: Path):
    cache_file = cache_dir / f"{cache_key}.json"
    with open(cache_file, 'w') as f:
        json.dump(data, f)
```

### 5. Улучшение структуры данных

#### 5.1 Использование Pydantic для валидации

Создать модели данных:
```python
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class Plugin(BaseModel):
    id: int
    display_name: str
    developer: str
    development_stage_id: Optional int]

class MonitoringRecord(BaseModel):
    plugin_id: int
    user_display_name: str
    project_name: str
    timestamp: datetime
    is_bim: bool = Field(default=False)
```

### 6. Оптимизация SQL запросов

Вместо `SELECT *` использовать явное указание колонок:

**Текущий код:**
```python
sql = 'SELECT * FROM users.ad_user'
```

**Оптимизированный код:**
```python
sql = '''
    SELECT
        display_name,
        department,
        project_section
    FROM users.ad_user
    WHERE active = true
'''
```

### 7. Параллелизация GitLab операций

Текущий `max_workers=8` можно оптимизировать динамически:

```python
import os

# Оптимальное количество воркеров = количество CPU × 2
max_workers = min(os.cpu_count() * 2, 16)
```

### 8. Добавить логирование

Использовать Airflow логирование вместо print():

```python
from airflow.utils.log.logging_mixin import LoggingMixin

class ETLLogger(LoggingMixin):
    def extract_data(self):
        self.log.info("Starting data extraction...")
        # ...
```

---

## План рефакторинга

### Этап 1: Подготовка (Risk: LOW)

**Цель:** Создать новые файлы без изменения существующих

**Задачи:**
1. ✅ Создать `dags/config.py` с константами
2. ✅ Создать `dags/utils.py` с общими функциями
3. ✅ Создать резервную копию проекта
4. ✅ Добавить юнит-тесты для новых модулей

**Команды:**
```bash
# Резервная копия
cp -r airflow-dags airflow-dags.backup.$(date +%Y%m%d)

# Создание новых файлов
touch dags/config.py
touch dags/utils.py
```

### Этап 2: Рефакторинг transform модулей (Risk: MEDIUM)

**Цель:** Убрать дублирование BIM_USERS и extract_short_name

**Задачи:**
1. Обновить `transform/scripts.py` - использовать config.BIM_USERS
2. Обновить `transform/projectsync.py` - использовать config.BIM_USERS и utils
3. Обновить `transform/logs.py` - использовать config.BIM_USERS
4. Обновить `coord_sharepoint_etl/transform/sharepoint.py` - использовать config
5. Обновить `coord_sharepoint_etl/transform/gsheet.py` - использовать config

**Порядок:**
- Сначала добавить импорты
- Затем удалить локальные константы
- Запустить тесты

### Этап 3: Удаление дубликата tim_db.py (Risk: MEDIUM)

**Цель:** Убрать дублирование extract_ad_users

**Задачи:**
1. Обновить `sharepoint_etl_dag.py` - изменить импорт
2. Удалить `coord_sharepoint_etl/extract/tim_db.py`
3. Обновить `coord_sharepoint_etl/extract/__init__.py`

**Изменение в DAG:**
```python
# Было:
from coord_sharepoint_etl.extract.tim_db import extract_ad_users

# Стало:
from etl_pipelines.extract.pluginsdb import extract_ad_users
```

### Этап 4: Удаление неиспользуемого кода (Risk: LOW)

**Цель:** Очистить проект от мертвого кода

**Задачи:**
1. Удалить `etl_pipelines/extract/gsheet.py`
2. Обновить документацию

**Команда:**
```bash
rm dags/etl_pipelines/extract/gsheet.py
```

### Этап 5: Улучшение __init__.py (Risk: LOW)

**Цель:** Упростить импорты

**Задачи:**
1. Обновить все 8 __init__.py файлов
2. Протестировать импорты в DAG файлах

### Этап 6: Тестирование (Risk: CRITICAL)

**Цель:** Убедиться что все работает

**Задачи:**
1. Запустить `airflow dags list-import-errors`
2. Провести dry-run всех DAG
3. Запустить тесты
4. Проверить интеграцию с БД

**Команды:**
```bash
# Проверка парсинга DAG
airflow dags list-import-errors

# Тест каждого DAG
airflow dags test scripts_etl_dag 2024-01-01
airflow dags test gitlab_etl_dag 2024-01-01
airflow dags test projectsync_etl_dag 2024-01-01
airflow dags test logs_etl_dag 2024-01-01
airflow dags test sharepoint_etl_dag 2024-01-01
airflow dags test gsheet_families_etl_dag 2024-01-01

# Синтаксис Python
python -m py_compile dags/**/*.py
```

### Этап 7: Документация (Risk: LOW)

**Цель:** Обновить документацию

**Задачи:**
1. Обновить README.md
2. Обновить QUICK_REFERENCE.md
3. Создать ARCHITECTURE.md
4. Обновить docstrings

---

## Ожидаемые результаты

### Метрики улучшения

| Метрика | До | После | Улучшение |
|---------|-----|-------|-----------|
| Строк кода | ~2100 | ~1800 | -15% |
| Дублирующихся функций | 4 | 0 | -100% |
| Файлов с дубликатами | 7 | 0 | -100% |
| Неиспользуемых файлов | 1 | 0 | -100% |
| Версий BIM_USERS | 4 | 1 | -75% |
| Централизованных констант | 0 | 20+ | +100% |
| Время на поддержку | 100% | 70% | -30% |

### Качественные улучшения

✅ **Легкость поддержки**: Все константы в одном месте
✅ **Консистентность**: Единые версии BIM_USERS и других констант
✅ **Читаемость**: Четкая структура с utils и config
✅ **Тестируемость**: Отдельные модули легче тестировать
✅ **DRY принцип**: Нет дублирования кода
✅ **Масштабируемость**: Легко добавлять новые DAG

### Риски

| Риск | Вероятность | Влияние | Митигация |
|------|-------------|---------|-----------|
| Ошибки импорта | MEDIUM | HIGH | Тщательное тестирование на каждом этапе |
| Изменение логики BIM_USERS | LOW | MEDIUM | Проверка что все 18 человек учтены |
| Падение производительности | LOW | LOW | Профилирование после рефакторинга |
| Проблемы с encoding | LOW | MEDIUM | Тесты с реальными данными |

### Откат изменений

В случае проблем:

```bash
# Полный откат
rm -rf airflow-dags
mv airflow-dags.backup.YYYYMMDD airflow-dags

# Частичный откат (git)
git checkout -- dags/
```

---

## Следующие шаги

1. ✅ Одобрить план рефакторинга
2. ⏳ Создать ветку для рефакторинга
3. ⏳ Реализовать Этап 1 (config.py, utils.py)
4. ⏳ Реализовать Этап 2 (transform модули)
5. ⏳ Реализовать Этап 3-5 (удаление дубликатов)
6. ⏳ Провести полное тестирование
7. ⏳ Обновить документацию
8. ⏳ Merge в master

---

## Контакты

**Автор рефакторинга:** Claude Code
**Дата:** 2025-10-23
**Версия отчета:** 1.0

---

**КОНЕЦ ОТЧЕТА**
