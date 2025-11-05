# Airflow ETL Pipeline

Комплексный ETL проект на Apache Airflow для аналитики плагинов, синхронизации данных и интеграций.

**Последнее обновление:** 2025-11-05
**Версия:** 1.3 (Plugin Engagement: исторические данные)

---

## 🚀 Быстрый старт

### Новый пользователь? Начните здесь:

1. **[DEPLOYMENT.md](DEPLOYMENT.md)** - Полная пошаговая инструкция по запуску на виртуалке
2. **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - Быстрая справка: таблица всех Connections и Variables
3. **[PROJECT_ANALYSIS.md](PROJECT_ANALYSIS.md)** - Анализ проекта на связность и исполняемость
4. **[REFACTORING_REPORT.md](REFACTORING_REPORT.md)** - Отчет по рефакторингу кода

### Дополнительная документация:

- **[README_ETL_MIGRATION.md](README_ETL_MIGRATION.md)** - История миграции и архитектура проекта
- **[config/README_CONFIG.md](config/README_CONFIG.md)** - Детали конфигурации

---

## 📊 Обзор DAG

| DAG | Schedule | Описание |
|-----|----------|----------|
| **scripts_etl_dag** | Ежечасно (@hourly) | Основной комплексный pipeline: monitoring, plugins, gitlab |
| **plugin_engagement_etl_dag** | Ежечасно (@hourly) | 🆕 Оценка использования плагинов проектировщиками |
| **gitlab_etl_dag** | Воскресенье 05:00 UTC | Синхронизация GitLab проектов и обновление маппинга |
| **projectsync_etl_dag** | Ежедневно 03:00 UTC | Инкрементальная загрузка синхронизации проектов |
| **logs_etl_dag** | Ежедневно 02:00 UTC | Аналитика логов плагинов |
| **sharepoint_etl_dag** | Каждые 15 минут | Синхронизация задач из SharePoint |
| **gsheet_families_etl_dag** | Каждые 2 часа | Синхронизация семейств из Google Sheets |

---

## 🏗️ Структура проекта

```
airflow-dags/
├── dags/
│   ├── config.py                   # 🆕 Централизованная конфигурация
│   ├── utils.py                    # 🆕 Общие утилиты для ETL
│   │
│   ├── scripts_etl_dag.py          # ⭐ Основной pipeline
│   ├── plugin_engagement_etl_dag.py # 🆕 Plugin Engagement Score
│   ├── gitlab_etl_dag.py           # GitLab LOC analytics
│   ├── projectsync_etl_dag.py      # Project sync analytics
│   ├── logs_etl_dag.py             # Logs analytics
│   ├── sharepoint_etl_dag.py       # SharePoint sync
│   ├── gsheet_families_etl_dag.py  # Google Sheets families
│   │
│   ├── etl_pipelines/              # Модули ETL (скрипты)
│   │   ├── extract/                # Extractors: PostgreSQL, GitLab
│   │   │   ├── pluginsdb.py       # PostgreSQL extractors (6 функций)
│   │   │   └── gitlab.py          # GitLab LOC extraction
│   │   ├── transform/              # Transformers
│   │   │   ├── scripts.py         # Scripts analytics
│   │   │   ├── plugin_engagement.py # 🆕 Plugin Engagement Score
│   │   │   ├── gitlab.py          # GitLab analytics
│   │   │   ├── projectsync.py     # Project sync analytics
│   │   │   └── logs.py            # Logs analytics
│   │   └── load/                   # Loader
│   │       └── datalake.py        # PostgreSQL datalake loader
│   │
│   └── coord_sharepoint_etl/       # Модули ETL (SharePoint & GSheets)
│       ├── extract/
│       │   ├── sharepoint.py      # SharePoint REST API
│       │   ├── gsheet.py          # Google Sheets API
│       │   └── tim_db.py          # PostgreSQL AD users
│       ├── transform/
│       │   ├── sharepoint.py      # SharePoint трансформации
│       │   └── gsheet.py          # GSheet трансформации
│       └── load/
│           └── sharepoint.py      # UPSERT в datalake
│
├── config/
│   ├── revitmaterials-db15db824f22.json  # Google Service Account
│   ├── tokens.json                        # Справочник токенов
│   └── README_CONFIG.md                   # Детали конфигурации
│
├── DEPLOYMENT.md                   # 🔥 ГЛАВНАЯ ИНСТРУКЦИЯ
├── QUICK_REFERENCE.md              # ⚡ Быстрая справка
├── PROJECT_ANALYSIS.md             # 📊 Анализ проекта
├── REFACTORING_REPORT.md           # 🆕 Отчет по рефакторингу
├── README_ETL_MIGRATION.md         # 📖 История миграции
├── requirements.txt                # Python dependencies
└── README.md                       # Этот файл
```

### 🆕 Новые модули (v1.1)

**dags/config.py** - Централизованная конфигурация:
- `BIM_USERS` - список BIM пользователей (18 человек)
- `FORBIDDEN_USERS`, `TO_REMOVE` - фильтры пользователей
- `DISCIPLINE_MAPPING` - маппинг дисциплин
- `TYPE_REQUEST_MAPPING` - маппинг типов запросов
- Константы для разделов и стадий проектов

**dags/utils.py** - Общие утилиты:
- `extract_short_name()` - извлечение короткого названия проекта
- `check_responsible()`, `remove_specific()` - фильтрация пользователей
- `clean_html_safe()` - обработка HTML
- `workdays_diff()` - подсчет рабочих дней
- `get_project_solution()`, `get_project_stage()` - определение разделов/стадий

---

## 📊 Plugin Engagement Score - Детальное описание

### 🎯 Назначение

DAG `plugin_engagement_etl_dag` реализует комплексную метрику для объективной оценки использования плагинов **проектировщиками** (не BIM-пользователями). Метрика позволяет:

- Выявить сотрудников с низкой вовлеченностью в использование плагинов
- Оценить как разнообразие инструментария, так и интенсивность использования
- Получить объективную картину для принятия решений об обучении и мотивации

### 📐 Методология расчета

#### 1. Исходные данные
- **Monitoring**: логи запуска плагинов пользователями
- **AD Users**: справочник пользователей с ФИО
- **BIM_USERS**: список BIM-специалистов (исключаются из анализа)

#### 2. Определение целевой аудитории (проектировщиков)
**Критерий отбора:** Уникальные пользователи из Monitoring минус BIM-специалисты
- Берутся **ТОЛЬКО те**, кто реально запускал плагины (есть в monitoring)
- Исключаются BIM-специалисты из `config.BIM_USERS`
- Результат: проектировщики, **активно использующие** плагины

#### 3. Метрики на пользователя
- **unique_plugins**: количество уникальных плагинов
- **total_launches**: общее количество запусков всех плагинов

#### 4. Нормализация Min-Max
Приведение к диапазону [0, 1]:

```
normalized_value = (value - min_value) / (max_value - min_value)
```

Если все значения одинаковые → нормализованное значение = 0.5

#### 5. Plugin Engagement Score
Взвешенное среднее нормализованных метрик:

```
Score = w1 × unique_plugins_norm + w2 × total_launches_norm
```

**По умолчанию:** w1 = 0.5, w2 = 0.5 (сбалансированная оценка)

### 📈 Интерпретация результатов

| Диапазон оценки | Интерпретация | Рекомендации |
|-----------------|---------------|--------------|
| **0.7 - 1.0** | Высокая вовлеченность | Активное использование плагинов |
| **0.4 - 0.7** | Умеренная вовлеченность | Стандартный уровень |
| **0.0 - 0.4** | Низкая вовлеченность | Требуется обучение/мотивация |

### 📋 Выходная таблица

**Таблица:** `datalake.ext_plugin_engagement`

| Колонка | Тип | Описание |
|---------|-----|----------|
| **day** | DATE | **Дата (конец дня)** |
| user_name | TEXT | ФИО проектировщика |
| unique_plugins | INTEGER | Кумулятивное кол-во уникальных плагинов (до конца дня) |
| total_launches | INTEGER | Кумулятивное общее кол-во запусков (до конца дня) |
| unique_plugins_norm | FLOAT | Нормализованное значение (0-1) в рамках дня |
| total_launches_norm | FLOAT | Нормализованное значение (0-1) в рамках дня |
| plugin_engagement_score | FLOAT | Итоговая оценка (0-1) в рамках дня |

**Особенность:** Данные представлены как временные ряды - для каждого дня и каждого проектировщика рассчитывается **кумулятивная** метрика на конец этого дня.

### 🔄 Процесс ETL

```
┌─────────────┐     ┌─────────────┐
│ AD Users    │────▶│  Extract    │
└─────────────┘     │  (parallel) │
                    └──────┬──────┘
┌─────────────┐            │
│ Monitoring  │────────────┘
└─────────────┘            │
                           ▼
                    ┌──────────────┐
                    │  Transform   │
                    │  - Фильтр    │
                    │  - Агрегация │
                    │  - Норма-ия  │
                    │  - Score     │
                    └──────┬───────┘
                           ▼
                    ┌──────────────┐
                    │    Load      │
                    │  (replace)   │
                    └──────────────┘
                           │
                           ▼
              datalake.ext_plugin_engagement
```

### ⚙️ Настройка весов

Веса можно изменить в файле `plugin_engagement_etl_dag.py`:

```python
# Приоритет на разнообразие плагинов
WEIGHT_UNIQUE_PLUGINS = 0.7
WEIGHT_TOTAL_LAUNCHES = 0.3

# Приоритет на интенсивность использования
WEIGHT_UNIQUE_PLUGINS = 0.3
WEIGHT_TOTAL_LAUNCHES = 0.7
```

**Важно:** сумма весов должна быть равна 1.0

### 📝 Примеры использования результатов

#### SQL: Топ-10 проектировщиков на последнюю дату

```sql
WITH last_day AS (
    SELECT MAX(day) as max_day FROM datalake.ext_plugin_engagement
)
SELECT 
    day,
    user_name,
    unique_plugins,
    total_launches,
    ROUND(plugin_engagement_score::numeric, 4) as score
FROM datalake.ext_plugin_engagement
WHERE day = (SELECT max_day FROM last_day)
ORDER BY plugin_engagement_score DESC
LIMIT 10;
```

#### SQL: Проектировщики, требующие внимания на последнюю дату (Score < 0.4)

```sql
WITH last_day AS (
    SELECT MAX(day) as max_day FROM datalake.ext_plugin_engagement
)
SELECT 
    day,
    user_name,
    unique_plugins,
    total_launches,
    ROUND(plugin_engagement_score::numeric, 4) as score
FROM datalake.ext_plugin_engagement
WHERE day = (SELECT max_day FROM last_day)
  AND plugin_engagement_score < 0.4
ORDER BY plugin_engagement_score ASC;
```

#### SQL: Распределение по уровням вовлеченности (последняя дата)

```sql
WITH last_day AS (
    SELECT MAX(day) as max_day FROM datalake.ext_plugin_engagement
)
SELECT 
    CASE 
        WHEN plugin_engagement_score >= 0.7 THEN 'Высокая'
        WHEN plugin_engagement_score >= 0.4 THEN 'Умеренная'
        ELSE 'Низкая'
    END as engagement_level,
    COUNT(*) as designers_count,
    ROUND(AVG(unique_plugins)::numeric, 1) as avg_plugins,
    ROUND(AVG(total_launches)::numeric, 1) as avg_launches
FROM datalake.ext_plugin_engagement
WHERE day = (SELECT max_day FROM last_day)
GROUP BY engagement_level
ORDER BY 
    CASE engagement_level
        WHEN 'Высокая' THEN 1
        WHEN 'Умеренная' THEN 2
        WHEN 'Низкая' THEN 3
    END;
```

#### SQL: Динамика вовлеченности проектировщика за последние 30 дней

```sql
SELECT 
    day,
    user_name,
    unique_plugins,
    total_launches,
    ROUND(plugin_engagement_score::numeric, 4) as score
FROM datalake.ext_plugin_engagement
WHERE user_name = 'Иванов Иван Иванович'  -- замените на нужное ФИО
  AND day >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY day DESC;
```

#### SQL: Рост вовлеченности - сравнение начала и конца периода

```sql
WITH first_last AS (
    SELECT 
        user_name,
        MIN(day) as first_day,
        MAX(day) as last_day
    FROM datalake.ext_plugin_engagement
    GROUP BY user_name
),
scores AS (
    SELECT 
        e1.user_name,
        e1.day as first_day,
        e1.plugin_engagement_score as first_score,
        e2.day as last_day,
        e2.plugin_engagement_score as last_score,
        ROUND((e2.plugin_engagement_score - e1.plugin_engagement_score)::numeric, 4) as growth
    FROM first_last fl
    JOIN datalake.ext_plugin_engagement e1 
        ON fl.user_name = e1.user_name AND fl.first_day = e1.day
    JOIN datalake.ext_plugin_engagement e2 
        ON fl.user_name = e2.user_name AND fl.last_day = e2.day
)
SELECT 
    user_name,
    first_day,
    ROUND(first_score::numeric, 4) as first_score,
    last_day,
    ROUND(last_score::numeric, 4) as last_score,
    growth,
    CASE 
        WHEN growth > 0.1 THEN '📈 Значительный рост'
        WHEN growth > 0 THEN '↗️ Рост'
        WHEN growth = 0 THEN '→ Стабильно'
        WHEN growth > -0.1 THEN '↘️ Снижение'
        ELSE '📉 Значительное снижение'
    END as trend
FROM scores
ORDER BY growth DESC;
```

---

## 🔧 Требования

- **Apache Airflow:** 2.x+
- **Python:** 3.8+
- **PostgreSQL:** 12+ (источник и целевая БД)
- **GitLab:** CE/EE (API доступ)
- **Google Sheets API:** Service Account с доступом к spreadsheets
- **SharePoint:** Доступ через NTLM auth

---

## 📦 Установка зависимостей

```bash
pip install -r requirements.txt
```

**Ключевые зависимости:**
- apache-airflow-providers-postgres
- apache-airflow-providers-http
- pandas, SQLAlchemy
- python-gitlab, GitPython
- gspread, google-auth-oauthlib
- requests_ntlm (SharePoint)

---

## ⚙️ Конфигурация

### Airflow Connections (4 шт)

| Connection ID | Type | Host | Port |
|--------------|------|------|------|
| tim_db_pluginsdb | postgres | 192.168.42.188 | 5430 |
| tim_db_postgres | postgres | 192.168.42.188 | 5430 |
| gitlab_api | http | http://192.168.42.188:13080 | - |
| askit_http_sharepoint_tim | http | <SHAREPOINT_URL> | - |

### Airflow Variables (5 шт)

| Variable Key | Type | Описание |
|-------------|------|----------|
| ETL_DATA_ROOT_PATH | string | Путь к папке для временных данных |
| gsheet_config | json | Конфигурация Google Sheets для GitLab DAG |
| gsheet_families_key | string | Ключ Google Sheets с семействами |
| gsheet_families_worksheet | string | Имя листа (DB_Семейства) |
| gsheet_service_account_json | json | Полный JSON service account |

**Детали:** См. [DEPLOYMENT.md](DEPLOYMENT.md) шаги 3-4

---

## 🎯 Источники и назначения данных

### Источники (pluginsdb):
- users.ad_user
- plugins.plugin
- plugins.plugin_development_stage
- monitoring.monitoring
- log.log
- project_sync.project_sync

### Назначение (datalake):
- datalake.ext_scripts_analytics_designers
- datalake.ext_scripts_analytics_bim
- datalake.ext_scripts_plugin
- datalake.ext_plugin_engagement 🆕
- datalake.ext_scripts_gitlab
- datalake.ext_project_sync_designers
- datalake.ext_project_sync_bim
- datalake.ext_logs_analytics_designers
- datalake.ext_logs_analytics_bim
- datalake.ext_sharepoint_coord

---

## 🔍 Мониторинг

### Airflow Web UI
```
http://localhost:8080
```

### CLI команды
```bash
# Список DAG
airflow dags list

# Проверка ошибок
airflow dags list-import-errors

# Просмотр логов
airflow tasks logs <dag_id> <task_id> <execution_date>

# Ручной запуск
airflow dags trigger <dag_id>
```

---

## 🐛 Troubleshooting

### DAG не запускается
```bash
# 1. Проверьте ошибки парсинга
airflow dags list-import-errors

# 2. Проверьте connections
airflow connections list

# 3. Проверьте variables
airflow variables list

# 4. Проверьте логи scheduler
tail -f $AIRFLOW_HOME/logs/scheduler/latest/*.log
```

### Подробное руководство
См. [DEPLOYMENT.md](DEPLOYMENT.md) раздел "Troubleshooting"

---

## 📝 Версии и история

**Текущая версия:** 1.3 (Plugin Engagement: исторические данные 2025-11-05)

### Версия 1.3 - Plugin Engagement: Временные ряды (2025-11-05)
- ✅ Добавлено поле `day` (дата) в таблицу результатов
- ✅ Реализован **кумулятивный расчет** метрики на конец каждого дня
- ✅ Поддержка исторических данных - все доступные дни из источника
- ✅ Отслеживание динамики вовлеченности во времени
- ✅ Обновлены примеры SQL запросов для работы с временными рядами
- ✅ Добавлены запросы для анализа тренда и роста вовлеченности

### Версия 1.2 - Plugin Engagement Score (2025-11-05)
- ✅ Создан новый DAG `plugin_engagement_etl_dag.py`
- ✅ Реализована метрика Plugin Engagement Score для оценки использования плагинов
- ✅ Создан модуль трансформации `etl_pipelines/transform/plugin_engagement.py`
- ✅ Методология: нормализация Min-Max + взвешенное среднее
- ✅ Фильтрация проектировщиков (не BIM-пользователей)
- ✅ Новая целевая таблица: `datalake.ext_plugin_engagement`
- ✅ Запуск одновременно со `scripts_etl_dag` (@hourly)

### Версия 1.1 - Рефакторинг кодовой базы (2025-10-23)
- ✅ Создан `dags/config.py` с централизованными константами
- ✅ Создан `dags/utils.py` с общими утилитами
- ✅ Устранено дублирование константы BIM_USERS (было 4 версии → стало 1)
- ✅ Проведен анализ связности кода
- ✅ Выявлен неиспользуемый код (etl_pipelines/extract/gsheet.py)
- ✅ Создан детальный отчет [REFACTORING_REPORT.md](REFACTORING_REPORT.md)
- ✅ Обновлена документация

### Версия 1.0 - После удаления Yougile
- ✅ Полностью удалён Yougile ETL DAG и все связанные метрики
- ✅ Очищен config/tokens.json от Yougile токенов
- ✅ Обновлена документация
- ✅ Создана полная инструкция по запуску

**Миграция:** Проект мигрирован из Jupyter notebooks в Airflow DAGs (см. [README_ETL_MIGRATION.md](README_ETL_MIGRATION.md))

---

## 📈 Статистика проекта

| Метрика | Значение |
|---------|----------|
| Python файлов | 30 |
| DAG файлов | 7 |
| Extract функций | 14 |
| Transform функций | 7 |
| Load функций | 4 |
| Строк кода | ~2400 |
| Целевых таблиц | 10 |
| Источников данных | 4 (PostgreSQL, GitLab, SharePoint, Google Sheets) |

### Архитектурные особенности

✅ **Сильные стороны:**
- Четкое разделение на Extract/Transform/Load
- Использование DataFrame для универсальности
- Параллельная обработка (GitLab extraction)
- Инкрементальная загрузка (projectsync)
- XCom для передачи данных между задачами
- Централизованная конфигурация (v1.1)

⚠️ **Области для улучшения:**
- См. [REFACTORING_REPORT.md](REFACTORING_REPORT.md) для детального плана

---

## 📚 Дополнительные ресурсы

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [TaskFlow API Guide](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)

---

## 👤 Авторы

**Миграция выполнена:** Claude Code
**Рефакторинг выполнен:** Claude Code (2025-10-23)
**Исходный проект:** ETL-pipeline

---

## 📄 Лицензия

Внутренний проект компании.

---

**Дата последнего обновления:** 2025-11-05
**Версия:** 1.3
