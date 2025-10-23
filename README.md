# Airflow ETL Pipeline

Комплексный ETL проект на Apache Airflow для аналитики плагинов, синхронизации данных и интеграций.

**Последнее обновление:** 2025-10-23
**Версия:** 1.1 (после рефакторинга)

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
| **scripts_etl_dag** | Ежедневно 06:00 UTC | Основной комплексный pipeline: monitoring, plugins, gitlab |
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

**Текущая версия:** 1.1 (после рефакторинга 2025-10-23)

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
| Python файлов | 28 |
| DAG файлов | 6 |
| Extract функций | 14 |
| Transform функций | 6 |
| Load функций | 4 |
| Строк кода | ~2100 |
| Целевых таблиц | 9 |
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

**Дата последнего обновления:** 2025-10-23
**Версия:** 1.1
