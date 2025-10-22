# ETL Pipeline Migration to Airflow

Успешная миграция Jupyter notebooks ETL pipeline в Apache Airflow DAGs.

## Обзор

Проект перенесен из оркестрации через `run.py` и Jupyter notebooks в полноценные Airflow DAGs с модульной структурой.

### Что было мигрировано

**Из проекта ETL-pipeline:**
- Python extractors → Модули в `etl_pipelines/extract/`
- Jupyter notebooks → Модули в `etl_pipelines/transform/`
- Оркестрация → 4 отдельных DAG'а

## Структура проекта

```
airflow-dags/
├── dags/
│   ├── scripts_etl_dag.py          # Основной комплексный pipeline
│   ├── gitlab_etl_dag.py           # GitLab LOC analytics
│   ├── projectsync_etl_dag.py      # Project sync analytics
│   ├── logs_etl_dag.py             # Logs analytics
│   │
│   └── etl_pipelines/              # Модули ETL
│       ├── extract/
│       │   ├── pluginsdb.py        # PostgreSQL extractors
│       │   ├── gitlab.py           # GitLab API extractor
│       │   ├── yougile.py          # Yougile API extractor
│       │   └── gsheet.py           # Google Sheets extractors
│       │
│       ├── transform/
│       │   ├── scripts.py          # Scripts analytics transform
│       │   ├── gitlab.py           # GitLab transform
│       │   ├── projectsync.py      # ProjectSync transform
│       │   ├── yougile.py          # Yougile transform
│       │   └── logs.py             # Logs transform
│       │
│       └── load/
│           └── datalake.py         # PostgreSQL datalake loader
│
├── config/
│   ├── tokens.example.json
│   └── README_CONFIG.md            # Инструкции по настройке
│
├── requirements.txt
└── README_ETL_MIGRATION.md         # Этот файл
```

## Архитектура DAG'ов

### 1. Scripts ETL DAG (`scripts_etl_dag.py`)
**Самый сложный и важный pipeline.**

- **Schedule:** Ежедневно в 6:00 UTC
- **Sources:**
  - PluginsDB: monitoring, plugins, development_stage, AD users
  - GitLab API: статистика LOC
- **Targets:**
  - `datalake.ext_scripts_analytics_designers`
  - `datalake.ext_scripts_analytics_bim`
  - `datalake.ext_scripts_plugin`

### 2. GitLab ETL DAG (`gitlab_etl_dag.py`)
**Еженедельная синхронизация GitLab проектов.**

- **Schedule:** Еженедельно по воскресеньям в 5:00 UTC
- **Sources:**
  - GitLab API: LOC статистика всех проектов
  - PluginsDB: plugins
  - Google Sheets: маппинг gitlab-plugins
- **Targets:**
  - `datalake.ext_scripts_gitlab`
  - Google Sheets: обновление маппинга

### 3. ProjectSync ETL DAG (`projectsync_etl_dag.py`)
**Инкрементальная загрузка синхронизации проектов.**

- **Schedule:** Ежедневно в 3:00 UTC
- **Sources:**
  - PluginsDB: project_sync, AD users
- **Targets:** (инкрементально по дате)
  - `datalake.ext_project_sync_designers`
  - `datalake.ext_project_sync_bim`

### 4. Logs ETL DAG (`logs_etl_dag.py`)
**Аналитика логов плагинов.**

- **Schedule:** Ежедневно в 2:00 UTC
- **Sources:**
  - PluginsDB: logs, plugins
- **Targets:**
  - `datalake.ext_logs_analytics_designers`
  - `datalake.ext_logs_analytics_bim`

## Ключевые особенности

### Модульность
- Каждый extractor, transformer и loader - отдельная функция
- Легко тестировать и переиспользовать
- Четкое разделение ответственности (ETL pattern)

### Параллелизация
- Extract задачи в каждом DAG выполняются параллельно
- Transform ждет завершения всех extract
- Load задачи (если их несколько) выполняются параллельно

### Инкрементальность
- ProjectSync использует инкрементальную загрузку по дате
- Остальные pipelines - полная перезагрузка (replace)

### Интеграции
- **PostgreSQL**: pluginsdb (источник), datalake (назначение)
- **GitLab API**: клонирование и анализ репозиториев
- **Yougile API**: REST API с обработкой rate limits
- **Google Sheets**: чтение/запись маппингов через service account

## Зависимости

Установлены в `requirements.txt`:
- `apache-airflow-providers-postgres`
- `apache-airflow-providers-http`
- `apache-airflow-providers-mysql`
- `pandas`
- `SQLAlchemy`
- `python-gitlab`
- `GitPython`
- `gspread`
- `google-auth-oauthlib`
- `workalendar` (расчет рабочих дней)
- `beautifulsoup4`
- и другие...

## Настройка и запуск

**📋 ГЛАВНАЯ ИНСТРУКЦИЯ:** См. `DEPLOYMENT.md` - полное пошаговое руководство по запуску на виртуалке

**⚡ БЫСТРАЯ СПРАВКА:** См. `QUICK_REFERENCE.md` - таблица всех Connections и Variables

**🔧 ДЕТАЛИ КОНФИГУРАЦИИ:** См. `config/README_CONFIG.md`

**Быстрый старт:**
1. Создайте Airflow Connections (PostgreSQL, APIs) - см. DEPLOYMENT.md шаг 3
2. Создайте Airflow Variables (пути, конфиги) - см. DEPLOYMENT.md шаг 4
3. Скопируйте Google Service Account JSON - см. DEPLOYMENT.md шаг 1
4. Перезапустите Airflow - см. DEPLOYMENT.md шаг 7

## Отличия от исходного проекта

| Аспект | ETL-pipeline (старое) | Airflow DAGs (новое) |
|--------|----------------------|---------------------|
| Оркестрация | `run.py` с subprocess | Airflow Scheduler |
| Transforms | Jupyter notebooks | Python модули |
| Логирование | Custom logger | Airflow task logs |
| Мониторинг | Консоль | Airflow UI |
| Retry | Ручной перезапуск | Встроенный retry |
| Расписание | Cron + скрипт | Airflow schedules |
| Зависимости | Порядок в списке | DAG dependencies |
| Конфиги | tokens.json | Airflow Connections/Variables |

## Порядок выполнения DAG'ов

```
02:00 UTC - Logs ETL
03:00 UTC - ProjectSync ETL
05:00 UTC - GitLab ETL (только воскресенье)
06:00 UTC - Scripts ETL (главный)
```

Scripts ETL идет последним, так как потенциально использует самые свежие данные.

## Troubleshooting


### Ошибка подключения к PostgreSQL
Проверьте, что connections `tim_db_pluginsdb` и `tim_db_postgres` созданы правильно.

### GitLab extraction слишком долго
- Уменьшите `max_workers` в `gitlab_etl_dag.py`
- Или увеличьте timeout задачи в DAG config

### Ошибка Google Sheets API
- Проверьте, что service account JSON файл на месте
- Убедитесь, что service account имеет доступ к spreadsheet

## Мониторинг

В Airflow UI вы можете:
- Отслеживать статус каждого DAG run
- Просматривать логи каждой задачи
- Настраивать alerts при ошибках
- Анализировать графики зависимостей
- Проверять длительность выполнения

## Дальнейшее развитие

Возможные улучшения:
- [ ] Добавить data quality checks
- [ ] Реализовать XCom для передачи небольших данных между задачами
- [ ] Настроить email alerts при ошибках
- [ ] Добавить метрики в Prometheus/Grafana
- [ ] Реализовать backfill для исторических данных

## Авторы

Миграция выполнена: Claude Code
Исходный проект: ETL-pipeline

---

**Дата миграции:** 2025
**Версия Airflow:** 2.x
**Python:** 3.8+
