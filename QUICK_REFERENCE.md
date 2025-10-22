# Быстрая справка по Airflow Connections и Variables

## Airflow Connections

| Connection ID | Type | Host | Port | Schema | Login | Password | Используется в |
|--------------|------|------|------|--------|-------|----------|----------------|
| **tim_db_pluginsdb** | postgres | 192.168.42.188 | 5430 | pluginsdb | postgres | см. config/tokens.json | scripts, gitlab, projectsync, logs, sharepoint |
| **tim_db_postgres** | postgres | 192.168.42.188 | 5430 | postgres | postgres | см. config/tokens.json | scripts, gitlab, projectsync, logs, sharepoint, gsheet_families |
| **gitlab_api** | http | http://192.168.42.188:13080 | - | - | - | см. config/tokens.json | scripts, gitlab |
| **askit_http_sharepoint_tim** | http | <SHAREPOINT_URL> | - | - | <USERNAME> | <PASSWORD> | sharepoint |

**ВАЖНО:** Реальные пароли и токены находятся в файле `config/tokens.json` (локальный, НЕ коммитится в git)

## Airflow Variables

| Variable Key | Type | Value | Используется в |
|-------------|------|-------|----------------|
| **ETL_DATA_ROOT_PATH** | string | /opt/airflow/data | Все DAG |
| **gsheet_config** | json | {"service_account_path":"/opt/airflow/config/revitmaterials-db15db824f22.json","spreadsheet_key":"19ZDWnS0Ft8bLVCbVyHsOatTTzidv55r5Rj7Woi9mNck"} | gitlab |
| **gsheet_families_key** | string | 1C3AJ-0uzoIr97ZaVqeOsi-JbqkYKhsirsoSRhOc1Xnw | gsheet_families |
| **gsheet_families_worksheet** | string | DB_Семейства | gsheet_families |
| **gsheet_service_account_json** | json | <полное содержимое revitmaterials-db15db824f22.json> | gsheet_families |

## Файлы конфигурации

| Файл | Путь на виртуалке | Описание |
|------|-------------------|----------|
| **Google Service Account** | /opt/airflow/config/revitmaterials-db15db824f22.json | Учетные данные для доступа к Google Sheets API |
| **Tokens Reference** | config/tokens.json | Справочный файл с актуальными токенами (не используется в коде) |

## Быстрые команды

### Создание всех Connections

```bash
# ВАЖНО: Замените <YOUR_POSTGRES_PASSWORD> и <YOUR_GITLAB_TOKEN> на реальные значения
# Реальные значения смотрите в config/tokens.json (локальный файл)

# PostgreSQL Connections
airflow connections add 'tim_db_pluginsdb' --conn-type 'postgres' --conn-host '192.168.42.188' --conn-schema 'pluginsdb' --conn-login 'postgres' --conn-password '<YOUR_POSTGRES_PASSWORD>' --conn-port '5430'
airflow connections add 'tim_db_postgres' --conn-type 'postgres' --conn-host '192.168.42.188' --conn-schema 'postgres' --conn-login 'postgres' --conn-password '<YOUR_POSTGRES_PASSWORD>' --conn-port '5430'

# GitLab API
airflow connections add 'gitlab_api' --conn-type 'http' --conn-host 'http://192.168.42.188:13080' --conn-password '<YOUR_GITLAB_TOKEN>'

# SharePoint (замените на реальные значения)
airflow connections add 'askit_http_sharepoint_tim' --conn-type 'http' --conn-host '<SHAREPOINT_URL>' --conn-login '<USERNAME>' --conn-password '<PASSWORD>'
```

### Создание всех Variables

```bash
# Базовые пути
airflow variables set ETL_DATA_ROOT_PATH '/opt/airflow/data'

# Google Sheets для GitLab DAG
airflow variables set gsheet_config '{"service_account_path":"/opt/airflow/config/revitmaterials-db15db824f22.json","spreadsheet_key":"19ZDWnS0Ft8bLVCbVyHsOatTTzidv55r5Rj7Woi9mNck"}'

# Google Sheets для Families DAG
airflow variables set gsheet_families_key '1C3AJ-0uzoIr97ZaVqeOsi-JbqkYKhsirsoSRhOc1Xnw'
airflow variables set gsheet_families_worksheet 'DB_Семейства'
airflow variables set gsheet_service_account_json "$(cat /opt/airflow/config/revitmaterials-db15db824f22.json | tr -d '\n')"
```

### Проверка настройки

```bash
# Проверка Connections
airflow connections list | grep -E "tim_db|gitlab_api|sharepoint"

# Проверка Variables
airflow variables list | grep -E "ETL_DATA_ROOT_PATH|gsheet"

# Проверка DAG
airflow dags list | grep etl_dag

# Проверка ошибок
airflow dags list-import-errors
```

## Расписание DAG

| DAG | Schedule | Описание |
|-----|----------|----------|
| logs_etl_dag | 0 2 * * * | Ежедневно в 02:00 UTC |
| projectsync_etl_dag | 0 3 * * * | Ежедневно в 03:00 UTC |
| gitlab_etl_dag | 0 5 * * 0 | Еженедельно по воскресеньям в 05:00 UTC |
| scripts_etl_dag | 0 6 * * * | Ежедневно в 06:00 UTC |
| sharepoint_etl_dag | */15 * * * * | Каждые 15 минут |
| gsheet_families_etl_dag | 0 */2 * * * | Каждые 2 часа |

## Структура данных

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

**Полная документация:** см. DEPLOYMENT.md
