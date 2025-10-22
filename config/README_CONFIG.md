# Конфигурация ETL Pipelines

## Необходимые файлы

### 1. Service Account для Google Sheets
Разместите JSON файл service account в этой папке:
```
config/revitmaterials-db15db824f22.json
```

**Актуальный файл:** `revitmaterials-db15db824f22.json` (уже скопирован)

### 2. Airflow Connections

Создайте следующие connections в Airflow UI (Admin -> Connections):

#### PostgreSQL Connections

**tim_db_pluginsdb** (источник данных pluginsdb)
- Connection Type: `Postgres`
- Host: `192.168.42.188`
- Schema: `pluginsdb`
- Login: `postgres`
- Password: `<YOUR_POSTGRES_PASSWORD>` *(см. tokens.json)*
- Port: `5430`

**tim_db_postgres** (datalake назначение)
- Connection Type: `Postgres`
- Host: `192.168.42.188`
- Schema: `postgres`
- Login: `postgres`
- Password: `<YOUR_POSTGRES_PASSWORD>` *(см. tokens.json)*
- Port: `5430`

#### API Connections

**gitlab_api**
- Connection Type: `HTTP`
- Host: `http://192.168.42.188:13080`
- Password: `<YOUR_GITLAB_TOKEN>` *(см. tokens.json → gitlab.token)*

### 3. Airflow Variables

Создайте следующие variables в Airflow UI (Admin -> Variables):

**ETL_DATA_ROOT_PATH**
```
/opt/airflow/data
```
Корневая папка для временных данных ETL процессов.

**ETL_CONFIG_PATH**
```
/opt/airflow/config
```
Путь к папке с конфигурационными файлами.

**gsheet_config** (JSON)
```json
{
  "service_account_path": "/opt/airflow/config/revitmaterials-db15db824f22.json",
  "spreadsheet_key": "19ZDWnS0Ft8bLVCbVyHsOatTTzidv55r5Rj7Woi9mNck"
}
```

**gsheet_service_account_json** (JSON - для gsheet_families_etl_dag)
```json
{
  "type": "service_account",
  "project_id": "revitmaterials",
  "private_key_id": "db15db824f224428d3fa4d393c98062315c35b4a",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
  "client_email": "extractor-gsheet-families@revitmaterials.iam.gserviceaccount.com",
  ...
}
```
*Полное содержимое возьмите из файла `config/revitmaterials-db15db824f22.json`*

**gsheet_families_key**
```
1C3AJ-0uzoIr97ZaVqeOsi-JbqkYKhsirsoSRhOc1Xnw
```
Ключ Google Sheet для семейств (используется в gsheet_families_etl_dag).

**gsheet_families_worksheet**
```
DB_Семейства
```
Имя листа в Google Sheet для семейств.

## Дополнительные Connections

**askit_http_sharepoint_tim** (для sharepoint_etl_dag)
- Connection Type: `HTTP`
- Host: `<SHAREPOINT_URL>`
- Login: `<USERNAME>`
- Password: `<PASSWORD>`

## Структура папок в контейнере

```
/opt/airflow/
├── config/
│   ├── revitmaterials-db15db824f22.json
│   └── tokens.json (информационный, не используется в коде)
├── data/
│   ├── scripts/
│   ├── gitlab/
│   ├── projectsync/
│   ├── logs/
│   ├── gsheet/
│   └── sharepoint/
└── dags/
    ├── scripts_etl_dag.py
    ├── gitlab_etl_dag.py
    ├── projectsync_etl_dag.py
    ├── logs_etl_dag.py
    ├── gsheet_families_etl_dag.py
    ├── sharepoint_etl_dag.py
    ├── etl_pipelines/
    │   ├── extract/
    │   ├── transform/
    │   └── load/
    └── coord_sharepoint_etl/
        ├── extract/
        ├── transform/
        └── load/
```

## Развертывание

1. Файл `revitmaterials-db15db824f22.json` уже скопирован в `config/`
2. Создайте все Airflow Connections (используйте значения из `config/tokens.json`)
3. Создайте все Airflow Variables
4. Убедитесь, что папки `/opt/airflow/data` и `/opt/airflow/config` существуют и доступны для записи
5. Перезапустите Airflow scheduler и webserver

## Примечание о tokens.json

Файл `config/tokens.json` содержит актуальные токены и ключи для справки, но **не используется напрямую в коде**. 
Все значения должны быть настроены через Airflow Connections и Variables, как описано выше.
