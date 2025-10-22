# Инструкция по запуску ETL Pipeline на виртуалке

## Обзор проекта

Проект содержит 6 Apache Airflow DAG для ETL процессов:
- **scripts_etl_dag** - основной комплексный pipeline (ежедневно 6:00 UTC)
- **gitlab_etl_dag** - синхронизация GitLab проектов (воскресенье 5:00 UTC)
- **projectsync_etl_dag** - синхронизация проектов (ежедневно 3:00 UTC)
- **logs_etl_dag** - аналитика логов (ежедневно 2:00 UTC)
- **sharepoint_etl_dag** - синхронизация SharePoint (каждые 15 минут)
- **gsheet_families_etl_dag** - синхронизация семейств из Google Sheets (каждые 2 часа)

---

## Шаг 1: Подготовка файлов конфигурации

### 1.1 Google Service Account JSON
Убедитесь, что файл service account находится в правильном месте:
```bash
# На виртуалке путь должен быть:
/opt/airflow/config/revitmaterials-db15db824f22.json
```

Если файл находится в другом месте, скопируйте его:
```bash
mkdir -p /opt/airflow/config
cp /path/to/revitmaterials-db15db824f22.json /opt/airflow/config/
chmod 644 /opt/airflow/config/revitmaterials-db15db824f22.json
```

### 1.2 Создание структуры папок для данных
```bash
mkdir -p /opt/airflow/data/{scripts,gitlab,projectsync,logs,gsheet,sharepoint}
chmod -R 755 /opt/airflow/data
```

---

## Шаг 2: Установка зависимостей

```bash
# Активируйте виртуальное окружение Airflow или установите глобально
pip install -r requirements.txt
```

---

## Шаг 3: Создание Airflow Connections

### 3.1 PostgreSQL Connections

**Connection 1: tim_db_pluginsdb** (источник данных)
```bash
# ВНИМАНИЕ: Реальные значения смотрите в config/tokens.json (НЕ в git)
airflow connections add 'tim_db_pluginsdb' \
    --conn-type 'postgres' \
    --conn-host '192.168.42.188' \
    --conn-schema 'pluginsdb' \
    --conn-login 'postgres' \
    --conn-password '<YOUR_POSTGRES_PASSWORD>' \
    --conn-port '5430'
```

**Connection 2: tim_db_postgres** (целевая БД datalake)
```bash
# ВНИМАНИЕ: Реальные значения смотрите в config/tokens.json (НЕ в git)
airflow connections add 'tim_db_postgres' \
    --conn-type 'postgres' \
    --conn-host '192.168.42.188' \
    --conn-schema 'postgres' \
    --conn-login 'postgres' \
    --conn-password '<YOUR_POSTGRES_PASSWORD>' \
    --conn-port '5430'
```

### 3.2 GitLab API Connection

```bash
# ВНИМАНИЕ: Реальный токен смотрите в config/tokens.json (НЕ в git)
airflow connections add 'gitlab_api' \
    --conn-type 'http' \
    --conn-host 'http://192.168.42.188:13080' \
    --conn-password '<YOUR_GITLAB_TOKEN>'
```

### 3.3 SharePoint Connection

```bash
# ВНИМАНИЕ: Замените <SHAREPOINT_URL>, <USERNAME> и <PASSWORD> на реальные значения
airflow connections add 'askit_http_sharepoint_tim' \
    --conn-type 'http' \
    --conn-host '<SHAREPOINT_URL>' \
    --conn-login '<USERNAME>' \
    --conn-password '<PASSWORD>'
```

**Пример:**
```bash
airflow connections add 'askit_http_sharepoint_tim' \
    --conn-type 'http' \
    --conn-host 'https://sharepoint.example.com/sites/yoursite' \
    --conn-login 'domain\username' \
    --conn-password 'your_password'
```

---

## Шаг 4: Создание Airflow Variables

### 4.1 Базовые пути

**ETL_DATA_ROOT_PATH** - корневая папка для временных данных:
```bash
airflow variables set ETL_DATA_ROOT_PATH '/opt/airflow/data'
```

### 4.2 Google Sheets конфигурация для GitLab DAG

**gsheet_config** (JSON variable):
```bash
airflow variables set gsheet_config \
  '{"service_account_path":"/opt/airflow/config/revitmaterials-db15db824f22.json","spreadsheet_key":"19ZDWnS0Ft8bLVCbVyHsOatTTzidv55r5Rj7Woi9mNck"}'
```

### 4.3 Google Sheets конфигурация для Families DAG

**gsheet_families_key** - ключ Google Sheets с семействами:
```bash
airflow variables set gsheet_families_key '1C3AJ-0uzoIr97ZaVqeOsi-JbqkYKhsirsoSRhOc1Xnw'
```

**gsheet_families_worksheet** - имя листа:
```bash
airflow variables set gsheet_families_worksheet 'DB_Семейства'
```

**gsheet_service_account_json** - полный JSON service account:
```bash
# Используйте prepare_credentials.py для форматирования JSON
python prepare_credentials.py /opt/airflow/config/revitmaterials-db15db824f22.json

# Скопируйте вывод команды выше и выполните:
airflow variables set gsheet_service_account_json '<ВСТАВЬТЕ_JSON_СЮДА>'
```

**Альтернативный способ (вручную):**
```bash
airflow variables set gsheet_service_account_json "$(cat /opt/airflow/config/revitmaterials-db15db824f22.json | tr -d '\n')"
```

---

## Шаг 5: Проверка настройки

### 5.1 Проверка Connections
```bash
airflow connections list
```

Должны быть видны:
- ✅ tim_db_pluginsdb
- ✅ tim_db_postgres
- ✅ gitlab_api
- ✅ askit_http_sharepoint_tim

Детальная проверка:
```bash
airflow connections get tim_db_pluginsdb
airflow connections get tim_db_postgres
airflow connections get gitlab_api
airflow connections get askit_http_sharepoint_tim
```

### 5.2 Проверка Variables
```bash
airflow variables list
```

Должны быть видны:
- ✅ ETL_DATA_ROOT_PATH
- ✅ gsheet_config
- ✅ gsheet_families_key
- ✅ gsheet_families_worksheet
- ✅ gsheet_service_account_json

Детальная проверка:
```bash
airflow variables get ETL_DATA_ROOT_PATH
airflow variables get gsheet_config
airflow variables get gsheet_families_key
airflow variables get gsheet_families_worksheet
# gsheet_service_account_json будет очень длинным, проверяйте только наличие
airflow variables get gsheet_service_account_json | head -c 100
```

### 5.3 Проверка структуры папок
```bash
ls -la /opt/airflow/config/
ls -la /opt/airflow/data/
```

Должны быть видны:
```
/opt/airflow/config/
├── revitmaterials-db15db824f22.json  ✅

/opt/airflow/data/
├── scripts/     ✅
├── gitlab/      ✅
├── projectsync/ ✅
├── logs/        ✅
├── gsheet/      ✅
└── sharepoint/  ✅
```

---

## Шаг 6: Проверка DAG

### 6.1 Список DAG
```bash
airflow dags list
```

Должны быть видны:
- ✅ scripts_etl_dag
- ✅ gitlab_etl_dag
- ✅ projectsync_etl_dag
- ✅ logs_etl_dag
- ✅ sharepoint_etl_dag
- ✅ gsheet_families_etl_dag

### 6.2 Проверка ошибок парсинга
```bash
airflow dags list-import-errors
```

Если есть ошибки, исправьте их перед запуском.

### 6.3 Тестовый запуск одного DAG
```bash
# Тест самого простого DAG (logs)
airflow dags test logs_etl_dag 2024-01-01
```

---

## Шаг 7: Запуск Airflow

### 7.1 Запуск через Docker Compose (если используется)
```bash
docker-compose up -d
```

### 7.2 Запуск нативно

**Терминал 1 - Webserver:**
```bash
airflow webserver --port 8080
```

**Терминал 2 - Scheduler:**
```bash
airflow scheduler
```

### 7.3 Доступ к Web UI
Откройте браузер:
```
http://localhost:8080
```

Или на виртуалке:
```
http://<IP_ВИРТУАЛКИ>:8080
```

---

## Шаг 8: Включение DAG

В Airflow Web UI:
1. Перейдите в раздел **DAGs**
2. Найдите нужные DAG
3. Переключите тумблер **ON** для каждого DAG:
   - ✅ scripts_etl_dag
   - ✅ gitlab_etl_dag
   - ✅ projectsync_etl_dag
   - ✅ logs_etl_dag
   - ✅ sharepoint_etl_dag
   - ✅ gsheet_families_etl_dag

---

## Шаг 9: Ручной запуск (опционально)

Если нужно запустить DAG вручную, не дожидаясь расписания:

**Через CLI:**
```bash
airflow dags trigger logs_etl_dag
airflow dags trigger scripts_etl_dag
```

**Через Web UI:**
1. Нажмите на название DAG
2. Нажмите кнопку **Trigger DAG** (иконка "Play")

---

## Расписание выполнения DAG

```
02:00 UTC - logs_etl_dag          (ежедневно)
03:00 UTC - projectsync_etl_dag   (ежедневно)
05:00 UTC - gitlab_etl_dag        (еженедельно по воскресеньям)
06:00 UTC - scripts_etl_dag       (ежедневно)

Каждые 15 минут  - sharepoint_etl_dag
Каждые 2 часа    - gsheet_families_etl_dag
```

---

## Troubleshooting

### Ошибка: "Connection 'tim_db_pluginsdb' not found"
```bash
# Проверьте, что connection создан
airflow connections get tim_db_pluginsdb

# Если нет, создайте заново (см. Шаг 3)
```

### Ошибка: "Variable 'ETL_DATA_ROOT_PATH' not found"
```bash
# Проверьте, что variable создана
airflow variables get ETL_DATA_ROOT_PATH

# Если нет, создайте заново (см. Шаг 4)
```

### Ошибка: "No such file or directory: '/opt/airflow/config/revitmaterials-db15db824f22.json'"
```bash
# Проверьте наличие файла
ls -la /opt/airflow/config/revitmaterials-db15db824f22.json

# Если нет, скопируйте (см. Шаг 1.1)
```

### Ошибка: GitLab extraction слишком долго
```bash
# Уменьшите max_workers в gitlab_etl_dag.py:
# max_workers=8 → max_workers=4
```

### Ошибка: Google Sheets API permission denied
```bash
# Убедитесь, что service account имеет доступ к spreadsheet:
# 1. Откройте Google Sheets в браузере
# 2. Нажмите "Share"
# 3. Добавьте email service account (из JSON файла)
# 4. Дайте права "Editor"
```

### DAG не появляется в списке
```bash
# Проверьте логи scheduler
tail -f $AIRFLOW_HOME/logs/scheduler/latest/*.log

# Проверьте ошибки импорта
airflow dags list-import-errors
```

---

## Мониторинг

### Просмотр логов через CLI
```bash
# Логи конкретной задачи
airflow tasks logs <dag_id> <task_id> <execution_date>

# Пример:
airflow tasks logs scripts_etl_dag extract_ad_users 2024-01-01
```

### Просмотр логов через Web UI
1. Откройте DAG
2. Нажмите на Graph View
3. Кликните на задачу
4. Нажмите **Log**

---

## Контрольный чек-лист

Перед запуском убедитесь:
- ✅ PostgreSQL доступен на 192.168.42.188:5430
- ✅ GitLab доступен на http://192.168.42.188:13080
- ✅ SharePoint доступен (проверьте URL)
- ✅ Файл service account на месте
- ✅ Все Connections созданы (4 штуки)
- ✅ Все Variables созданы (5 штук)
- ✅ Папки /opt/airflow/data/* созданы
- ✅ requirements.txt установлен
- ✅ Нет ошибок в `airflow dags list-import-errors`
- ✅ Airflow scheduler запущен
- ✅ Airflow webserver запущен
- ✅ DAG включены в Web UI

---

## Дополнительная информация

**Версия Airflow:** 2.x+
**Python:** 3.8+
**Справочный файл токенов:** `config/tokens.json`
**Документация миграции:** `README_ETL_MIGRATION.md`
**Детальная конфигурация:** `config/README_CONFIG.md`
