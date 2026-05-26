# Project Brief — ASK-Apache-Airflow

## Что это

ETL-платформа на **Apache Airflow 3.0.3** (CeleryExecutor + Redis + PostgreSQL).  
Извлекает данные из корпоративной БД `tim_db_pluginsdb` (PostgreSQL), трансформирует и загружает в `datalake` схему отдельного PostgreSQL (`tim_db_postgres`).

## Назначение

Аналитика использования Revit-плагинов и рабочих процессов проектировщиков строительной компании:
- кто из сотрудников использует плагины и как активно (Plugin Engagement Score)
- какие транзакции Revit выполняют проектировщики vs BIM-специалисты
- синхронизация проектов, логи использования

## Целевая аудитория результатов

Данные отображаются в **DataLens** (BI-система). Таблицы результата: `datalake.ext_*`.

## Деплой

Docker Compose на одном сервере Linux. Образ: `apache/airflow:3.0.3` + custom `requirements.txt`.  
Конфигурация Airflow — только через `AIRFLOW__*` env-переменные в `docker-compose.yaml`.  
Файл `config/airflow.cfg` **не редактируется**.

## Репозиторий

GitHub: `https://github.com/AtomicBim/ASK-Apache-Airflow`  
Основная ветка: `development` (или `main`).  
Текущая рабочая ветка: `refactor/etl-reliability-and-performance`.
