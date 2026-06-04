-- =============================================================================
-- 00_schema.sql
-- Подготовка схемы и расширений для ETL-пайплайна added_elements.
--
-- Идемпотентен: можно выполнять повторно. Безопасно для существующего datalake.
-- Запускается ОДИН РАЗ при инициализации окружения (не в составе runtime DAG-а).
--
-- Connection: tim_db_postgres (датаслой).
-- =============================================================================

-- Расширение для подключения внешних PostgreSQL-источников (revit, legacy).
-- Конкретная настройка SERVER/USER MAPPING/FOREIGN TABLES — в seeds/setup_fdw.py,
-- потому что параметры подключения (host, port, password) приходят из Airflow Connection.
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Целевая схема для всех слоёв (raw / dim / stg / ext).
CREATE SCHEMA IF NOT EXISTS datalake;

-- Отдельная схема для foreign tables, чтобы не смешивать с локальными.
-- Имена внутри будут зеркалить исходные: revit_ext.added_element, revit_ext.modified_element,
-- legacy_ext.added_element_legacy. Это разруливается в seeds/setup_fdw.py.
CREATE SCHEMA IF NOT EXISTS revit_ext;
CREATE SCHEMA IF NOT EXISTS legacy_ext;
