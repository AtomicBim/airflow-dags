-- =============================================================================
-- 04_marts.sql
-- Витрины (data marts) — целевые таблицы для DataLens / отчётности.
--
-- Структура — копия stg_added_elements БЕЗ колонки is_bim.
-- Разделение:
--   ext_added_elements_designers — НЕ-BIM (LEFT ANTI JOIN с dim_bim_users)
--   ext_added_elements_bim       — BIM (INNER JOIN с dim_bim_users)
--
-- Заполняются в transform/03_build_marts.sql (DELETE+INSERT в одной транзакции).
--
-- ВНИМАНИЕ: эти же таблицы уже существуют в prod и наполняются старым DAG-ом.
-- При первом запуске нового DAG-а старые данные должны быть удалены (см. шапку
-- старого DAG про DROP TABLE). DDL ниже использует IF NOT EXISTS — старая
-- таблица не пересоздаётся, и структуру нужно сверять отдельно.
--
-- Если структура отличается (старая таблица сформирована pandas COPY с
-- автогенерацией типов через _generate_create_table_sql) — потребуется
-- DROP TABLE + повторный CREATE через этот файл.
-- =============================================================================

CREATE TABLE IF NOT EXISTS datalake.ext_added_elements_designers (
    date                  TIMESTAMP   NOT NULL,
    action_type           TEXT        NOT NULL,
    user_name             TEXT,
    department            TEXT,
    project_section       TEXT,
    short_project_name    TEXT,
    file_storage_name     TEXT,
    object_name           TEXT,
    project_solution_name TEXT,
    project_stage_name    TEXT,
    program_version       INTEGER,
    transaction_name      TEXT,
    class                 TEXT,
    is_plugin             BOOLEAN,
    elements_count        INTEGER NOT NULL DEFAULT 0,
    time_since_prev_sec   INTEGER NOT NULL DEFAULT 0,
    is_session_start      BOOLEAN NOT NULL DEFAULT FALSE,
    project_path          TEXT,
    group_model           TEXT,
    description           TEXT,
    builtin_category      TEXT,
    family_name           TEXT,
    floor                 TEXT,
    element_type_name     TEXT,
    trace_id              TEXT,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ext_added_elements_designers_date_idx
    ON datalake.ext_added_elements_designers (date);
CREATE INDEX IF NOT EXISTS ext_added_elements_designers_user_idx
    ON datalake.ext_added_elements_designers (user_name);

CREATE TABLE IF NOT EXISTS datalake.ext_added_elements_bim (
    date                  TIMESTAMP   NOT NULL,
    action_type           TEXT        NOT NULL,
    user_name             TEXT,
    department            TEXT,
    project_section       TEXT,
    short_project_name    TEXT,
    file_storage_name     TEXT,
    object_name           TEXT,
    project_solution_name TEXT,
    project_stage_name    TEXT,
    program_version       INTEGER,
    transaction_name      TEXT,
    class                 TEXT,
    is_plugin             BOOLEAN,
    elements_count        INTEGER NOT NULL DEFAULT 0,
    time_since_prev_sec   INTEGER NOT NULL DEFAULT 0,
    is_session_start      BOOLEAN NOT NULL DEFAULT FALSE,
    project_path          TEXT,
    group_model           TEXT,
    description           TEXT,
    builtin_category      TEXT,
    family_name           TEXT,
    floor                 TEXT,
    element_type_name     TEXT,
    trace_id              TEXT,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ext_added_elements_bim_date_idx
    ON datalake.ext_added_elements_bim (date);
CREATE INDEX IF NOT EXISTS ext_added_elements_bim_user_idx
    ON datalake.ext_added_elements_bim (user_name);

COMMENT ON TABLE datalake.ext_added_elements_designers IS
    'Витрина: добавленные/модифицированные элементы Revit для проектировщиков (не-BIM).';
COMMENT ON TABLE datalake.ext_added_elements_bim IS
    'Витрина: добавленные/модифицированные элементы Revit для BIM-специалистов.';
