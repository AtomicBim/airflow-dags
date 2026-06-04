-- =============================================================================
-- 03_stg.sql
-- STG-слой: обогащённые данные перед публикацией в витрины.
--
-- Структура повторяет итоговый DataFrame из transform_added_elements()
-- (см. dags/etl_pipelines/transform/added_elements.py:587).
-- Колонка is_bim сохраняется здесь (в витринах будет удалена при INSERT).
--
-- Заполняется в transform/02_transform_staging.sql.
-- =============================================================================

CREATE TABLE IF NOT EXISTS datalake.stg_added_elements (
    -- --- Идентификация / время ---
    date                  TIMESTAMP   NOT NULL,
    action_type           TEXT        NOT NULL
                          CHECK (action_type IN ('added', 'modified')),

    -- --- Пользователь и подразделение (JOIN с dim_ad_users) ---
    user_name             TEXT,
    department            TEXT,
    project_section       TEXT,

    -- --- Названия проекта (парсинг из project_title) ---
    short_project_name    TEXT,    -- первые 2 части по '_'
    file_storage_name     TEXT,    -- всё, кроме последней части по '_'
    object_name           TEXT,    -- 'Кортрос' / 'АТОМ' / 'ИНПРО' / ...
    project_solution_name TEXT,    -- 'АР' / 'КР' / ... / 'Нет данных'
    project_stage_name    TEXT,    -- 'П' / 'Р' / 'ЭП' / 'ГК' / 'Нет данных'

    -- --- Параметры Revit ---
    program_version       BIGINT,           -- nullable; в Python хранится как Int64
    transaction_name      TEXT,
    class                 TEXT,    -- категория транзакции (из dim_transactions или fallback)
    is_plugin             BOOLEAN,
    elements_count        INTEGER NOT NULL DEFAULT 0,
    is_bim                BOOLEAN NOT NULL DEFAULT FALSE,

    -- --- Сессии (расчёт через LAG) ---
    -- DOUBLE PRECISION для совпадения с float64 в текущем Python (diff().dt.total_seconds()).
    -- Округление до INTEGER привело бы к расхождению на дробных частях секунд (<0.1%).
    time_since_prev_sec   DOUBLE PRECISION NOT NULL DEFAULT 0,
    is_session_start      BOOLEAN NOT NULL DEFAULT FALSE,

    -- --- Дополнительные поля из revit.added_element / revit.modified_element ---
    -- (для legacy-строк остаются NULL)
    project_path          TEXT,
    group_model           TEXT,
    description           TEXT,
    builtin_category      TEXT,
    family_name           TEXT,
    floor                 TEXT,
    element_type_name     TEXT,
    trace_id              TEXT,

    -- --- Технические метаданные ---
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Индексы под витрины и аналитику.
CREATE INDEX IF NOT EXISTS stg_added_elements_date_idx
    ON datalake.stg_added_elements (date);

CREATE INDEX IF NOT EXISTS stg_added_elements_user_date_idx
    ON datalake.stg_added_elements (user_name, date);

CREATE INDEX IF NOT EXISTS stg_added_elements_is_bim_idx
    ON datalake.stg_added_elements (is_bim);

COMMENT ON TABLE datalake.stg_added_elements IS
    'STG-слой: обогащённые added/modified элементы. Содержит is_bim для последующего разделения витрин.';
