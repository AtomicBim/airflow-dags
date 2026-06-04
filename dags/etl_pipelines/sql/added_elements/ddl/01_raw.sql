-- =============================================================================
-- 01_raw.sql
-- RAW-слой: сырая копия данных из источников.
--
-- Структура: объединение полей legacy.added_element_legacy + revit.added_element
-- + revit.modified_element. Колонка source различает происхождение строки.
--
-- Заполняется в transform/01_extract_load_raw.sql инкрементально по дате.
-- Идемпотентен.
-- =============================================================================

CREATE TABLE IF NOT EXISTS datalake.raw_added_elements (
    -- Технический PK для дедуплицирующих DELETE/INSERT и для трассировки.
    -- Не использовать в бизнес-логике.
    id                  BIGSERIAL PRIMARY KEY,

    -- Источник строки. Соответствует action_type в текущей логике трансформа:
    --   'legacy'   - legacy.added_element_legacy (date < 2026-02-14)
    --   'added'    - revit.added_element        (date >= 2026-02-14)
    --   'modified' - revit.modified_element     (date >= 2026-02-14)
    source              TEXT NOT NULL
                        CHECK (source IN ('legacy', 'added', 'modified')),

    -- Общие поля (есть во всех трёх источниках)
    date                TIMESTAMP NOT NULL,
    -- В источнике (public.ad_user.id и revit.*.user_id) это UUID. Храним как TEXT,
    -- чтобы не зависеть от точного типа источника и не падать на FDW-импорте.
    user_id             TEXT,
    project_title       TEXT,
    transaction_name    TEXT,
    -- Хранится в исходном виде: либо PostgreSQL array literal '{14476419,14476420}'
    -- (legacy varchar[]), либо JSON-массив '["17279361","17279362"]' (new jsonb).
    -- Парсинг количества — в STG через функцию datalake.parse_elements_count().
    element_ids         TEXT,

    -- Поля, существующие только в revit.added_element / revit.modified_element.
    -- Для source='legacy' остаются NULL.
    program_version     INTEGER,
    project_path        TEXT,
    group_model         TEXT,
    description         TEXT,
    builtin_category    TEXT,
    family_name         TEXT,
    floor               TEXT,
    element_type_name   TEXT,
    trace_id            TEXT,

    -- Технические метаданные: когда строка попала в RAW.
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Индексы под основные паттерны доступа:
-- 1) Инкрементальный DELETE/INSERT в transform — фильтр по date.
-- 2) Расчёт LAG(date) OVER (PARTITION BY user_id ORDER BY date) в STG.
CREATE INDEX IF NOT EXISTS raw_added_elements_date_idx
    ON datalake.raw_added_elements (date);

CREATE INDEX IF NOT EXISTS raw_added_elements_user_date_idx
    ON datalake.raw_added_elements (user_id, date);

-- Индекс по source ускоряет аналитические запросы вида "сколько строк из modified за период".
CREATE INDEX IF NOT EXISTS raw_added_elements_source_date_idx
    ON datalake.raw_added_elements (source, date);

COMMENT ON TABLE datalake.raw_added_elements IS
    'RAW-слой: сырая копия legacy.added_element_legacy + revit.added_element + revit.modified_element. Заполняется в etl_pipelines/sql/added_elements/transform/01_extract_load_raw.sql.';
