-- =============================================================================
-- transform/01_extract_load_raw.sql
-- Этап 1: инкрементальная загрузка datalake.raw_added_elements из источников.
--
-- Параметры:
--   %(last_date)s : TIMESTAMP — нижняя граница окна (включительно)
--   %(run_date)s  : TIMESTAMP — верхняя граница окна (исключительно)
--
-- Источники (через postgres_fdw → revit_ext / legacy_ext):
--   revit_ext.added_element      -> source='added',    date >= LEGACY_CUTOFF
--   revit_ext.modified_element   -> source='modified', date >= LEGACY_CUTOFF
--   legacy_ext.added_element_legacy -> source='legacy', date <  LEGACY_CUTOFF
--
-- LEGACY_CUTOFF = '2026-02-14' (см. transform/added_elements.py:LEGACY_CUTOFF_DATE).
--
-- Идемпотентен: DELETE+INSERT в окне. При повторе одного и того же окна результат
-- идентичен. PK id (BIGSERIAL) при повторе будет новым — это нормально, он только
-- технический.
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- DELETE предыдущих данных в окне (защита от дубликатов при повторе/retry).
-- ---------------------------------------------------------------------------
DELETE FROM datalake.raw_added_elements
 WHERE date >= %(last_date)s::timestamp
   AND date <  %(run_date)s::timestamp;

-- ---------------------------------------------------------------------------
-- revit.added_element  ->  source='added'
-- Только date >= 2026-02-14 (как в transform/added_elements.py:373).
-- ---------------------------------------------------------------------------
INSERT INTO datalake.raw_added_elements (
    source, date, user_id, project_title, transaction_name, element_ids,
    program_version, project_path, group_model, description,
    builtin_category, family_name, floor, element_type_name, trace_id
)
SELECT
    'added',
    date,
    user_id::TEXT,
    project_title,
    transaction_name,
    element_ids::TEXT,
    cad_program_version::BIGINT,
    project_path,
    group_model,
    description,
    builtin_category,
    family_name,
    floor::TEXT,
    element_type_name,
    trace_id
  FROM revit_ext.added_element
 WHERE date >= GREATEST(%(last_date)s::timestamp, TIMESTAMP '2026-02-14')
   AND date <  %(run_date)s::timestamp;

-- ---------------------------------------------------------------------------
-- revit.modified_element  ->  source='modified'
-- Структура идентична added_element.
-- ---------------------------------------------------------------------------
INSERT INTO datalake.raw_added_elements (
    source, date, user_id, project_title, transaction_name, element_ids,
    program_version, project_path, group_model, description,
    builtin_category, family_name, floor, element_type_name, trace_id
)
SELECT
    'modified',
    date,
    user_id::TEXT,
    project_title,
    transaction_name,
    element_ids::TEXT,
    cad_program_version::BIGINT,
    project_path,
    group_model,
    description,
    builtin_category,
    family_name,
    floor::TEXT,
    element_type_name,
    trace_id
  FROM revit_ext.modified_element
 WHERE date >= GREATEST(%(last_date)s::timestamp, TIMESTAMP '2026-02-14')
   AND date <  %(run_date)s::timestamp;

-- ---------------------------------------------------------------------------
-- legacy.added_element_legacy  ->  source='legacy'
-- Только date < 2026-02-14. Поля специфичные для revit (program_version,
-- project_path, ...) отсутствуют — остаются NULL.
-- В legacy колонка называется project_name, в RAW храним как project_title
-- (см. transform/added_elements.py:355).
-- program_name всегда 'Revit' — отбрасываем (как в Python).
-- ---------------------------------------------------------------------------
INSERT INTO datalake.raw_added_elements (
    source, date, user_id, project_title, transaction_name, element_ids
)
SELECT
    'legacy',
    date,
    user_id::TEXT,
    project_name AS project_title,
    transaction_name,
    element_ids::TEXT
  FROM legacy_ext.added_element_legacy
 WHERE date >= %(last_date)s::timestamp
   AND date <  LEAST(%(run_date)s::timestamp, TIMESTAMP '2026-02-14');

COMMIT;
