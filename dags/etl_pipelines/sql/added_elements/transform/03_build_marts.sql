-- =============================================================================
-- transform/03_build_marts.sql
-- Этап 3: разделение stg -> две витрины (designers / bim) по флагу is_bim.
--
-- Параметры:
--   %(last_date)s : TIMESTAMP — нижняя граница окна (включительно)
--   %(run_date)s  : TIMESTAMP — верхняя граница окна (исключительно)
--
-- ОКНО МАРТОВ:
--   [last_date, run_date) — БЕЗ буфера в 1 день, который есть в STG.
--   Буферный день [last_date - 1 day, last_date) в STG нужен только для
--   корректного расчёта LAG в текущем запуске и в витрины не льётся
--   (он уже залился предыдущим запуском).
--
-- Идемпотентен: DELETE+INSERT обоих витрин в одной транзакции.
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- DELETE окна в обеих витринах.
-- ---------------------------------------------------------------------------
DELETE FROM datalake.ext_added_elements_designers
 WHERE date >= %(last_date)s::timestamp
   AND date <  %(run_date)s::timestamp;

DELETE FROM datalake.ext_added_elements_bim
 WHERE date >= %(last_date)s::timestamp
   AND date <  %(run_date)s::timestamp;

-- ---------------------------------------------------------------------------
-- BIM-витрина (INNER JOIN с dim_bim_users через is_bim=TRUE в STG).
-- ---------------------------------------------------------------------------
INSERT INTO datalake.ext_added_elements_bim (
    date, action_type, user_name, department, project_section,
    short_project_name, file_storage_name, object_name,
    project_solution_name, project_stage_name,
    program_version, transaction_name, class, is_plugin, elements_count,
    time_since_prev_sec, is_session_start,
    project_path, group_model, description, builtin_category,
    family_name, floor, element_type_name, trace_id
)
SELECT
    date, action_type, user_name, department, project_section,
    short_project_name, file_storage_name, object_name,
    project_solution_name, project_stage_name,
    program_version, transaction_name, class, is_plugin, elements_count,
    time_since_prev_sec, is_session_start,
    project_path, group_model, description, builtin_category,
    family_name, floor, element_type_name, trace_id
  FROM datalake.stg_added_elements
 WHERE is_bim = TRUE
   AND date >= %(last_date)s::timestamp
   AND date <  %(run_date)s::timestamp;

-- ---------------------------------------------------------------------------
-- Designers-витрина (LEFT ANTI: is_bim = FALSE в STG).
-- ---------------------------------------------------------------------------
INSERT INTO datalake.ext_added_elements_designers (
    date, action_type, user_name, department, project_section,
    short_project_name, file_storage_name, object_name,
    project_solution_name, project_stage_name,
    program_version, transaction_name, class, is_plugin, elements_count,
    time_since_prev_sec, is_session_start,
    project_path, group_model, description, builtin_category,
    family_name, floor, element_type_name, trace_id
)
SELECT
    date, action_type, user_name, department, project_section,
    short_project_name, file_storage_name, object_name,
    project_solution_name, project_stage_name,
    program_version, transaction_name, class, is_plugin, elements_count,
    time_since_prev_sec, is_session_start,
    project_path, group_model, description, builtin_category,
    family_name, floor, element_type_name, trace_id
  FROM datalake.stg_added_elements
 WHERE is_bim = FALSE
   AND date >= %(last_date)s::timestamp
   AND date <  %(run_date)s::timestamp;

COMMIT;
