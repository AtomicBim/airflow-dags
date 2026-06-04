-- =============================================================================
-- transform/02_transform_staging.sql
-- Этап 2: обогащение raw -> stg + расчёт сессий через LAG.
--
-- Параметры:
--   %(last_date)s : TIMESTAMP — нижняя граница окна (включительно)
--   %(run_date)s  : TIMESTAMP — верхняя граница окна (исключительно)
--
-- ВАЖНО: окно пересчёта STG расширено на 1 день назад относительно RAW —
-- [last_date - 1 day, run_date). Это нужно, чтобы LAG(date) для первой строки
-- в новом окне видел предыдущую транзакцию того же пользователя и не порвал
-- сессию на границе. SESSION_GAP_SECONDS = 900 (15 минут), так что 1 дня
-- буфера достаточно с большим запасом.
--
-- Витрины (этап 03) переписываются только за [last_date, run_date) —
-- буферная зона [last_date - 1 day, last_date) в STG пересчитывается каждый
-- запуск, но в витрины не льётся.
--
-- Логика расчётов соответствует transform/added_elements.py (раздел 9):
--   - LAG(date) OVER (PARTITION BY user_name ORDER BY date) — единая цепочка
--     по пользователю независимо от source (added/modified).
--   - dup_rn — флаг строк с одинаковым timestamp в пределах user_name
--     (одна транзакция Revit, несколько затронутых элементов).
--   - is_session_start:
--       * dup_rn > 1                          -> FALSE (не новая сессия)
--       * prev_gap_sec IS NULL (первая)       -> TRUE
--       * prev_gap_sec > SESSION_GAP_SECONDS  -> TRUE
--       * иначе                               -> FALSE
--   - time_since_prev_sec:
--       * dup_rn > 1                          -> 0
--       * is_session_start                    -> MIN_TRANSACTION_SEC (30)
--       * prev_gap_sec > SESSION_GAP_SECONDS  -> 0  (паузу обнуляем)
--       * иначе                               -> prev_gap_sec
--
-- Константы захардкожены в SQL для соответствия Python:
--   SESSION_GAP_SECONDS = 900
--   MIN_TRANSACTION_SEC = 30
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- DELETE окна (включая буферный день для пересчёта сессий).
-- ---------------------------------------------------------------------------
DELETE FROM datalake.stg_added_elements
 WHERE date >= %(last_date)s::timestamp - INTERVAL '1 day'
   AND date <  %(run_date)s::timestamp;

-- ---------------------------------------------------------------------------
-- INSERT обогащённых строк.
-- ---------------------------------------------------------------------------
INSERT INTO datalake.stg_added_elements (
    date, action_type, user_name, department, project_section,
    short_project_name, file_storage_name, object_name,
    project_solution_name, project_stage_name,
    program_version, transaction_name, class, is_plugin,
    elements_count, is_bim,
    time_since_prev_sec, is_session_start,
    project_path, group_model, description, builtin_category,
    family_name, floor, element_type_name, trace_id
)
WITH enriched AS (
    -- JOIN с словарями + парсинг проектов + классификация транзакций.
    SELECT
        r.id,
        r.date,
        -- action_type: legacy -> 'added' (в Python df_legacy["action_type"] = "added").
        CASE WHEN r.source = 'legacy' THEN 'added' ELSE r.source END   AS action_type,

        a.user_name,
        a.department,
        a.project_section,

        r.project_title,
        datalake.parse_short_project_name(r.project_title)             AS short_project_name,
        datalake.parse_file_storage_name(r.project_title)              AS file_storage_name,
        datalake.parse_object_name(r.project_title)                    AS object_name,

        r.program_version,
        r.transaction_name,

        -- Классификация: приоритет dim_transactions, fallback на regex-функцию.
        -- LATERAL вызывает classify_transaction_fallback ровно один раз на строку,
        -- даже если оба её выхода (class, is_plugin) используются.
        COALESCE(dt.class,     fb.class)                               AS class,
        COALESCE(dt.is_plugin, fb.is_plugin)                           AS is_plugin,

        datalake.parse_elements_count(r.element_ids)                   AS elements_count,
        (b.user_name IS NOT NULL)                                      AS is_bim,

        r.project_path,
        r.group_model,
        r.description,
        r.builtin_category,
        r.family_name,
        r.floor,
        r.element_type_name,
        r.trace_id
      FROM datalake.raw_added_elements r
      LEFT JOIN datalake.dim_ad_users      a  ON a.ad_user_id        = r.user_id
      LEFT JOIN datalake.dim_transactions  dt ON dt.transaction_name = r.transaction_name
      LEFT JOIN LATERAL datalake.classify_transaction_fallback(r.transaction_name) AS fb ON TRUE
      LEFT JOIN datalake.dim_bim_users     b  ON b.user_name         = a.user_name
     WHERE r.date >= %(last_date)s::timestamp - INTERVAL '1 day'
       AND r.date <  %(run_date)s::timestamp
),
windowed AS (
    -- Оконные функции: LAG для расчёта интервала, ROW_NUMBER для дедуп-флага.
    -- PARTITION BY user_name (единая цепочка независимо от action_type).
    -- ORDER BY (date, id) — стабильный порядок при равных date (важно для dup_rn).
    SELECT
        e.*,
        EXTRACT(EPOCH FROM (
            e.date - LAG(e.date) OVER (PARTITION BY e.user_name ORDER BY e.date, e.id)
        )) AS prev_gap_sec,
        ROW_NUMBER() OVER (PARTITION BY e.user_name, e.date ORDER BY e.id) AS dup_rn
      FROM enriched e
)
SELECT
    date,
    action_type,
    user_name,
    department,
    project_section,
    short_project_name,
    file_storage_name,
    object_name,
    -- solution и stage зависят от object_name; функции IMMUTABLE,
    -- Postgres соптимизирует повторный вызов parse_object_name.
    datalake.parse_project_solution(project_title, object_name)        AS project_solution_name,
    datalake.parse_project_stage(project_title, object_name)           AS project_stage_name,
    program_version,
    transaction_name,
    class,
    is_plugin,
    elements_count,
    is_bim,
    -- time_since_prev_sec. Логика — см. шапку файла.
    CASE
        WHEN dup_rn > 1                                THEN 0::double precision
        WHEN prev_gap_sec IS NULL                      THEN 30::double precision
        WHEN prev_gap_sec > 900                        THEN 30::double precision
        ELSE prev_gap_sec
    END                                                                AS time_since_prev_sec,
    -- is_session_start: новая сессия = первая транзакция или разрыв > SESSION_GAP.
    -- Дубль-строки (одна транзакция → несколько элементов) НЕ помечаются как
    -- новая сессия — иначе раздували бы суммарное "время работы" на 30 сек × N.
    CASE
        WHEN dup_rn > 1                                THEN FALSE
        WHEN prev_gap_sec IS NULL                      THEN TRUE
        WHEN prev_gap_sec > 900                        THEN TRUE
        ELSE FALSE
    END                                                                AS is_session_start,
    project_path,
    group_model,
    description,
    builtin_category,
    family_name,
    floor,
    element_type_name,
    trace_id
  FROM windowed;

COMMIT;
