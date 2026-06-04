-- =============================================================================
-- 02_dim.sql
-- Словари (dimension tables) для обогащения raw -> stg.
--
-- Все таблицы — идемпотентные, наполняются seed-скриптами либо отдельным DAG-ом.
-- Структура максимально близка к исходным датафреймам в текущем transform.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_ad_users
-- Зеркало public.ad_user из AD-инстанса (Airflow Connection tim_db_ad).
-- Сейчас транформ берёт колонки: id, display_name, department, project_section
-- (или project_doc_section), company. См. dags/etl_pipelines/transform/added_elements.py:397.
--
-- Наполняется отдельным DAG-ом ad_sync_dag (вне рамок текущего рефакторинга);
-- временно — через seeds/load_dim_ad_users.py.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS datalake.dim_ad_users (
    -- ID пользователя из public.ad_user (= revit.added_element.user_id).
    -- В источнике это UUID; храним как TEXT — компромисс между типобезопасностью
    -- и гибкостью к разным источникам.
    ad_user_id       TEXT PRIMARY KEY,
    user_name        TEXT,        -- = ad_user.display_name
    department       TEXT,
    project_section  TEXT,        -- унифицировано из project_section / project_doc_section
    company          TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dim_ad_users_user_name_idx
    ON datalake.dim_ad_users (user_name);

COMMENT ON TABLE datalake.dim_ad_users IS
    'Словарь AD-пользователей. Зеркало public.ad_user из tim_db_ad. Унифицирует project_section/project_doc_section.';

-- -----------------------------------------------------------------------------
-- dim_transactions
-- Маппинг названий транзакций Revit -> класс + флаг плагина.
--
-- Источник: mappings/transactions.csv (875 строк, разделитель ';').
-- Логика классификации сохраняет приоритет CSV над regex-fallback'ом
-- (см. classify_transaction в transform/added_elements.py:154).
-- Сам fallback реализован в functions/parse_project.sql -> datalake.classify_transaction().
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS datalake.dim_transactions (
    transaction_name TEXT PRIMARY KEY,
    class            TEXT NOT NULL,
    is_plugin        BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE datalake.dim_transactions IS
    'Словарь транзакций Revit с классификацией. Наполняется из mappings/transactions.csv через seeds/load_dim_transactions.py.';

-- -----------------------------------------------------------------------------
-- dim_bim_users
-- Список BIM-специалистов для разделения витрин на designers/bim.
--
-- Источник: dags/common/config.py:BIM_USERS. Наполняется через
-- seeds/load_dim_bim_users.py. При обновлении BIM_USERS — нужно перенакатить seed.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS datalake.dim_bim_users (
    user_name  TEXT PRIMARY KEY,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE datalake.dim_bim_users IS
    'Список BIM-специалистов. Используется для разделения витрин ext_added_elements_bim / ext_added_elements_designers.';
