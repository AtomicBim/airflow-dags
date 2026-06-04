-- =============================================================================
-- parse_project.sql
-- PL/pgSQL функции для обогащения raw -> stg в SQL.
--
-- Все функции — IMMUTABLE / STABLE, помечены PARALLEL SAFE для использования
-- в больших агрегациях. Реализация — портирование один-в-один из:
--   - dags/common/utils.py (extract_short_name, extract_short_project_name,
--                           get_object_name, get_project_solution, get_project_stage)
--   - dags/common/config.py (SECTION_MAP_KORTROS, SECTION_MAP_RUS,
--                            STAGE_MAP_KORTROS, STAGE_MAP_RUS)
--   - dags/etl_pipelines/transform/added_elements.py (count_elements,
--                                                     classify_transaction)
--
-- ВАЖНО: при изменении логики в Python — синхронизировать здесь, иначе
-- расхождение со старыми витринами на этапе 5.3 (см. план рефакторинга).
--
-- Все функции живут в схеме datalake.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- datalake.parse_short_project_name(text) -> text
-- Первые 2 части по '_'. Если частей < 2 — возвращает исходную строку.
-- Соответствие: common/utils.py:extract_short_name
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION datalake.parse_short_project_name(p_name TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $$
DECLARE
    v_parts TEXT[];
BEGIN
    IF p_name IS NULL THEN
        RETURN NULL;
    END IF;

    v_parts := string_to_array(p_name, '_');

    IF array_length(v_parts, 1) IS NULL OR array_length(v_parts, 1) < 2 THEN
        RETURN p_name;
    END IF;

    RETURN v_parts[1] || '_' || v_parts[2];
END;
$$;

-- -----------------------------------------------------------------------------
-- datalake.parse_file_storage_name(text) -> text
-- Всё, кроме последней части по '_'. Если частей <= 1 — возвращает исходную.
-- Соответствие: common/utils.py:extract_short_project_name (да, имя в Python
-- именно такое, путаное; здесь даём более точное имя).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION datalake.parse_file_storage_name(p_name TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $$
DECLARE
    v_parts TEXT[];
    v_len   INT;
BEGIN
    IF p_name IS NULL OR p_name = '' THEN
        RETURN p_name;
    END IF;

    v_parts := string_to_array(p_name, '_');
    v_len   := array_length(v_parts, 1);

    IF v_len IS NULL OR v_len <= 1 THEN
        RETURN p_name;
    END IF;

    RETURN array_to_string(v_parts[1:v_len - 1], '_');
END;
$$;

-- -----------------------------------------------------------------------------
-- datalake.parse_object_name(text) -> text
-- Определение объекта по имени проекта. Соответствие: common/utils.py:get_object_name
-- Порядок проверок — критичен: первая совпавшая ветка побеждает.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION datalake.parse_object_name(p_name TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $$
BEGIN
    IF p_name IS NULL THEN
        RETURN 'Неизвестные проекты';
    END IF;

    -- Регулярки взяты из common/utils.py:get_object_name (re.IGNORECASE).
    -- В Postgres ~* — case-insensitive POSIX regex.
    IF p_name ~* 'СП\.ЛЛУ|стандарт|узлы|узел|библиотека' THEN
        RETURN 'Узлы и стандарты';
    ELSIF p_name ~* 'АТОМ|ДОУ|08-12|ИКП|ATOM|АПУ' THEN
        RETURN 'АТОМ';
    ELSIF p_name ~* 'K01' THEN
        RETURN 'Кортрос';
    ELSIF p_name ~* 'ИНПРО' THEN
        RETURN 'ИНПРО';
    ELSIF p_name ~* 'Ялта' THEN
        RETURN 'Ялта';
    ELSE
        RETURN 'Неизвестные проекты';
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- datalake.parse_project_solution(text, text) -> text
-- Раздел проекта. Соответствие: common/utils.py:get_project_solution +
-- common/config.py:SECTION_MAP_KORTROS / SECTION_MAP_RUS.
--
-- ВАЖНО: возвращает 'Нет данных' (а не 'НД') при отсутствии совпадений —
-- именно так в Python (см. utils.py:174).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION datalake.parse_project_solution(
    p_name        TEXT,
    p_object_name TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $$
BEGIN
    IF p_name IS NULL THEN
        RETURN 'Нет данных';
    END IF;

    IF p_object_name = 'Кортрос' THEN
        -- SECTION_MAP_KORTROS (порядок как в common/config.py)
        IF    position('_AR'  IN p_name) > 0 THEN RETURN 'АР';
        ELSIF position('_AI'  IN p_name) > 0 THEN RETURN 'АИ';
        ELSIF position('_KR'  IN p_name) > 0 THEN RETURN 'КР';
        ELSIF position('_AGK' IN p_name) > 0 THEN RETURN 'АГК';
        ELSIF position('_VK'  IN p_name) > 0 THEN RETURN 'ВК';
        ELSIF position('_EL'  IN p_name) > 0 THEN RETURN 'ЭЛ';
        ELSIF position('_OV'  IN p_name) > 0 THEN RETURN 'ОВ';
        ELSIF position('_AK'  IN p_name) > 0 THEN RETURN 'АК';
        ELSIF position('_SS'  IN p_name) > 0 THEN RETURN 'СС';
        ELSIF position('_TS'  IN p_name) > 0 THEN RETURN 'ТС';
        ELSIF position('_AP'  IN p_name) > 0 THEN RETURN 'АП';
        ELSE  RETURN 'Нет данных';
        END IF;
    ELSE
        -- SECTION_MAP_RUS (порядок как в common/config.py)
        IF    position('_АР'        IN p_name) > 0 THEN RETURN 'АР';
        ELSIF position('_Форэскиз'  IN p_name) > 0 THEN RETURN 'АР';
        ELSIF position('_АИ'        IN p_name) > 0 THEN RETURN 'АИ';
        ELSIF position('_КЖ'        IN p_name) > 0 THEN RETURN 'КЖ';
        ELSIF position('_ВК'        IN p_name) > 0 THEN RETURN 'ВК';
        ELSIF position('_ЭЛ'        IN p_name) > 0 THEN RETURN 'ЭЛ';
        ELSIF position('_ТС'        IN p_name) > 0 THEN RETURN 'ТС';
        ELSIF position('_ТХ'        IN p_name) > 0 THEN RETURN 'ТХ';
        ELSIF position('_ОВ'        IN p_name) > 0 THEN RETURN 'ОВ';
        ELSIF position('_КР'        IN p_name) > 0 THEN RETURN 'КР';
        ELSIF position('_КМ'        IN p_name) > 0 THEN RETURN 'КМ';
        ELSIF position('_АП'        IN p_name) > 0 THEN RETURN 'АП';
        ELSIF position('_ПП'        IN p_name) > 0 THEN RETURN 'ПП';
        ELSIF position('_ПТ'        IN p_name) > 0 THEN RETURN 'ПТ';
        ELSIF position('_СС'        IN p_name) > 0 THEN RETURN 'СС';
        ELSIF position('_ПБ'        IN p_name) > 0 THEN RETURN 'ПБ';
        ELSIF position('_ЭГ'        IN p_name) > 0 THEN RETURN 'ЭГ';
        ELSE  RETURN 'Нет данных';
        END IF;
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- datalake.parse_project_stage(text, text) -> text
-- Стадия проекта. Соответствие: common/utils.py:get_project_stage +
-- common/config.py:STAGE_MAP_KORTROS / STAGE_MAP_RUS.
--
-- В STAGE_MAP есть два режима: 'contains' и 'endswith'. Порядок проверок —
-- ровно как в Python-словарях (порядок вставки сохраняется в dict с Python 3.7+).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION datalake.parse_project_stage(
    p_name        TEXT,
    p_object_name TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $$
BEGIN
    IF p_name IS NULL THEN
        RETURN 'Нет данных';
    END IF;

    IF p_object_name = 'Кортрос' THEN
        -- STAGE_MAP_KORTROS
        IF    position('_P_'   IN p_name) > 0 THEN RETURN 'П';
        ELSIF position('_R_'   IN p_name) > 0 THEN RETURN 'Р';
        ELSIF position('_RD_'  IN p_name) > 0 THEN RETURN 'Р';
        ELSIF position('_AGK_' IN p_name) > 0 THEN RETURN 'ГК';
        ELSIF p_name LIKE '%\_P'  ESCAPE '\' THEN RETURN 'П';
        ELSIF p_name LIKE '%\_PD' ESCAPE '\' THEN RETURN 'П';
        ELSIF p_name LIKE '%\_RD' ESCAPE '\' THEN RETURN 'Р';
        ELSIF p_name LIKE '%\_AGK' ESCAPE '\' THEN RETURN 'ГК';
        ELSE  RETURN 'Нет данных';
        END IF;
    ELSE
        -- STAGE_MAP_RUS
        IF    position('_П_'        IN p_name) > 0 THEN RETURN 'П';
        ELSIF position('_Р_'        IN p_name) > 0 THEN RETURN 'Р';
        ELSIF position('_РД_'       IN p_name) > 0 THEN RETURN 'Р';
        ELSIF position('_ЭП_'       IN p_name) > 0 THEN RETURN 'ЭП';
        ELSIF position('_Форэскиз_' IN p_name) > 0 THEN RETURN 'ЭП';
        ELSIF position('_Эскиз_'    IN p_name) > 0 THEN RETURN 'ЭП';
        ELSIF position('_ФЭ_'       IN p_name) > 0 THEN RETURN 'ЭП';
        ELSIF p_name LIKE '%\_П'        ESCAPE '\' THEN RETURN 'П';
        ELSIF p_name LIKE '%\_Р'        ESCAPE '\' THEN RETURN 'Р';
        ELSIF p_name LIKE '%\_РД'       ESCAPE '\' THEN RETURN 'Р';
        ELSIF p_name LIKE '%\_ЭП'       ESCAPE '\' THEN RETURN 'ЭП';
        ELSIF p_name LIKE '%\_Форэскиз' ESCAPE '\' THEN RETURN 'ЭП';
        ELSIF p_name LIKE '%\_Эскиз'    ESCAPE '\' THEN RETURN 'ЭП';
        ELSIF p_name LIKE '%\_ФЭ'       ESCAPE '\' THEN RETURN 'ЭП';
        ELSE  RETURN 'Нет данных';
        END IF;
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- datalake.parse_elements_count(text) -> int
-- Подсчёт элементов в строке element_ids.
-- Поддерживает 2 формата:
--   - Legacy (varchar[]): '{14476419,14476420}'           -> 2
--   - New (jsonb text):   '["17279361", "17279362"]'      -> 2
--
-- Соответствие: transform/added_elements.py:count_elements
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION datalake.parse_elements_count(p_element_ids TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $$
DECLARE
    v_trimmed TEXT;
    v_jsonb   JSONB;
    v_parts   TEXT[];
    v_cleaned TEXT;
BEGIN
    IF p_element_ids IS NULL THEN
        RETURN 0;
    END IF;

    v_trimmed := btrim(p_element_ids);
    IF v_trimmed = '' OR v_trimmed IN ('[]', '{}', 'nan', 'NaN', 'NULL') THEN
        RETURN 0;
    END IF;

    -- Попытка распарсить как JSON-массив (новый формат revit.added_element).
    IF left(v_trimmed, 1) = '[' THEN
        BEGIN
            v_jsonb := v_trimmed::JSONB;
            IF jsonb_typeof(v_jsonb) = 'array' THEN
                RETURN jsonb_array_length(v_jsonb);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Не валидный JSON — провалимся в legacy-парсинг ниже.
            NULL;
        END;
    END IF;

    -- Legacy varchar[]: '{id1,id2,id3}'. Срезаем фигурные/квадратные скобки и считаем
    -- непустые элементы — поведение strip('{}[]') + split(',') из Python.
    v_cleaned := btrim(v_trimmed, '{}[]');
    IF v_cleaned = '' THEN
        RETURN 0;
    END IF;

    v_parts := string_to_array(v_cleaned, ',');
    -- Отфильтровываем пустые элементы (Python: [e for e in elements if e.strip()])
    RETURN (
        SELECT count(*)::INTEGER
        FROM unnest(v_parts) AS e
        WHERE btrim(e) <> ''
    );
END;
$$;

-- -----------------------------------------------------------------------------
-- datalake.classify_transaction_fallback(text) -> (class TEXT, is_plugin BOOLEAN)
-- Fallback-классификация транзакции, когда её НЕТ в dim_transactions.
-- Соответствие: transform/added_elements.py:FALLBACK_PATTERNS + PLUGIN_PATTERN.
--
-- Используется в transform/02_transform_staging.sql после LEFT JOIN с dim_transactions:
--   COALESCE(dt.class, (datalake.classify_transaction_fallback(t.transaction_name)).class)
--
-- Порядок regex-веток КРИТИЧЕН — соответствует словарю FALLBACK_PATTERNS
-- (Python 3.7+ сохраняет порядок вставки).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION datalake.classify_transaction_fallback(
    p_transaction_name TEXT,
    OUT class          TEXT,
    OUT is_plugin      BOOLEAN
)
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $$
DECLARE
    v_lower TEXT;
BEGIN
    IF p_transaction_name IS NULL THEN
        class     := 'Системные Revit';
        is_plugin := FALSE;
        RETURN;
    END IF;

    v_lower := lower(p_transaction_name);

    -- Флаг плагина (PLUGIN_PATTERN из transform/added_elements.py:48).
    -- Оригинал: r'^аск:|^ack:|microdesk|mpr[A-Z]|modplus|квартирография'
    -- В Postgres ~ (POSIX, case-sensitive после lower()). Для 'mpr[A-Z]'
    -- сохраняем case-sensitive проверку: применяем к оригинальной строке.
    is_plugin := (
        v_lower ~ '^аск:'
        OR v_lower ~ '^ack:'
        OR v_lower ~ 'microdesk'
        OR p_transaction_name ~ 'mpr[A-Z]'
        OR v_lower ~ 'modplus'
        OR v_lower ~ 'квартирография'
    );

    -- FALLBACK_PATTERNS в порядке их объявления в Python.
    IF v_lower ~ 'удал|delete|remove|purge' THEN
        class := 'Удаление';
    ELSIF v_lower ~ 'печать|экспорт|export|pdf|dwg|ifc' THEN
        class := 'Печать и экспорт';
    ELSIF v_lower ~ 'материал' THEN
        class := 'Материалы';
    ELSIF v_lower ~ 'вид|размер|марк|лист|спецификац|легенд|разрез|фасад|план|текст|аннотац|нумерац|фрагмент|фильтр|шаблон|tag|dim' THEN
        class := 'Оформление';
    ELSIF v_lower ~ 'параметр|связь|семейств|группа|загруз|сохран|рабоч|уровень|штрихов|стил|настро|выгруз|блокир|workset|setting|parameter' THEN
        class := 'Управление моделью';
    ELSIF v_lower ~ 'стена|труба|перекрыт|крыш|дверь|окно|колонн|балк|потолок|армир|арматур|воздуховод|фитинг|проем|отверст|лестниц|ограждение|компонент|эскиз|копир|вставк|соедин|создать|редактир|wall|pipe|duct|floor|roof|door|window|create|bend' THEN
        class := 'Построение (моделирование)';
    ELSE
        class := 'Системные Revit';
    END IF;
END;
$$;

COMMENT ON FUNCTION datalake.parse_short_project_name(TEXT)        IS 'Первые 2 части project_title по "_". Портирование common/utils.py:extract_short_name.';
COMMENT ON FUNCTION datalake.parse_file_storage_name(TEXT)         IS 'project_title без последней части по "_". Портирование common/utils.py:extract_short_project_name.';
COMMENT ON FUNCTION datalake.parse_object_name(TEXT)               IS 'Объект ("Кортрос"/"АТОМ"/...). Портирование common/utils.py:get_object_name.';
COMMENT ON FUNCTION datalake.parse_project_solution(TEXT, TEXT)    IS 'Раздел проекта. Портирование common/utils.py:get_project_solution.';
COMMENT ON FUNCTION datalake.parse_project_stage(TEXT, TEXT)       IS 'Стадия проекта. Портирование common/utils.py:get_project_stage.';
COMMENT ON FUNCTION datalake.parse_elements_count(TEXT)            IS 'Количество элементов в element_ids (JSONB или Postgres array literal).';
COMMENT ON FUNCTION datalake.classify_transaction_fallback(TEXT)   IS 'Fallback-классификация транзакции по regex, если её нет в dim_transactions.';
