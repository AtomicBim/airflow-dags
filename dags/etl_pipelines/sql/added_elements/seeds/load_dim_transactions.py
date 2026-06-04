"""
seeds/load_dim_transactions.py
==============================

Загрузка словаря Revit-транзакций из ``mappings/transactions.csv`` в
``datalake.dim_transactions``.

Логика дублирует ``transform/added_elements.py:_load_transaction_mapping``
(включая перебор кодировок utf-8 / utf-8-sig / cp1251).

Запуск
------
Внутри Airflow-контейнера::

    python /opt/airflow/dags/etl_pipelines/sql/added_elements/seeds/load_dim_transactions.py

Стратегия загрузки — TRUNCATE + COPY: словарь маленький (~875 строк), но
включает редкие повторы и опечатки, поэтому проще полностью заменять.
Если в будущем CSV будет вести бизнес — переделать на upsert по transaction_name.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import sys

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- Конфигурация ----------------------------------------------------------

TARGET_CONN_ID = "tim_db_postgres"
TARGET_SCHEMA  = "datalake"
TARGET_TABLE   = "dim_transactions"

# Возможные расположения CSV (как в transform/added_elements.py:_load_transaction_mapping).
CSV_CANDIDATES = (
    "/opt/airflow/mappings/transactions.csv",
    os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "mappings", "transactions.csv")
    ),
)
# utf-8-sig идёт ПЕРВЫМ: транзакционный CSV сохранён с BOM (\ufeff). Если поставить
# utf-8 раньше, он успешно прочитает файл, но BOM прилипнет к первому имени колонки
# ('\ufefftransaction_name'), и валидация заголовков упадёт. utf-8-sig автоматически
# срезает BOM. cp1251 — на случай, если в будущем CSV пересохранят в windows-1251.
CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1251")

# True-значения для колонки is_plugin (русские и английские варианты).
TRUE_TOKENS = {"истина", "true", "1", "да", "yes"}


# --- Реализация ------------------------------------------------------------

def _find_csv() -> str:
    for path in CSV_CANDIDATES:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    raise FileNotFoundError(
        f"transactions.csv не найден ни по одному из путей: {CSV_CANDIDATES}"
    )


def _read_csv(path: str) -> list[tuple[str, str, bool]]:
    """Возвращает список (transaction_name, class, is_plugin)."""
    last_err: Exception | None = None
    for enc in CSV_ENCODINGS:
        try:
            with open(path, "r", encoding=enc, newline="") as fh:
                reader = csv.DictReader(fh, delimiter=";")
                if reader.fieldnames is None or not {
                    "transaction_name", "class", "is_plugin"
                }.issubset(reader.fieldnames):
                    raise ValueError(
                        f"CSV не содержит требуемых колонок. Найдено: {reader.fieldnames}"
                    )
                rows: list[tuple[str, str, bool]] = []
                for row in reader:
                    name = (row.get("transaction_name") or "").strip()
                    if not name:
                        continue
                    cls = (row.get("class") or "").strip()
                    is_plugin_token = (row.get("is_plugin") or "").strip().lower()
                    is_plugin = is_plugin_token in TRUE_TOKENS
                    rows.append((name, cls, is_plugin))
                logger.info("Прочитано %s строк из %s (encoding=%s)", len(rows), path, enc)
                return rows
        except UnicodeDecodeError as exc:
            last_err = exc
            continue
    raise RuntimeError(f"Не удалось прочитать {path} ни в одной кодировке: {last_err}")


def _copy_into_target(rows: list[tuple[str, str, bool]]) -> int:
    """TRUNCATE целевой таблицы и COPY новых данных. Возвращает число загруженных строк."""
    hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    full = f'"{TARGET_SCHEMA}"."{TARGET_TABLE}"'

    # Буферим CSV в памяти для COPY FROM STDIN.
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    for name, cls, is_plugin in rows:
        # Postgres COPY понимает 'true'/'false' как BOOLEAN.
        writer.writerow([name, cls, "true" if is_plugin else "false"])
    buffer.seek(0)

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {full}")
            copy_sql = (
                f"COPY {full} (transaction_name, class, is_plugin) "
                f"FROM STDIN WITH (FORMAT CSV)"
            )
            cur.copy_expert(copy_sql, buffer)
            # updated_at заполнится DEFAULT now() автоматически.
            cur.execute(f"SELECT count(*) FROM {full}")
            (loaded,) = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return int(loaded)


def main() -> None:
    csv_path = _find_csv()
    rows = _read_csv(csv_path)
    if not rows:
        logger.warning("CSV пустой — таблица %s.%s не будет тронута", TARGET_SCHEMA, TARGET_TABLE)
        return
    loaded = _copy_into_target(rows)
    logger.info("Загружено %s строк в %s.%s", loaded, TARGET_SCHEMA, TARGET_TABLE)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        logger.exception("Ошибка загрузки dim_transactions: %s", exc)
        sys.exit(1)
