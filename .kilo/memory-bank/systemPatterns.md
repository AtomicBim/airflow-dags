# System Patterns

## Архитектура DAG

Все DAG используют **TaskFlow API** (`@dag` + `@task` декораторы). Нет классических операторов.  
Паттерн: `extract → transform → load` с параллельными extract-задачами.

```
extract_1()  extract_2()  ...  (параллельно)
      └──────────┬──────────┘
            transform()
                │
         load_1()  load_2()   (параллельно)
```

## Правильный паттерн инициализации путей в DAG

```python
# ПРАВИЛЬНО (reference: added_elements_etl_dag.py, plugin_engagement_etl_dag.py)
def my_etl():
    data_root = Path(Variable.get("ETL_DATA_ROOT_PATH", default_var="/tmp/data")) / "subdir"
    data_root.mkdir(parents=True, exist_ok=True)
    
    @task
    def extract():
        output_path = str(data_root / "file.csv")  # closure over data_root
        ...

# НЕПРАВИЛЬНО — вызывается при каждом парсинге DAG-процессором
DATA_ROOT = Path(Variable.get(...))  # на уровне модуля!
DATA_ROOT.mkdir(...)
```

## Соединения с БД

### Источник данных (pluginsdb)
- Conn ID: `tim_db_pluginsdb`
- Используется только в `extract/pluginsdb.py`

### Хранилище результатов (datalake)
- Conn ID: `tim_db_postgres`  
- Используется в `load/datalake.py` и `transform/added_elements.py` (для подгрузки предыдущих транзакций)

### pg_connection helper (`common/db.py`)
```python
with pg_connection("tim_db_pluginsdb") as conn:
    df = pd.read_sql(sql, conn)
# conn.close() вызывается автоматически в finally
# statement_timeout = 900s выставляется перед yield
```
**Важно**: `psycopg2.connection` НЕ поддерживает `with conn:` как closing context-manager — только управление транзакциями. Поэтому helper использует явный `try/finally: conn.close()`.

## Передача данных между задачами

Через **временные CSV-файлы** в `data_root` (не через XCom для больших данных).  
Пути передаются через XCom как строки (return value из @task).

## Общий extract_plugins_task

В `common/common_tasks.py` — переиспользуется в `logs_etl_dag` и `scripts_etl_dag`.  
Изменения сигнатуры `extract_plugins` должны сохранять совместимость.

## Загрузка в datalake

`load_to_postgres` — `DROP TABLE + CREATE + COPY` (replace-стратегия).  
`load_incremental_to_postgres` — `DELETE WHERE date IN (...) + COPY` (incremental).  
Оба используют `cursor.copy_expert` для быстрой вставки.  
Оба имеют `try/except + conn.rollback()` + `finally: cursor.close(); conn.close()`.  
**Не переводить на `pg_connection` — логика rollback должна остаться в функции.**

## Инкрементальная стратегия (added_elements)

Watermark хранится в Airflow Variable `added_elements_last_date` (формат `YYYY-MM-DD`).  
Pattern: extract с `WHERE date > last_date`, load с DELETE+INSERT, обновление Variable.

## Классификация транзакций (added_elements)

Гибридный подход: сначала CSV-маппинг (`mappings/transactions.csv`), затем regex fallback.  
CSV кэшируется через `@lru_cache(maxsize=1)`.
