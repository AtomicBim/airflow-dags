# Product Context

## Проблемы, которые решает проект

| Проблема | Причина | Решение (реализовано) |
|---|---|---|
| Висящие воркеры / zombie-процессы | Нет `execution_timeout`, нет `max_active_runs` | `DEFAULT_DAG_ARGS` + `max_active_runs=1` на всех DAG |
| `plugin_engagement_etl_dag` падает каждые 60 мин | `from utils import` → `ModuleNotFoundError` | Исправлен импорт на `from common.utils import` |
| Накопление соединений с БД | `conn.close()` не вызывался при исключении | `pg_connection` context-manager + try/finally |
| SQL-инъекция в `_fetch_last_user_transactions` | f-string с ручным экранированием `'` → `''` | Параметризованный SQL (`%s` + `params=`) |
| `Variable.get()` вызывался при каждом парсинге DAG | Модульный уровень в 3 DAG-файлах | Перенесено внутрь функции DAG |
| Медленный `plugin_engagement` transform (O(N²)) | Цикл `for day in all_dates: filter + groupby` | Векторизация через cumsum + first_seen_day метод |

## Что специально НЕ меняем

- Схема выходных таблиц `datalake.ext_*` — не трогаем
- Расписания DAG — не меняем  
- `if_exists="replace"` → остаётся `replace`
- Секреты (`fernet_key`, `jwt_secret`) — не ротируем
- `config/airflow.cfg` — не редактируем (только env в docker-compose)
- Переход на UPSERT / incremental load для replace-таблиц — вне scope

## Бизнес-логика (ключевые концепции)

- **BIM_USERS** — набор имён BIM-специалистов (в `common/config.py`). Данные делятся на `_designers` и `_bim`.
- **Plugin Engagement Score** — взвешенная метрика (w1 × unique_plugins_norm + w2 × total_launches_norm), рассчитывается кумулятивно на конец каждого дня.
- **SESSION_GAP_SECONDS = 900** — разрыв ≥15 мин = начало новой сессии Revit.
- **ETL Variable `added_elements_last_date`** — водяной знак для инкрементальной загрузки added_elements.
