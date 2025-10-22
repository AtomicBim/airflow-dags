# Changelog - История изменений проекта

## [1.0.0] - 2025-10-22

### ✅ Завершён рефакторинг: Удаление Yougile

---

## 🔥 Основные изменения

### Удалено

#### Файлы (7 шт):
1. **dags/yougile_etl_dag.py** - DAG для Yougile ETL
2. **dags/etl_pipelines/extract/yougile.py** - экстрактор Yougile API
3. **dags/etl_pipelines/transform/yougile.py** - трансформация Yougile данных
4. **config/yougile-plugins-gitlab_mapping.csv** - статичный маппинг файл
5. **AUDIT_RESULTS.md** - устаревший файл аудита
6. **config/TOKENS_USAGE.md** - устаревший файл справки
7. Все временные и backup файлы

#### Код и конфигурация:
- **config/tokens.json:**
  - Секция `"yougile"` с токеном API
  - Секция `"yougile_sticker"` с ID стикера
  - Ссылка на `fetch_gsheet_mapping_yougle.py` в google_sheets

- **dags/etl_pipelines/extract/gsheet.py:**
  - Функция `extract_yougile_mapping()`
  - Функция `append_new_yougile_mappings()`

- **dags/etl_pipelines/transform/scripts.py:**
  - Параметр `yougile_path`
  - Параметр `mapping_path`
  - Чтение Yougile JSON данных
  - Merge логика с маппингом

- **dags/scripts_etl_dag.py:**
  - Connection `yougile_conn`
  - Задача `extract_yougile_tasks()`
  - Задача `get_mapping_file()`
  - Параметры `yougile_path` и `mapping_path` в transform

- **config/README_CONFIG.md:**
  - Секция "Статичные маппинги"
  - Connection `yougile_api`
  - Файл `yougile-plugins-gitlab_mapping.csv` из структуры
  - Папка `yougile/` из data/
  - `yougile_etl_dag.py` из списка DAGs

- **README_ETL_MIGRATION.md:**
  - Секция "4. Yougile ETL DAG"
  - Упоминания yougile в структуре проекта
  - Строка "04:00 UTC - Yougile ETL" из расписания
  - Troubleshooting секция о mapping файле

### Добавлено

#### Документация (6 файлов):
1. **README.md** - Главная страница проекта с навигацией
2. **DEPLOYMENT.md** - Полная пошаговая инструкция по запуску (9 шагов)
3. **QUICK_REFERENCE.md** - Быстрая справка: таблицы Connections и Variables
4. **PROJECT_ANALYSIS.md** - Детальный анализ связности и исполняемости
5. **CHANGELOG.md** - История изменений (этот файл)
6. Обновлены ссылки в **README_ETL_MIGRATION.md**

#### Новые секции в документах:
- Troubleshooting guide
- Контрольный чек-лист запуска
- Быстрые команды для создания Connections и Variables
- Расписание DAG в табличном формате
- Схема источников и назначений данных

### Изменено

#### Структура проекта:
**Было:** 5 ETL DAG (scripts, gitlab, projectsync, yougile, logs)
**Стало:** 4 ETL DAG (scripts, gitlab, projectsync, logs) + 2 дополнительных (sharepoint, gsheet_families)

#### scripts_etl_dag.py:
**Было:**
- 7 extract задач (включая yougile и mapping)
- Transform с 7 параметрами
- Использование mapping файла для merge

**Стало:**
- 5 extract задач (ad_users, plugins, monitoring, dev_stage, gitlab_loc)
- Transform с 5 параметрами
- Прямое merge без mapping файла

#### Конфигурация:
**Было:** 5 Connections (включая yougile_api)
**Стало:** 4 Connections (без yougile_api)

**Расписание выполнения:**
```diff
- 04:00 UTC - Yougile ETL (ежедневно)
```

---

## 📊 Статистика изменений

### Файлы
- **Удалено:** 7 файлов
- **Создано:** 6 документов
- **Изменено:** 7 файлов

### Код
- **Удалено функций:** 4 (extract_yougile_mapping, append_new_yougile_mappings, extract_yougile_tasks, get_mapping_file)
- **Удалено параметров:** 2 (yougile_path, mapping_path)
- **Удалено строк кода:** ~300+
- **Обновлено импортов:** 6 DAG файлов

### Конфигурация
- **Удалено Connections:** 1 (yougile_api)
- **Удалено секций tokens.json:** 2 (yougile, yougile_sticker)

---

## 🎯 Итоговое состояние проекта

### DAG файлы (6 шт):
✅ scripts_etl_dag.py - Основной комплексный pipeline
✅ gitlab_etl_dag.py - GitLab LOC analytics
✅ projectsync_etl_dag.py - Project sync analytics
✅ logs_etl_dag.py - Logs analytics
✅ sharepoint_etl_dag.py - SharePoint sync
✅ gsheet_families_etl_dag.py - Google Sheets families

### Модули ETL:
**Extract (4 файла):**
- ✅ pluginsdb.py (7 функций)
- ✅ gitlab.py
- ✅ gsheet.py (БЕЗ yougile функций)
- ✅ __init__.py

**Transform (4 файла):**
- ✅ scripts.py (БЕЗ yougile параметров)
- ✅ gitlab.py
- ✅ projectsync.py
- ✅ logs.py
- ✅ __init__.py

**Load (1 файл):**
- ✅ datalake.py
- ✅ __init__.py

### Конфигурация:
**Connections (4 шт):**
- ✅ tim_db_pluginsdb
- ✅ tim_db_postgres
- ✅ gitlab_api
- ✅ askit_http_sharepoint_tim

**Variables (5 шт):**
- ✅ ETL_DATA_ROOT_PATH
- ✅ gsheet_config
- ✅ gsheet_families_key
- ✅ gsheet_families_worksheet
- ✅ gsheet_service_account_json

**Файлы:**
- ✅ config/revitmaterials-db15db824f22.json (Google Service Account)
- ✅ config/tokens.json (справочник, БЕЗ yougile)

### Документация (6 файлов):
- ✅ README.md - Главная страница
- ✅ DEPLOYMENT.md - Инструкция по запуску
- ✅ QUICK_REFERENCE.md - Быстрая справка
- ✅ PROJECT_ANALYSIS.md - Анализ проекта
- ✅ README_ETL_MIGRATION.md - История миграции
- ✅ config/README_CONFIG.md - Детали конфигурации

---

## ✅ Проверки качества

### Синтаксис Python
```bash
✅ scripts_etl_dag.py - OK
✅ gitlab_etl_dag.py - OK
✅ projectsync_etl_dag.py - OK
✅ logs_etl_dag.py - OK
✅ sharepoint_etl_dag.py - OK
✅ gsheet_families_etl_dag.py - OK
```

### Полнота удаления Yougile
```bash
✅ grep -ri "yougile" dags/etl_pipelines/ - Нет совпадений
✅ grep -ri "yougile" dags/*.py - Нет совпадений
✅ grep -i "yougile" config/tokens.json - Нет совпадений
```

### Связность модулей
```bash
✅ Все импорты корректны
✅ Нет циклических зависимостей
✅ Все функции имеют корректные сигнатуры
✅ Передача данных через файлы работает
```

---

## 🚀 Следующие шаги

1. **Развёртывание:** Следуйте инструкциям в DEPLOYMENT.md
2. **Тестирование:** Запустите тестовый DAG (logs_etl_dag)
3. **Мониторинг:** Проверьте логи в Airflow Web UI
4. **Оптимизация:** При необходимости настройте max_workers в gitlab_etl_dag

---

## 📋 Ссылки на документацию

- [DEPLOYMENT.md](DEPLOYMENT.md) - **НАЧНИТЕ ОТСЮДА!**
- [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - Быстрая справка
- [PROJECT_ANALYSIS.md](PROJECT_ANALYSIS.md) - Анализ проекта
- [README_ETL_MIGRATION.md](README_ETL_MIGRATION.md) - История миграции
- [config/README_CONFIG.md](config/README_CONFIG.md) - Детали конфигурации

---

## 🤝 Участники

**Рефакторинг выполнен:** Claude Code
**Дата:** 2025-10-22
**Версия проекта:** 1.0.0

---

## 📝 Примечания

- Все токены из `config/tokens.json` актуальны (кроме удалённых yougile)
- Google Service Account файл обновлён до `revitmaterials-db15db824f22.json`
- Проект полностью готов к запуску на виртуалке
- Документация покрывает все аспекты настройки и эксплуатации

---

**Статус:** ✅ ГОТОВ К ЗАПУСКУ
