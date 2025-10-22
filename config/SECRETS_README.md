# Где взять реальные токены и пароли

## 🔐 ВАЖНО: Секреты НЕ коммитятся в git!

Файл `config/tokens.json` содержит **реальные** токены и пароли, но он **НЕ коммитится** в git (добавлен в .gitignore).

---

## 📋 Как получить реальные значения

### Вариант 1: У вас уже есть tokens.json локально

Если вы клонировали проект и у вас есть локальный файл `config/tokens.json`:

```bash
# Посмотрите реальные значения
cat config/tokens.json
```

### Вариант 2: У вас только tokens.example.json

1. Скопируйте пример:
   ```bash
   cp config/tokens.example.json config/tokens.json
   ```

2. Откройте `config/tokens.json` и замените плейсхолдеры на реальные значения:
   - **GitLab токен:** Запросите у владельца проекта или создайте в GitLab Settings → Access Tokens
   - **PostgreSQL пароль:** Запросите у DBA или владельца проекта
   - **SharePoint credentials:** Запросите у администратора SharePoint

3. Сохраните файл. Он автоматически будет проигнорирован git.

---

## 🔑 Какие токены нужны

### 1. GitLab Personal Access Token

**Где получить:**
- Перейдите в GitLab: http://192.168.42.188:13080
- Settings → Access Tokens
- Создайте токен с правами: `api`, `read_repository`

**Формат:** `glpat-XXXXXXXXXXXXXXXXXXXXX`

**Используется в:**
- Airflow Connection: `gitlab_api`
- DAG: `scripts_etl_dag`, `gitlab_etl_dag`

---

### 2. PostgreSQL Password

**Где получить:**
- Запросите у администратора БД
- Или посмотрите в конфигурации PostgreSQL

**Используется в:**
- Airflow Connection: `tim_db_pluginsdb` (источник)
- Airflow Connection: `tim_db_postgres` (назначение datalake)
- Все 6 DAG используют эти connections

---

### 3. Google Service Account JSON

**Файл:** `config/revitmaterials-db15db824f22.json`

**Где получить:**
- Google Cloud Console → IAM & Admin → Service Accounts
- Создайте service account или запросите готовый JSON

**Используется в:**
- Airflow Variable: `gsheet_config`
- Airflow Variable: `gsheet_service_account_json`
- DAG: `gitlab_etl_dag`, `gsheet_families_etl_dag`

---

### 4. SharePoint Credentials

**Где получить:**
- Используйте свои корпоративные credentials для SharePoint
- Формат: `domain\username`

**Используется в:**
- Airflow Connection: `askit_http_sharepoint_tim`
- DAG: `sharepoint_etl_dag`

---

## 🚀 Быстрое заполнение tokens.json

```json
{
  "gitlab": {
    "token": "glpat-ВАШТОКЕН",
    "file": "config/gitlab_token.json",
    "variable_name": "gitlab_token"
  },
  "postgres": {
    "password": "ВАШПАРОЛЬ",
    "description": "PostgreSQL password для pluginsdb и datalake"
  },
  "google_sheets": {
    "spreadsheet_keys": [
      {
        "key": "19ZDWnS0Ft8bLVCbVyHsOatTTzidv55r5Rj7Woi9mNck",
        "description": "GitLab-plugins маппинг (уже публичный ID)"
      },
      {
        "key": "1C3AJ-0uzoIr97ZaVqeOsi-JbqkYKhsirsoSRhOc1Xnw",
        "description": "Семейства Revit (уже публичный ID)"
      }
    ]
  },
  "sharepoint": {
    "url": "https://ваш-sharepoint.com/sites/yoursite",
    "username": "domain\\username",
    "password": "ВАШПАРОЛЬ"
  }
}
```

---

## ✅ Проверка

После заполнения `config/tokens.json`:

```bash
# Проверьте, что файл НЕ отслеживается git
git status config/tokens.json
# Должно быть: "Untracked" или вообще не показываться

# Проверьте, что файл в .gitignore
grep "tokens.json" .gitignore
# Должно быть: config/tokens.json
```

---

## 🛡️ Безопасность

### ✅ Хорошо:
- `config/tokens.json` в .gitignore
- Реальные токены только в локальном файле
- Документация использует плейсхолдеры

### ❌ Плохо:
- НЕ коммитьте tokens.json в git!
- НЕ публикуйте токены в issue/PR
- НЕ отправляйте tokens.json по почте (используйте secure vault)

---

## 📞 Контакты

Если у вас нет доступа к реальным токенам, свяжитесь с:
- **GitLab:** Администратор GitLab
- **PostgreSQL:** DBA команда
- **Google Sheets:** Владелец сервисного аккаунта
- **SharePoint:** IT Support

---

**Последнее обновление:** 2025-10-22
