#!/usr/bin/env python3
"""
Скрипт для подготовки JSON credentials для сохранения в Airflow Variable.
Использование:
1. Сохраните этот скрипт как prepare_credentials.py
2. Запустите: python prepare_credentials.py /path/to/revitmaterials-d96ae3c7a1d1.json
3. Скопируйте вывод и вставьте в Airflow Variable
"""

import json
import sys

def prepare_service_account_json(file_path):
    """Читает JSON файл и подготавливает его для Airflow Variable."""
    
    # Читаем файл
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Проверяем наличие необходимых полей
    required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email']
    missing_fields = [field for field in required_fields if field not in data]
    
    if missing_fields:
        print(f"⚠️  Предупреждение: отсутствуют поля: {missing_fields}")
    
    # Выводим JSON в компактном формате (одна строка)
    print("\n" + "="*60)
    print("Скопируйте текст ниже и вставьте в Airflow Variable:")
    print("="*60 + "\n")
    
    # Выводим JSON без дополнительного экранирования
    print(json.dumps(data, separators=(',', ':')))
    
    print("\n" + "="*60)
    print("Инструкция:")
    print("1. Откройте Airflow UI → Admin → Variables")
    print("2. Создайте переменную 'gsheet_service_account_json'")
    print("3. Вставьте скопированный JSON как значение")
    print("4. Сохраните")
    print("="*60)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python prepare_credentials.py /path/to/service-account.json")
        sys.exit(1)
    
    try:
        prepare_service_account_json(sys.argv[1])
    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)