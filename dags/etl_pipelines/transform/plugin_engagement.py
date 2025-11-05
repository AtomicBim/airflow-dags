"""
Transform модуль для расчета метрики Plugin Engagement Score.
Оценивает использование плагинов проектировщиками (не BIM-пользователями).
Поддерживает исторические данные с разбивкой по дням.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def transform_plugin_engagement(
    ad_path: str,
    monitoring_path: str,
    bim_users: set,
    w1: float = 0.5,
    w2: float = 0.5
) -> pd.DataFrame:
    """
    Вычисляет Plugin Engagement Score для проектировщиков с разбивкой по дням.
    
    Метрика рассчитывается кумулятивно на конец каждого дня:
    - Для каждого дня учитываются ВСЕ данные до конца этого дня включительно
    - Отслеживается динамика изменения вовлеченности во времени
    
    Метрика учитывает:
    - Количество уникальных плагинов (разнообразие инструментария)
    - Общее количество запусков (интенсивность использования)
    
    Методология:
    1. Определение целевой аудитории:
       - Берутся ТОЛЬКО уникальные пользователи из monitoring (кто реально использует плагины)
       - Исключаются BIM-специалисты из config.BIM_USERS
       - Результат: проектировщики, активно использующие плагины
    2. Извлечение дат: все уникальные даты из мониторинга
    3. Для КАЖДОГО дня:
       - Агрегация кумулятивно (данные до конца дня)
       - Нормализация Min-Max (в рамках дня)
       - Расчет метрики
    
    Args:
        ad_path: Путь к CSV с AD пользователями
        monitoring_path: Путь к CSV с данными мониторинга плагинов
        bim_users: Множество BIM-пользователей для фильтрации
        w1: Вес для количества уникальных плагинов (default: 0.5)
        w2: Вес для количества запусков (default: 0.5)
    
    Returns:
        DataFrame с колонками:
        - day: Дата (конец дня)
        - user_name: ФИО проектировщика
        - unique_plugins: Кумулятивное количество уникальных плагинов
        - total_launches: Кумулятивное количество запусков
        - unique_plugins_norm: Нормализованное количество плагинов (в рамках дня)
        - total_launches_norm: Нормализованное количество запусков (в рамках дня)
        - plugin_engagement_score: Итоговая оценка вовлеченности (в рамках дня)
    """
    
    print("=" * 80)
    print("НАЧАЛО ТРАНСФОРМАЦИИ: Plugin Engagement Score (Исторические данные)")
    print("=" * 80)
    
    # Проверка весов
    if not np.isclose(w1 + w2, 1.0):
        raise ValueError(f"Сумма весов должна быть равна 1. Получено: w1={w1}, w2={w2}, сумма={w1+w2}")
    
    print(f"Весовые коэффициенты: w1 (уникальные плагины) = {w1}, w2 (запуски) = {w2}")
    print(f"Режим: Кумулятивный расчет на конец каждого дня")
    
    # 1. Загрузка данных
    print("\n1. Загрузка данных...")
    df_ad = pd.read_csv(ad_path, encoding='utf-8')
    df_monitoring = pd.read_csv(monitoring_path, encoding='utf-8')
    
    print(f"   - AD пользователей: {len(df_ad)}")
    print(f"   - Записей мониторинга: {len(df_monitoring)}")
    print(f"   - BIM-пользователей для фильтрации: {len(bim_users)}")
    
    # 2. Определение колонки с датой
    print("\n2. Обработка дат...")
    
    # Возможные варианты колонок с датой
    date_columns = ['launch_date', 'created_at', 'timestamp', 'date', 'created', 'datetime']
    date_column = None
    
    for col in date_columns:
        if col in df_monitoring.columns:
            date_column = col
            break
    
    if date_column is None:
        raise ValueError(f"Не найдена колонка с датой. Доступные колонки: {list(df_monitoring.columns)}")
    
    print(f"   - Используется колонка с датой: '{date_column}'")
    
    # Парсим даты
    df_monitoring['date_parsed'] = pd.to_datetime(df_monitoring[date_column], errors='coerce')
    df_monitoring['day'] = df_monitoring['date_parsed'].dt.date
    
    # Удаляем строки с невалидными датами
    df_monitoring = df_monitoring.dropna(subset=['day'])
    
    print(f"   - Записей после обработки дат: {len(df_monitoring)}")
    print(f"   - Диапазон дат: {df_monitoring['day'].min()} - {df_monitoring['day'].max()}")
    print(f"   - Уникальных дней: {df_monitoring['day'].nunique()}")
    
    # 3. Определение уникальных пользователей из monitoring
    print("\n3. Определение активных пользователей плагинов...")
    
    # Определяем колонки для join
    # Варианты join: ad_user_id->id, username->username/login, user_display_name->name
    print(f"   - Колонки в monitoring: {list(df_monitoring.columns)}")
    print(f"   - Колонки в AD users: {list(df_ad.columns)}")
    
    # Попытка 1: ad_user_id -> id
    if 'ad_user_id' in df_monitoring.columns and 'id' in df_ad.columns:
        df_merged = df_monitoring.merge(
            df_ad[['id', 'name']],
            left_on='ad_user_id',
            right_on='id',
            how='left'
        )
        df_merged.rename(columns={'name': 'user_name'}, inplace=True)
        print(f"   - Join: ad_user_id <-> id")
    
    # Попытка 2: username -> username или login
    elif 'username' in df_monitoring.columns:
        ad_username_col = None
        if 'username' in df_ad.columns:
            ad_username_col = 'username'
        elif 'login' in df_ad.columns:
            ad_username_col = 'login'
        
        if ad_username_col and 'name' in df_ad.columns:
            df_merged = df_monitoring.merge(
                df_ad[[ad_username_col, 'name']],
                left_on='username',
                right_on=ad_username_col,
                how='left'
            )
            df_merged.rename(columns={'name': 'user_name'}, inplace=True)
            print(f"   - Join: username <-> {ad_username_col}")
        else:
            raise ValueError(f"Не найдена колонка username/login в AD users. Доступные: {list(df_ad.columns)}")
    
    # Попытка 3: user_display_name уже есть ФИО
    elif 'user_display_name' in df_monitoring.columns:
        # Используем user_display_name как есть
        df_merged = df_monitoring.copy()
        df_merged.rename(columns={'user_display_name': 'user_name'}, inplace=True)
        print(f"   - Используется user_display_name напрямую (ФИО уже в мониторинге)")
    
    else:
        raise ValueError(
            f"Не найдены подходящие колонки для join.\n"
            f"Monitoring: {list(df_monitoring.columns)}\n"
            f"AD Users: {list(df_ad.columns)}"
        )
    
    # Удаляем записи без ФИО (если есть)
    df_merged = df_merged.dropna(subset=['user_name'])
    
    print(f"   - Всего уникальных пользователей в monitoring: {df_merged['user_name'].nunique()}")
    print(f"   - BIM-специалистов для исключения: {len(bim_users)}")
    
    # КРИТЕРИЙ ОТБОРА: Проектировщики = уникальные пользователи из monitoring минус BIM-специалисты
    df_designers = df_merged[~df_merged['user_name'].isin(bim_users)].copy()
    
    unique_designers = df_designers['user_name'].nunique()
    print(f"   - Проектировщиков (активных пользователей минус BIM): {unique_designers}")
    print(f"   - Записей мониторинга проектировщиков: {len(df_designers)}")
    
    if df_designers.empty:
        print("   ВНИМАНИЕ: Нет данных по проектировщикам!")
        return pd.DataFrame(columns=[
            'day', 'user_name', 'unique_plugins', 'total_launches',
            'unique_plugins_norm', 'total_launches_norm', 'plugin_engagement_score'
        ])
    
    # 4. Определение колонки плагина
    plugin_column = 'plugin_id' if 'plugin_id' in df_designers.columns else 'plugin'
    print(f"   - Используется колонка плагина: '{plugin_column}'")
    
    # 5. Кумулятивный расчет по дням
    print("\n4. Кумулятивный расчет метрик по дням...")
    
    # Получаем все уникальные даты, отсортированные
    all_dates = sorted(df_designers['day'].unique())
    print(f"   - Обработка {len(all_dates)} дней...")
    
    # Список для накопления результатов
    daily_results = []
    
    # Функция нормализации
    def min_max_normalize(series: pd.Series) -> pd.Series:
        """Нормализация Min-Max в диапазон [0, 1]."""
        min_val = series.min()
        max_val = series.max()
        
        if max_val == min_val:
            return pd.Series([0.5] * len(series), index=series.index)
        
        return (series - min_val) / (max_val - min_val)
    
    # Для каждого дня рассчитываем кумулятивную метрику
    # ОПТИМИЗАЦИЯ: убрана .copy() для экономии памяти - filtered view используется только для агрегации
    for current_day in all_dates:
        # Фильтруем данные до конца текущего дня включительно (без копирования)
        df_until_day = df_designers[df_designers['day'] <= current_day]

        # Агрегация кумулятивных метрик
        df_day_agg = df_until_day.groupby('user_name').agg(
            unique_plugins=(plugin_column, 'nunique'),
            total_launches=(plugin_column, 'count')
        ).reset_index()
        
        # Нормализация в рамках текущего дня
        df_day_agg['unique_plugins_norm'] = min_max_normalize(df_day_agg['unique_plugins'])
        df_day_agg['total_launches_norm'] = min_max_normalize(df_day_agg['total_launches'])
        
        # Расчет Plugin Engagement Score
        df_day_agg['plugin_engagement_score'] = (
            w1 * df_day_agg['unique_plugins_norm'] + 
            w2 * df_day_agg['total_launches_norm']
        )
        
        # Добавляем дату
        df_day_agg['day'] = current_day
        
        daily_results.append(df_day_agg)
    
    # Объединяем все дни
    df_final = pd.concat(daily_results, ignore_index=True)
    
    # Переупорядочиваем колонки
    df_final = df_final[['day', 'user_name', 'unique_plugins', 'total_launches',
                         'unique_plugins_norm', 'total_launches_norm', 'plugin_engagement_score']]
    
    # Сортируем по дате и оценке
    df_final = df_final.sort_values(['day', 'plugin_engagement_score'], ascending=[True, False])
    
    print(f"   - Всего записей: {len(df_final)}")
    print(f"   - Дней: {df_final['day'].nunique()}")
    print(f"   - Проектировщиков: {df_final['user_name'].nunique()}")
    
    # 6. Статистика за последний день
    print("\n5. Статистика за последний день...")
    
    last_day = df_final['day'].max()
    df_last_day = df_final[df_final['day'] == last_day].copy()
    
    print(f"   - Дата: {last_day}")
    print(f"   - Проектировщиков: {len(df_last_day)}")
    print(f"   - Средняя оценка: {df_last_day['plugin_engagement_score'].mean():.4f}")
    print(f"   - Средн. кол-во плагинов: {df_last_day['unique_plugins'].mean():.2f}")
    print(f"   - Средн. кол-во запусков: {df_last_day['total_launches'].mean():.2f}")
    
    # 7. Вывод топ-5 за последний день
    print("\n" + "=" * 80)
    print(f"ТОП-5 проектировщиков на {last_day}:")
    print("=" * 80)
    for idx, row in df_last_day.head(5).iterrows():
        print(f"{list(df_last_day.head(5).index).index(idx)+1}. {row['user_name']}")
        print(f"   Оценка: {row['plugin_engagement_score']:.4f} | "
              f"Плагинов: {row['unique_plugins']} | "
              f"Запусков: {row['total_launches']}")
    
    print("\n" + "=" * 80)
    print(f"НИЗ-5 проектировщиков на {last_day} (требуется внимание):")
    print("=" * 80)
    for idx, row in df_last_day.tail(5).iterrows():
        print(f"{len(df_last_day)-4+list(df_last_day.tail(5).index).index(idx)}. {row['user_name']}")
        print(f"   Оценка: {row['plugin_engagement_score']:.4f} | "
              f"Плагинов: {row['unique_plugins']} | "
              f"Запусков: {row['total_launches']}")
    
    print("\n" + "=" * 80)
    print("ТРАНСФОРМАЦИЯ ЗАВЕРШЕНА")
    print("=" * 80)
    
    return df_final

