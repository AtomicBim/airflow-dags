"""
Transform модуль для расчета метрики Plugin Engagement Score.
Оценивает использование плагинов проектировщиками (не BIM-пользователями).
Поддерживает исторические данные с разбивкой по дням.
"""
import pandas as pd
import numpy as np


def transform_plugin_engagement(
    ad_path: str,
    monitoring_path: str,
    legacy_monitoring_path: str,
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
    1. Объединение исторических (legacy) и новых данных мониторинга:
       - Legacy: переименование user_display_name -> display_name, project_name -> project_title
       - New: JOIN с AD по user_id -> id (получаем display_name, email, company, department)
       - Concat обеих частей в единый DataFrame
    2. Определение целевой аудитории:
       - Берутся ТОЛЬКО уникальные пользователи из объединенного monitoring
       - Исключаются BIM-специалисты из config.BIM_USERS
    3. Извлечение дат: все уникальные даты из мониторинга
    4. Для КАЖДОГО дня:
       - Агрегация кумулятивно (данные до конца дня)
       - Нормализация Min-Max (в рамках дня)
       - Расчет метрики

    Args:
        ad_path: Путь к CSV с AD пользователями
        monitoring_path: Путь к CSV с НОВЫМИ данными мониторинга (plugins.monitoring)
        legacy_monitoring_path: Путь к CSV со СТАРЫМИ данными мониторинга (legacy.monitoring_legacy)
        bim_users: Множество BIM-пользователей для фильтрации
        w1: Вес для количества уникальных плагинов (default: 0.5)
        w2: Вес для количества запусков (default: 0.5)

    Returns:
        DataFrame с колонками:
        - day: Дата (конец дня)
        - user_name: ФИО проектировщика
        - email, company, department, project_section: Доп. поля из AD
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

    # 1. Загрузка данных
    print("\n1. Загрузка данных...")
    df_ad = pd.read_csv(ad_path, encoding='utf-8')
    df_monitoring_legacy = pd.read_csv(legacy_monitoring_path, encoding='utf-8')
    df_monitoring_new = pd.read_csv(monitoring_path, encoding='utf-8')

    print(f"   - AD пользователей: {len(df_ad)}")
    print(f"   - Legacy записей мониторинга: {len(df_monitoring_legacy)}")
    print(f"   - New записей мониторинга: {len(df_monitoring_new)}")
    print(f"   - BIM-пользователей для фильтрации: {len(bim_users)}")

    # 2. Подготовка СТАРЫХ данных: переименование колонок к общему стандарту.
    # В legacy уже есть user_display_name и username, поэтому AD не подключаем.
    df_monitoring_legacy = df_monitoring_legacy.rename(columns={
        "project_name": "project_title",
        "user_display_name": "display_name"
    })

    # 3. Подготовка НОВЫХ данных: JOIN с AD по user_id, чтобы получить ФИО и доп. поля.
    # В AD колонки могут отличаться по проекту, поэтому собираем доступные.
    ad_join_candidates = ['email', 'display_name', 'company', 'department', 'project_section', 'project_doc_section']
    ad_columns_available = ['id'] + [c for c in ad_join_candidates if c in df_ad.columns]
    df_ad_clean = df_ad[ad_columns_available].copy()

    df_monitoring_new = df_monitoring_new.merge(
        df_ad_clean,
        how="left",
        left_on="user_id",
        right_on="id"
    ).drop(columns=["id"], errors="ignore")

    # 4. Объединение СТАРЫХ и НОВЫХ данных
    df_monitoring = pd.concat([df_monitoring_legacy, df_monitoring_new], ignore_index=True)
    print(f"   - Объединенных записей мониторинга: {len(df_monitoring)}")

    # 5. Обработка дат
    print("\n2. Обработка дат...")

    # Возможные варианты колонок с датой (legacy и new могут отличаться)
    date_columns = ['date', 'launch_date', 'created_at', 'timestamp', 'created', 'datetime']
    date_column = None

    for col in date_columns:
        if col in df_monitoring.columns:
            date_column = col
            break

    if date_column is None:
        raise ValueError(f"Не найдена колонка с датой. Доступные колонки: {list(df_monitoring.columns)}")

    print(f"   - Используется колонка с датой: '{date_column}'")

    df_monitoring['date_parsed'] = pd.to_datetime(df_monitoring[date_column], errors='coerce')
    df_monitoring['day'] = df_monitoring['date_parsed'].dt.date

    # Удаляем строки с невалидными датами
    df_monitoring = df_monitoring.dropna(subset=['day'])

    print(f"   - Записей после обработки дат: {len(df_monitoring)}")
    print(f"   - Диапазон дат: {df_monitoring['day'].min()} - {df_monitoring['day'].max()}")
    print(f"   - Уникальных дней: {df_monitoring['day'].nunique()}")

    # 6. Определение целевой аудитории: проектировщики (не BIM)
    print("\n3. Определение проектировщиков...")

    if 'display_name' not in df_monitoring.columns:
        raise ValueError(
            f"Не найдена колонка 'display_name' в объединенном monitoring. "
            f"Проверьте rename legacy и merge new с AD. "
            f"Доступные колонки: {list(df_monitoring.columns)}"
        )

    # user_name = display_name (унифицированное ФИО из legacy/AD)
    df_monitoring['user_name'] = df_monitoring['display_name']

    # Удаляем записи без ФИО (для new — это значит, что user_id отсутствует в AD)
    df_monitoring = df_monitoring.dropna(subset=['user_name'])

    print(f"   - Всего уникальных пользователей в monitoring: {df_monitoring['user_name'].nunique()}")
    print(f"   - BIM-специалистов для исключения: {len(bim_users)}")

    df_designers = df_monitoring[~df_monitoring['user_name'].isin(bim_users)].copy()

    unique_designers = df_designers['user_name'].nunique()
    print(f"   - Проектировщиков (активных пользователей минус BIM): {unique_designers}")
    print(f"   - Записей мониторинга проектировщиков: {len(df_designers)}")

    if df_designers.empty:
        print("   ВНИМАНИЕ: Нет данных по проектировщикам!")
        return pd.DataFrame(columns=[
            'day', 'user_name', 'email', 'company', 'department', 'project_section',
            'unique_plugins', 'total_launches',
            'unique_plugins_norm', 'total_launches_norm', 'plugin_engagement_score'
        ])

    # 7. Определение колонки плагина
    plugin_column = 'plugin_id' if 'plugin_id' in df_designers.columns else 'plugin'
    print(f"   - Используется колонка плагина: '{plugin_column}'")

    # 8. Кумулятивный расчет по дням
    print("\n4. Кумулятивный расчет метрик по дням...")

    all_dates = sorted(df_designers['day'].unique())
    print(f"   - Обработка {len(all_dates)} дней...")

    daily_results = []

    def min_max_normalize(series: pd.Series) -> pd.Series:
        """Нормализация Min-Max в диапазон [0, 1]."""
        min_val = series.min()
        max_val = series.max()

        if max_val == min_val:
            return pd.Series([0.5] * len(series), index=series.index)

        return (series - min_val) / (max_val - min_val)

    # Доп. поля, которые нужно сохранить (берем 'first' для каждого пользователя)
    optional_fields = [f for f in ['email', 'company', 'department', 'project_section', 'project_doc_section']
                       if f in df_designers.columns]

    for current_day in all_dates:
        # Кумулятивно: все данные до конца текущего дня включительно
        df_until_day = df_designers[df_designers['day'] <= current_day]

        agg_kwargs = {
            'unique_plugins': (plugin_column, 'nunique'),
            'total_launches': (plugin_column, 'count'),
            **{field: (field, 'first') for field in optional_fields}
        }

        df_day_agg = df_until_day.groupby('user_name').agg(**agg_kwargs).reset_index()

        # Нормализация в рамках текущего дня
        df_day_agg['unique_plugins_norm'] = min_max_normalize(df_day_agg['unique_plugins'])
        df_day_agg['total_launches_norm'] = min_max_normalize(df_day_agg['total_launches'])

        # Расчет Plugin Engagement Score
        df_day_agg['plugin_engagement_score'] = (
            w1 * df_day_agg['unique_plugins_norm'] +
            w2 * df_day_agg['total_launches_norm']
        )

        df_day_agg['day'] = current_day
        daily_results.append(df_day_agg)

    # Объединяем все дни
    df_final = pd.concat(daily_results, ignore_index=True)

    # Переупорядочиваем колонки
    base_columns = ['day', 'user_name', 'email', 'company', 'department', 'project_section', 'project_doc_section',
                    'unique_plugins', 'total_launches',
                    'unique_plugins_norm', 'total_launches_norm', 'plugin_engagement_score']
    available_columns = [col for col in base_columns if col in df_final.columns]
    df_final = df_final[available_columns]

    # Сортируем по дате и оценке
    df_final = df_final.sort_values(['day', 'plugin_engagement_score'], ascending=[True, False])

    print(f"   - Всего записей: {len(df_final)}")
    print(f"   - Дней: {df_final['day'].nunique()}")
    print(f"   - Проектировщиков: {df_final['user_name'].nunique()}")

    # 9. Статистика за последний день
    print("\n5. Статистика за последний день...")

    last_day = df_final['day'].max()
    df_last_day = df_final[df_final['day'] == last_day].copy()

    print(f"   - Дата: {last_day}")
    print(f"   - Проектировщиков: {len(df_last_day)}")
    print(f"   - Средняя оценка: {df_last_day['plugin_engagement_score'].mean():.4f}")
    print(f"   - Средн. кол-во плагинов: {df_last_day['unique_plugins'].mean():.2f}")
    print(f"   - Средн. кол-во запусков: {df_last_day['total_launches'].mean():.2f}")

    # 10. Вывод топ-5 и низ-5 за последний день
    print("\n" + "=" * 80)
    print(f"ТОП-5 проектировщиков на {last_day}:")
    print("=" * 80)
    for i, (_, row) in enumerate(df_last_day.head(5).iterrows(), start=1):
        print(f"{i}. {row['user_name']}")
        print(f"   Оценка: {row['plugin_engagement_score']:.4f} | "
              f"Плагинов: {row['unique_plugins']} | "
              f"Запусков: {row['total_launches']}")

    print("\n" + "=" * 80)
    print(f"НИЗ-5 проектировщиков на {last_day} (требуется внимание):")
    print("=" * 80)
    tail_start = max(len(df_last_day) - 5, 0) + 1
    for i, (_, row) in enumerate(df_last_day.tail(5).iterrows(), start=tail_start):
        print(f"{i}. {row['user_name']}")
        print(f"   Оценка: {row['plugin_engagement_score']:.4f} | "
              f"Плагинов: {row['unique_plugins']} | "
              f"Запусков: {row['total_launches']}")

    print("\n" + "=" * 80)
    print("ТРАНСФОРМАЦИЯ ЗАВЕРШЕНА")
    print("=" * 80)

    return df_final
