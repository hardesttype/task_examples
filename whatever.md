Сгенерируй фреймворк, используя библиотеку polars (!), с помощью которого можно будет генерировать признаки для последовательных данных.
Основные идеи генерации фичей описываются далее:

# Этап 1 (FEATURE CONFIG): Создание конфига фичей

Ниже то, как должна выглядеть фича:
feature = {
    'name': 'max_CREDITSUM_MTSB_AND_POTREB_v0', # название признака
    'source': 'cr_loan', # источник (название таблицы)
    'id_column': 'REQUESTID', # id, по которому считаем агрегат
    'column': 'CREDITSUM', # исходное название фичи
    'dtype': float, # тип переменной
    'aggregation': 'max', # агрегация
    'filter_expression': [ # описание фильтров
        {
            'name': 'MTSB',
            'column': 'IS_OWN',
            'operator': 'eq',
            'value': 1,
        },
        {
            'name': 'POTREB',
            'column': 'LOAN_TYPE',
            'operator': 'is_in',
            'value': [9],
        },
    ],
    'filter_conjunction': 'MTSB & POTREB', # А может быть и MTSB | POTREB, например
    'filter_name': 'MTSB_AND_POTREB', 
    'description': 'Максимальная сумма кредита по потребительским кредитам МТС-Банка',
    'version': 0,
    
    'logs': [
        {
            'created': '2024-09-19 12:41:15', 
            'filename': 'cr_loan_batch_001.parquet.gzip',
            'importance': 123.0324,
            'importance_cv': [123.0324, 34.235, 532.234],
            'count': 45269,
            'n_unique': 1536,
            'is_nan': 3264,
            'is_zero': 184,
            'is_infinite': 0,
        }
    ],
}

# Этап 2 (FEATURE MERGED-CONFIG): Merge фичей по фильтрам и аггрегациям
На этом этапе делаются совместные конфиги признаков с одинаковыми фильтрами для оптимизации расчетов

# FOR SOURCE IN SOURCES:
Идем по каждому источнику (таблице) отдельно

# ... FOR MERGED-CONFIG-BATCH IN MERGED-CONFIG-BATCHES (SHUFFLE = True):
Делим фичи на батчи, чтобы помещалось в память

# ... Этап 3 (FEATURE FILTER-APPLY): Расчет фильтра

# ... Этап 4 (FEATURE COUNT-ACCERT): Проверка кол-ва > 0 и уникальных значений > 1
Отбрасываем столбцы с нулевыми значениями

# ... Этап 5 (FEATURE CALCULATE): Расчет аггрегации

# ... Этап 5.1 (Optional - FEATURE SELECT [IMPORTANCE | SHAP]): Отбор фичи по CV
Это не нужно пока делать - NotImplementedError

# ... Этап 6 (FEATURE STORE): Сохранение фичей + статистик
# ...                         in memory или в файл
Заисываем статистики по фиче в блок `logs`
