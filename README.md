# Optimize dataframe memory usage (python, pandas)

    import pandas as pd
    import numpy as np

    def optimize_memory_usage(df, print_size=True):
    # Function optimizes memory usage in dataframe.
    # (RU) Функция оптимизации типов в dataframe.
    
    # Types for optimization.
        # Типы, которые будем проверять на оптимизацию.
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        # Memory usage size before optimize (Mb).
        # (RU) Размер занимаемой памяти до оптимизации (в Мб).
        before_size = df.memory_usage().sum() / 1024**2    
        for column in df.columns:
            column_type = df[column].dtypes
            if column_type in numerics:
                column_min = df[column].min()
                column_max = df[column].max()
                if str(column_type).startswith('int'):
                    if column_min > np.iinfo(np.int8).min and column_max < np.iinfo(np.int8).max:
                        df[column] = df[column].astype(np.int8)
                    elif column_min > np.iinfo(np.int16).min and column_max < np.iinfo(np.int16).max:
                        df[column] = df[column].astype(np.int16)
                    elif column_min > np.iinfo(np.int32).min and column_max < np.iinfo(np.int32).max:
                        df[column] = df[column].astype(np.int32)
                    elif column_min > np.iinfo(np.int64).min and column_max < np.iinfo(np.int64).max:
                        df[column] = df[column].astype(np.int64)  
                else:
                    if column_min > np.finfo(np.float32).min and column_max < np.finfo(np.float32).max:
                        df[column] = df[column].astype(np.float32)
                    else:
                        df[column] = df[column].astype(np.float64)    
        # Memory usage size after optimize (Mb).
        # (RU) Размер занимаемой памяти после оптимизации (в Мб).
        after_size = df.memory_usage().sum() / 1024**2
        if print_size: print('Memory usage size: before {:5.4f} Mb - after {:5.4f} Mb ({:.1f}%).'.format(before_size, after_size, 100 * (before_size - after_size) / before_size))
        return df

    def import_data_from_csv(filePath):
        # Load a dataframe from csv-file and optimize its memory usage.
        # (RU) Загрузка данных из csv-файла и оптимизация числовых типов для оптимизации использования памяти
        df = pd.read_csv(filePath, parse_dates=True, keep_date_col=True)
        # Show dataframe info before optimize.
        # (RU) Показать информацию о таблице до оптимизации.
        print('-' * 80)
        print(df.info())
        print('-' * 80)
        # (RU) Оптимизация типов в dataframe.
        df = optimize_memory_usage(df)
        # Show dataframe info after optimize.
        # (RU) Показать информацию о таблице после оптимизации.
        print('-' * 80)
        print(df.info())
        print('-' * 80)
        return df
    
    df = import_data_from_csv('data.csv')
