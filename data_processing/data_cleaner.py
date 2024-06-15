"""
DataCleaner Class
This class handles the cleaning of dataframes.
"""

import pandas as pd

class DataCleaner:
    @staticmethod
    def reorder_and_rename_columns(df, order, column_mapping):
        df = df[order]
        df = df.rename(columns=column_mapping)
        return df

    @staticmethod
    def convert_to_datetime(df, date_columns):
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], unit='ms')
        return df

    @staticmethod
    def convert_columns_to_numeric(df, columns):
        for col in columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
