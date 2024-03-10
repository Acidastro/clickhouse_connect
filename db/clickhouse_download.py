"""
Загрузка данных из кликхауса
"""
from collections.abc import Iterable
from typing import List

import pandas as pd

from db.db_client import get_client


class ClickHouseConnect:

    def __init__(self):
        self.client = self._connect()

    @staticmethod
    def _connect():
        client = get_client()
        return client


class ClickhouseBaseDownload(ClickHouseConnect):

    def select_df_from_table(self, table_name: str, cols: str = '*') -> pd.DataFrame:
        """
        Достанет нужные колонки из таблицы
        :return: DataFrame с данными из таблицы.
        """
        query = f"SELECT {cols} FROM {table_name}"
        result = self.client.query_dataframe(query)
        return result

    def select_df_from_table_where_value(
            self,
            table_name: str,
            col_name_1: str,
            col_name_1_value: int | str,
            col_name_2: int,
            col_name_2_value: int | str,
            cols: str | List[str] = '*',
    ):
        """
        Достанет нужные колонки из таблицы csv_table
        :param table_name: Название таблицы
        :param col_name_1: Совпадение колонки 1
        :param col_name_1_value: Ожидаемое значение в колонке 1
        :param col_name_2: Название скважины
        :param col_name_2_value: Ожидаемое значение в колонке 2
        :param cols: Колонки для выборки. Списком, либо готовой строкой имена колонок через запятую.
        :return: DataFrame с данными
        """
        if not isinstance(cols, str) and isinstance(cols, Iterable):
            cols = ', '.join(cols)

        query = (
            f"SELECT {cols} "
            f"FROM {table_name} "
            f"WHERE {col_name_2} = {col_name_2_value} AND {col_name_1} = {col_name_1_value};"
        )
        result = self.client.query_dataframe(query)
        return result
