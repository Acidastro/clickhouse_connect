import asyncio
import concurrent.futures
import json
import os
from typing import List

import pandas as pd
from clickhouse_driver import Client

from db.db_client import get_client
from db.db_config import JSN_TABLE_NAME, CSV_TABLE_NAME, PICKLE_TABLE_NAME
from logs.logger_conf import logger


class BaseClickHouseManager:
    """ Базовый класс, на его основе создаются узконаправленные классы для загрузки данных в кх. """

    def __init__(self, table_name: str):
        self.table_name: str = table_name
        self.client = get_client()

    def create_table(self, df: pd.DataFrame, drop: bool):
        """
        Создает таблицу в clickhouse с именем self.table_name, если она не существует.
        :param df: Датафрейм с данными и именами колонок.
        :param drop: Если True, то удаляет таблицу перед созданием.
        """
        if drop:
            self.client.execute(f'DROP TABLE IF EXISTS {self.table_name}')
            logger.info(f'{self.table_name} dropped')
        add_cols_que = self._create_col_names(df)

        self.client.execute(
            f'CREATE TABLE IF NOT EXISTS {self.table_name} ({add_cols_que}) '
            f'ENGINE = MergeTree() ORDER BY tuple()')
        logger.info(f'{self.table_name} created')

    def insert_into(self, df: pd.DataFrame):
        """
        Вставляет данные в таблицу self.table_name
        :param df: Датафрейм с данными и именами колонок. Все данные будут вставлены в строковом формате.
        """
        self.client.insert_dataframe(
            f'INSERT INTO {self.table_name} VALUES', df.astype('str'))
        logger.info(f'in {self.table_name} {len(df)} rows inserted')

    @staticmethod
    def _create_col_names(df: pd.DataFrame) -> str:
        """
        Создает запрос с именами колонок и их типом данных, для создания таблицы.
        """
        col_types = list(zip(df.dtypes.index, df.dtypes))
        add_cols_que = ', '.join(f'{col} {type_}' for col, type_ in col_types)
        add_cols_que = add_cols_que.replace('object', 'String')
        add_cols_que = add_cols_que.replace('float64', 'Float64')
        add_cols_que = add_cols_que.replace('int64', 'Int64')
        return add_cols_que


class ClickhouseCSV(BaseClickHouseManager):
    """
    Класс для загрузки данных из csv в clickhouse.

    Внимание! Некоторые колонки могут создаться строками вместо числовых..
    Поэтому можно будет пересоздать таблицу вручную запросом.
    CREATE TABLE new_csv_table
        (
            column_name_1 Int32,
            column_name_2 Int32,
            my_time_col DateTime,
            col_3 Float64,
            col_4 Float64,
            col_5 Float64,
            col_6 Float64
        ) ENGINE = MergeTree ORDER BY (column_name_1, column_name_2) AS
        SELECT
            CAST(column_name_1 AS Int32) AS column_name_1,
            CAST(column_name_2 AS Int32) AS column_name_2,
            my_time_col,
            col_3,
            col_4,
            col_5,
            col_6
        FROM csv_new;
    """

    def __init__(self, directory: str, table_name: str):
        super().__init__(table_name)
        self.dir: str = directory

    async def to_clickhouse(self, drop: bool = True):
        """
        Добавляет каждый CSV по очереди
        :param drop: Если True, то удаляет таблицу перед созданием
        """
        loop = asyncio.get_event_loop()
        with concurrent.futures.ProcessPoolExecutor(max_workers=12) as ex:
            await loop.run_in_executor(ex, self.csv_insert_clickhouse, drop)

    def csv_insert_clickhouse(self, drop: bool):
        """
        Добавляет csv файлы из директории self.dir в таблицу self.name.
        :param drop: Если True, то удаляет таблицу перед созданием
        """
        _len = 0
        sum_rows = 0
        for filename in os.listdir(self.dir):
            if filename.endswith('.csv'):
                df = pd.read_csv(os.path.join(self.dir, filename))
                if df.size > 0:
                    df.drop_duplicates(inplace=True)
                    if _len == 0:
                        self.create_table(df, drop)
                    self.insert_into(df)  # данные в бд
                    logger.info(
                        f'{filename} ok {len(df.columns)} cols {len(df)} rows')
                    _len += 1
                    sum_rows += len(df)
                else:
                    logger.info(f'{filename}, size 0')
        logger.info(f'{_len} files write to db, rows={sum_rows}')


class ClickhouseJSON(BaseClickHouseManager):
    """
    Класс для загрузки данных из json в clickhouse.
    """

    def __init__(self, directory: str, table_name: str, incl_table_name: str):
        super().__init__(table_name)
        self.directory: str = directory
        self.incl_table_name: str = incl_table_name

    def to_clickhouse(self, drop: bool):
        """
        Добавляет каждый JSON по очереди
        :param drop: Если True, то удаляет таблицу перед созданием
        """
        json_list = self._collect_list_from_jsons()
        df = pd.DataFrame(json_list)
        self.create_table(df, drop)
        self.insert_into(df)
        logger.info('JSON write successful')

    def _collect_list_from_jsons(self) -> List[dict]:
        """
        Собрать в один список все файлы
        """
        res = []
        for filename in os.listdir(self.directory):
            if filename.endswith('.json'):
                with open(os.path.join(self.directory, filename), encoding='utf-8') as f:
                    data = json.load(f)
                    res.append(data)
        logger.info(f'{len(res)} jsons downloads')
        return res


class ClickhouseXLSX(BaseClickHouseManager):

    def __init__(self, directory: str, table_name: str, sheet: str | int | None):
        """
        One file
        :param str directory: Путь к одному xlsx файлу
        :param str | int | None sheet: Имя страницы
        """
        super().__init__(table_name)
        self.dir: str = directory
        self.sheet = sheet

    def to_clickhouse(self, drop: bool = True, col_names: List | None = None):
        """
        :param bool drop: Дропнуть таблицу перед созданием
        :param list col_names: Принудительные имена колонок
        """
        self._xlsx_insert_clickhouse(drop, col_names)

    def _xlsx_insert_clickhouse(self, drop: bool, col_names: List | None):
        """
        Добавляет xls / xlsx файлы из директории self.dir в таблицу self.name.
        :param drop:  Если True, то удаляет таблицу перед созданием.
        :param col_names: Принудительные имена колонок, которые подставятся вместо тех, что есть в файле.
        """
        sum_rows = 0
        if self.dir.endswith('.xlsx'):
            df = pd.read_excel(self.dir, sheet_name=self.sheet)
            if col_names:
                df.columns = col_names
            if df.size > 0:
                self.create_table(df, drop)
                self.insert_into(df)
                logger.info(f'{self.dir} ok {len(df.columns)} cols {len(df)} rows')
                sum_rows += len(df)
            else:
                logger.info(f'{self.dir}, size 0')
        logger.info(f'{self.table_name} file write to db, rows={sum_rows}')


def look_table_cols(client: Client, table_name: str):
    """ Посмотреть колонки таблицы """
    cols = client.execute(f'DESCRIBE TABLE {table_name}')
    return cols


def pickle_to_db(chunk_size=10000):
    """ Загрузка данных из файла .pkl чанками. """
    df = pd.read_pickle('../data/df.pkl')
    db_manager = BaseClickHouseManager(PICKLE_TABLE_NAME)
    db_manager.create_table(df, drop=True)

    chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    [db_manager.insert_into(chunk) for chunk in chunks]

    len_ = int(db_manager.client.execute(
        f"select count() from {PICKLE_TABLE_NAME}")[0][0])
    logger.info(f'pickle len_df={len(df)}; len_table={len_}')


def _create_table_from_json():
    """ Пример использования класса ClickhouseJSON """
    jsn_directory = '../data/JSON/'
    ClickhouseJSON(jsn_directory,
                   JSN_TABLE_NAME,
                   INCL_TABLE_NAME).to_clickhouse(drop=True)


async def _create_table_from_csv():
    """ Пример использования класса ClickhouseCSV """
    csv_directory = '../data/CSV/'
    await ClickhouseCSV(csv_directory, CSV_TABLE_NAME).to_clickhouse(drop=True)
