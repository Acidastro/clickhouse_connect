from clickhouse_driver import Client

from db.db_config import CLICKHOUSE_HOST


def get_client(use_numpy: bool = True):
    client_data = {"host": CLICKHOUSE_HOST, "settings": {'use_numpy': use_numpy}}
    with Client(**client_data) as c:
        return c
