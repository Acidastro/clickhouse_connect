import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent.parent / ".env")

load_dotenv()
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'default')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'default')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT', '8123')

CSV_TABLE_NAME = 'csv_table'
JSN_TABLE_NAME = 'json_table'
PICKLE_TABLE_NAME = 'pickle_table'

CLICKHOUSE_TO_SUPERSET = 'clickhousedb://127.0.0.1:8123/default'
