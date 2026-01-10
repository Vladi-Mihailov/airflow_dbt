from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import clickhouse_connect
import datetime as dt
import requests

# -------------------------------
# ClickHouse connection settings
# -------------------------------
CLICKHOUSE_HOST = 'clickhouse-server'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = 'default'

# -------------------------------
# CoinGecko API URL
# -------------------------------
BITCOIN_API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

# -------------------------------
# Task function
# -------------------------------
def load_real_bitcoin_data():
    updated = dt.datetime.utcnow()

    # Получаем актуальный курс Bitcoin
    response = requests.get(BITCOIN_API_URL, timeout=10)
    response.raise_for_status()  # выброс исключения, если код ответа != 200

    data = response.json()
    rate_usd = float(data['bitcoin']['usd'])

    # Подключаемся к ClickHouse
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB
    )

    # Создаем таблицу, если она еще не существует
    client.command("""
        CREATE TABLE IF NOT EXISTS bitcoin_price (
            updated DateTime,
            rate_usd Float64
        ) ENGINE = MergeTree()
        ORDER BY updated
    """)

    # Вставляем новую запись
    client.insert(
        table='bitcoin_price',
        data=[(updated, rate_usd)],
        column_names=['updated', 'rate_usd']
    )

# -------------------------------
# DAG definition
# -------------------------------
with DAG(
    dag_id='bitcoin_to_clickhouse_real_ip',
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    tags=['clickhouse', 'bitcoin', 'no-dns'],
) as dag:

    fetch_and_load = PythonOperator(
        task_id='fetch_and_load_real_price',
        python_callable=load_real_bitcoin_data
    )
