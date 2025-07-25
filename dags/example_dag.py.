from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import ta
from sqlalchemy import create_engine

def fetch_and_process_data():
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "5m",
        "limit": 100
    }
    response = requests.get(url, params=params)
    data = response.json()

    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)

    # Teknik indikatörler
    df['sma'] = ta.trend.sma_indicator(df['close'], window=14)
    df['ema'] = ta.trend.ema_indicator(df['close'], window=14)
    df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()

    df = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 'sma', 'ema', 'rsi']]
    df = df.dropna()

    # Veritabanına yaz (NeonDB bağlantı bilgilerini ayarla!)
    db_url = "postgresql+psycopg2://neondb_owner:npg_IRiYav7TKA1Z@ep-divine-mouse-a29jty4e-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    engine = create_engine(db_url)

    df.to_sql("btc_usdt_technical", engine, if_exists="append", index=False)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='btc_technical_indicators',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='fetch_process_store_btc_data',
        python_callable=fetch_and_process_data
    )
