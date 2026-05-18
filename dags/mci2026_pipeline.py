"""
mci2026_pipeline.py
--------------------
Apache Airflow DAG untuk pipeline Orders API → ClickHouse.

Alur task:
    fetch_data → process_data → validate_data

Jadwal  : @daily (setiap hari pukul 00:00)
Dataset : http://96.9.212.102:8000/orders
Target  : ClickHouse → database mci2026, tabel orders
"""

import sys
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Tambahkan path scripts agar bisa di-import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))

import fetch_orders
import process_orders

logger = logging.getLogger(__name__)

# ============================================================
# Default Arguments
# ============================================================
default_args = {
    'owner'           : 'mci2026',
    'depends_on_past' : False,
    'start_date'      : datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 3,
    'retry_delay'     : timedelta(minutes=5),
}


# ============================================================
# Task Functions (wrapper untuk Airflow)
# ============================================================

def task_fetch_data(**context):
    """
    Task 1: Ambil data dari Orders API → simpan ke data_lake/ sebagai .parquet
    """
    api_url        = os.getenv('ORDERS_API_URL', 'http://96.9.212.102:8000/orders')
    data_lake_path = os.getenv('DATA_LAKE_PATH', '/opt/airflow/data_lake')

    logger.info(f"[fetch_data] Mulai fetch dari {api_url}")
    filepath = fetch_orders.run(api_url=api_url, output_dir=data_lake_path)

    # Push path file ke XCom agar bisa dibaca task berikutnya
    context['ti'].xcom_push(key='parquet_filepath', value=filepath)
    logger.info(f"[fetch_data] Selesai. File: {filepath}")
    return filepath


def task_process_data(**context):
    """
    Task 2: Baca .parquet → transform → load ke ClickHouse → hapus file
    """
    data_lake_path = os.getenv('DATA_LAKE_PATH', '/opt/airflow/data_lake')

    logger.info("[process_data] Mulai proses parquet → ClickHouse")
    stats = process_orders.run(data_lake_path=data_lake_path)

    logger.info(f"[process_data] Selesai. Stats: {stats}")
    return stats


def task_validate_data(**context):
    """
    Task 3: Validasi data berhasil masuk ke ClickHouse
    """
    import clickhouse_connect

    host      = os.getenv('CLICKHOUSE_HOST',      'clickhouse')
    port      = int(os.getenv('CLICKHOUSE_HTTP_PORT', '8123'))
    user      = os.getenv('CLICKHOUSE_USER',      'default')
    password  = os.getenv('CLICKHOUSE_PASSWORD',  '')
    database  = os.getenv('CLICKHOUSE_DATABASE',  'mci2026')
    table     = os.getenv('CLICKHOUSE_TABLE',     'orders')

    logger.info("[validate_data] Connecting ke ClickHouse untuk validasi...")

    client = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password
    )

    # 1. Cek total row
    result    = client.query(f'SELECT count() FROM {database}.{table}')
    row_count = result.first_row[0]

    if row_count == 0:
        raise ValueError(f"❌ Validasi GAGAL: tabel {database}.{table} kosong!")

    # 2. Sample data untuk log
    sample = client.query(f'SELECT * FROM {database}.{table} LIMIT 3')
    logger.info(f"[validate_data] Sample data: {sample.result_rows}")

    logger.info(f"[validate_data] ✅ Validasi berhasil! Total baris: {row_count}")
    return row_count


# ============================================================
# Definisi DAG
# ============================================================
with DAG(
    dag_id           = 'mci2026_orders_pipeline',
    default_args     = default_args,
    description      = 'Pipeline: Orders API → data_lake (parquet) → ClickHouse',
    schedule_interval= '@daily',
    catchup          = False,
    tags             = ['mci2026', 'orders', 'pipeline', 'clickhouse'],
) as dag:

    fetch = PythonOperator(
        task_id         = 'fetch_data',
        python_callable = task_fetch_data,
    )

    process = PythonOperator(
        task_id         = 'process_data',
        python_callable = task_process_data,
    )

    validate = PythonOperator(
        task_id         = 'validate_data',
        python_callable = task_validate_data,
    )

    # Alur: fetch_data → process_data → validate_data
    fetch >> process >> validate
