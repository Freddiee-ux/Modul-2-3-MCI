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


def validate_data():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="default",
        password="",
        database="mci2026"
    )

    orders_count = client.query(
        "SELECT count() FROM mci2026.fact_orders"
    ).first_row[0]

    products_count = client.query(
        "SELECT count() FROM mci2026.fact_order_products"
    ).first_row[0]

    duplicate_orders = client.query(
        """
        SELECT count() - countDistinct(order_id)
        FROM mci2026.fact_orders
        """
    ).first_row[0]

    invalid_day = client.query(
        """
        SELECT countIf(order_dow < 0 OR order_dow > 6)
        FROM mci2026.fact_orders
        """
    ).first_row[0]

    invalid_hour = client.query(
        """
        SELECT countIf(order_hour_of_day < 0 OR order_hour_of_day > 23)
        FROM mci2026.fact_orders
        """
    ).first_row[0]

    if orders_count == 0:
        raise ValueError("fact_orders kosong.")

    if duplicate_orders > 0:
        raise ValueError(f"Terdapat duplicate order_id: {duplicate_orders}")

    if invalid_day > 0:
        raise ValueError(f"Terdapat order_dow tidak valid: {invalid_day}")

    if invalid_hour > 0:
        raise ValueError(f"Terdapat order_hour_of_day tidak valid: {invalid_hour}")

    print(f"Validation passed.")
    print(f"Total orders: {orders_count}")
    print(f"Total order products: {products_count}")


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
        python_callable = validate_data,
    )

    # Alur: fetch_data → process_data → validate_data
    fetch >> process >> validate
