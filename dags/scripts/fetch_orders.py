"""
fetch_orders.py
---------------
Script untuk mengambil data dari Orders API dan menyimpannya
sebagai file .parquet di data_lake/.

Dipanggil oleh DAG Airflow pada task: fetch_data
"""

import os
import json
import requests
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ============================================================
# Konfigurasi (dibaca dari environment variable)
# ============================================================
API_URL       = os.getenv('ORDERS_API_URL', 'http://96.9.212.102:8000/orders')
DATA_LAKE_PATH = os.getenv('DATA_LAKE_PATH', '/opt/airflow/data_lake')


def fetch_orders(api_url: str = API_URL) -> pd.DataFrame:
    """
    Ambil data orders dari API.
    Return: DataFrame berisi data mentah
    """
    logger.info(f"Fetching data dari: {api_url}")

    response = requests.get(api_url, timeout=60)
    response.raise_for_status()

    raw_data = response.json()

    # Normalisasi berbagai format response
    if isinstance(raw_data, list):
        records = raw_data
    elif isinstance(raw_data, dict):
        # Cari key yang berisi list of records
        records = next(
            (v for v in raw_data.values() if isinstance(v, list)),
            [raw_data]
        )
    else:
        records = [raw_data]

    df = pd.DataFrame(records)

    # Parquet lebih stabil kalau nested list/dict disimpan sebagai JSON string.
    # API orders punya kolom `products` yang berupa list of dict.
    for col in df.columns:
        if df[col].dtype == object:
            first_valid = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(first_valid, (list, dict)):
                df[col] = df[col].apply(
                    lambda value: json.dumps(value, ensure_ascii=False) if value is not None else ''
                )

    logger.info(f"Berhasil fetch {len(df)} records | Kolom: {list(df.columns)}")
    return df


def save_to_parquet(df: pd.DataFrame, output_dir: str = DATA_LAKE_PATH) -> str:
    """
    Simpan DataFrame ke file .parquet di data_lake/.
    Nama file mengandung timestamp agar tidak saling overwrite.
    Return: path file yang disimpan
    """
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename  = f"orders_{timestamp}.parquet"
    filepath  = os.path.join(output_dir, filename)

    tmp_filepath = f"{filepath}.tmp"
    df.to_parquet(tmp_filepath, index=False, engine='pyarrow')
    os.replace(tmp_filepath, filepath)
    logger.info(f"Data disimpan ke: {filepath} ({os.path.getsize(filepath):,} bytes)")
    return filepath


def run(api_url: str = API_URL, output_dir: str = DATA_LAKE_PATH) -> str:
    """
    Entry point utama. Dipanggil oleh Airflow task.
    Return: path file parquet yang baru dibuat.
    """
    df       = fetch_orders(api_url)
    filepath = save_to_parquet(df, output_dir)
    return filepath


# Untuk testing langsung: python fetch_orders.py
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    path = run()
    print(f"\n✅ Selesai! File tersimpan di: {path}")
