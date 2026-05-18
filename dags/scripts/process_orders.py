import os
import glob
import logging
import json
import pandas as pd
import clickhouse_connect
from datetime import datetime
 
logger = logging.getLogger(__name__)
 
# ============================================================
# Konfigurasi (dibaca dari environment variable)
# ============================================================
CLICKHOUSE_HOST      = os.getenv('CLICKHOUSE_HOST',      'clickhouse')
CLICKHOUSE_HTTP_PORT = int(os.getenv('CLICKHOUSE_HTTP_PORT', '8123'))
CLICKHOUSE_USER      = os.getenv('CLICKHOUSE_USER',      'default')
CLICKHOUSE_PASSWORD  = os.getenv('CLICKHOUSE_PASSWORD',  '')
CLICKHOUSE_DATABASE  = os.getenv('CLICKHOUSE_DATABASE',  'mci2026')
CLICKHOUSE_TABLE     = os.getenv('CLICKHOUSE_TABLE',     'orders')
DATA_LAKE_PATH       = os.getenv('DATA_LAKE_PATH',       '/opt/airflow/data_lake')
 
 
# ============================================================
# Mapping dtype pandas → ClickHouse
# ============================================================
def pandas_dtype_to_clickhouse(dtype) -> str:
    dtype_str = str(dtype)
    if 'int' in dtype_str:
        return 'Int64'
    elif 'float' in dtype_str:
        return 'Float64'
    elif 'bool' in dtype_str:
        return 'UInt8'
    elif 'datetime' in dtype_str:
        return 'DateTime'
    else:
        return 'String'
 
 
def get_clickhouse_client():
    """Buat koneksi ke ClickHouse."""
    logger.info(f"Connecting ke ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}")
    return clickhouse_connect.get_client(
        host     = CLICKHOUSE_HOST,
        port     = CLICKHOUSE_HTTP_PORT,
        username = CLICKHOUSE_USER,
        password = CLICKHOUSE_PASSWORD,
    )
 
 
def ensure_table_exists(client, df: pd.DataFrame):
    """
    Buat database & tabel jika belum ada.
    Schema di-generate otomatis dari DataFrame.
    """
    # Buat database
    client.command(f'CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}')
    logger.info(f"Database '{CLICKHOUSE_DATABASE}' siap")

    existing_columns = client.query(f"""
        SELECT name
        FROM system.columns
        WHERE database = '{CLICKHOUSE_DATABASE}' AND table = '{CLICKHOUSE_TABLE}'
        ORDER BY position
    """).result_rows
    if existing_columns:
        existing_names = [row[0] for row in existing_columns]
        incoming_names = list(df.columns)
        if existing_names != incoming_names:
            raise RuntimeError(
                f"Schema tabel {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} tidak cocok. "
                f"Kolom di ClickHouse: {existing_names}. "
                f"Kolom dari API: {incoming_names}. "
                "Jika tabel dibuat dari sql/clickhouse_ddl.sql lama, drop tabelnya lalu jalankan DAG lagi."
            )
        logger.info(f"Tabel '{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}' sudah ada dan schema cocok")
        return
 
    # Generate DDL kolom dari dtype DataFrame
    columns_ddl = []
    for col in df.columns:
        ch_type = pandas_dtype_to_clickhouse(df[col].dtype)
        if df[col].isnull().any():
            ch_type = f'Nullable({ch_type})'
        columns_ddl.append(f'    `{col}` {ch_type}')
 
    # Pilih ORDER BY key — prioritaskan kolom 'id'
    id_candidates = [c for c in df.columns if c.lower() in ('id', 'order_id', 'order_number')]
    order_by_col  = id_candidates[0] if id_candidates else df.columns[0]
 
    # PERBAIKAN: Gabungkan kolom ke variabel baru agar tidak error di f-string
    columns_str = ",\n".join(columns_ddl)
 
    ddl = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
(
{columns_str}
)
ENGINE = MergeTree()
ORDER BY (`{order_by_col}`)
SETTINGS index_granularity = 8192
"""
    client.command(ddl)
    logger.info(f"Tabel '{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}' siap")
 
 
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bersihkan & transformasi data sebelum diload ke ClickHouse.
    """
    logger.info(f"Transform — input: {df.shape}")
 
    # 1. Flatten kolom yang berisi array/list/dict → convert ke string dulu
    for col in df.columns:
        if df[col].dtype == object:
            first_valid = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(first_valid, (list, dict)):
                logger.info(f"  Kolom '{col}' berisi nested data, dikonversi ke JSON string")
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if x is not None else '')
 
    # 2. Drop duplikat (aman setelah flatten)
    try:
        df = df.drop_duplicates()
    except TypeError:
        # Fallback: drop duplikat hanya berdasarkan kolom yang hashable
        hashable_cols = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, (str, int, float, bool, type(None)))).all()]
        df = df.drop_duplicates(subset=hashable_cols)
        logger.warning(f"  drop_duplicates fallback, subset: {hashable_cols}")
 
    # 3. Isi missing values berdasarkan tipe kolom
    for col in df.select_dtypes(include=['number']).columns:
        df[col] = df[col].fillna(0)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('').astype(str)
 
    # 3. Konversi kolom tanggal ke string format ClickHouse
    date_cols = [c for c in df.columns
                 if any(x in c.lower() for x in ['date', 'time', 'created', 'updated', 'at'])]
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce') \
                        .dt.strftime('%Y-%m-%d %H:%M:%S') \
                        .fillna('1970-01-01 00:00:00')
            logger.info(f"  Kolom tanggal dikonversi: {col}")
        except Exception as e:
            logger.warning(f"  Gagal konversi kolom {col}: {e}")
 
    # 4. Tambahkan kolom metadata
    df['_loaded_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
 
    logger.info(f"Transform — output: {df.shape} | Kolom: {list(df.columns)}")
    return df
 
 
def load_parquet_files(data_lake_path: str = DATA_LAKE_PATH) -> list[str]:
    """Ambil semua file .parquet yang belum diproses dari data_lake/."""
    pattern = os.path.join(data_lake_path, '*.parquet')
    files   = sorted(glob.glob(pattern))
    logger.info(f"Ditemukan {len(files)} file parquet di {data_lake_path}")
    return files
 
 
def run(data_lake_path: str = DATA_LAKE_PATH) -> dict:
    """
    Entry point utama. Dipanggil oleh Airflow task.
    Return: dict berisi statistik hasil proses.
    """
    parquet_files = load_parquet_files(data_lake_path)
 
    if not parquet_files:
        logger.warning("Tidak ada file parquet untuk diproses!")
        return {'files_processed': 0, 'rows_loaded': 0}
 
    client      = get_clickhouse_client()
    total_rows  = 0
    files_ok    = 0
 
    for filepath in parquet_files:
        logger.info(f"Memproses file: {filepath}")
        try:
            # Baca parquet
            df = pd.read_parquet(filepath, engine='pyarrow')
            logger.info(f"  Dibaca: {len(df)} baris")
 
            # Transform
            df = transform(df)
 
            # Pastikan tabel ada (otomatis buat jika belum)
            ensure_table_exists(client, df)
 
            # Insert ke ClickHouse
            client.insert_df(f'{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}', df)
            logger.info(f"  ✅ Berhasil insert {len(df)} baris")
 
            total_rows += len(df)
            files_ok   += 1
 
            # Hapus file parquet setelah berhasil diload
            os.remove(filepath)
            logger.info(f"  🗑️  File dihapus: {filepath}")
 
        except Exception as e:
            logger.error(f"  ❌ Gagal proses {filepath}: {e}")
            raise
 
    # Verifikasi total row di ClickHouse
    result = client.query(
        f'SELECT count() FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}'
    )
    total_in_db = result.first_row[0]
    logger.info(f"✅ Selesai! {files_ok} file, {total_rows} baris diload. "
                f"Total di DB: {total_in_db}")
 
    return {
        'files_processed': files_ok,
        'rows_loaded'    : total_rows,
        'total_in_db'    : total_in_db,
    }
 
 
# Untuk testing langsung: python process_orders.py
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    stats = run()
    print(f"\n✅ Selesai! Stats: {stats}")
