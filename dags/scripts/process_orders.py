import os
import glob
import json
import shutil
import logging
from datetime import datetime

import pandas as pd
import clickhouse_connect


logger = logging.getLogger(__name__)


# ============================================================
# Konfigurasi Environment
# ============================================================
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "mci2026")

DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "/opt/airflow/data_lake")
PROCESSED_PATH = os.getenv("PROCESSED_PATH", "/opt/airflow/data_lake/processed")

FACT_ORDERS_TABLE = "fact_orders"
FACT_ORDER_PRODUCTS_TABLE = "fact_order_products"


# ============================================================
# ClickHouse Connection
# ============================================================
def get_clickhouse_client():
    """Membuat koneksi ke ClickHouse."""
    logger.info(
        f"Connecting to ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}"
    )

    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


# ============================================================
# Load Parquet Files
# ============================================================
def load_parquet_files(data_lake_path: str = DATA_LAKE_PATH) -> list[str]:
    """Mengambil semua file Parquet yang ada pada folder data_lake."""
    pattern = os.path.join(data_lake_path, "*.parquet")
    files = sorted(glob.glob(pattern))

    logger.info(f"Ditemukan {len(files)} file parquet di {data_lake_path}")

    return files


# ============================================================
# Validasi Struktur Tabel
# ============================================================
def validate_required_tables(client):
    """
    Memastikan tabel fact_orders dan fact_order_products sudah dibuat.
    DDL sebaiknya dijalankan dari sql/clickhouse_ddl.sql.
    """

    required_tables = [FACT_ORDERS_TABLE, FACT_ORDER_PRODUCTS_TABLE]

    for table in required_tables:
        result = client.query(
            f"""
            SELECT count()
            FROM system.tables
            WHERE database = '{CLICKHOUSE_DATABASE}'
              AND name = '{table}'
            """
        )

        table_exists = result.first_row[0]

        if table_exists == 0:
            raise RuntimeError(
                f"Tabel {CLICKHOUSE_DATABASE}.{table} belum ada. "
                f"Jalankan dulu sql/clickhouse_ddl.sql."
            )

    logger.info("Semua tabel target sudah tersedia di ClickHouse.")


# ============================================================
# Helper untuk parsing products
# ============================================================
def parse_products(products):
    """
    Mengubah kolom products menjadi list of dict.
    Products bisa berbentuk list asli atau string JSON.
    """

    if products is None:
        return []

    if isinstance(products, list):
        return products

    if isinstance(products, str):
        if products.strip() == "":
            return []

        try:
            parsed = json.loads(products)
            if isinstance(parsed, list):
                return parsed
            return []
        except json.JSONDecodeError:
            logger.warning("Gagal parse products JSON.")
            return []

    return []


# ============================================================
# Prepare fact_orders
# ============================================================
def prepare_fact_orders(df: pd.DataFrame, loaded_at: datetime) -> pd.DataFrame:
    """
    Membentuk dataframe fact_orders dari data mentah.
    Data ini berada pada level order.
    """

    required_columns = [
        "order_id",
        "user_id",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
        "eval_set",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise RuntimeError(
            f"Kolom wajib tidak ditemukan untuk fact_orders: {missing_columns}"
        )

    fact_orders = df[required_columns].copy()

    # Drop duplicate berdasarkan order_id
    fact_orders = fact_orders.drop_duplicates(subset=["order_id"])

    # Standarisasi tipe data
    fact_orders["order_id"] = pd.to_numeric(
        fact_orders["order_id"], errors="coerce"
    ).astype("Int64")

    fact_orders["user_id"] = pd.to_numeric(
        fact_orders["user_id"], errors="coerce"
    ).astype("Int64")

    fact_orders["order_number"] = pd.to_numeric(
        fact_orders["order_number"], errors="coerce"
    ).astype("Int64")

    fact_orders["order_dow"] = pd.to_numeric(
        fact_orders["order_dow"], errors="coerce"
    ).astype("Int64")

    fact_orders["order_hour_of_day"] = pd.to_numeric(
        fact_orders["order_hour_of_day"], errors="coerce"
    ).astype("Int64")

    fact_orders["days_since_prior_order"] = pd.to_numeric(
        fact_orders["days_since_prior_order"], errors="coerce"
    )

    fact_orders["eval_set"] = fact_orders["eval_set"].fillna("").astype(str)

    # Metadata load timestamp
    fact_orders["loaded_at"] = loaded_at

    # Hapus baris yang order_id atau user_id kosong
    fact_orders = fact_orders.dropna(subset=["order_id", "user_id"])

    return fact_orders


# ============================================================
# Prepare fact_order_products
# ============================================================
def prepare_fact_order_products(
    df: pd.DataFrame,
    loaded_at: datetime
) -> pd.DataFrame:
    """
    Membentuk dataframe fact_order_products dari nested products.
    Data ini berada pada level produk dalam order.
    """

    if "order_id" not in df.columns:
        raise RuntimeError("Kolom order_id tidak ditemukan.")

    if "products" not in df.columns:
        logger.warning("Kolom products tidak ditemukan. fact_order_products kosong.")
        return pd.DataFrame(
            columns=[
                "order_id",
                "product_id",
                "product_name",
                "aisle",
                "department",
                "add_to_cart_order",
                "reordered",
                "loaded_at",
            ]
        )

    product_rows = []

    for _, row in df.iterrows():
        order_id = row["order_id"]
        products = parse_products(row["products"])

        for product in products:
            if not isinstance(product, dict):
                continue

            product_rows.append(
                {
                    "order_id": order_id,
                    "product_id": product.get("product_id"),
                    "product_name": product.get("product_name", ""),
                    "aisle": product.get("aisle", ""),
                    "department": product.get("department", ""),
                    "add_to_cart_order": product.get("add_to_cart_order"),
                    "reordered": product.get("reordered"),
                    "loaded_at": loaded_at,
                }
            )

    fact_order_products = pd.DataFrame(product_rows)

    if fact_order_products.empty:
        logger.warning("Tidak ada data product yang berhasil diekstrak.")
        return fact_order_products

    # Standarisasi tipe data
    fact_order_products["order_id"] = pd.to_numeric(
        fact_order_products["order_id"], errors="coerce"
    ).astype("Int64")

    fact_order_products["product_id"] = pd.to_numeric(
        fact_order_products["product_id"], errors="coerce"
    ).astype("Int64")

    fact_order_products["product_name"] = (
        fact_order_products["product_name"]
        .fillna("")
        .astype(str)
    )

    fact_order_products["aisle"] = (
        fact_order_products["aisle"]
        .fillna("")
        .astype(str)
    )

    fact_order_products["department"] = (
        fact_order_products["department"]
        .fillna("")
        .astype(str)
    )

    fact_order_products["add_to_cart_order"] = pd.to_numeric(
        fact_order_products["add_to_cart_order"], errors="coerce"
    ).astype("Int64")

    fact_order_products["reordered"] = pd.to_numeric(
        fact_order_products["reordered"], errors="coerce"
    ).astype("Int64")

    fact_order_products["loaded_at"] = loaded_at

    # Hapus baris tanpa order_id
    fact_order_products = fact_order_products.dropna(subset=["order_id"])

    # Drop duplicate agar tidak ada produk ganda dalam order yang sama
    fact_order_products = fact_order_products.drop_duplicates(
        subset=["order_id", "product_id", "add_to_cart_order"]
    )

    return fact_order_products


# ============================================================
# Insert ke ClickHouse
# ============================================================
def insert_fact_orders(client, fact_orders: pd.DataFrame):
    """Insert dataframe fact_orders ke ClickHouse."""

    if fact_orders.empty:
        logger.warning("fact_orders kosong. Insert dilewati.")
        return 0

    columns = [
        "order_id",
        "user_id",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
        "eval_set",
        "loaded_at",
    ]

    client.insert_df(
        f"{CLICKHOUSE_DATABASE}.{FACT_ORDERS_TABLE}",
        fact_orders[columns],
    )

    logger.info(f"Berhasil insert {len(fact_orders)} baris ke fact_orders.")

    return len(fact_orders)


def insert_fact_order_products(client, fact_order_products: pd.DataFrame):
    """Insert dataframe fact_order_products ke ClickHouse."""

    if fact_order_products.empty:
        logger.warning("fact_order_products kosong. Insert dilewati.")
        return 0

    columns = [
        "order_id",
        "product_id",
        "product_name",
        "aisle",
        "department",
        "add_to_cart_order",
        "reordered",
        "loaded_at",
    ]

    client.insert_df(
        f"{CLICKHOUSE_DATABASE}.{FACT_ORDER_PRODUCTS_TABLE}",
        fact_order_products[columns],
    )

    logger.info(
        f"Berhasil insert {len(fact_order_products)} baris "
        f"ke fact_order_products."
    )

    return len(fact_order_products)


# ============================================================
# Move Processed File
# ============================================================
def move_to_processed(filepath: str):
    """
    Memindahkan file Parquet yang sudah diproses ke folder processed.
    Lebih baik daripada menghapus file raw.
    """

    os.makedirs(PROCESSED_PATH, exist_ok=True)

    filename = os.path.basename(filepath)
    destination = os.path.join(PROCESSED_PATH, filename)

    shutil.move(filepath, destination)

    logger.info(f"File dipindahkan ke processed: {destination}")


# ============================================================
# Main Run Function
# ============================================================
def run(data_lake_path: str = DATA_LAKE_PATH) -> dict:
    """
    Entry point utama.
    Dipanggil oleh Airflow task.
    """

    parquet_files = load_parquet_files(data_lake_path)

    if not parquet_files:
        logger.warning("Tidak ada file parquet untuk diproses.")
        return {
            "files_processed": 0,
            "orders_loaded": 0,
            "products_loaded": 0,
        }

    client = get_clickhouse_client()
    validate_required_tables(client)

    total_orders_loaded = 0
    total_products_loaded = 0
    files_ok = 0

    for filepath in parquet_files:
        logger.info(f"Memproses file: {filepath}")

        try:
            df = pd.read_parquet(filepath, engine="pyarrow")

            logger.info(f"File dibaca: {len(df)} baris")
            logger.info(f"Kolom input: {list(df.columns)}")

            loaded_at = datetime.now()

            fact_orders = prepare_fact_orders(df, loaded_at)
            fact_order_products = prepare_fact_order_products(df, loaded_at)

            orders_loaded = insert_fact_orders(client, fact_orders)
            products_loaded = insert_fact_order_products(
                client,
                fact_order_products,
            )

            total_orders_loaded += orders_loaded
            total_products_loaded += products_loaded
            files_ok += 1

            move_to_processed(filepath)

        except Exception as e:
            logger.error(f"Gagal memproses file {filepath}: {e}")
            raise

    # Verifikasi total data di database
    total_orders_in_db = client.query(
        f"SELECT count() FROM {CLICKHOUSE_DATABASE}.{FACT_ORDERS_TABLE}"
    ).first_row[0]

    total_products_in_db = client.query(
        f"SELECT count() FROM {CLICKHOUSE_DATABASE}.{FACT_ORDER_PRODUCTS_TABLE}"
    ).first_row[0]

    logger.info(
        "Selesai. "
        f"Files processed: {files_ok}, "
        f"Orders loaded: {total_orders_loaded}, "
        f"Products loaded: {total_products_loaded}, "
        f"Total orders in DB: {total_orders_in_db}, "
        f"Total products in DB: {total_products_in_db}"
    )

    return {
        "files_processed": files_ok,
        "orders_loaded": total_orders_loaded,
        "products_loaded": total_products_loaded,
        "total_orders_in_db": total_orders_in_db,
        "total_products_in_db": total_products_in_db,
    }


# ============================================================
# Local Testing
# ============================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    stats = run()
    print(f"\nSelesai. Stats: {stats}")