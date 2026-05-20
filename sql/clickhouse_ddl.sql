-- ============================================================
-- MCI2026 Task 2 — DDL ClickHouse
-- ============================================================

-- 1. Buat Database
CREATE DATABASE IF NOT EXISTS mci2026;

-- 2. Buat Tabel orders
--    Schema ini sesuai response aktual dari API:
--    http://96.9.212.102:8000/orders
CREATE TABLE IF NOT EXISTS mci2026.fact_orders
(
    order_id Int64,
    user_id Int64,
    order_number Int64,
    order_dow Int64,
    order_hour_of_day Int64,
    days_since_prior_order Nullable(Float64),
    eval_set String,
    loaded_at DateTime
)
ENGINE = MergeTree()
ORDER BY (order_id);

CREATE TABLE IF NOT EXISTS mci2026.fact_order_products
(
    order_id Int64,
    product_id Nullable(Int64),
    product_name String,
    aisle String,
    department String,
    add_to_cart_order Nullable(Int64),
    reordered Nullable(Int64),
    loaded_at DateTime
)
ENGINE = MergeTree()
ORDER BY (order_id, product_name);

SETTINGS index_granularity = 8192;

-- 3. Verifikasi tabel berhasil dibuat
DESCRIBE TABLE mci2026.fact_orders;

-- 4. Cek jumlah data setelah pipeline berjalan
SELECT count() AS total_rows FROM mci2026.fact_orders;

-- 5. Preview data
SELECT * FROM mci2026.fact_orders LIMIT 10;
SELECT * FROM mci2026.fact_order_products LIMIT 10;