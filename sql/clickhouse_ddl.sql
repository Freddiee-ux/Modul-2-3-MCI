-- ============================================================
-- MCI2026 Task 2 — DDL ClickHouse
-- ============================================================

-- 1. Buat Database
CREATE DATABASE IF NOT EXISTS mci2026;

-- 2. Buat Tabel orders
--    Schema ini sesuai response aktual dari API:
--    http://96.9.212.102:8000/orders
CREATE TABLE IF NOT EXISTS mci2026.orders
(
    `order_id` Int64,
    `user_id` Int64,
    `order_number` Int64,
    `order_dow` Int64,
    `order_hour_of_day` Int64,
    `days_since_prior_order` Float64,
    `eval_set` String,
    `products` String,
    `_loaded_at`    String
)
ENGINE = MergeTree()
ORDER BY (`order_id`)
SETTINGS index_granularity = 8192;

-- 3. Verifikasi tabel berhasil dibuat
DESCRIBE TABLE mci2026.orders;

-- 4. Cek jumlah data setelah pipeline berjalan
SELECT count() AS total_rows FROM mci2026.orders;

-- 5. Preview data
SELECT * FROM mci2026.orders LIMIT 10;
