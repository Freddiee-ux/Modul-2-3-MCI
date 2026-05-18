-- ============================================================
-- MCI2026 Task 2 — SQL Queries untuk Metabase
-- Database: mci2026 | Table: orders
-- Cara pakai: Metabase → New Question → Native Query → paste query
-- ============================================================


-- ==============================================
-- Query 1: Overview — Total Orders & Revenue
-- Tipe visualisasi: Number / Scorecard
-- ==============================================
SELECT
    count(*)                         AS total_orders,
    sum(total_amount)                AS total_revenue,
    round(avg(total_amount), 2)      AS avg_order_value,
    min(total_amount)                AS min_order_value,
    max(total_amount)                AS max_order_value
FROM mci2026.orders;


-- ==============================================
-- Query 2: Distribusi Orders per Status
-- Tipe visualisasi: Pie Chart / Bar Chart
-- ==============================================
SELECT
    status                           AS status_order,
    count(*)                         AS jumlah_orders,
    sum(total_amount)                AS total_revenue,
    round(avg(total_amount), 2)      AS rata_rata_nilai
FROM mci2026.orders
GROUP BY status
ORDER BY jumlah_orders DESC;


-- ==============================================
-- Query 3: Tren Orders per Bulan (Time Series)
-- Tipe visualisasi: Line Chart
-- ==============================================
SELECT
    toStartOfMonth(toDateTime(order_date))  AS bulan,
    count(*)                                AS jumlah_orders,
    sum(total_amount)                       AS total_revenue
FROM mci2026.orders
WHERE order_date != '' AND order_date != '1970-01-01 00:00:00'
GROUP BY bulan
ORDER BY bulan ASC;


-- ==============================================
-- Query 4: Top 10 Kategori berdasarkan Revenue
-- Tipe visualisasi: Bar Chart (horizontal)
-- ==============================================
SELECT
    category                         AS kategori,
    count(*)                         AS jumlah_orders,
    sum(total_amount)                AS total_revenue,
    round(avg(total_amount), 2)      AS avg_nilai_order
FROM mci2026.orders
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10;


-- ==============================================
-- Query 5: Perbandingan Revenue per Kategori & Status
-- Tipe visualisasi: Pivot Table / Stacked Bar
-- ==============================================
SELECT
    category                         AS kategori,
    status                           AS status_order,
    count(*)                         AS jumlah_orders,
    sum(total_amount)                AS total_revenue
FROM mci2026.orders
GROUP BY category, status
ORDER BY kategori, status;


-- ==============================================
-- Query 6: Sample Data Terbaru
-- Tipe visualisasi: Table
-- ==============================================
SELECT
    id,
    customer_name,
    product_name,
    category,
    status,
    quantity,
    price,
    total_amount,
    order_date,
    _loaded_at
FROM mci2026.orders
ORDER BY _loaded_at DESC
LIMIT 100;
