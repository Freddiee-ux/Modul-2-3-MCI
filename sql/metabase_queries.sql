-- ============================================================
-- MCI2026 Task 2 — Metabase Analytics Queries
-- Dashboard: E-Commerce Order Behavior Analytics Dashboard
-- ============================================================


-- ============================================================
-- SECTION 1: EXECUTIVE OVERVIEW
-- ============================================================


-- ------------------------------------------------------------
-- Query 1: Total Orders
-- Visualization: Number / KPI Card
-- ------------------------------------------------------------
SELECT
    count() AS total_orders
FROM mci2026.fact_orders;


-- ------------------------------------------------------------
-- Query 2: Total Users
-- Visualization: Number / KPI Card
-- ------------------------------------------------------------
SELECT
    uniqExact(user_id) AS total_users
FROM mci2026.fact_orders;


-- ------------------------------------------------------------
-- Query 3: Total Product Line Items
-- Visualization: Number / KPI Card
-- ------------------------------------------------------------
SELECT
    count() AS total_product_line_items
FROM mci2026.fact_order_products;


-- ------------------------------------------------------------
-- Query 4: Average Basket Size
-- Visualization: Number / KPI Card
-- Meaning: rata-rata jumlah produk dalam setiap order
-- ------------------------------------------------------------
SELECT
    round(avg(total_products), 2) AS avg_products_per_order
FROM
(
    SELECT
        order_id,
        count() AS total_products
    FROM mci2026.fact_order_products
    GROUP BY order_id
);


-- ------------------------------------------------------------
-- Query 5: Product Reorder Rate
-- Visualization: Number / KPI Card
-- Meaning: persentase product line yang merupakan reorder
-- ------------------------------------------------------------
SELECT
    round(countIf(reordered = 1) * 100.0 / count(), 2) AS product_reorder_rate
FROM mci2026.fact_order_products;


-- ------------------------------------------------------------
-- Query 6: Average Days Since Prior Order
-- Visualization: Number / KPI Card
-- ------------------------------------------------------------
SELECT
    round(avg(days_since_prior_order), 2) AS avg_days_since_prior_order
FROM mci2026.fact_orders
WHERE days_since_prior_order IS NOT NULL;


-- ------------------------------------------------------------
-- Query 7: Repeat Order Rate
-- Visualization: Number / KPI Card
-- Meaning: persentase order yang bukan order pertama
-- ------------------------------------------------------------
SELECT
    round(countIf(order_number > 1) * 100.0 / count(), 2) AS repeat_order_rate
FROM mci2026.fact_orders;


-- ------------------------------------------------------------
-- Query 8: Executive Summary in One Query
-- Visualization: Table
-- Optional: bisa dipakai sebagai summary table
-- ------------------------------------------------------------
SELECT
    count() AS total_orders,
    uniqExact(user_id) AS total_users,
    round(avg(order_number), 2) AS avg_order_number,
    round(avg(days_since_prior_order), 2) AS avg_days_since_prior_order,
    round(countIf(order_number > 1) * 100.0 / count(), 2) AS repeat_order_rate
FROM mci2026.fact_orders;



-- ============================================================
-- SECTION 2: TIME-BASED ORDER PATTERN
-- ============================================================


-- ------------------------------------------------------------
-- Query 9: Orders by Hour of Day
-- Visualization: Bar Chart
-- X-axis: order_hour_of_day
-- Y-axis: total_orders
-- ------------------------------------------------------------
SELECT
    order_hour_of_day,
    count() AS total_orders
FROM mci2026.fact_orders
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day;


-- ------------------------------------------------------------
-- Query 10: Orders by Day of Week
-- Visualization: Bar Chart
-- X-axis: day_name
-- Y-axis: total_orders
-- ------------------------------------------------------------
SELECT
    order_dow,
    multiIf(
        order_dow = 0, 'Sunday',
        order_dow = 1, 'Monday',
        order_dow = 2, 'Tuesday',
        order_dow = 3, 'Wednesday',
        order_dow = 4, 'Thursday',
        order_dow = 5, 'Friday',
        order_dow = 6, 'Saturday',
        'Unknown'
    ) AS day_name,
    count() AS total_orders
FROM mci2026.fact_orders
GROUP BY
    order_dow,
    day_name
ORDER BY order_dow;


-- ------------------------------------------------------------
-- Query 11: Day-Hour Order Activity
-- Visualization: Heatmap / Pivot Table
-- Rows: day_name
-- Columns: order_hour_of_day
-- Values: total_orders
-- ------------------------------------------------------------
SELECT
    order_dow,
    multiIf(
        order_dow = 0, 'Sunday',
        order_dow = 1, 'Monday',
        order_dow = 2, 'Tuesday',
        order_dow = 3, 'Wednesday',
        order_dow = 4, 'Thursday',
        order_dow = 5, 'Friday',
        order_dow = 6, 'Saturday',
        'Unknown'
    ) AS day_name,
    order_hour_of_day,
    count() AS total_orders
FROM mci2026.fact_orders
GROUP BY
    order_dow,
    day_name,
    order_hour_of_day
ORDER BY
    order_dow,
    order_hour_of_day;


-- ------------------------------------------------------------
-- Query 12: Peak Order Hour
-- Visualization: Table / Single Value
-- Meaning: jam dengan order tertinggi
-- ------------------------------------------------------------
SELECT
    order_hour_of_day,
    count() AS total_orders
FROM mci2026.fact_orders
GROUP BY order_hour_of_day
ORDER BY total_orders DESC
LIMIT 1;


-- ------------------------------------------------------------
-- Query 13: Peak Order Day
-- Visualization: Table / Single Value
-- Meaning: hari dengan order tertinggi
-- ------------------------------------------------------------
SELECT
    order_dow,
    multiIf(
        order_dow = 0, 'Sunday',
        order_dow = 1, 'Monday',
        order_dow = 2, 'Tuesday',
        order_dow = 3, 'Wednesday',
        order_dow = 4, 'Thursday',
        order_dow = 5, 'Friday',
        order_dow = 6, 'Saturday',
        'Unknown'
    ) AS day_name,
    count() AS total_orders
FROM mci2026.fact_orders
GROUP BY
    order_dow,
    day_name
ORDER BY total_orders DESC
LIMIT 1;



-- ============================================================
-- SECTION 3: REORDER BEHAVIOR
-- ============================================================


-- ------------------------------------------------------------
-- Query 14: Reorder Interval Distribution
-- Visualization: Bar Chart
-- Meaning: distribusi jarak waktu antar order
-- ------------------------------------------------------------
SELECT
    multiIf(
        days_since_prior_order IS NULL, 'First Order / Unknown',
        days_since_prior_order = 0, 'Same Day',
        days_since_prior_order BETWEEN 1 AND 7, '1-7 Days',
        days_since_prior_order BETWEEN 8 AND 14, '8-14 Days',
        days_since_prior_order BETWEEN 15 AND 21, '15-21 Days',
        days_since_prior_order > 21, '>21 Days',
        'Unknown'
    ) AS reorder_interval,
    count() AS total_orders,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS percentage
FROM mci2026.fact_orders
GROUP BY reorder_interval
ORDER BY total_orders DESC;


-- ------------------------------------------------------------
-- Query 15: Average Reorder Gap by Order Number
-- Visualization: Line Chart
-- X-axis: order_number
-- Y-axis: avg_reorder_gap
-- ------------------------------------------------------------
SELECT
    order_number,
    round(avg(days_since_prior_order), 2) AS avg_reorder_gap,
    count() AS total_orders
FROM mci2026.fact_orders
WHERE days_since_prior_order IS NOT NULL
GROUP BY order_number
ORDER BY order_number;


-- ------------------------------------------------------------
-- Query 16: Order Number Distribution
-- Visualization: Bar Chart / Histogram
-- Meaning: melihat distribusi urutan order user
-- ------------------------------------------------------------
SELECT
    order_number,
    count() AS total_orders
FROM mci2026.fact_orders
GROUP BY order_number
ORDER BY order_number;


-- ------------------------------------------------------------
-- Query 17: Reorder Gap Statistics
-- Visualization: Table
-- ------------------------------------------------------------
SELECT
    round(avg(days_since_prior_order), 2) AS avg_reorder_gap,
    min(days_since_prior_order) AS min_reorder_gap,
    max(days_since_prior_order) AS max_reorder_gap,
    quantile(0.5)(days_since_prior_order) AS median_reorder_gap
FROM mci2026.fact_orders
WHERE days_since_prior_order IS NOT NULL;


-- ------------------------------------------------------------
-- Query 18: Product Reorder vs Non-Reorder
-- Visualization: Pie Chart / Donut Chart
-- ------------------------------------------------------------
SELECT
    multiIf(
        reordered = 1, 'Reordered',
        reordered = 0, 'Not Reordered',
        'Unknown'
    ) AS reorder_status,
    count() AS total_products,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS percentage
FROM mci2026.fact_order_products
GROUP BY reorder_status
ORDER BY total_products DESC;



-- ============================================================
-- SECTION 4: USER SEGMENTATION
-- ============================================================


-- ------------------------------------------------------------
-- Query 19: User Frequency Segmentation
-- Visualization: Bar Chart
-- Meaning: segmentasi user berdasarkan jumlah order
-- ------------------------------------------------------------
WITH user_order_frequency AS
(
    SELECT
        user_id,
        count() AS total_orders
    FROM mci2026.fact_orders
    GROUP BY user_id
)
SELECT
    multiIf(
        total_orders = 1, 'One-time User',
        total_orders BETWEEN 2 AND 3, 'Low Frequency User',
        total_orders BETWEEN 4 AND 7, 'Medium Frequency User',
        total_orders > 7, 'High Frequency User',
        'Unknown'
    ) AS user_segment,
    count() AS total_users
FROM user_order_frequency
GROUP BY user_segment
ORDER BY total_users DESC;


-- ------------------------------------------------------------
-- Query 20: Top Users by Total Orders
-- Visualization: Table
-- ------------------------------------------------------------
SELECT
    user_id,
    count() AS total_orders,
    max(order_number) AS latest_order_number,
    round(avg(days_since_prior_order), 2) AS avg_reorder_gap
FROM mci2026.fact_orders
GROUP BY user_id
ORDER BY total_orders DESC, latest_order_number DESC
LIMIT 10;



-- ============================================================
-- SECTION 5: PRODUCT ANALYTICS
-- ============================================================


-- ------------------------------------------------------------
-- Query 21: Top 10 Purchased Products
-- Visualization: Horizontal Bar Chart
-- ------------------------------------------------------------
SELECT
    product_name,
    count() AS total_purchased
FROM mci2026.fact_order_products
GROUP BY product_name
ORDER BY total_purchased DESC
LIMIT 10;


-- ------------------------------------------------------------
-- Query 22: Top Reordered Products
-- Visualization: Table / Bar Chart
-- Meaning: produk dengan reorder rate tinggi
-- ------------------------------------------------------------
SELECT
    product_name,
    count() AS total_purchased,
    countIf(reordered = 1) AS total_reordered,
    round(countIf(reordered = 1) * 100.0 / count(), 2) AS reorder_rate
FROM mci2026.fact_order_products
GROUP BY product_name
HAVING total_purchased >= 3
ORDER BY reorder_rate DESC, total_reordered DESC
LIMIT 10;


-- ------------------------------------------------------------
-- Query 23: Product Distribution by Department
-- Visualization: Bar Chart
-- ------------------------------------------------------------
SELECT
    department,
    count() AS total_products
FROM mci2026.fact_order_products
GROUP BY department
ORDER BY total_products DESC;


-- ------------------------------------------------------------
-- Query 24: Product Distribution by Aisle
-- Visualization: Bar Chart / Table
-- ------------------------------------------------------------
SELECT
    aisle,
    department,
    count() AS total_products
FROM mci2026.fact_order_products
GROUP BY
    aisle,
    department
ORDER BY total_products DESC
LIMIT 20;


-- ------------------------------------------------------------
-- Query 25: Reorder Rate by Department
-- Visualization: Bar Chart / Table
-- ------------------------------------------------------------
SELECT
    department,
    count() AS total_products,
    countIf(reordered = 1) AS total_reordered,
    round(countIf(reordered = 1) * 100.0 / count(), 2) AS reorder_rate
FROM mci2026.fact_order_products
GROUP BY department
HAVING total_products >= 5
ORDER BY reorder_rate DESC;


-- ------------------------------------------------------------
-- Query 26: Reorder Rate by Aisle
-- Visualization: Table / Bar Chart
-- ------------------------------------------------------------
SELECT
    aisle,
    department,
    count() AS total_products,
    countIf(reordered = 1) AS total_reordered,
    round(countIf(reordered = 1) * 100.0 / count(), 2) AS reorder_rate
FROM mci2026.fact_order_products
GROUP BY
    aisle,
    department
HAVING total_products >= 5
ORDER BY reorder_rate DESC, total_products DESC
LIMIT 20;


-- ------------------------------------------------------------
-- Query 27: Top Products by Department
-- Visualization: Table
-- ------------------------------------------------------------
SELECT
    department,
    product_name,
    count() AS total_purchased
FROM mci2026.fact_order_products
GROUP BY
    department,
    product_name
ORDER BY
    department ASC,
    total_purchased DESC
LIMIT 50;


-- ------------------------------------------------------------
-- Query 28: Basket Size Distribution
-- Visualization: Bar Chart / Histogram
-- ------------------------------------------------------------
SELECT
    total_products,
    count() AS total_orders
FROM
(
    SELECT
        order_id,
        count() AS total_products
    FROM mci2026.fact_order_products
    GROUP BY order_id
)
GROUP BY total_products
ORDER BY total_products;


-- ------------------------------------------------------------
-- Query 29: Basket Size Category
-- Visualization: Bar Chart
-- ------------------------------------------------------------
SELECT
    multiIf(
        total_products BETWEEN 1 AND 5, '1-5 Products',
        total_products BETWEEN 6 AND 10, '6-10 Products',
        total_products BETWEEN 11 AND 20, '11-20 Products',
        total_products > 20, '>20 Products',
        'Unknown'
    ) AS basket_size_category,
    count() AS total_orders,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS percentage
FROM
(
    SELECT
        order_id,
        count() AS total_products
    FROM mci2026.fact_order_products
    GROUP BY order_id
)
GROUP BY basket_size_category
ORDER BY total_orders DESC;


-- ------------------------------------------------------------
-- Query 30: Largest Basket Orders
-- Visualization: Table
-- ------------------------------------------------------------
SELECT
    o.order_id,
    o.user_id,
    o.order_number,
    o.order_dow,
    o.order_hour_of_day,
    count(p.product_id) AS total_products
FROM mci2026.fact_orders o
LEFT JOIN mci2026.fact_order_products p
    ON o.order_id = p.order_id
GROUP BY
    o.order_id,
    o.user_id,
    o.order_number,
    o.order_dow,
    o.order_hour_of_day
ORDER BY total_products DESC
LIMIT 10;



-- ============================================================
-- SECTION 6: DATA QUALITY SUMMARY
-- Optional section for portfolio documentation
-- ============================================================


-- ------------------------------------------------------------
-- Query 31: Order Duplicate Check
-- Visualization: Table
-- ------------------------------------------------------------
SELECT
    count() AS total_rows,
    countDistinct(order_id) AS unique_orders,
    count() - countDistinct(order_id) AS duplicate_orders
FROM mci2026.fact_orders;


-- ------------------------------------------------------------
-- Query 32: Product Line Duplicate Check
-- Visualization: Table
-- ------------------------------------------------------------
SELECT
    count() AS total_product_rows,
    countDistinct(order_id, product_id, add_to_cart_order) AS unique_product_lines,
    count() - countDistinct(order_id, product_id, add_to_cart_order) AS duplicate_product_lines
FROM mci2026.fact_order_products;


-- ------------------------------------------------------------
-- Query 33: Product-Orphan Check
-- Visualization: Table
-- Meaning: memastikan semua product rows punya pasangan order
-- ------------------------------------------------------------
SELECT
    count() AS orphan_product_rows
FROM mci2026.fact_order_products p
LEFT JOIN mci2026.fact_orders o
    ON p.order_id = o.order_id
WHERE o.order_id IS NULL;


-- ------------------------------------------------------------
-- Query 34: Order Range Validation
-- Visualization: Table
-- ------------------------------------------------------------
SELECT
    countIf(order_dow < 0 OR order_dow > 6) AS invalid_order_dow,
    countIf(order_hour_of_day < 0 OR order_hour_of_day > 23) AS invalid_order_hour,
    countIf(order_number < 1) AS invalid_order_number,
    countIf(days_since_prior_order < 0) AS invalid_days_since_prior_order
FROM mci2026.fact_orders;


-- ------------------------------------------------------------
-- Query 35: Product Data Validation
-- Visualization: Table
-- ------------------------------------------------------------
SELECT
    countIf(product_name = '') AS empty_product_name,
    countIf(aisle = '') AS empty_aisle,
    countIf(department = '') AS empty_department,
    countIf(add_to_cart_order < 1) AS invalid_add_to_cart_order
FROM mci2026.fact_order_products;


-- ------------------------------------------------------------
-- Query 36: Eval Set Distribution
-- Visualization: Table / Bar Chart
-- ------------------------------------------------------------
SELECT
    eval_set,
    count() AS total_orders,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS percentage
FROM mci2026.fact_orders
GROUP BY eval_set
ORDER BY total_orders DESC;