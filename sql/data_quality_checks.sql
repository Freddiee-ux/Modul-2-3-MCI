-- ============================================================
-- MCI2026 Task 2 — Data Quality Checks
-- ============================================================

-- 1. Row count and duplicate check
SELECT
    count() AS total_rows,
    countDistinct(order_id) AS unique_orders,
    count() - countDistinct(order_id) AS duplicate_orders
FROM mci2026.fact_orders;

-- 2. Null check pada fact_orders
SELECT
    countIf(order_id IS NULL) AS null_order_id,
    countIf(user_id IS NULL) AS null_user_id,
    countIf(order_number IS NULL) AS null_order_number,
    countIf(order_dow IS NULL) AS null_order_dow,
    countIf(order_hour_of_day IS NULL) AS null_order_hour
FROM mci2026.fact_orders;

-- 3. Range validation pada fact_orders
SELECT
    countIf(order_dow < 0 OR order_dow > 6) AS invalid_order_dow,
    countIf(order_hour_of_day < 0 OR order_hour_of_day > 23) AS invalid_order_hour,
    countIf(order_number < 1) AS invalid_order_number,
    countIf(days_since_prior_order < 0) AS invalid_days_since_prior_order
FROM mci2026.fact_orders;

-- 4. Eval set distribution
SELECT
    eval_set,
    count() AS total_orders
FROM mci2026.fact_orders
GROUP BY eval_set
ORDER BY total_orders DESC;

-- 5. Product row validation
SELECT
    count() AS total_product_rows,
    countIf(order_id IS NULL) AS null_order_id,
    countIf(product_name = '') AS empty_product_name,
    countIf(add_to_cart_order < 1) AS invalid_add_to_cart_order
FROM mci2026.fact_order_products;

-- 6. Average basket size
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