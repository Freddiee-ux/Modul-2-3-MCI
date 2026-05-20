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
-- Query 2: Total Product Line Items
-- Visualization: Number / KPI Card
-- ------------------------------------------------------------
SELECT
    count() AS total_product_line_items
FROM mci2026.fact_order_products;


-- ------------------------------------------------------------
-- Query 3: Product Reorder Rate
-- Visualization: Number / KPI Card
-- ------------------------------------------------------------
SELECT
    round(countIf(reordered = 1) * 100.0 / count(), 2) AS product_reorder_rate
FROM mci2026.fact_order_products;


-- ------------------------------------------------------------
-- Query 4: Average Basket Size
-- Visualization: Number / KPI Card
-- Meaning: average number of products per order
-- ------------------------------------------------------------
SELECT
    round(avg(total_products), 2) AS average_basket_size
FROM
(
    SELECT
        order_id,
        count() AS total_products
    FROM mci2026.fact_order_products
    GROUP BY order_id
);


-- ============================================================
-- SECTION 2: TIME-BASED ORDER PATTERN
-- ============================================================


-- ------------------------------------------------------------
-- Query 5: Orders by Day of Week
-- Visualization: Bar Chart
-- ------------------------------------------------------------
SELECT
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
-- Query 6: Orders by Hour of Day
-- Visualization: Bar Chart
-- ------------------------------------------------------------
SELECT
    order_hour_of_day AS hour_of_day,
    count() AS total_orders
FROM mci2026.fact_orders
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day;


-- ------------------------------------------------------------
-- Query 7: Top Ordering Time Windows
-- Visualization: Table
-- Meaning: top day-hour combinations with the highest order volume
-- ------------------------------------------------------------
SELECT
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
    concat(toString(order_hour_of_day), ':00') AS hour_of_day,
    count() AS total_orders
FROM mci2026.fact_orders
GROUP BY
    order_dow,
    day_name,
    order_hour_of_day
ORDER BY total_orders DESC
LIMIT 5;


-- ============================================================
-- SECTION 3: REORDER BEHAVIOR
-- ============================================================


-- ------------------------------------------------------------
-- Query 8: Reorder Interval Distribution
-- Visualization: Bar Chart / Line Chart
-- Meaning: distribution of days since prior order
-- ------------------------------------------------------------
SELECT
    multiIf(
        days_since_prior_order IS NULL, 'Unknown',
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
-- Query 9: Product Reorder vs Non-Reorder
-- Visualization: Donut Chart / Pie Chart
-- ------------------------------------------------------------
SELECT
    multiIf(
        reordered = 1, 'Reordered',
        reordered = 0, 'Not Reordered',
        'Unknown'
    ) AS reorder_status,
    count() AS total_product_line_items,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS percentage
FROM mci2026.fact_order_products
GROUP BY reorder_status
ORDER BY total_product_line_items DESC;


-- ============================================================
-- SECTION 4: BASKET SIZE ANALYSIS
-- ============================================================


-- ------------------------------------------------------------
-- Query 10: Basket Size Category
-- Visualization: Donut Chart / Pie Chart
-- Meaning: distribution of orders by number of products
-- ------------------------------------------------------------
SELECT
    multiIf(
        total_products BETWEEN 1 AND 5, 'Small Basket (1-5)',
        total_products BETWEEN 6 AND 10, 'Medium Basket (6-10)',
        total_products BETWEEN 11 AND 20, 'Large Basket (11-20)',
        total_products > 20, 'Very Large Basket (>20)',
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
-- Query 11: Largest Basket Orders
-- Visualization: Table
-- Meaning: orders with the highest number of products
-- ------------------------------------------------------------
SELECT
    o.order_id,
    o.user_id,
    o.order_number,
    multiIf(
        o.order_dow = 0, 'Sunday',
        o.order_dow = 1, 'Monday',
        o.order_dow = 2, 'Tuesday',
        o.order_dow = 3, 'Wednesday',
        o.order_dow = 4, 'Thursday',
        o.order_dow = 5, 'Friday',
        o.order_dow = 6, 'Saturday',
        'Unknown'
    ) AS day_name,
    concat(toString(o.order_hour_of_day), ':00') AS hour_of_day,
    count(p.product_id) AS total_products
FROM mci2026.fact_orders o
LEFT JOIN mci2026.fact_order_products p
    ON o.order_id = p.order_id
GROUP BY
    o.order_id,
    o.user_id,
    o.order_number,
    o.order_dow,
    day_name,
    o.order_hour_of_day
ORDER BY total_products DESC
LIMIT 10;


-- ============================================================
-- SECTION 5: PRODUCT PERFORMANCE
-- ============================================================


-- ------------------------------------------------------------
-- Query 12: Top 10 Purchased Products
-- Visualization: Bar Chart / Horizontal Bar Chart
-- ------------------------------------------------------------
SELECT
    product_name,
    count() AS total_purchased
FROM mci2026.fact_order_products
GROUP BY product_name
ORDER BY total_purchased DESC
LIMIT 10;


-- ------------------------------------------------------------
-- Query 13: Product Distribution by Department
-- Visualization: Bar Chart
-- ------------------------------------------------------------
SELECT
    department,
    count() AS total_products
FROM mci2026.fact_order_products
GROUP BY department
ORDER BY total_products DESC;
