
# MCI2026 Task 2 - E-Commerce Order Analytics Pipeline

## 1. Project Overview

This project is part of the MCI2026 Task 2 assignment, which focuses on building an end-to-end **Pipeline Orchestration and Data Visualization** workflow.

The project uses an e-commerce orders dataset from a REST API endpoint:

```text
http://96.9.212.102:8000/orders
````

The pipeline is designed to extract order data from the API, store it as Parquet files in a data lake, process the nested order-product structure, load the cleaned data into ClickHouse, and visualize the analytical results using Metabase.

The final output is an interactive dashboard titled:

```text
E-Commerce Order Behavior Analytics Dashboard
```

The dashboard helps analyze customer ordering behavior, reorder patterns, basket size, and product performance to support business decisions related to campaign timing, retention strategy, product recommendation, and inventory planning.

---

## 2. Assignment Scope

Based on the task requirements, this project covers the following stages:

1. Designing an Apache Airflow DAG.
2. Managing data in ClickHouse:

   * Creating database and tables.
   * Defining table schema.
   * Ensuring data is successfully loaded from the pipeline into ClickHouse.
3. Creating Questions and visualizations in Metabase.
4. Building a dashboard in Metabase.

---

## 3. Tech Stack

| Tool           | Function                             |
| -------------- | ------------------------------------ |
| Python         | Data ingestion and data processing   |
| Apache Airflow | Pipeline orchestration               |
| Pandas         | Data transformation                  |
| Parquet        | Data lake storage format             |
| ClickHouse     | Analytical database / data warehouse |
| Metabase       | Data visualization and dashboard     |
| Docker Compose | Container orchestration              |

---

## 4. Project Architecture

The project follows an end-to-end analytics pipeline architecture.

```text
REST API
   ↓
Apache Airflow DAG
   ↓
Data Lake (Parquet)
   ↓
Python Data Processing
   ↓
ClickHouse Data Warehouse
   ↓
Metabase Questions
   ↓
Metabase Dashboard
```

### Architecture Flow

1. **Data Ingestion**
   The pipeline fetches order data from the REST API endpoint.

2. **Data Lake Storage**
   The fetched data is saved as a Parquet file in the `data_lake` directory.

3. **Data Processing**
   The raw order data is processed using Python. The nested `products` field is extracted and normalized into a separate product-level table.

4. **Data Warehouse Loading**
   The processed data is loaded into ClickHouse tables.

5. **Data Visualization**
   SQL queries are used in Metabase to create Questions and build an analytical dashboard.

---

## 5. Repository Structure

```text
MCI2026_Task2_Kelompok<NomorKelompok>/
├── dags/
│   ├── mci2026_pipeline.py
│   └── scripts/
│       ├── fetch_orders.py
│       └── process_orders.py
├── sql/
│   ├── clickhouse_ddl.sql
│   ├── data_quality_checks.sql
│   └── metabase_queries.sql
├── docs/
│   |──dashboard.pdf
│   └──image.png
├── data_lake/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## 6. Dataset Description

The dataset is retrieved from:

```text
http://96.9.212.102:8000/orders
```

The API returns order-level data with nested product information.

### Main Order Fields

| Field                    | Description                               |
| ------------------------ | ----------------------------------------- |
| `order_id`               | Unique identifier of each order           |
| `user_id`                | Unique identifier of each user            |
| `order_number`           | Order sequence number of the user         |
| `order_dow`              | Day of week when the order was placed     |
| `order_hour_of_day`      | Hour of day when the order was placed     |
| `days_since_prior_order` | Number of days since the previous order   |
| `eval_set`               | Dataset category                          |
| `products`               | Nested list of products within each order |

### Product Fields

| Field               | Description                                 |
| ------------------- | ------------------------------------------- |
| `product_id`        | Unique identifier of the product            |
| `product_name`      | Product name                                |
| `aisle`             | Product aisle                               |
| `department`        | Product department                          |
| `add_to_cart_order` | Product position in cart                    |
| `reordered`         | Indicates whether the product was reordered |

---

## 7. Airflow DAG Design

The pipeline is orchestrated using Apache Airflow. The DAG consists of three main tasks:

```text
fetch_data → process_data → validate_data
```

### DAG Tasks

| Task            | Description                                                    |
| --------------- | -------------------------------------------------------------- |
| `fetch_data`    | Fetches order data from the API and saves it as a Parquet file |
| `process_data`  | Processes the Parquet file and loads data into ClickHouse      |
| `validate_data` | Validates loaded data in ClickHouse                            |

### DAG Flow

```text
1. fetch_data
   ↓
2. process_data
   ↓
3. validate_data
```

### DAG Screenshot


<img width="2868" height="1098" alt="image" src="https://github.com/user-attachments/assets/36b4b53d-8e75-47ed-81c9-0ba8f92ac6ca" />


---

## 8. Data Ingestion

The ingestion process is handled by:

```text
dags/scripts/fetch_orders.py
```

This script performs the following steps:

1. Sends a request to the orders API.
2. Retrieves JSON response data.
3. Converts the response into a DataFrame.
4. Saves the data into a Parquet file inside the data lake.

Example output:

```text
Fetching data from: http://96.9.212.102:8000/orders
Successfully fetched order records
Data saved to data_lake/orders_<timestamp>.parquet
```

---

## 9. Data Processing

The processing process is handled by:

```text
dags/scripts/process_orders.py
```

This script processes the raw Parquet file and separates the data into two analytical tables:

1. `fact_orders`
2. `fact_order_products`

### Processing Logic

The original API response contains nested product data inside each order. To make the data easier to analyze, the pipeline normalizes the nested structure.

Original structure:

```text
Order
 └── Products
```

Processed structure:

```text
fact_orders
fact_order_products
```

This design allows order-level and product-level analysis to be performed separately.

---

## 10. ClickHouse Data Warehouse

ClickHouse is used as the analytical database for this project.

The database name is:

```sql
mci2026
```

### Table 1: fact_orders

The `fact_orders` table stores order-level data.

```sql
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
ORDER BY (order_id)
SETTINGS index_granularity = 8192;
```

### Table 2: fact_order_products

The `fact_order_products` table stores product-level data for each order.

```sql
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
ORDER BY (order_id, product_name)
SETTINGS index_granularity = 8192;
```

---

## 11. Data Quality Validation

Data quality validation is performed to ensure that the loaded data is complete and consistent.

The validation checks include:

1. Total order rows.
2. Unique order IDs.
3. Duplicate order detection.
4. Invalid day of week values.
5. Invalid order hour values.
6. Product line consistency.
7. Orphan product rows.

### Example Validation Query

```sql
SELECT
    count() AS total_rows,
    countDistinct(order_id) AS unique_orders,
    count() - countDistinct(order_id) AS duplicate_orders
FROM mci2026.fact_orders;
```

### Validation Result

The final dataset used in the dashboard contains:

| Metric               |  Value |
| -------------------- | -----: |
| Total Orders         |    200 |
| Unique Orders        |    200 |
| Duplicate Orders     |      0 |
| Product Line Items   |  2,405 |
| Average Basket Size  |  12.02 |
| Product Reorder Rate | 61.12% |

These results indicate that the dataset is valid for analytical visualization.

---

## 12. Metabase Questions

Metabase Questions are created using SQL queries from:

```text
sql/metabase_queries.sql
```

Each visualization is created as one Metabase Question.

### Main Questions Used in the Dashboard

| Section                      | Question                           |
| ---------------------------- | ---------------------------------- |
| Business Summary             | Total Orders                       |
| Business Summary             | Product Line Items                 |
| Business Summary             | Average Basket Size                |
| Business Summary             | Product Reorder Rate (%)           |                   
| Ordering Time Opportunity    | Orders by Hour of Day              |
| Ordering Time Opportunity    | Orders by Day of Week              |
| Ordering Time Opportunity    | Top Ordering Time Windows          |
| Reorder and Retention Signal | Reorder Interval Distribution      |
| Reorder and Retention Signal | Product Reorder vs Non-Reorder     |
| Basket Size Analysis         | Basket Size Category               |
| Basket Size Analysis         | Largest Basket Orders              |
| Product Performance          | Top 10 Purchased Products          |
| Product Performance          | Product Distribution by Department |

---

## 13. Dashboard Design

The dashboard is designed to answer the following business questions:

1. How many orders and product line items are available in the dataset?
2. What is the average number of products per order?
3. When do customers place orders most frequently?
4. How strong is the product reorder behavior?
5. What are the most frequently purchased products?
6. Which departments dominate product purchases?

### Dashboard Title

```text
E-Commerce Order Behavior Analytics Dashboard
```

### Dashboard Sections

```text
1. Executive Overview
2. Time-Based Order Pattern
3. Reorder Behavior
4. Basket Size Analysis
5. Product Performance
6. Product Reorder Opportunity
```

---

## 14. Dashboard Preview

### Page 1: Executive Overview and Time-Based Order Pattern

The first page shows the executive overview and ordering time analysis. It includes total orders, product line items, product reorder rate, average basket size, orders by day of week, orders by hour of day, and top ordering time windows.

<img width="722" height="696" alt="image" src="https://github.com/user-attachments/assets/3e161747-b686-4b9c-95e0-6edfcd58f056" />

### Page 2: Reorder Behavior, Basket Size, and Product Performance

The second page shows reorder behavior, basket size category, largest basket orders, top purchased products, and product distribution by department.

<img width="734" height="786" alt="image" src="https://github.com/user-attachments/assets/7d5914ca-369e-40a7-b0f7-45484f8ff4d8" />


---

## 15. Key Dashboard Insights

Based on the Metabase dashboard, several insights can be identified:

Berikut **key insights singkat** yang bisa kamu masukkan ke README:

## Key Insights

### 1. **The dataset contains 200 unique orders and 2,405 product line items.**
   This shows that each order contains multiple products, with an average basket size of **12.02 products per order**.

### 2. **Product reorder behavior is strong.**
   The product reorder rate reaches **61.12%**, meaning more than half of the product line items are repeat purchases.

### 3. **Monday is the peak order day.**
   Order activity is highest on Monday, which can be used as a reference for campaign scheduling or promotional timing.

### 4. **Customers are more active during daytime and afternoon hours.**
   The time-based order pattern indicates that order activity is concentrated during active daily hours.

### 5. **Large and medium baskets dominate the order pattern.**
   Many orders contain multiple products, indicating potential opportunities for bundling and cross-selling strategies.

### 6. **Product demand is dominated by daily grocery categories.**
   Products from departments such as **produce**, **dairy eggs**, and **snacks** appear frequently, making them important categories for inventory planning.

### 7. **Top purchased products can support recommendation strategy.**
   Frequently purchased products such as **Banana**, **Bag of Organic Bananas**, and other grocery items can be used for product recommendation, bundling, or promotion planning.

---

## 16. How to Run the Project

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd MCI2026_Task2_Kelompok<NomorKelompok>
```

### Step 2: Start Docker Containers

```bash
docker compose up -d
```

### Step 3: Check Running Containers

```bash
docker ps
```

Make sure the following containers are running:

```text
mci2026_airflow_webserver
mci2026_airflow_scheduler
mci2026_clickhouse
mci2026_metabase
mci2026_postgres
```

### Step 4: Run ClickHouse DDL

From PowerShell:

```powershell
Get-Content .\sql\clickhouse_ddl.sql | docker exec -i mci2026_clickhouse clickhouse-client --multiquery
```

### Step 5: Open Airflow

Open Airflow in the browser:

```text
http://localhost:8080
```

Trigger the DAG:

```text
mci2026_orders_pipeline
```

Expected DAG flow:

```text
fetch_data → process_data → validate_data
```

### Step 6: Verify Data in ClickHouse

```bash
docker exec -it mci2026_clickhouse clickhouse-client
```

Run:

```sql
SELECT count() FROM mci2026.fact_orders;
```

```sql
SELECT count() FROM mci2026.fact_order_products;
```

### Step 7: Open Metabase

Open Metabase in the browser:

```text
http://localhost:3000
```

Connect Metabase to ClickHouse:

```text
Host: clickhouse
Port: 8123
Database: mci2026
Username: default
Password: empty
```

Then create Questions using queries from:

```text
sql/metabase_queries.sql
```

---

## 17. SQL Files

### clickhouse_ddl.sql

This file contains SQL statements to create the ClickHouse database and tables.

### data_quality_checks.sql

This file contains SQL queries for validating loaded data.

### metabase_queries.sql

This file contains analytical SQL queries used to create Metabase Questions and dashboard visualizations.

---

## 18. Challenges and Solutions

| Challenge                                            | Solution                                                                |
| ---------------------------------------------------- | ----------------------------------------------------------------------- |
| Nested product data in API response                  | Normalized products into `fact_order_products`                          |
| ClickHouse table design                              | Created separate fact tables for order-level and product-level analysis |
| Dashboard initially lacked business focus            | Redesigned dashboard around business questions and decision-making      |
| Pivot table limitation in Metabase SQL               | Used table-based matrix and simpler visualizations                      |
| Potential duplicate data from multiple pipeline runs | Added duplicate checks using `countDistinct(order_id)`                  |

---

## 19. Conclusion

This project successfully implements an end-to-end pipeline orchestration and visualization workflow using Apache Airflow, ClickHouse, and Metabase.

The pipeline extracts order data from a REST API, stores it as Parquet files, processes nested product data, loads the result into ClickHouse, validates the loaded data, and visualizes the results in Metabase.

The final dashboard provides insights into order behavior, reorder patterns, basket size, and product performance. These insights can support business decisions related to campaign timing, customer retention, product recommendations, and inventory planning.


| `order_hour_of_day` | Jam order 0-23 |
| `days_since_prior_order` | Jarak hari dari order sebelumnya |
| `eval_set` | Jenis data evaluasi |
| `products` | Daftar produk dalam format JSON string |
| `_loaded_at` | Waktu data dimuat ke warehouse |

