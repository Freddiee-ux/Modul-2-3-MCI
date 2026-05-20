
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

# Laporan Pipeline Orchestration MCI2026 Task 2

## Identitas Kelompok

| Nama | NRP |
|------|-----|
| Ferdian Ardra Hafizhan | 5025241033 |
| Muhammad Hilbran Akmal Abrar | 5025241052 |

## Ringkasan

Proyek ini membangun pipeline data end-to-end untuk mengambil data orders dari REST API, menyimpan data mentah dalam format Parquet, memproses dan memuatnya ke ClickHouse, lalu menampilkan hasil analisis melalui dashboard Metabase. Orkestrasi pipeline dilakukan menggunakan Apache Airflow dengan tiga task utama: `fetch_data`, `process_data`, dan `validate_data`.

Dataset yang digunakan berasal dari endpoint:

```text
http://96.9.212.102:8000/orders
```

Hasil akhir pipeline menunjukkan data berhasil dimuat ke tabel `mci2026.orders` dan divisualisasikan pada dashboard Metabase. Berdasarkan dashboard yang dibuat, total data orders yang berhasil dianalisis adalah 300 orders dengan rata-rata `days_since_prior_order` sebesar 10.01 hari.

## Tujuan

Tujuan pengerjaan proyek ini adalah:

1. Membuat pipeline ingestion data dari REST API.
2. Menyimpan hasil ingestion ke data lake lokal dalam format Parquet.
3. Melakukan transformasi data sebelum dimasukkan ke data warehouse.
4. Memuat data ke ClickHouse sebagai database analitik.
5. Memvalidasi data yang berhasil dimuat.
6. Membuat dashboard analitik menggunakan Metabase.

## Arsitektur Sistem

Alur sistem yang dibangun adalah sebagai berikut:

```text
Orders REST API
    -> fetch_data
    -> data_lake/*.parquet
    -> process_data
    -> ClickHouse: mci2026.orders
    -> validate_data
    -> Metabase Dashboard
```

Komponen utama:

| Komponen | Fungsi |
|----------|--------|
| Apache Airflow | Mengatur urutan eksekusi pipeline |
| Python | Mengambil, menyimpan, dan memproses data |
| Parquet | Format penyimpanan sementara di data lake |
| ClickHouse | Data warehouse untuk analisis |
| Metabase | Dashboard dan visualisasi |
| Docker Compose | Menjalankan seluruh service secara terintegrasi |
| PostgreSQL | Metadata database untuk Airflow |

## Struktur Repository

```text
mci2026_task2/
|-- dags/
|   |-- mci2026_pipeline.py
|   `-- scripts/
|       |-- fetch_orders.py
|       `-- process_orders.py
|-- data_lake/
|-- docs/
|   `-- metabase_orders_analytics_dashboard.pdf
|-- sql/
|   |-- clickhouse_ddl.sql
|   `-- metabase_queries.sql
|-- docker-compose.yml
|-- Dockerfile
|-- requirements.txt
|-- README.md
`-- LAPORAN.md
```

## Implementasi Pipeline

### 1. Task `fetch_data`

Task ini menjalankan script `fetch_orders.py`. Proses yang dilakukan:

- Mengambil data dari REST API orders.
- Melakukan normalisasi response JSON menjadi DataFrame.
- Mengubah kolom nested seperti `products` menjadi JSON string agar stabil saat disimpan sebagai Parquet.
- Menyimpan data ke folder `data_lake/` dengan nama file berbasis timestamp.

Contoh nama file hasil ingestion:

```text
orders_YYYYMMDD_HHMMSS.parquet
```

### 2. Task `process_data`

Task ini menjalankan script `process_orders.py`. Proses yang dilakukan:

- Membaca file Parquet dari `data_lake/`.
- Membersihkan duplikasi data.
- Mengisi missing value berdasarkan tipe data.
- Menambahkan metadata `_loaded_at`.
- Membuat database dan tabel ClickHouse jika belum tersedia.
- Memasukkan data ke tabel `mci2026.orders`.
- Menghapus file Parquet setelah berhasil diproses.

Schema utama tabel `mci2026.orders`:

| Kolom | Keterangan |
|-------|------------|
| `order_id` | ID unik order |
| `user_id` | ID user |
| `order_number` | Urutan order user |
| `order_dow` | Hari order dalam angka 0-6 |
| `order_hour_of_day` | Jam order 0-23 |
| `days_since_prior_order` | Jarak hari dari order sebelumnya |
| `eval_set` | Jenis data evaluasi |
| `products` | Daftar produk dalam format JSON string |
| `_loaded_at` | Waktu data dimuat ke warehouse |

### 3. Task `validate_data`

Task ini melakukan validasi ke ClickHouse. Validasi dilakukan dengan:

- Menghubungkan Airflow ke ClickHouse.
- Mengecek jumlah baris pada tabel `mci2026.orders`.
- Mengambil sample data untuk memastikan tabel dapat dibaca.
- Menggagalkan task jika tabel kosong.

## Konfigurasi Service

Service dijalankan menggunakan Docker Compose:

| Service | Port | Keterangan |
|---------|------|------------|
| Airflow Webserver | 8080 | UI Airflow |
| ClickHouse HTTP | 8123 | Koneksi HTTP/JDBC |
| ClickHouse Native | 9000 | Koneksi native client |
| Metabase | 3000 | Dashboard BI |
| PostgreSQL | internal | Metadata Airflow |

Credential utama:

| Service | Username | Password |
|---------|----------|----------|
| Airflow | `admin` | `admin` |
| ClickHouse | `default` | kosong |
| Metabase | dibuat saat setup pertama |

## Cara Menjalankan

Jalankan dari root project:

```powershell
cd "C:\Users\LENOVO\Documents\MCI\modul 3\mci2026_task2"
docker compose up -d --build
```

Jika ingin menjalankan task satu per satu untuk debugging:

```powershell
docker compose exec airflow-scheduler airflow tasks test mci2026_orders_pipeline fetch_data 2026-05-18
docker compose exec airflow-scheduler airflow tasks test mci2026_orders_pipeline process_data 2026-05-18
docker compose exec airflow-scheduler airflow tasks test mci2026_orders_pipeline validate_data 2026-05-18
```

Verifikasi data di ClickHouse:

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM mci2026.orders"
docker compose exec clickhouse clickhouse-client --query "DESCRIBE TABLE mci2026.orders"
```

## Query Analitik

### Total Orders

```sql
SELECT count(*) AS total_orders
FROM mci2026.orders;
```

### Average Days Since Prior Order

```sql
SELECT
  round(avg(days_since_prior_order), 2) AS avg_days_since_prior_order
FROM mci2026.orders;
```

### Orders by Hour

```sql
SELECT
  order_hour_of_day,
  count(*) AS total_orders
FROM mci2026.orders
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day;
```

### Orders by Day of Week

```sql
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
  count(*) AS total_orders
FROM mci2026.orders
GROUP BY order_dow, day_name
ORDER BY order_dow;
```

### Orders by Eval Set

```sql
SELECT
  eval_set,
  count(*) AS total_orders
FROM mci2026.orders
GROUP BY eval_set
ORDER BY total_orders DESC;
```

### Sample Orders

```sql
SELECT
  order_id,
  user_id,
  order_number,
  order_dow,
  order_hour_of_day,
  days_since_prior_order,
  eval_set
FROM mci2026.orders
LIMIT 100;
```

## Dashboard Metabase


<img width="829" height="1024" alt="1779152501907-b18c72d1-7652-492e-9b9f-824f11b0f7c1_1" src="https://github.com/user-attachments/assets/3a013053-0ea9-4d0b-9410-af459651c94e" />

Dashboard Metabase yang dibuat berisi beberapa kartu analitik:

| Visualisasi | Tipe Chart | Tujuan |
|-------------|------------|--------|
| Total Orders | Number | Menampilkan jumlah order yang masuk |
| Average Days Since Prior Order | Number | Menampilkan rata-rata jarak hari antar order |
| Orders by Hour | Line chart | Melihat pola order berdasarkan jam |
| Orders by Day of Week | Bar chart | Melihat distribusi order per hari |
| Orders by Eval Set | Donut chart | Melihat komposisi `eval_set` |
| Sample Orders | Table | Melihat contoh data order |

Hasil utama dashboard:

| Metrik | Nilai |
|--------|------:|
| Total Orders | 300 |
| Average Days Since Prior Order | 10.01 |
| Eval Set Dominan | prior |

Lampiran dashboard tersimpan di:

```text
docs/metabase_orders_analytics_dashboard.pdf
```

## Kendala dan Perbaikan

Beberapa kendala yang ditemukan selama pengerjaan:

1. File Parquet gagal dibaca oleh task `process_data`.
   - Penyebab: kolom nested `products` masih disimpan sebagai struktur list/dict.
   - Solusi: kolom nested dikonversi menjadi JSON string sebelum disimpan ke Parquet.

2. Metabase tidak dapat dibuka.
   - Penyebab: konfigurasi database internal Metabase diarahkan ke database PostgreSQL `metabase`, sedangkan database tersebut tidak dibuat.
   - Solusi: Metabase menggunakan storage bawaan dan dijalankan ulang.

3. Metabase gagal terkoneksi ke ClickHouse.
   - Penyebab: terdapat spasi pada field host `clickhouse `.
   - Solusi: host diisi ulang menjadi `clickhouse` tanpa spasi.

4. ClickHouse dianggap `unhealthy`.
   - Penyebab: healthcheck menggunakan `curl`, sedangkan image ClickHouse tidak selalu menyediakan `curl`.
   - Solusi: healthcheck diganti menggunakan `wget`.

## Kesimpulan

Pipeline berhasil dijalankan secara end-to-end. Data dari Orders API berhasil diambil, disimpan dalam format Parquet, diproses, dimuat ke ClickHouse, divalidasi oleh Airflow, dan divisualisasikan melalui Metabase. Dashboard yang dibuat mampu menampilkan ringkasan jumlah order, rata-rata interval order, distribusi order berdasarkan jam, distribusi order berdasarkan hari, komposisi eval set, dan sample data order.

Secara keseluruhan, implementasi ini sudah memenuhi alur utama pipeline data modern: ingestion, staging, transformation, loading, validation, dan visualization.
