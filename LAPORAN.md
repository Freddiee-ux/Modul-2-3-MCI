# Laporan Pipeline Orchestration MCI2026 Task 2

## Identitas Kelompok

| Nama | NRP |
|------|-----|
| [Nama 1] | [NRP 1] |
| [Nama 2] | [NRP 2] |
| [Nama 3] | [NRP 3] |

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
