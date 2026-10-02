# Superstore ELT Pipeline with DuckDB, dbt, and Apache Airflow

## Overview

This project implements an end-to-end ELT data engineering pipeline using the Superstore dataset.

The pipeline loads raw Superstore data into DuckDB, transforms and validates the data with dbt, and orchestrates the full workflow using Apache Airflow running locally through Astronomer Astro.

The final warehouse follows a star-schema style design with dimension tables, a fact table, and an aggregated monthly sales table for analytics and reporting.

---

## Tech Stack

- Python
- DuckDB
- dbt Core
- dbt-duckdb
- Apache Airflow
- Astronomer Astro CLI
- Docker Desktop
- openpyxl

---

## Pipeline Architecture

```text
Superstore Dataset
        |
        v
Python Loader
        |
        v
DuckDB ODS Layer
ods.raw_superstore
        |
        v
dbt Staging Layer
main_stg.stg_superstore
        |
        v
Data Warehouse Layer
        |
        +--> dim_customers
        +--> dim_products
        +--> dim_locations
        +--> dim_dates
        +--> fact_sales
        +--> agg_monthly_sales
        |
        v
Airflow / Astro Orchestration
```

---

## Project Structure

```text
superstore_pipeline/
|
+-- dags/
|   +-- superstore_pipeline.py
|
+-- scripts/
|   +-- load_to_ods.py
|
+-- dbt_superstore/
|   +-- dbt_project.yml
|   +-- profiles.yml
|   +-- models/
|       +-- staging/
|       +-- marts/
|
+-- raw_superstore.csv
+-- dev.duckdb
+-- Dockerfile
+-- requirements.txt
+-- packages.txt
+-- airflow_settings.yaml
+-- README.md
```

---

## Dataset

The Superstore dataset contains 9,994 rows.

It includes fields related to:

- Orders
- Customers
- Products
- Categories and sub-categories
- Geographic locations
- Sales
- Quantity
- Discounts
- Profit
- Order dates
- Shipping dates

---

## Data Layers

### 1. ODS / Raw Layer

The Python ingestion script loads the raw dataset into DuckDB.

Target table:

```text
ods.raw_superstore
```

Run manually:

```powershell
python scripts\load_to_ods.py
```

Expected result:

```text
Loaded ods.raw_superstore: 9994 rows
```

---

### 2. Staging Layer

The main staging model is:

```text
main_stg.stg_superstore
```

The staging layer prepares the raw data for analytics by standardizing and cleaning the source data before it is used to build the dimensional models.

---

### 3. Data Warehouse Layer

The project builds the following warehouse models:

#### `dim_customers`
Stores customer-related attributes.

Typical analytics:

- Sales by customer
- Sales by segment
- Customer profitability

#### `dim_products`
Stores product-related attributes.

Typical analytics:

- Sales by product
- Sales by category
- Profit by sub-category

#### `dim_locations`
Stores geographic attributes.

Typical analytics:

- Sales by region
- Sales by state
- Sales by city

#### `dim_dates`
Stores date-related attributes for time-based analysis.

Typical analytics:

- Monthly sales
- Quarterly sales
- Yearly trends

#### `fact_sales`
The central fact table containing sales-line level measures and foreign keys to dimensions.

Typical measures include:

- Sales
- Quantity
- Discount
- Profit

#### `agg_monthly_sales`
An aggregated monthly sales model intended for reporting and dashboard workloads.

---

## dbt Data Quality Tests

The project includes dbt tests for data quality and referential integrity.

Examples include:

```text
not_null
unique
relationships
```

Important validations include:

- `row_id` is unique
- Important IDs are not null
- `customer_id` in `fact_sales` exists in `dim_customers`
- `product_id` in `fact_sales` exists in `dim_products`
- `location_key` in `fact_sales` exists in `dim_locations`

The successful dbt build executed:

```text
7 models
23 data tests
1 source
```

Final result:

```text
PASS=30
WARN=0
ERROR=0
SKIP=0
TOTAL=30
```

---

## Local Setup

### 1. Create a Virtual Environment

From the project directory:

```powershell
py -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

---

### 2. Install Python Dependencies

```powershell
python -m pip install --upgrade pip
python -m pip install duckdb openpyxl dbt-duckdb
```

Verify DuckDB:

```powershell
python -c "import duckdb; print('DuckDB:', duckdb.__version__)"
```

Verify dbt:

```powershell
dbt --version
```

Example working versions used during development:

```text
DuckDB: 1.5.6
dbt-core: 1.12.5
dbt-duckdb: 1.11.0
```

---

## Run the Pipeline Manually

### Step 1: Load the Raw Data

From the project root:

```powershell
python scripts\load_to_ods.py
```

### Step 2: Enter the dbt Project

```powershell
cd dbt_superstore
```

### Step 3: Validate dbt Configuration

```powershell
dbt debug --profiles-dir .
```

Expected result:

```text
All checks passed!
```

### Step 4: Build Models and Run Tests

```powershell
dbt build --profiles-dir .
```

A successful run builds the staging and warehouse models and executes all configured tests.

---

## Airflow with Astronomer Astro

Apache Airflow is run locally through Astronomer Astro and Docker.

### Prerequisites

Make sure these commands work:

```powershell
docker --version
docker compose version
astro version
```

---

## Astro Project Initialization

From the project root:

```powershell
astro dev init
```

The Astro project uses the runtime image defined in the Dockerfile.

Example:

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.3-8
```

---

## Python Dependencies inside Astro

The Astro `requirements.txt` should include:

```text
duckdb==1.5.6
dbt-duckdb==1.11.0
openpyxl==3.1.5
```

---

## System Dependencies

The dbt debug command checks for Git.

Add the following to `packages.txt`:

```text
git
```

---

## Airflow DAG

The DAG ID is:

```text
superstore_duckdb_dbt_pipeline
```

The workflow contains three tasks:

```text
load_superstore_to_duckdb
            |
            v
        dbt_debug
            |
            v
        dbt_build
```

### Task 1: `load_superstore_to_duckdb`

Runs the Python ingestion script and loads the raw dataset into DuckDB.

### Task 2: `dbt_debug`

Validates the dbt project, profile, adapter, dependencies, and DuckDB connection.

### Task 3: `dbt_build`

Builds the dbt models and executes the configured data tests.

---

## dbt Log and Target Directories in Airflow

Inside the Airflow container, dbt may not have permission to write directly into the mounted project directory.

The DAG therefore uses writable temporary locations:

```text
/tmp/dbt_logs
/tmp/dbt_target
```

Example command:

```bash
DBT_LOG_PATH=/tmp/dbt_logs \
DBT_TARGET_PATH=/tmp/dbt_target \
dbt build --profiles-dir .
```

---

## Start Airflow

From the project root:

```powershell
astro dev start
```

Then open:

```text
http://localhost:8080
```

Search for:

```text
superstore_duckdb_dbt_pipeline
```

Trigger the DAG manually from the Airflow UI.

A successful DAG run should show all three tasks in green:

```text
load_superstore_to_duckdb  -> success
dbt_debug                  -> success
dbt_build                  -> success
```

---

## Stop Airflow

```powershell
astro dev stop
```

Restart when needed:

```powershell
astro dev restart
```

---

## Troubleshooting

### Docker Linux Engine Not Running

If Astro reports an error similar to:

```text
failed to connect to the docker API
```

Open Docker Desktop and wait until the Linux engine is running.

Then verify:

```powershell
docker info
```

---

### Missing `raw_superstore.csv`

If Docker reports:

```text
COPY raw_superstore.csv ... not found
```

Make sure the dataset file exists in the project root, or rely on the Astro runtime project copy behavior instead of duplicating COPY commands unnecessarily.

---

### dbt Permission Error

If dbt fails with:

```text
PermissionError: [Errno 13] Permission denied: .../logs/dbt.log
```

Use temporary writable locations:

```text
DBT_LOG_PATH=/tmp/dbt_logs
DBT_TARGET_PATH=/tmp/dbt_target
```

---

### Git Not Found

If `dbt debug` reports:

```text
Could not find command: git
```

Add this to `packages.txt`:

```text
git
```

Then rebuild or restart Astro.

---

## Final Result

The completed project provides a fully working local ELT pipeline:

```text
Raw Superstore Data
        |
        v
DuckDB ODS
        |
        v
dbt Transformations
        |
        v
Star Schema Data Warehouse
        |
        v
dbt Tests
        |
        v
Apache Airflow Orchestration
```

The final Airflow execution successfully completed all tasks:

```text
load_superstore_to_duckdb  -> success
dbt_debug                  -> success
dbt_build                  -> success
```

This confirms that the ingestion, transformation, validation, and orchestration layers are integrated and working end to end.

---

## Possible Next Steps

The project can be extended with:

- Power BI or Tableau dashboards
- Incremental dbt models
- dbt snapshots
- Additional data quality tests
- Airflow scheduling instead of manual triggering
- Data freshness checks
- CI/CD for dbt and Airflow
- Cloud deployment with Astronomer

