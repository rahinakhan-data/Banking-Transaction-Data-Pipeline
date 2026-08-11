

## Week 3 Assignment – PostgreSQL Data Warehouse & ETL Integration

**Deadline:** **12 August 2026, 11:59 PM IST**
**Project Stage:** Week 3 of 6
**Difficulty:** Intermediate → Advanced

---

## 🎯 Week 3 Objective

In Week 3, students will take the cleaned and validated output from **Week 2** and build the project's first proper **database layer**.

Students must:

**Raw CSV → Python ETL → Validation → Transformation → PostgreSQL → Data Warehouse Tables**

The objective is to move away from storing everything as CSV files and start implementing a **production-style database pipeline**.

---

# Dataset

Continue using the same three datasets provided in Week 1:

```text
north_transactions.csv
south_transactions.csv
west_transactions.csv
```

Students must **not download another dataset**.

The output generated from Week 2 should be used as the input for Week 3.

---

# Task 1 – PostgreSQL Environment Setup

Install and configure:

* PostgreSQL
* pgAdmin
* Python PostgreSQL connector

Recommended:

```bash
pip install psycopg2-binary
```

Students must create a database:

```text
banking_dw
```

---

# Task 2 – Create Database Schemas

Create the following schemas:

```text
banking_dw
│
├── staging
├── warehouse
└── audit
```

Purpose:

| Schema      | Purpose                      |
| ----------- | ---------------------------- |
| `staging`   | Temporary/raw processed data |
| `warehouse` | Final analytical tables      |
| `audit`     | ETL execution information    |

---

# Task 3 – Create Staging Table

Create:

```text
staging.transactions_staging
```

It should contain the cleaned transaction fields from Week 2.

Recommended columns:

```text
transaction_id
account_number
customer_id
transaction_datetime
transaction_type
amount
branch_code
region
channel
status
fraud_flag
```

Students should choose appropriate PostgreSQL data types.

For example:

```sql
amount NUMERIC(18,2)
```

and:

```sql
transaction_datetime TIMESTAMP
```

---

# Task 4 – Load Week 2 Output into PostgreSQL

The Python pipeline must load:

```text
processed/clean_transactions.csv
```

into:

```text
staging.transactions_staging
```

### Important

Students should **not manually import the CSV using pgAdmin**.

The data must be loaded programmatically using Python.

---

# Task 5 – Create Dimension Tables

Students must implement a basic **Star Schema**.

Create:

### `warehouse.dim_customer`

```text
customer_key
customer_id
account_number
```

---

### `warehouse.dim_branch`

```text
branch_key
branch_code
region
```

---

### `warehouse.dim_date`

```text
date_key
full_date
year
quarter
month
month_name
day
day_of_week
```

---

# Task 6 – Create Fact Table

Create:

```text
warehouse.fact_transactions
```

Recommended structure:

```text
transaction_key
transaction_id
customer_key
branch_key
date_key
transaction_type
channel
amount
status
fraud_flag
```

Students must use the dimension keys instead of repeatedly storing descriptive information.

---

# Task 7 – Implement Dimension Loading

Write Python functions that load unique customers into:

```text
dim_customer
```

and unique branches into:

```text
dim_branch
```

Students must make sure duplicate customers/branches are not inserted repeatedly.

---

# Task 8 – Generate Date Dimension

Students must create the Date Dimension programmatically.

The table should contain dates covering the transaction period.

Example:

```text
2025-01-01
2025-01-02
2025-01-03
...
```

Include:

* Year
* Quarter
* Month
* Month Name
* Day
* Day of Week

---

# Task 9 – Load Fact Table

Transform the staging data into the final fact table.

Students must perform the necessary joins:

```text
transactions_staging
        │
        ├── dim_customer
        │
        ├── dim_branch
        │
        └── dim_date
                ↓
        fact_transactions
```

The final fact table should contain the corresponding surrogate keys.

---

# Task 10 – Primary Keys & Foreign Keys

Students must implement appropriate database constraints.

For example:

```text
dim_customer.customer_key
        ↓
fact_transactions.customer_key
```

and:

```text
dim_branch.branch_key
        ↓
fact_transactions.branch_key
```

and:

```text
dim_date.date_key
        ↓
fact_transactions.date_key
```

---

# Task 11 – Audit Table

Create:

```text
audit.etl_run_log
```

Recommended columns:

```text
run_id
pipeline_name
start_time
end_time
records_extracted
records_valid
records_rejected
records_loaded
fraud_records
status
error_message
```

Example:

| run_id | records_extracted | records_loaded | status  |
| ------ | ----------------: | -------------: | ------- |
| 1      |            300000 |         299000 | SUCCESS |

---

# Task 12 – ETL Run Logging

Your Python pipeline must insert an entry into the audit table every time it runs.

Example:

```text
Pipeline Started
        ↓
Data Loaded
        ↓
Validation Completed
        ↓
Transformation Completed
        ↓
PostgreSQL Load Completed
        ↓
Pipeline Completed
```

The database should retain the history of each execution.

---

# Task 13 – Data Validation After Loading

Students must verify that:

```text
CSV Records
      =
Staging Records
      =
Fact Records + Rejected Records
```

They should also check:

* Duplicate transaction IDs
* NULL transaction IDs
* NULL foreign keys
* Incorrect amounts
* Missing dates
* Orphan records

---

# Task 14 – Write Analytical SQL Queries

Create:

```text
sql/analysis_queries.sql
```

Students must write at least **10 SQL queries**.

### Query 1

Total transaction amount by region.

### Query 2

Total number of transactions by channel.

### Query 3

Successful vs failed transactions.

### Query 4

Top 10 branches by transaction value.

### Query 5

Daily transaction volume.

### Query 6

Monthly transaction amount.

### Query 7

Top 10 customers by transaction value.

### Query 8

Number of fraud-flagged transactions by region.

### Query 9

Average transaction amount by transaction type.

### Query 10

Highest-value transactions.

---

# Task 15 – Python Database Module

Create:

```text
src/database.py
```

It should contain reusable functions such as:

```python
create_connection()
execute_query()
insert_data()
bulk_insert()
close_connection()
```

Database credentials should **not** be hardcoded.

---

# Task 16 – Environment Variables

Create:

```text
.env
```

Example:

```text
DB_HOST=localhost
DB_PORT=5432
DB_NAME=banking_dw
DB_USER=postgres
DB_PASSWORD=your_password
```

Add `.env` to:

```text
.gitignore
```

### Important

Students must **never upload database passwords to GitHub.**

---

# Task 17 – Update the Project Architecture

Students must update their architecture diagram to:

```text
                 ┌──────────────────────┐
                 │ North Transactions    │
                 │ South Transactions    │
                 │ West Transactions     │
                 └──────────┬───────────┘
                            ↓
                     Python Extract
                            ↓
                     Data Validation
                            ↓
                    Data Transformation
                            ↓
                     Fraud Detection
                            ↓
                    ┌───────────────┐
                    │   PostgreSQL  │
                    └───────┬───────┘
                            ↓
                       Staging
                            ↓
                     Data Warehouse
                            ↓
              ┌─────────────┼─────────────┐
              ↓             ↓             ↓
        dim_customer   dim_branch     dim_date
              \             |             /
               \            |            /
                └──── fact_transactions ─┘
                            ↓
                      SQL Analytics
```

---

# Expected Project Structure

By the end of Week 3:

```text
Banking_Data_Pipeline/

│
├── raw_data/
│
├── processed/
│   └── clean_transactions.csv
│
├── quarantine/
│   ├── quarantine_transactions.csv
│   └── fraud_transactions.csv
│
├── reports/
│
├── logs/
│
├── sql/
│   ├── create_schemas.sql
│   ├── create_tables.sql
│   └── analysis_queries.sql
│
├── src/
│   ├── extract.py
│   ├── validate.py
│   ├── transform.py
│   ├── fraud.py
│   ├── load.py
│   ├── database.py
│   ├── logger.py
│   ├── config.py
│   └── main.py
│
├── screenshots/
│
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```

---

# Week 3 Deliverables

Students must submit:

### 1. GitHub Repository

Updated repository containing all Week 1–3 work.

### 2. PostgreSQL Database

Screenshots showing:

* Database
* Schemas
* Tables
* Row counts
* Relationships

### 3. Python ETL

Working Python pipeline that loads data into PostgreSQL.

### 4. SQL Scripts

```text
create_schemas.sql
create_tables.sql
analysis_queries.sql
```

### 5. Audit Logs

Screenshot of:

```text
audit.etl_run_log
```

### 6. Architecture Diagram

Updated ETL + Data Warehouse architecture.

### 7. README

Document:

* Setup
* Database configuration
* ETL execution
* Database schema
* Data quality checks
* SQL analysis
* Screenshots

---

# Week 3 Evaluation – 100 Marks

| Area                            |   Marks |
| ------------------------------- | ------: |
| PostgreSQL Setup                |      10 |
| Staging Layer                   |      10 |
| Dimension Tables                |      15 |
| Fact Table                      |      15 |
| Python → PostgreSQL Integration |      15 |
| Audit/ETL Logging               |      10 |
| Data Validation                 |      10 |
| SQL Queries                     |      10 |
| Documentation                   |       5 |
| **Total**                       | **100** |

---

## 🚨 Important for Tomorrow's Deadline

Since the deadline is **tomorrow**, I would **not overload students with Airflow in this assignment**.

Their Week 3 target should simply be:

> **Take the working Week 2 ETL pipeline and successfully load its output into PostgreSQL using a proper Star Schema with audit logging.**

Then the progression becomes very logical:

**Week 1:** Understand & profile data
↓
**Week 2:** Build Python ETL
↓
**Week 3:** PostgreSQL + Data Warehouse
↓
**Week 4:** Apache Airflow orchestration
↓
**Week 5:** Advanced ETL, monitoring & optimization
↓
**Week 6:** Final production pipeline + presentation
