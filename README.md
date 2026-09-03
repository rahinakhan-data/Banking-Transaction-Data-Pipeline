# Banking Transaction Data Warehouse Pipeline

An automated, production-grade Data Engineering pipeline designed to ingest, validate, transform, and profile multi-regional banking transaction datasets (North, South, and West). This architecture moves raw data through an automated ETL cycle directly into a highly optimized **PostgreSQL Star Schema Data Warehouse Framework** featuring dynamic audit logging, strict quality checking thresholds, and advanced risk profiling.

---
## Project Overview
Financial institutions generate massive volumes of transaction data across different regional nodes daily. This project builds the production infrastructure to:
* **Automatic Ingestion:** Automatically reads and merges CSV files from different regions into one big dataset.
* **Data Quality Guards:** Drops bad records (like missing data or negative amounts) and moves them to a separate quarantine folder.
* **Fraud Detection:** Flags suspicious behavioral profiles based on frequency anomalies and financial compliance value boundaries.
* **Star Schema Warehousing:** Organizes clean messy files into neat PostgreSQL Dimension and Fact tables.
* **Automated Scheduling (Airflow):** Runs the entire 11-step pipeline automatically every night at midnight without any manual work.
* **Smart Retry System:** Automatically tries a failed step again 2 times before giving up, so the pipeline does not crash during minor database issues.
* **True Audit Trail Logs:** Uses Airflow XCom to track exact file counts from start to finish, saving accurate run summaries into the database.

---

## Dataset Description
The source layer integrates production transaction feeds from three distinct geographical operational operational territories:
* **`north_transactions.csv`**: Daily transaction records from bank branches in the North region.
* **`south_transactions.csv`**: Performance logs tracking corporate and retail branches in the South region.
* **`west_transactions.csv`**: Transactional data packets from digital and physical channels in the West region.

### Core Attributes Profiled:
* `transaction_id`: The unique tracking identification number for each transaction (Primary Key baseline).
* `account_number`: The alphanumeric bank account number used for the financial interaction.
* `customer_id`: The unique profile identification number of the customer who owns the account.
* `transaction_datetime`: The exact timestamp when the transaction was executed.
* `transaction_type`: The normalized category of the transaction (`Deposit` / `Withdrawal` / `Transfer`).
* `amount`: The precise decimal valuation of money involved in the transaction.
* `branch_code`: The unique identifier of the physical bank branch hosting the transaction.
* `channel`: The digital/physical platform used (`RTGS`, `NEFT`, `ATM`, `UPI`, `IMPS`, `BRANCH`).
* `status`: The final processing state result of the transaction (`Success` / `Failed`).
* `region`: The explicit geographical territory metadata tag (`North` / `South` / `West`).
* `fraud_flag`: Dynamic behavioral analytics classification tag applied by the risk engine (`YES` / `NO`).

---
### Main Data Columns:
`transaction_id` (Unique Key), `account_number`, `customer_id`, `transaction_datetime`, `transaction_type`, `amount`, `branch_code`, `channel`, `status`, `region`, `fraud_flag`.

---

## Folder Structure Layout
```text
Banking_Data_Pipeline/
│
├── airflow/                        # Core Apache Airflow setup folder (Added in Week 4)
│   ├── config/                     # Settings file to configure the Airflow environment
│   ├── dags/                       # Folder where workflow automation scripts live
│   │   ├── banking_transaction_pipeline.py  # Master script running all tasks in order
│   │   └── db_init_util.py         # Utility script to set up database tables automatically
│   ├── include/                    # External static binary hooks or metadata sets
│   ├── logs/                       # History folder tracking everyday step-by-step task logs
│   └── plugins/                    # Custom custom tools or extensions added to Airflow
│
├── raw_data/                       # Original, raw regional source CSV files (Excluded from Git)
│   ├── north_transactions.csv
│   ├── south_transactions.csv
│   └── west_transactions.csv
│
├── processed/                      # Target directory for verified pipeline output backup
│   └── clean_transactions.csv      # Clean structured dataset containing all fraud_flag tags
│
├── quarantine/                     # Folder for failed pipeline records
│   └── quarantine_transactions.csv # Invalid records tagged with specific validation reasons
│
├── fraud_data/                     # Folder for isolated high-risk records
│   └── fraud_transactions.csv      # Suspicious transactions isolated for local archiving
│
├── reports/                        # Analysis files and pipeline documentation deliverables
│   ├── graphs/                     # Standard saved exploratory EDA plots
│   └── Data_Quality_Report.pdf     # Final data quality and audit report (PDF)
│
├── logs/                           # Automated continuous system activity logging track
│   └── etl.log                     # Tracks continuous timestamps and step actions
│
├── notebooks/                      # Exploratory script spaces
│   └── data_exploration.ipynb      # Data ingestion, merging, profiling & analysis workbook
│
├── sql/                            # PostgreSQL Data Warehouse Script Packs (Week 3 Core)
│   ├── create_schemas.sql          # Creates staging, warehouse, and audit layers
│   ├── create_tables.sql           # Creates dimension, fact tables and core database keys
│   └── analysis_queries.sql        # Contains the 10 core business analysis queries
│
├── src/                            # Executable Python scripts for the pipeline architecture
│   ├── extract.py                  # Extracts multi-regional distributed data files
│   ├── validate.py                 # Evaluates structural parsing quality validations
│   ├── quarantine.py               # Isolates invalid records to local files
│   ├── transform.py                # Performs data type casting, sorting and trailing cleaning
│   ├── fraud.py                    # Evaluates transaction behaviors to flag risk anomalies
│   ├── load.py                     # Standardized Star Schema loading engine and validations
│   ├── database.py                 # Reusable SQLAlchemy connection layer wrapper module
│   ├── logger.py                   # Continuous pipeline system logging setup configuration
│   ├── config.py                   # Dynamic absolute project directory path router constants
│   └── main.py                     # Central controller script orchestrating the execution loop
│
├── screenshots/                    # Evidences of terminal execution logs & pgAdmin tables
│
├── .env                            # Protected file storing local database access passwords
├── .gitignore                      # Manifest detailing files completely masked from GitHub push
└── requirements.txt                # List detailing all required external python libraries
```

---

## Technologies Used
* **Core Language:** Python 3.11+
* **Data Processing Libraries:** Pandas, NumPy
* **Relational Database Engine:** PostgreSQL 15+
* **Database Driver & Object Mapping:** SQLAlchemy, Psycopg2-binary
* **Visualization Layer:** Matplotlib, Seaborn
* **Configuration & Security Management:** Python-dotenv
---

## ETL Process
The operational workflow moves data sequentially through decoupled modular layers matching standard dimensional model guidelines:

1. **Extraction (`extract.py`)**: Automatically scans path parameters from `config.py` to stream multi-region input feeds from `raw_data/` into memory safely.
2. **Quality Verification (`validate.py`)**: Executes strict structural masks. Blanks values, negative records, local duplicates, or invalid timestamps are immediately routed to `quarantine.py` to keep the ingestion clean.
3. **Data Transformation (`transform.py`)**: Parses fields into proper datatypes, sanitizes string spacing, forces uppercase/title casing uniformity, and handles float integer type collisions to remove the `.0` trailing account format bug.
4. **Fraud Evaluation Engine (`fraud.py`)**: Applies rules-based compliance criteria. Flag rows as `YES` if single amount exceeds $10,000$ or if an account triggers more than 5 transactions in a single day.
5. **PostgreSQL Loading Engine (`load.py`)**: 
   * Truncates the temporary staging table (`staging.transactions_staging`) and streams the fresh clean file batch inside it via bulk execution methods.
   * Runs unique constraints upsert logs to sync dimensions (`dim_customer`, `dim_branch`).
   * Programmatically computes a continuous chronological calendar range inside `dim_date` avoiding structural key violations.
   * Compiles data into the centralized star schema core layer (`warehouse.fact_transactions`) via surrogate relational key mapping.

---

## Setup & Database Configuration Instructions

### 1. Database Credentials Configuration
Create a file named `.env` in the root folder (this path is protected by `.gitignore`) and add your active workspace access settings:
```ini
DB_HOST = localhost
DB_PORT = 5432
DB_NAME = banking_dw
DB_USER = postgres
DB_PASSWORD = your_actual_postgresql_password_here
```

### 2. Database Schema Initialization
Open pgAdmin 4 or any query shell, connect to your local database instance, and execute the structural scripts in the following order:
1. Run `sql/create_schemas.sql` to initialize schema borders space.
2. Run `sql/create_tables.sql` to compile tables, foreign relations, and primary keys constraints.

### 3. Ingestion Initialization & Requirements Boot
```bash
# Install external software engine libraries dependencies
pip install -r requirements.txt

# Manually place raw testing targets to bypass gitignore mask rules
# Ensure raw_data folder contains: north_transactions.csv, south_transactions.csv, west_transactions.csv
```

### 4. Running the Pipeline (Production Mode)
To launch the end-to-end automated ETL framework script, run the main controller orchestrator:
```bash
python src/main.py
```
---
### 5. PostgreSQL Setup
To run the project, ensure your PostgreSQL database is instantiated with the following schema spaces:

```sql
-- Create necessary isolated schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS audit;

-- Structure for the Audit Log Table
CREATE TABLE audit.etl_run_log (
   run_id SERIAL PRIMARY KEY,
   pipeline_name VARCHAR(100),
   start_time TIMESTAMP,
   end_time TIMESTAMP,
   records_extracted INT,
   records_valid INT,
   records_rejected INT,
   records_loaded INT,
   fraud_records INT,
   status VARCHAR(20),
   error_message TEXT);
   
```

## Airflow Setup
1. Copy the project files inside your Airflow environment paths (usually `~/airflow/dags`).
2. Open your terminal and install all application package requirements:
   ```bash
   pip install apache-airflow pandas numpy sqlalchemy psycopg2-binary python-dotenv matplotlib seaborn
   ```
3. Initialize the backend database and boot up the server tasks:
   ```bash
   airflow db init
   airflow webserver -p 8080
   airflow scheduler
   ```
---

## DAG Structure & Task Dependencies
       
```text
       [ start ]
           |
     [ init_database ]
           |
     [ extract_data ]
           |
    [ validate_data ]
           |
   [ transform_data ]
           |
    [ detect_fraud ]
           |
    [ load_staging ]
           |
   [ load_dimensions ]
           |
      [ load_fact ]
           |
  [ run_quality_checks ]
           |
   [ write_audit_log ]
           |
        [ end ]
```

## Task Dependencies
The pipeline connects and schedules 11 synchronized processing states in a clean row:

```python
start >> extract_task >> validate_task >> transform_task >> fraud_detection_task
fraud_detection_task >> load_staging_task >> load_dimensions_task >> load_fact_task
load_fact_task >> quality_checks_task >> audit_task >> end
```

##  Configuration (`config.py`)
This file uses **Dynamic Hostname Recognition**. It automatically detects if the code is running inside a Docker Container or on a local desktop terminal, switching the database host address dynamically so it never crashes:

```python
import os

DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'airflow')

# DYNAMIC HOSTNAME RECOGNITION:
if os.path.exists('/.dockerenv') or 'AIRFLOW_HOME' in os.environ:
   # Inside Airflow/Docker container environment
   DB_HOST = os.getenv('DB_HOST', 'postgres')
else:
   # Running via local terminal instance (main.py)
   DB_HOST = 'localhost'

# Connection string syntax creation for SQLAlchemy Engine
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
```
---

## How to Run the Project
* **Option 1 (Using Airflow Dashboard):** Open `http://localhost:8080` in your web browser, find the `banking_transaction_pipeline` DAG tag, turn it on, and click **Trigger DAG**.
* **Option 2 (Using Local Terminal Command):** Run the central controller script directly using your local python command line:
  ```bash
  python src/main.py
  ```
---

## Data Quality Checks

### Airflow Production Settings
* **Owner Handle:** `data_engineering_team`
* **Schedule:** `@daily` (Runs automatically every night at midnight).
* **Error Handling:** If a step fails, it tries again **2 times**, waiting **5 minutes** between retries.
* **Catchup Safety:** `catchup=False` (Prevents running old missed tasks automatically if the server was turned off).

### Automated 6-Point Data Controls
To keep metrics matching perfectly across all logs (**True Audit Trail**), the quality check task pulls initial row counts straight from the live extraction state using Airflow XCom.
1. Removes all duplicate transaction identification keys in the staging area.
2. Blocks any empty (NULL) transaction records from entering final analytical tables.
3. Runs foreign key checks to link transactions perfectly to customer and branch details.
4. Checks if the final data warehouse tables actually received new rows of data.
5. Sends an operational alert flag if the total data load look suspiciously low.
6. Stops any transaction rows that have an invalid zero or negative money amount.

---

## Monitoring Logs
You can easily track the health, status, and raw row histories of every pipeline execution by running this SQL query inside pgAdmin:

```sql
SELECT run_id, pipeline_name, status, records_extracted, records_valid, records_rejected, records_loaded, fraud_records, end_time - start_time AS execution_duration
FROM audit.etl_run_log ORDER BY start_time DESC;
```
---

## Troubleshooting

### 1. Data Mismatch Errors (e.g., Extracted: 300,000 vs 297,937 in logs)
* **Symptom:** Task validation logs report differing numbers for raw ingestion counts vs database rows.
* **Resolution:** Verify your run_quality_checks task signature matches `run_data_validation(run_id, extracted_raw_count)`. Ensure `extracted_raw_count` is passed explicitly from the raw extraction `XCom` state rather than calculated via a `COUNT(*)` query on the staging table.

### 2. Database Connection Errors (`psycopg2.OperationalError`)
* **Fix:** Look at where your script is running. Standalone scripts running on a local desktop require `DB_HOST = 'localhost'`, while workflows running inside a Docker network bridge require pointing to the container network hostname.

