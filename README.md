# Banking Transaction Data Warehouse Pipeline

An automated, production-grade Data Engineering pipeline designed to ingest, validate, transform, and profile multi-regional banking transaction datasets (North, South, and West). This architecture moves raw data through an automated ETL cycle directly into a highly optimized **PostgreSQL Star Schema Data Warehouse Framework** featuring dynamic audit logging, strict quality checking thresholds, and advanced risk profiling.

---

## Project Overview
Financial institutions generate massive volumes of transaction data across different regional nodes daily. This project builds the production infrastructure to:
* **Programmatic Ingestion & Merge:** Dynamically streams multi-region CSV datasets into a unified master ledger without manual interaction.
* **Strict Quality Safeguards:** Filters missing elements, negative amounts, or bad schemas, routing corrupted artifacts straight to a quarantine layer.
* **Advanced Risk Analytics:** Flags suspicious behavioral profiles based on frequency anomalies and financial compliance value boundaries.
* **Star Schema Warehousing:** Normalizes unstructured data streams into optimized PostgreSQL Dimension and Fact tables using relational constraints.
* **Dynamic Process Auditing:** Automatically logs execution durations, telemetry counts, and structural integrity reports for every single batch run.

---

## Dataset Description
The source layer integrates production transaction feeds from three distinct geographical operational operational territories:
* **`north_transactions.csv`**: Daily transaction records from bank branches in the North region.
* **`south_transactions.csv`**: Performance logs tracking corporate and retail branches in the South region.
* **`west_transactions.csv`**: Transactional data packets from digital and physical channels in the West region.

### Core Core Attributes Profiled:
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

## Folder Structure Layout
```text
Banking_Data_Pipeline/
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

## Pipeline Architecture Workflow
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
## Data Quality Checks
The framework executes native database-level validation queries to capture telemetry for:
* **Completeness Validation:** Identifies if any row contains missing or NULL transaction identifiers.
* **Math Boundary Rule:** Catches zero or negative amounts escaping downstream validation layers.

## Week 4: Apache Airflow Orchestration & Data Quality

### Pipeline Workflow Topology
The entire ETL workflow is orchestrated via Apache Airflow across 11 synchronized processing states:
`start` ➔ `init_database` ➔ `extract_data` ➔ `validate_data` ➔ `transform_data` ➔ `detect_fraud` ➔ `load_staging` ➔ `load_dimensions` ➔ `load_fact` ➔ `run_quality_checks` ➔ `write_audit_log` ➔ `end`

### Production Configuration Parameters
* **Orchestration Owner Registry:** `data_engineering_team`
* **Automated Run Scheduling:** `@daily` (Configured to process financial incoming ledger feeds automatically at midnight).
* **Fault Tolerance Gateways:** Configured with `2 Retries` spaced across `5-minute` back-off intervals.
* **Idempotency Safeguard:** `catchup=False` to explicitly prevent automatic backfilling stress during server outages.

### Automated 6-Point Database Quality Controls
1. Total filtration of any duplicate financial transaction identifiers in the staging layer.
2. Complete schema validation ensuring zero NULL transaction records populate the target tables.
3. Automated Foreign Key Integrity validations to verify that all transactional lines accurately resolve to valid dimension tracks.
4. Active Row-Count evaluation checks ensuring target analytical schemas successfully receive records.
5. Out-of-bounds dataset volume checking to trigger system warnings on abnormally low loads.
6. Validation checks explicitly preventing invalid non-positive or negative financial transfer values from writing into systems.
