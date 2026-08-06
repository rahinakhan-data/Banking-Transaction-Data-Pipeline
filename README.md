# Banking Transaction Data Pipeline
An automated data engineering pipeline designed to ingest, profile, and assess the quality of regional banking transaction datasets (North, South, and West). This project establishes a robust baseline for an Enterprise ETL pipeline by identifying data anomalies, generating analytical charts, and producing a comprehensive Data Quality Report.

## Project Overview
Financial institutions generate massive volumes of transaction data across different regions daily. This project builds the foundational infrastructure to:
* **Ingest & Merge** multi-region CSV datasets into a unified master ledger.
* **Profile Data** to understand distributions, summary statistics, and categorical unique values.
* **Assess Data Quality** by detecting anomalies like negative transaction amounts, missing customer/account IDs, duplicate records, and high-value transactions.
* **Visualize Patterns** through exploratory data analysis (EDA) using Matplotlib and Seaborn.
---

## Dataset Description
The source layer integrates transaction feeds from three primary geographical operational regions:

*   **`north_transactions.csv`** : This file contains the daily transaction records from the bank branches in the North region.
*   **`south_transactions.csv`**: This file tracks daily transaction data from both corporate offices and normal bank branches in the South region.
*   **`west_transactions.csv`**: This file stores transaction logs and operational details from the banking channels in the West region.

**Key Features Profiled:** 
The system tracks and parses historical banking interactions across the following attributes:
* `transaction_id`: The unique tracking number for each transaction.
* `account_number`: The bank account number used for the transaction.
* `customer_id` : The unique profile identification number of the customer who owns the account.
* `transaction_datetime` : The exact date and time when the transaction happened.
* `transaction_type`: The category of the transaction (Deposit, Withdrawal, Transfer)
* `amount`: The total amount of money involved in the transaction.
* `branch_code` : The code name or number of the physical bank branch where the transaction was started or processed.
* `channel`:The method or platform the customer used to make the transaction.
* `status` : The final result of the transaction. It shows if the transaction was a Success (completed), Failed (did not work).
* `region`: The geographic location or territory of the transaction (`North` / `South` / `West`)


## Folder Structure
```text
Banking_Data_Pipeline/
│
├── raw_data/                       # Original, raw source CSV files
│   ├── north_transactions.csv
│   ├── south_transactions.csv
│   └── west_transactions.csv
│
├── processed/                      # Target directory for verified data
│   └── clean_transactions.csv      # 100% Valid and safe non-fraud transactions
│
├── quarantine/                     # Folder for failed records
│   └── quarantine_transactions.csv # Invalid records tagged with validation reasons
│
├── fraud_data/                     # Folder for high-risk records
│   └── fraud_transactions.csv      # Suspicious transactions separated from clean rows
│
├── reports/                        # Analysis files and documents
│   ├── graphs/                     # Standard saved EDA plots
│   └── Data_Quality_Report.pdf     # Final data quality and audit report (PDF)
│
├── logs/                           # Application and execution pipeline logs
│   └── etl.log                     # Automated continuous system activity logs
│
├── notebooks/                      # Exploratory script spaces
│   └── data_exploration.ipynb      # Data ingestion, merging, profiling & analysis script
│
├── src/                            # Executable Python scripts for the pipeline
│   ├── extract.py                  # Script to read data files
│   ├── validate.py                 # Structural parsing validations
|   ├── quarantine.py               # Quarantine mechanism for invalid records
│   ├── transform.py                # Value normalizations and sorting
│   ├── fraud.py                    # Module to flag fraud risks
│   ├── load.py                     # Module to save clean output to CSV
│   ├── logger.py                   # Configurations to write log files
│   ├── config.py                   # File paths and constants configuration
│   └── main.py                     # Central controller script to run everything
│
├── screenshots/                    # Terminal execution console logs
│
├── README.md                       # Project documentation (This file)
└── requirements.txt                # Python external library list
```

## Technologies Used
* **Languages**: Python 3.11+
* **Data Processing**: Pandas, NumPy
* **Visualization Layer**: Matplotlib, Seaborn

## Pipeline Architecture Workflow

The system processes banking data sequentially through the following decoupled operational modules:

1. **Data Ingestion (`extract.py`)**: Automatically scans and streams multi-region transaction files from the `raw_data/` directory into memory.
2. **Schema Verification (`validate.py`)**: Runs strict structural validations. Missing IDs or broken schemas are routed to `quarantine.py` to prevent pipeline failures.
3. **Log Transformation (`transform.py`)**: Handles data scrubbing, handles categorical normalizations, applies correct datetime indexing, and executes logical record sorting.
4. **Risk Analytics Engine (`fraud.py`)**: Evaluates transactional attributes against specific compliance metrics to compute fraud probability metrics and flag anomalous profiles.
5. **Persistence Management (`load.py`)**: Persists safe, non-fraud audited transaction schemas directly down into the structured target `processed/` directory.

---

 ## Setup & Execution Instructions

1. **Clone the Repository**
   ```bash
   git clone <your-repository-url>
   cd Banking_Data_Pipeline
   ```

2. **Initialize Environment & Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Data Setup Precaution**
   * Since data folders are excluded via `.gitignore` for security and privacy, you must manually create a `raw_data/` directory in the project root.
   * Place your source files (`north_transactions.csv`, `south_transactions.csv`, `west_transactions.csv`) inside it before running the project.

4. **Execute the Core Pipeline (Production Mode)**
   To execute the end-to-end automated ETL pipeline script via the orchestration controller, run:
   ```bash
   python src/main.py
   ```

5. **Execute the Data Exploration (Research Mode)**
   To analyze data profiles and review generated charts manually:
   * Launch Jupyter Notebook:
     ```bash
     jupyter notebook
     ```
   * Open `notebooks/data_exploration.ipynb` and run all cells sequentially to evaluate metrics and review plotting layers.

