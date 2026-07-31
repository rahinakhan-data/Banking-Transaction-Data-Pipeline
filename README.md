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
├── raw_data/                   # Original, raw source CSV files
│   ├── north_transactions.csv
│   ├── south_transactions.csv
│   └── west_transactions.csv
│
├── processed/                  # Contains the merged master dataset
│   └── master_transactions.csv
│
├── reports/                    # Generated analytical documents & charts
│   ├── graphs/                 # Saved EDA charts (Chart 1 to Chart 6)
│   └── Data_Quality_Report.pdf # Final data quality and audit report (PDF)
│
├── logs/                       # Application and execution pipeline logs
│
├── notebooks/                  # Jupyter Notebooks used for interactive EDA
|   └── data_exploration.ipynb  # Data ingestion, merging, profiling & analysis script
│
├── src/                        # Executable Python scripts for the pipeline
│
├── screenshots/                # Terminal outputs and execution proof
│
├── README.md                   # Project documentation (This file)
└── requirements.txt            # Python dependencies
```

## Technologies Used
* **Languages**: Python 3.11+
* **Data Processing**: Pandas, NumPy
* **Visualization Layer**: Matplotlib, Seaborn

## Setup & Execution Instructions

1. **Clone the Repository**
   ```bash
   git clone https://github.com
   cd Banking-Transaction-Data-Pipeline
   ```

2. **Initialize Environment & Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Execute the Data Pipeline & Exploration**
   * Launch Jupyter Notebook by running this command in your terminal:
     ```bash
     jupyter notebook
     ```
   * Open the file named `notebooks/data_exploration.ipynb` in your browser.
   * Run all cells sequentially from top to bottom to load, merge, profile the data, and generate all final analytical graphs.
