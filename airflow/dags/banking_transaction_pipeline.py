import pandas as pd
import sys
import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# define base project directories
PROJECT_ROOT = '/opt/airflow'
SRC_PATH = '/opt/airflow/src'

# add path to python search path if not already present
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)


# ETL Pipeline Core Functions
from src.extract import extract_records
from src.validate import validate_records
from src.transform import transform_records
from src.fraud import fraud_transactions
from src.quarantine import quarantine_records

# Database Loading & Logging Utilities
from src.load import (
    load_records, 
    load_csv_to_staging, 
    loading_dim_tables, 
    generate_dim_date, 
    load_fact_table, 
    log_pipeline_status,
    run_data_validation
)

# File Paths & Environmental Configurations 
from src.config import NORTH_FILE_PATH, SOUTH_FILE_PATH, WEST_FILE_PATH, CLEAN_CSV_PATH, PROCESSED_DIR

# Database Connection & Query Tools
from sqlalchemy import engine, text
from db_init_util import initialize_database_structures

# Connection ID for Airflow UI Database Channel
POSTGRES_CONN_ID = "banking_postgres"

# Best Practice Storage Path Handshakes within your official PROCESSED_DIR folder
TEMP_EXTRACTED_CSV_FILE = os.path.join(PROCESSED_DIR, 'temp_extracted.csv')
TEMP_VALID_CSV_FILE = os.path.join(PROCESSED_DIR, 'temp_valid.csv')
TEMP_TRANSFORMED_CSV_FILE = os.path.join(PROCESSED_DIR, 'temp_transformed.csv')
TEMP_FINAL_LOAD_CSV_FILE = os.path.join(PROCESSED_DIR, 'temp_final_load.csv')

# ===========================================================
# Extract Task
# ===========================================================
def extract_data(**kwargs):
    # Extract data from regional files (North, South, West) into a single DataFrame
    df = extract_records(NORTH_FILE_PATH,SOUTH_FILE_PATH, WEST_FILE_PATH)

    if df is not None:
        # Calculate total records extracted
        total_extracted_records = int(len(df))
        print(f"Total {total_extracted_records} are extracted")

        # Persist extracted data to a temporary CSV file for downstream tasks
        df.to_csv(TEMP_EXTRACTED_CSV_FILE, index= False)

        # Push records count to XCom for structural auditing
        ti = kwargs['ti']
        ti.xcom_push(key = 'extracted_records', value = total_extracted_records)
    
    else:
        # Fail the Airflow task explicitly if no data is returned
        raise RuntimeError("Extraction completed, but DataFrame returned is empty/None.")

# ===============================================================
# Validate Task
# ===============================================================
def validate_data(**kwargs):
    # Ensure the source file from the extraction step exists before proceeding
    if not os.path.exists(TEMP_EXTRACTED_CSV_FILE):
        raise FileNotFoundError(f"File not found at {TEMP_EXTRACTED_CSV_FILE}")

    # Load the temporarily cached raw data
    extracted_df = pd.read_csv(TEMP_EXTRACTED_CSV_FILE)

    # Segregate dataset into passing (valid) and failing (invalid) dataframes
    valid_df, invalid_df = validate_records(extracted_df)

    # Save clean records to a temporary staging file for downstream transformation
    valid_df.to_csv(TEMP_VALID_CSV_FILE, index= False)

    total_valid_records = int(len(valid_df))
    total_invalid_records = int(len(invalid_df))
    print(f"Total {total_valid_records} are validated records")
    print(f"Total {total_invalid_records} are invalidated records")

    # Push audited record counts to XCom for lineage and monitoring
    ti = kwargs['ti']
    ti.xcom_push(key = 'valid_records', value = total_valid_records)
    ti.xcom_push(key = 'rejected_records', value = total_invalid_records)

# ================================================================
# Transform Task
# ================================================================
def transform_data(**kwargs):

    # Ensure the valid records file from the validation step exists
    if not os.path.exists(TEMP_VALID_CSV_FILE):
        raise FileNotFoundError(f"File not found {TEMP_VALID_CSV_FILE}")

    # Load pre-validated clean data into a DataFrame
    valid_df = pd.read_csv(TEMP_VALID_CSV_FILE)

    # Apply core data transformations, enrichment, and business logic
    transformed_df = transform_records(valid_df)

    total_transformed_records = int(len(transformed_df))
    print(f"Total {total_transformed_records} are transformed")

    # Persist fully transformed data to a temporary CSV file for the loading stage
    transformed_df.to_csv(TEMP_TRANSFORMED_CSV_FILE, index = False)

    # Push transformation metrics to Airflow XCom for audit trails
    ti = kwargs['ti']
    ti.xcom_push(key = 'transformed_records', value = total_transformed_records)

# =================================================================
# Fraud Detection Task
# =================================================================
def detect_fraud(**kwargs):
    # Ensure the transformed data file from the previous step exists
    if not os.path.exists(TEMP_TRANSFORMED_CSV_FILE):
        raise FileNotFoundError(f"File not found at {TEMP_TRANSFORMED_CSV_FILE}")

    # Load data with timestamp parsing enabled
    transformed_df = pd.read_csv(TEMP_TRANSFORMED_CSV_FILE, parse_dates=['transaction_datetime'])

    # Segment data into fraud-flagged, clean, and comprehensively processed datasets
    fraud_df, clean_df, full_processed_df = fraud_transactions(transformed_df)
    total_fraud_records = int(len(fraud_df))
    print(f"Total {total_fraud_records} fraud transactions are found.")

    # Save the final consolidated and flagged dataset for the data warehouse load stage
    full_processed_df.to_csv(TEMP_FINAL_LOAD_CSV_FILE, index=False)

    # Push audited fraud metrics to Airflow XCom for monitoring dashboards
    ti = kwargs['ti']
    ti.xcom_push(key = 'fraud_records', value = total_fraud_records)

# ==================================================================
# Load staging Task
# ==================================================================
def load_staging():
    print("--- PostgreSQL Staging Load Started ---")

    # Verify that the finalized dataset from fraud detection exists
    if not os.path.exists(TEMP_FINAL_LOAD_CSV_FILE):
        raise FileNotFoundError(f"Missing staging final dataset: {TEMP_FINAL_LOAD_CSV_FILE}")

    # Load the fully processed dataset containing security flags
    full_processed_df = pd.read_csv(TEMP_FINAL_LOAD_CSV_FILE)

    # Sync the memory dataframe to the permanent central clean CSV repository
    full_processed_df.to_csv(CLEAN_CSV_PATH, index=False)

    print(f"Synced {len(full_processed_df)} processed rows with clean_transactions.csv path.")

    # Execute DDL scripts to create or drop/recreate database staging tables
    initialize_database_structures()

    # Bulk copy/insert records from the synced CSV file into the database staging area
    load_csv_to_staging()

# ==================================================================
# Dimensiond Loading Task
# ==================================================================
def load_dimensions():
    # Populate core dimensional tables
    loading_dim_tables()

    # Generate or update the Date Dimension table to support time-series reporting
    generate_dim_date()
    print("--- Dimension tables are loaded successfully ---")

# ===================================================================
# Fact Loading Task
# ===================================================================
def load_fact(**kwargs):
    # Execute ETL logic to load metrics and foreign keys into the transactional Fact table
    load_fact_table()

    # Fetch the master valid records count from the 'validate_data' task via XCom
    ti = kwargs['ti']
    valid_count = ti.xcom_pull(key='valid_records', task_ids='validate_data') or 0

    # Log the final loaded count to XCom for downstream auditing and reports
    ti.xcom_push(key='records_loaded', value=int(valid_count))

# ===================================================================
# Quality Check Tasks
# ===================================================================
def run_quality_checks(**kwargs):
    print("--- STARTING DATA VALIDATION CHECKS ---")

    ti = kwargs['ti']
    raw_count = ti.xcom_pull(key ='extracted_records', task_ids = 'extract_data') or 0
    # Call real validation function! Pass a run_id placeholder (e.g., 0)

    metrics = run_data_validation(run_id=0, total_raw_extracted=int(raw_count))

    ti.xcom_push(key='validation_metrics', value=metrics)

    return metrics
# ===================================================================
# Write Audit log Task
# ===================================================================
def write_audit_log(**kwargs):
    ti = kwargs['ti']
    # metrics = ti.xcom_pull(key='validation_metrics', task_ids='run_quality_checks')
    metrics = ti.xcom_pull(key='return_value', task_ids='run_quality_checks')

    # Airflow XCom sometimes serializes dicts into raw JSON strings depending on backend configuration
    if not metrics or not isinstance(metrics, dict):
        ext = ti.xcom_pull(key='extracted_records', task_ids='extract_data') or 0
        val = ti.xcom_pull(key='valid_records', task_ids='validate_data') or 0
        rej = ti.xcom_pull(key='rejected_records', task_ids='validate_data') or 0
        frd = ti.xcom_pull(key='fraud_records', task_ids='detect_fraud') or 0
        # metrics = {'extracted': ext, 'valid': val, 'rejected': rej, 'fraud': frd}
        metrics = {
        'extracted': int(ext),   
        'valid': int(val),     
        'rejected': int(rej), 
        'loaded': int(val),     
        'fraud': int(frd)    
    }

    print(f"Final resolved metrics payload for database write: {metrics}")

    # 1. Initialize the audit run
    assigned_run_id = log_pipeline_status(run_id=None, status="STARTED")
    
    # 2. Update it to SUCCESS using captured metrics dictionary
    if assigned_run_id:
        log_pipeline_status(run_id=assigned_run_id, status="SUCCESS", metrics=metrics)
        print(f"Audit log committed to database under Run ID: {assigned_run_id}")
# --------------------------------------------------------------------------------------

default_args = {
                "owner": "data_engineering_team",
                "retries": 2,
                "retry_delay": timedelta(minutes=1)
            }

# Create DAG
with DAG(
            dag_id = 'banking_transaction_pipeline', 
            start_date = datetime(2026,9,1), 
            default_args = default_args, 
            schedule = None,
            catchup = False,
            tags = ['banking', 'etl', 'clean_code'],
        ) as dag:

    start = EmptyOperator(task_id = 'start')

    extract_task = PythonOperator(task_id = 'extract_data', python_callable = extract_data)
    validate_task = PythonOperator(task_id = 'validate_data', python_callable = validate_data)
    transform_task = PythonOperator(task_id = 'transform_data', python_callable = transform_data)
    fraud_detection_task = PythonOperator(task_id = 'detect_fraud', python_callable = detect_fraud)
    load_staging_task = PythonOperator(task_id = 'load_staging', python_callable = load_staging)
    load_dimensions_task = PythonOperator(task_id= 'load_dimensions', python_callable = load_dimensions)
    load_fact_task = PythonOperator(task_id = 'load_fact', python_callable = load_fact)
    quality_checks_task = PythonOperator(task_id = 'run_quality_checks', python_callable = run_quality_checks)
    audit_task = PythonOperator(task_id = 'write_audit_log', python_callable = write_audit_log)

    end = EmptyOperator(task_id = 'end')

    # task running order
    (
        start
        >> extract_task
        >> validate_task
        >> transform_task
        >> fraud_detection_task
        >> load_staging_task
        >> load_dimensions_task
        >> load_fact_task
        >> quality_checks_task
        >> audit_task
        >> end
    )
    


