import pandas as pd
import sys
import os
import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.providers.http.hooks.http import HttpHook

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
    # Task 12 Scenario A: Continuous structural monitoring for missing file scenarios
    for file_path in [NORTH_FILE_PATH, SOUTH_FILE_PATH, WEST_FILE_PATH]:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Scenario A Triggered: Input data file missing at path {file_path}")
        
    # Call the gracefully updated incremental extractor
    df = extract_records(NORTH_FILE_PATH, SOUTH_FILE_PATH, WEST_FILE_PATH)
    ti = kwargs['ti']

    if df is None or df.empty:
        print("Log: 0 records detected in source files or incremental window.")
        print("Status: 0 records are extracted from source files.")
        pd.DataFrame().to_csv(TEMP_EXTRACTED_CSV_FILE, index=False)
        ti.xcom_push(key='extracted_records', value=0)
        return
    total_extracted_records = int(len(df))
    print(f"Total {total_extracted_records} records are extracted.")
    df.to_csv(TEMP_EXTRACTED_CSV_FILE, index=False)
    ti.xcom_push(key='extracted_records', value=total_extracted_records)

# ===============================================================
# Validate Task
# ===============================================================
def validate_data(**kwargs):
    ti = kwargs['ti']
    
    if not os.path.exists(TEMP_EXTRACTED_CSV_FILE) or os.path.getsize(TEMP_EXTRACTED_CSV_FILE) <= 1:
        print("Log: Skipping validation matrix operations due to empty extract channel.")
        print("Status: 0 records are validated. 0 records are invalidated.")
        pd.DataFrame().to_csv(TEMP_VALID_CSV_FILE, index=False)
        ti.xcom_push(key='valid_records', value=0)
        ti.xcom_push(key='rejected_records', value=0)
        return

    extracted_df = pd.read_csv(TEMP_EXTRACTED_CSV_FILE)
    if extracted_df.empty:
        print("Status: 0 records are validated. 0 records are invalidated.")
        pd.DataFrame().to_csv(TEMP_VALID_CSV_FILE, index=False)
        ti.xcom_push(key='valid_records', value=0)
        ti.xcom_push(key='rejected_records', value=0)
        return

    valid_df, invalid_df = validate_records(extracted_df)
    valid_df.to_csv(TEMP_VALID_CSV_FILE, index=False)

    total_valid_records = int(len(valid_df))
    total_invalid_records = int(len(invalid_df))
    print(f"Total {total_valid_records} are validated records")
    print(f"Total {total_invalid_records} are invalidated records")

    ti.xcom_push(key='valid_records', value=total_valid_records)
    ti.xcom_push(key='rejected_records', value=total_invalid_records)

# ================================================================
# Transform Task
# ================================================================
def transform_data(**kwargs):

    ti = kwargs['ti']
    if not os.path.exists(TEMP_VALID_CSV_FILE) or os.path.getsize(TEMP_VALID_CSV_FILE) <= 1:
        print("Log: Skipping transformations because valid records matrix is empty.")
        print("Status: 0 records are transformed.")
        pd.DataFrame().to_csv(TEMP_TRANSFORMED_CSV_FILE, index=False)
        ti.xcom_push(key='transformed_records', value=0)
        return

    valid_df = pd.read_csv(TEMP_VALID_CSV_FILE)
    if valid_df.empty:
        print("Status: 0 records are transformed.")
        pd.DataFrame().to_csv(TEMP_TRANSFORMED_CSV_FILE, index=False)
        ti.xcom_push(key='transformed_records', value=0)
        return

    transformed_df = transform_records(valid_df)
    total_transformed_records = int(len(transformed_df))
    print(f"Total {total_transformed_records} are transformed")

    transformed_df.to_csv(TEMP_TRANSFORMED_CSV_FILE, index=False)
    ti.xcom_push(key='transformed_records', value=total_transformed_records)
# =================================================================
# Fraud Detection Task
# =================================================================
def detect_fraud(**kwargs):
    ti = kwargs['ti']

    if not os.path.exists(TEMP_TRANSFORMED_CSV_FILE) or os.path.getsize(TEMP_TRANSFORMED_CSV_FILE) <= 1:
        print("Log: Skipping anomaly detection routines due to empty transformations array.")
        print("Status: 0 fraud transactions are found.")
        pd.DataFrame().to_csv(TEMP_FINAL_LOAD_CSV_FILE, index=False)
        ti.xcom_push(key='fraud_records', value=0)
        return

    transformed_df = pd.read_csv(TEMP_TRANSFORMED_CSV_FILE)
    if transformed_df.empty:
        print("Status: 0 fraud transactions are found.")
        pd.DataFrame().to_csv(TEMP_FINAL_LOAD_CSV_FILE, index=False)
        ti.xcom_push(key='fraud_records', value=0)
        return

    transformed_df['transaction_datetime'] = pd.to_datetime(transformed_df['transaction_datetime'], errors='coerce')
    fraud_df, clean_df, full_processed_df = fraud_transactions(transformed_df)
    
    total_fraud_records = int(len(fraud_df))
    print(f"Total {total_fraud_records} fraud transactions are found.")

    full_processed_df.to_csv(TEMP_FINAL_LOAD_CSV_FILE, index=False)
    ti.xcom_push(key='fraud_records', value=total_fraud_records)

# ==================================================================
# Load staging Task
# ==================================================================
def load_staging():
    print("--- PostgreSQL Staging Load Started ---")

    # Task 12 Scenario B: Verify Database availability before running target procedures
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        conn.close()
    except Exception as db_err:
        raise RuntimeError(f"Scenario B Triggered: Database Unavailable or Connection Refused. Details: {db_err}")
    
    if not os.path.exists(TEMP_FINAL_LOAD_CSV_FILE) or os.path.getsize(TEMP_FINAL_LOAD_CSV_FILE) <= 1:
        print("Warning: Parsed transformation channel is empty.")
        print("Status: 0 records are inserted into database staging area.")
        initialize_database_structures()
        return

    full_processed_df = pd.read_csv(TEMP_FINAL_LOAD_CSV_FILE)
    if full_processed_df.empty:
        print("Warning: Parsed dataset contains 0 transactional metadata records.")
        print("Status: 0 records are inserted into database staging area.")
        initialize_database_structures()
        return

    full_processed_df.to_csv(CLEAN_CSV_PATH, index=False)
    if full_processed_df.empty:
        print("Warning: Parsed dataframe contains 0 transaction rows. Skipping database staging steps.")
        initialize_database_structures()
        return

    # Sync the memory dataframe to the permanent central clean CSV repository
    full_processed_df.to_csv(CLEAN_CSV_PATH, index=False)
    print(f"Synced {len(full_processed_df)} processed rows with clean_transactions.csv path.")
    
    initialize_database_structures()
    load_csv_to_staging()
    print("Success: Bulk loaded tracking instances into staging.")

# ==================================================================
# Dimensiond Loading Task
# ==================================================================
def load_dimensions(**kwargs):
    ti = kwargs.get('ti')
    valid_count = 0
    if ti:
        valid_count = ti.xcom_pull(key='valid_records', task_ids='validate_data') or 0
        
    if int(valid_count) == 0:
        print("Log: 0 records recognized for system update dimensions maps.")
        print("Status: 0 records are inserted into database dimension tables.")
        generate_dim_date()
        return

    loading_dim_tables()
    generate_dim_date()
    print("--- Dimension tables are loaded successfully ---")

# ===================================================================
# Fact Loading Task
# ===================================================================
def load_fact(**kwargs):
    ti = kwargs['ti']
    valid_count = ti.xcom_pull(key='valid_records', task_ids='validate_data') or 0
    
    if int(valid_count) == 0:
        print("Log: Zero operational vectors mapped to build warehouse logs.")
        print("Status: 0 records are inserted into database warehouse.fact_transactions.")
        ti.xcom_push(key='records_loaded', value=0)
        return

    load_fact_table()
    ti.xcom_push(key='records_loaded', value=int(valid_count))

    # Task 2 Configuration: Dynamic Time Watermark Tracing
    if os.path.exists(TEMP_FINAL_LOAD_CSV_FILE):
        final_df = pd.read_csv(TEMP_FINAL_LOAD_CSV_FILE)
        if not final_df.empty and 'transaction_datetime' in final_df.columns:
            final_df['transaction_datetime'] = pd.to_datetime(final_df['transaction_datetime'])
            max_ts = str(final_df['transaction_datetime'].max())
            print(f"--- Task 2 Checkpoint Generated: {max_ts} ---")
            ti.xcom_push(key='max_transaction_timestamp', value=max_ts)

# ===================================================================
# Quality Check Tasks
# ===================================================================
def run_quality_checks(**kwargs):
    print("--- STARTING DATA VALIDATION CHECKS ---")
    ti = kwargs['ti']
    raw_count = ti.xcom_pull(key='extracted_records', task_ids='extract_data') or 0
    
    if int(raw_count) == 0:
        print("Log: Bypassing metric matrix evaluations since input length is 0.")
        metrics = {'extracted': 0, 'valid': 0, 'rejected': 0, 'loaded': 0, 'fraud': 0}
        ti.xcom_push(key='validation_metrics', value=metrics)
        return metrics

    metrics = run_data_validation(run_id=0, total_raw_extracted=int(raw_count))
    ti.xcom_push(key='validation_metrics', value=metrics)
    return metrics

# ===================================================================
# Write Audit log Task
# ===================================================================
def write_audit_log(**kwargs):
    logger = logging.getLogger("airflow.task")
    ti = kwargs['ti']
    metrics = ti.xcom_pull(key='return_value', task_ids='run_quality_checks')

    # Backup metadata gathering from upstream components if metrics block is empty
    if not metrics or not isinstance(metrics, dict):
        ext = ti.xcom_pull(key='extracted_records', task_ids='extract_data') or 0
        val = ti.xcom_pull(key='valid_records', task_ids='validate_data') or 0
        rej = ti.xcom_pull(key='rejected_records', task_ids='validate_data') or 0
        frd = ti.xcom_pull(key='fraud_records', task_ids='detect_fraud') or 0
        metrics = {
            'extracted': int(ext),
            'valid': int(val),
            'rejected': int(rej),
            'loaded': int(val) if int(ext) > 0 else 0,
            'fraud': int(frd)
        }

    # Task 2 high-watermark parsing
    max_ts_val = ti.xcom_pull(key='max_transaction_timestamp', task_ids='load_fact') or "N/A"

    # 1. Pipeline Status Database Operations
    assigned_run_id = log_pipeline_status(run_id=None, status="STARTED")
    latest_status = "SUCCESS" if assigned_run_id else "FAILED"
    
    if assigned_run_id:
        log_pipeline_status(run_id=assigned_run_id, status="SUCCESS", metrics=metrics)

    # =========================================================================
    # TASK 14: TEXT-BASED PERFORMANCE MONITORING DASHBOARD
    # =========================================================================
    dashboard_layout = f"""
    ===========================================================================
    PIPELINE PERFORMANCE MONITORING DASHBOARD (TASK 14)
    ===========================================================================
    PIPELINE DETAILS
    ---------------------------------------------------------------------------
    Pipeline Name          : banking_transaction_pipeline
    Assigned Run ID        : {assigned_run_id or 'Local-Test'}
    Latest Execution Status: {latest_status}
    Incremental Timestamp  : {max_ts_val}
    
    DATA QUALITY RECONCILIATION SUMMARY (TASK 6)
    ---------------------------------------------------------------------------
    Total Extracted Rows   : {metrics['extracted']:,}
    Total Validated Rows   : {metrics['valid']:,}
    Total Rejected Rows    : {metrics['rejected']:,}
    Total Loaded Warehouse : {metrics['loaded']:,}
    
    SECURITY & FRAUD METRICS
    ---------------------------------------------------------------------------
    Fraud Records Caught   : {metrics['fraud']:,}
    ===========================================================================
    Status Log: 0 records inserted check completed successfully.
    ===========================================================================
    """
    # Direct print statement dumps this into your Airflow Standard Out logs terminal
    # print(dashboard_layout)
    logger.warning(dashboard_layout)
# --------------------------------------------------------------------------------------
# =========================================================================
# WEEK 5: TASK 13: Add Pipeline Alerts (Airflow Failure Callback Mechanism)
# =========================================================================

def send_pipeline_failure_alert(context):
    """
    Automated failure interceptor callback. 
    Sends alerts ONLY via Webhook without exposing secret keys or URLs.
    """
    ti = context.get('task_instance')
    task_id = ti.task_id
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    log_url = ti.log_url 

    # Plain text layout for terminal tracking
    alert_message = f"""
    =====================================================================
    TASK ALERTS: CRITICAL DATA PIPELINE FAILURE DETECTED
    =====================================================================
    DAG Identifier : {dag_id}
    Failed Node ID : {task_id}
    Timestamp Log  : {execution_date}
    Error Traceback: {exception}
    Log Link       : {log_url}
    =====================================================================
    """
    print(alert_message)

    # [Task 13 Webhook Implementation Only]
    try:
        # Standard json payload for webhook triggers
        webhook_payload = {
            "text": f" *Airflow Task Failure Alert* \n"
                    f"*DAG:* `{dag_id}`\n"
                    f"*Task:* `{task_id}`\n"
                    f"*Error:* `{str(exception)[:200]}`\n"
                    f"<{log_url}|View Airflow Logs >"
        }
        
        # Pulls the endpoint securely from Airflow Connection Manager (Task 16 compliant)
        http_hook = HttpHook(http_conn_id='webhook_alert', method='POST')
        http_hook.run(
            endpoint='', 
            data=json.dumps(webhook_payload), 
            headers={"Content-Type": "application/json"}
        )
        print("Success Alert: Webhook notification delivered successfully.")
    except Exception as webhook_err:
        print(f"Warning: Webhook dispatch failed. Error: {webhook_err}")

default_args = {
               "owner": "data_engineering_team",
                # 1. Enforcing Retries Framework Controls
                "retries": 1,
                # 2. Enforcing Dynamic Retry Delays (Gap of 5 minutes between execution tries)
                "retry_delay": timedelta(minutes=2),
                # 3. Enforcing Task Execution Hanging Timeout Thresholds (15 minutes)
                "execution_timeout": timedelta(minutes=15),
                # 4. Connecting Failure Callback Routing (Task 13 Alerts Integration)
                "on_failure_callback": send_pipeline_failure_alert
            }

# Create DAG
with DAG(
            dag_id = 'banking_transaction_pipeline', 
            start_date = datetime(2026,9,1), 
            default_args = default_args, 
            schedule="@daily",
            catchup = False,
            max_active_runs=1, # Concurrency control to avoid overlapping runs
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
    


