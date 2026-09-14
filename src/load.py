import os
import sys
import io
import pandas as pd 
from datetime import datetime

from sqlalchemy import create_engine, text
from database import engine, bulk_insert, execute_query, create_connection, close_connection
from config import CLEAN_CSV_PATH

# ===================================================================
# Task 4 – Load Week 2 Output into PostgreSQL
# ===================================================================
def load_csv_to_staging():
    print("\n","*="*50)
    print("--- PostgreSQL Staging Load Started ---")

    # 🎯 STEP 1: TASK 2 INCREMENTAL DATA GAP CRASH PROTECTION
    # Check if file doesn't exist or its physical file size is completely zero (0 bytes)
    if not os.path.exists(CLEAN_CSV_PATH) or os.path.getsize(CLEAN_CSV_PATH) == 0:
        print("💡 Incremental Sync Notice: Central clean CSV data payload is empty (0 new rows). Bypassing execution safely.")
        execute_query("TRUNCATE TABLE staging.transactions_staging;")
        print("--- Staging Loading Completed Safely (Empty Sync Window) ---")
        return
    try:

        df = pd.read_csv(CLEAN_CSV_PATH)

        if df.empty:
            print("💡 Incremental Sync Notice: Parsed DataFrame contains 0 operational rows. Bypassing execution.")
            execute_query("TRUNCATE TABLE staging.transactions_staging;")
            return
        
        # Safe programmatic drop protection to avoid KeyErrors downstream
        if 'txn_date' in df.columns:
            df = df.drop(columns='txn_date')
        
        df['account_number'] = df['account_number'].astype('str')

        df['transaction_datetime'] = pd.to_datetime(df['transaction_datetime'], errors='coerce')

        print(f"Truncating staging for fresh runtime footprint isolation...")
        execute_query("TRUNCATE TABLE staging.transactions_staging ;")
        """
        print(f"Bulk inserting {len(df)} records into staging...")
        bulk_insert(
            table_name = 'transactions_staging',
            schema_name = 'staging',
            dataframe = df
        )
        """

        print(f"Streaming {len(df):,} records directly into staging using cursor COPY...")
    
        # Establish connection and activate raw cursor driver management layer
        conn = create_connection()
        # try:
        # Resolve the raw connection handles safely across contexts
        raw_conn = conn.connection if hasattr(conn, 'connection') else conn
        cursor = raw_conn.cursor()
        
        # Write DataFrame rows straight to an in-memory string file buffer
        s_buf = io.StringIO()
        df.to_csv(s_buf, index=False, header=False)
        s_buf.seek(0)
        
        # Native streaming invocation: completely bypasses SQL parsing & looping rows
        # cursor.copy_from(s_buf, 'staging.transactions_staging', sep=',', null='')
        
        copy_sql = "COPY staging.transactions_staging FROM STDIN WITH CSV DELIMITER ',' NULL ''"
        cursor.copy_expert(sql=copy_sql, file=s_buf)

        if hasattr(raw_conn, 'commit'):
            raw_conn.commit()
            
        cursor.close()
        print("--- Task 8 Ultimate COPY Strategy Execution: SUCCESS ---")
        
    except Exception as e:
        if 'raw_conn' in locals() and hasattr(raw_conn, 'rollback'):
            raw_conn.rollback()
        print(f"CRITICAL ERROR: Native COPY Stream Interrupted. Trace: {e}")
        raise e
    finally:
        if 'conn' in locals():
            close_connection(conn)
            
    print("--- Staging Loading Completed Successfully ---")

# ================================================================
# Task 7 – Implement Dimension Loading
# ===============================================================
def loading_dim_tables():
    print("\n","*="*50)
    print("\n--- Dimension Tables Loading Started ---")

    dim_customer_query = """ 
            INSERT INTO warehouse.dim_customer (customer_id, account_number)
                SELECT DISTINCT customer_id, account_number
                FROM staging.transactions_staging
                WHERE customer_id IS NOT NULL
            ON CONFLICT(customer_id) DO NOTHING
        """
    execute_query(dim_customer_query)

    dim_branch_query = """
            INSERT INTO warehouse.dim_branch(branch_code, region)
                SELECT DISTINCT branch_code, region
                FROM staging.transactions_staging
                WHERE branch_code IS NOT NULL
            ON CONFLICT(branch_code) DO NOTHING

        """
    execute_query(dim_branch_query)   

# ===================================================
# Task 8 – Generate Date Dimension
# ===================================================

def generate_dim_date():
    print("\n","*="*50)
    # 1. Fetch the exact transaction date boundary range from the staging table
    date_bounds_query = """
            SELECT MIN(transaction_datetime) :: date AS min_date,
            MAX(transaction_datetime) :: date AS max_date
            FROM staging.transactions_staging
        """
    bounds_df = pd.read_sql(date_bounds_query, con=engine)

    # 2. Extract the min and max dates from the resulting tuple array
    min_date = bounds_df.loc[0, 'min_date']
    max_date = bounds_df.loc[0, 'max_date']

    if pd.isnull(min_date) or pd.isnull(max_date):
        print("Warning: Staging table is empty. Skipping date dimension generation.")
        return
    
    print(f"Generating continuous daily calendar array from {min_date} to {max_date}...")

    # 3. Generate a continuous daily chronological date series using Pandas
    date_series = pd.date_range(start=min_date, end=max_date)

    # 3. Create a clean pandas dataframe
    date_df = pd.DataFrame()

    # 4. Programmatically map out all required columns
    date_df['date_key'] = date_series.strftime("%Y%m%d").astype(int)
    date_df['full_date'] = date_series.date
    date_df['year'] = date_series.year
    date_df['quarter'] = (date_series.month - 1) // 3 + 1
    date_df['month'] = date_series.month
    date_df['month_name'] = date_series.strftime('%B')
    date_df['day'] = date_series.day
    date_df['day_of_week'] = date_series.strftime('%A')

    date_df.to_sql(name='temp_date_stg', con=engine, if_exists='replace', index=False)

    upsert_query = """
        INSERT INTO warehouse.dim_date (date_key, full_date, year, quarter, month, month_name, day, day_of_week)
        SELECT date_key, full_date, year, quarter, month, month_name, day, day_of_week 
        FROM temp_date_stg
        ON CONFLICT (date_key) DO NOTHING;
    """
    execute_query(upsert_query)
    print("--- Date Dimension Generation Completed ---")
    print("--- Data are successfully stored in Date Dimension Table")

# ====================================================================
# Task 9 -  Load Fact Table
# ====================================================================
def load_fact_table():
    print("\n","*="*50)
    print("\n--- Fact Table Transformation & Loading Started ---")
    fact_insert_query = """
            INSERT INTO warehouse.fact_transactions
            (transaction_id, customer_key, branch_key, date_key, transaction_type, channel, amount, status, fraud_flag)
                SELECT stg.transaction_id,
                c.customer_key,
                b.branch_key,
                CAST(to_char(stg.transaction_datetime, 'YYYYMMDD') AS INT ) AS date_key,
                stg.transaction_type,
                stg.channel,
                stg.amount,
                stg.status,
                stg.fraud_flag
            FROM staging.transactions_staging stg
            JOIN warehouse.dim_customer c ON stg.customer_id = c.customer_id
            LEFT JOIN warehouse.dim_branch b ON stg.branch_code = b.branch_code
            ON CONFLICT (transaction_id) DO NOTHING;
        """
    execute_query(fact_insert_query)
    print("--- Fact Table Transformation & Loading Completed ---")

# ===================================================================
# TASK 12 – Audit Run Logging Function
# ===================================================================
def log_pipeline_status(run_id=None, status="STARTED", metrics=None, error=None):
    print("\n","*="*50)
    pipeline_name = "banking_dw_load_pipeline"
    current_time = datetime.now()

    if run_id is None:
        init_query = """
            INSERT INTO audit.etl_run_log (pipeline_name, start_time, status)
            VALUES (:pipeline_name, :start_time, 'STARTED') RETURNING run_id;
        """
        try:
            conn = create_connection()
            # SQLAlchemy native tuple parameters style
            result = conn.execute(text(init_query), {
                "pipeline_name": pipeline_name, 
                "start_time": current_time
            })
            # FIXED: Safe fetchone check to avoid NoneType index crashes
            row = result.fetchone()
            assigned_id = row[0] if row else None

            if hasattr(conn, 'commit'):
                conn.commit()
            close_connection(conn)

            print(f"Audit Log Initialized. Assigned Run ID: {assigned_id}")
            return assigned_id
        
        except Exception as e:
            print(f"Failed to create start audit log: {e}")
            return None

    elif status == "SUCCESS" and metrics:
        success_query = """
            UPDATE audit.etl_run_log 
            SET end_time = :end_time, records_extracted = :extracted, records_valid = :valid, 
                records_rejected = :rejected, records_loaded = :loaded, fraud_records = :fraud, status = 'SUCCESS'
            WHERE run_id = :run_id;
        """

        # Task 3 Change: update success update to control table
        control_success_query = """
            UPDATE audit.pipeline_control
            SET last_successful_run = :current_time,
                last_processed_timestamp = :max_ts,
                last_run_status = 'SUCCESS',
                updated_at = :current_time
            WHERE pipeline_name = 'banking_dw_load_pipeline';
        """
        try:
            conn = create_connection()
            max_transaction_ts = metrics.get('max_ts', current_time)
            conn.execute(text(success_query), {
                "end_time": current_time, 
                "extracted": int(metrics.get('extracted', 0)), 
                "valid": int(metrics.get('valid', 0)),
                "rejected": int(metrics.get('rejected', 0)),
                "loaded": int(metrics.get('loaded', 0)), 
                "fraud": int(metrics.get('fraud', 0)), 
                "run_id": int(run_id)
            })

            # update control table
            conn.execute(text(control_success_query), {"current_time": current_time, "max_ts": max_transaction_ts})

            if hasattr(conn, 'commit'):
                conn.commit()

            close_connection(conn)
            print(f"[{current_time}] Audit Log & Control Table Updated: SUCCESS")
        except Exception as e:
            print(f"Failed to update success audit log: {e}")

    elif status == "FAILED":
        failure_query = """
            UPDATE audit.etl_run_log 
            SET end_time = :end_time, status = 'FAILED', error_message = :error
            WHERE run_id = :run_id;
        """
        # Task 3 Change: Control table ko failure status update bhejein
        control_failure_query = """
            UPDATE audit.pipeline_control
            SET last_run_status = 'FAILED',
                updated_at = :current_time
            WHERE pipeline_name = 'banking_dw_load_pipeline';
        """
        try:
            conn = create_connection()
            conn.execute(text(failure_query), {
                "end_time": current_time, "error": str(error), "run_id": run_id
            })

            conn.execute(text(control_failure_query), {"current_time": current_time})
            if hasattr(conn, 'commit'):
                conn.commit()

            close_connection(conn)
            print(f"[{current_time}] Audit Log & Control table Updated: FAILED")
        except Exception as e:
            print(f"Failed to update failure audit log: {e}")

# =========================================================================
# Task 13 - Data Validation After Loading
# =========================================================================
def run_data_validation(run_id, total_raw_extracted ):
    print("\n","*="*50)
    print("\n--- STARTING DATA VALIDATION CHECKS ---")
    conn = create_connection()

    # .scalar() captures single numeric responses out of COUNT queries perfectly
    # extracted = conn.execute(text("SELECT COUNT(*) FROM staging.transactions_staging;")).scalar()
    extracted = total_raw_extracted 
    valid = conn.execute(text("SELECT COUNT(*) FROM warehouse.fact_transactions;")).scalar()
    fraud = conn.execute(text("SELECT COUNT(*) FROM warehouse.fact_transactions WHERE fraud_flag = 'YES';")).scalar()

    rejected = extracted - valid
    if rejected < 0:
        rejected = 0

    null_ids = conn.execute(text("SELECT COUNT(*) FROM warehouse.fact_transactions WHERE transaction_id IS NULL;")).scalar()
    invalid_amounts = conn.execute(text("SELECT COUNT(*) FROM warehouse.fact_transactions WHERE amount <= 0;")).scalar()
    
    null_fks = conn.execute(text("""
        SELECT COUNT(*) FROM warehouse.fact_transactions 
        WHERE customer_key IS NULL OR branch_key IS NULL OR date_key IS NULL;
    """)).scalar()

    close_connection(conn)

    # -------------------------------------------------------------------------
    # WEEK 5: TASK 6: Dynamic Visual Formatting Output Matrix
    # -------------------------------------------------------------------------
    print("\n" + "="*20 + " TASK 6: FINAL RECONCILIATION REPORT " + "="*20)
    print(f"{'Pipeline Process Stage':<25} | {'Active Record Balance Metrics':<25}")
    print("-" * 50)
    print(f"{'1. Extracted':<30} | {extracted:,}")
    print(f"{'2. Validated':<30} | {valid:,}")
    print(f"{'3. Rejected':<30} | {rejected:,}")
    print(f"{'4. Transformed':<30} | {valid:,}")
    print(f"{'5. Loaded (Warehouse)':<30} | {valid:,}")
    print("=" * 50)
    
    # Structural verification balancing checks
    if extracted == (valid + rejected):
        print("Audit Check Clear: Extracted = Validated + Rejected.")
    else:
        print("Balance Anomaly: Unaccounted record divergence detected across tracking stages.")

    print(f"\nIntegrity Audit -> NULL IDs: {null_ids} | Invalid Amounts: {invalid_amounts} | Orphan Foreign Keys: {null_fks}")
    print("="*50 + "\n")

    return {"extracted": extracted, "valid": valid, "rejected": rejected,"loaded":valid, "fraud": fraud}

def load_records(full_processed_data, total_raw_extracted ):
    print("\n","*="*50)
    print("--- Clean Data Loading Started ---")

     # 1. Pipeline Start log  (Task 12)
    run_id = log_pipeline_status(status="STARTED")

    try:
        # Create the copy of incoming DataFrame to ensure the original dataframe is not modified
        df = full_processed_data.copy()
        # CRITICAL FIX FOR TASK 2: Agar processing delta khali hai (0 rows), toh db procedures skip karo
        if df.empty:
            print("Incremental Sync: No new transaction records to load into Database. Skipping loading execution loops safely.")
            
            # Khali metrics payload banayein aur max_ts ki jagah current history save rehne dein
            metrics = {
                "extracted": total_raw_extracted,
                "valid": 0,
                "rejected": total_raw_extracted, # Baki data context rules skipped indicators
                "loaded": 0,
                "fraud": 0
            }
            
            # Base logic fallback check to retain old watermark
            # Control table pichle timestamp par freeze rahegi, last_successful_run update ho jayega
            log_pipeline_status(run_id=run_id, status="SUCCESS", metrics=metrics)
            print("--- Clean Data Loading Bypassed Safely (0 rows) ---")
            return


        total_raw_extracted = total_raw_extracted 

        # Save the clean data to the specified file path
        df.to_csv(CLEAN_CSV_PATH, index = False)

        print(f"Successfully Loaded {len(df)} Clean Records to csv")
        
        load_csv_to_staging()
        
        # Run Task 7: Dimension tables master items sync
        loading_dim_tables()
        
        # Run Task 8: Programmatic date calendar generator
        generate_dim_date()
        
        # Run Task 9: Final Star Schema Fact table compilation
        load_fact_table()

        metrics = run_data_validation(run_id,total_raw_extracted)

        # 
        if 'transaction_datetime' in df.columns and not df.empty:
            df['transaction_datetime'] = pd.to_datetime(df['transaction_datetime'])
            metrics['max_ts'] = str(df['transaction_datetime'].max())
            
        # 3. Pipeline Success log (Task 12)
        log_pipeline_status(run_id=run_id, status="SUCCESS", metrics=metrics)

    except Exception as pipeline_error:
        # 4. Global fallback logging trigger to intercept crashes safely
        log_pipeline_status(run_id=run_id, status="FAILED", error=pipeline_error)

        # ADD THIS LINE: To see what went wrong instantly on your powershell terminal
        print(f"\nCRITICAL PIPELINE CRASH EXCEPTION DETECTED: {pipeline_error}\n")
        raise pipeline_error

