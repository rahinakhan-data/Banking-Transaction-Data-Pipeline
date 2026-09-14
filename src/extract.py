import os
import pandas as pd
from datetime import datetime
from sqlalchemy import text  
from database import engine

def extract_records(north_file_path, south_file_path, west_file_path):
    print("\n","*="*50)
    print("Extraction Started")

    files_valid = True
    for p in [north_file_path, south_file_path, west_file_path]:
        if not os.path.exists(p) or os.path.getsize(p) == 0:
            print(f"Warning: File {p} is missing or empty (0 bytes).")
            files_valid = False

    if not files_valid:
        print("Log: One or more source files are missing/empty. Initializing empty DataFrame for downstream tasks.")
        print("Status: 0 records are extracted from source files.")
        print("--- Extraction completed gracefully ---")
        return pd.DataFrame()  

    try:
        north_df = pd.read_csv(north_file_path)
        south_df = pd.read_csv(south_file_path)
        west_df = pd.read_csv(west_file_path)
    except Exception as err:
        
        print(f"Warning: Error while reading files ({err}). Fallback to empty processing workflow.")
        print("Status: 0 records are extracted from source files.")
        return pd.DataFrame()

    
    print ("Total Files Loaded : 3")
    print("North Records : ", north_df.shape[0])
    print("South Records : ",south_df.shape[0])
    print("West Records : ",west_df.shape[0])
    total_raw_rows = len(north_df) + len(south_df) + len(west_df)
    print("Total Raw Records Available across files: ", total_raw_rows)

    try:
        merged_df  = pd.concat([north_df, south_df, west_df], ignore_index=True)
    except Exception as err:
        raise err

    # =========================================================================
    # WEEK 5: TASK 2: INCREMENTAL FILTER LOGIC (CHECKPOINT LAYER)
    # =========================================================================
    pipeline_name = 'banking_dw_load_pipeline'
    last_processed_ts = None

    try:
        
        with engine.connect() as conn:
            ts_query = text("""
                SELECT last_processed_timestamp 
                FROM audit.pipeline_control 
                WHERE pipeline_name = :pipeline_name
            """)
            result = conn.execute(ts_query, {"pipeline_name": pipeline_name}).fetchone()
            if result and result[0]:
                last_processed_ts = result[0]
                print(f"Database Audit Checkpoint Found: {last_processed_ts}")
    except Exception as db_err:
        print(f"Warning: Control timestamp nahi mila (Running FULL extract instead). Error: {db_err}")

    if last_processed_ts is not None and not merged_df.empty:
        merged_df['transaction_datetime'] = pd.to_datetime(merged_df['transaction_datetime'], errors='coerce')
        last_processed_ts = pd.to_datetime(last_processed_ts)
        
        merged_df = merged_df[merged_df['transaction_datetime'] > last_processed_ts]
        print(f"Incremental Filter Applied: Kept {merged_df.shape[0]} new records out of {total_raw_rows}")
    else:
        print("No active checkpoint timestamp found. Processing full source dataset loop.")

    print(f"Total incremental records passing to downstream tasks: {merged_df.shape[0]}")
    print("--- Extraction completed ---")
    return merged_df

