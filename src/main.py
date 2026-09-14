import time
import sys
import os

# Python runtime search path resolution management layer mapping
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.logger import logger  # Central logging trace singleton handle
from src.config import NORTH_FILE_PATH, SOUTH_FILE_PATH, WEST_FILE_PATH
from src.extract import extract_records
from src.validate import validate_records
from src.quarantine import quarantine_records
from src.transform import transform_records
from src.fraud import fraud_transactions
from src.load import load_records

if __name__ == "__main__":
    
    # Step: ETL Started
    logger.info("ETL Started")
    pipeline_start_time = time.time()


    try:
        # ------------------------------------------------------------
        # DATA EXTRACTION STAGE
        # ------------------------------------------------------------
        t_start = time.time() 
        extracted_df = extract_records(NORTH_FILE_PATH, SOUTH_FILE_PATH, WEST_FILE_PATH)
        total_raw_extracted  = len(extracted_df)
        extract_duration = time.time() - t_start

        # Step: Extraction Completed
        logger.info(f"Extraction Completed - Processed: {total_raw_extracted } rows")

        # -----------------------------------------------------------
        # DATA VALIDATION
        # ------------------------------------------------------------
        t_start = time.time()
        valid_data, invalid_data = validate_records(extracted_df)
        total_valid = len(valid_data)
        total_invalid = len(invalid_data)

        # Step: Validation Completed
        logger.info(f"Validation Completed - Valid: {total_valid}, Invalid: {total_invalid}")

        # ---------------------------------------------------------------
        # QUARANTINE STAGE
        # ---------------------------------------------------------------
        quarantine_data = quarantine_records(invalid_data)
        # step: Quarantined completed
        logger.info(f"Quarantine Completed - {len(quarantine_data)}")

        # --------------------------------------------------------------
        # Data TRANSFORM STAGE
        # ---------------------------------------------------------------
        transform_data = transform_records(valid_data)
        
        # Step: Transformation Completed
        logger.info("Transformation Completed")

        # -------------------------------------------------------------
        # FRAUD TRANSACTION DETECTION STAGE
        # -------------------------------------------------------------
        fraud_df, clean_df, full_processed_df = fraud_transactions(transform_data)
        fraud_records_count = len(fraud_df)
        
        # Step: Fraud Detection Completed
        logger.info(f"Fraud Detection Completed - Flagged Anomalies: {fraud_records_count}")

        transform_duration = time.time() - t_start

        # --------------------------------------------------------------
        # DATA LOAD STAGE
        # --------------------------------------------------------------
        t_start = time.time()
        load_records(full_processed_df, total_raw_extracted )
        load_duration = time.time() - t_start

        # Step: Data Loaded Successfully
        logger.info("Data Loaded Successfully")
        
        print("\n","*="*50)

        total_pipeline_duration = time.time() - pipeline_start_time

        # -------------------------------------------------------------------------
        # WEEK 5: TASK 7: Performance Measurement Console Dashboard Output
        # -------------------------------------------------------------------------

        print("\n" + "="*20 + " TASK 7: PERFORMANCE METRICS REPORT " + "="*20)
        print(f"{'Pipeline Process Phase':<25} | {'Execution Time Metric':<25}")
        print("-" * 55)
        print(f"{'1. Extraction Time':<25} | {extract_duration:.2f} Seconds")
        print(f"{'2. Transformation Time':<25} | {transform_duration:.2f} Seconds")
        print(f"{'3. Database Loading Time':<25} | {load_duration:.2f} Seconds")
        print("-" * 55)
        print(f"{'Total Pipeline Runtime':<25} | {total_pipeline_duration:.2f} Seconds")
        print("=" * 55 + "\n")
        
        # Step: ETL Finished
        logger.info(f"ETL Finished successfully in {total_pipeline_duration:.4f} seconds \n")

    except Exception as pipeline_error:
        # If any component crashes, log the critical trace back immediately
        logger.error(f"ETL Pipeline Failed Abruptly! Reason: {pipeline_error}", exc_info=True)
