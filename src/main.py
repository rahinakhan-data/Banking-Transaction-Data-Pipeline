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
    start_time = time.time()

    try:
        # ------------------------------------------------------------
        # DATA EXTRACTION STAGE
        # ------------------------------------------------------------
        extracted_df = extract_records(NORTH_FILE_PATH, SOUTH_FILE_PATH, WEST_FILE_PATH)
        total_processed = len(extracted_df)
        
        # Step: Extraction Completed
        logger.info(f"Extraction Completed - Processed: {total_processed} rows")

        # -----------------------------------------------------------
        # DATA VALIDATION
        # ------------------------------------------------------------
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

        # --------------------------------------------------------------
        # DATA LOAD STAGE
        # --------------------------------------------------------------
        load_records(full_processed_df)
        # Step: Data Loaded Successfully
        logger.info("Data Loaded Successfully")
        
        print("\n","*="*50)

        end_time = time.time() - start_time
        
        # Step: ETL Finished
        logger.info(f"ETL Finished successfully in {end_time:.4f} seconds \n")

    except Exception as pipeline_error:
        # If any component crashes, log the critical trace back immediately
        logger.error(f"ETL Pipeline Failed Abruptly! Reason: {pipeline_error}", exc_info=True)
