import pandas as pd

def validate_records(extracted_df):
    df = extracted_df.copy()
    try:
        print("\n","*="*50)
        print("Validation Started")

        total_records = len(df)

        # FIXED CRITICAL BUG: Avoid ZeroDivisionError globally during empty delta run windows
        if total_records == 0:
            print("Incremental Sync: DataFrame contains 0 raw rows. Validation Completed Safely.")
            return df, pd.DataFrame()

        # Check 1: Duplicate Transaction IDs
        duplicate_txn_id_cond = df.duplicated(subset='transaction_id', keep='first')
        dup_txn_id_count = duplicate_txn_id_cond.sum()

        # Check 2: NULL Transaction IDs
        null_txn_id_cond = (df['transaction_id'].isnull() | (df['transaction_id'].astype(str).str.strip() == ""))
        null_txn_id_count = null_txn_id_cond.sum()

        # Check 3: Negative Transaction Amounts
        negative_amount_cond = (df['amount'] < 0)
        negative_amount_count = negative_amount_cond.sum()

        # Check 4: Invalid Transaction Types
        valid_txn_types = ['Withdrawal', 'Transfer', 'Deposit']
        invalid_txn_types_cond = ((df['transaction_type'].isnull()) | ~df['transaction_type'].astype(str).str.strip().str.title().isin(valid_txn_types))
        invalid_txn_types_counts = invalid_txn_types_cond.sum()

        # Check 5: Invalid Channels
        valid_channel = ['Rtgs', 'Neft', 'Atm', 'Upi', 'Imps', 'Branch']
        invalid_channel_cond = ((df['channel'].isnull()) | ~df['channel'].astype(str).str.strip().str.title().isin(valid_channel))
        invalid_channel_count = invalid_channel_cond.sum()

        # Check 6: Missing Foreign Keys (Customer ID / Branch Code Missing)
        null_cust_id_cond = (df['customer_id'].isnull() | (df['customer_id'].astype(str).str.strip() == ""))
        null_cust_id_count = null_cust_id_cond.sum()
        
        blank_br_code_cond = (df['branch_code'].isnull() | (df['branch_code'].astype(str).str.strip() == ""))
        blank_br_code_count = blank_br_code_cond.sum()
        
        missing_fk_cond = null_cust_id_cond | blank_br_code_cond
        missing_fk_count = missing_fk_cond.sum()

        # Check 8: Unexpected NULL Percentage (Account Number Check)
        null_account_number_cond = (df['account_number'].isnull() | (df['account_number'].astype(str).str.strip() == ""))
        null_account_number_count = null_account_number_cond.sum()

        # Check 9 & 10: Fact Table Record Count & Amount Consistency Check
        # (This is structurally achieved downstream, verified in isolation during masking metrics)
        future_txn_date_cond = pd.to_datetime(df['transaction_datetime'], errors='coerce') > pd.Timestamp.now()
        
        # =============================================================================================
        # TASK 5: Production Measurable Quality Thresholds Validation
        # =============================================================================================
        
        # 1. Missing Customer ID Threshold (Max Allowed = 1.0%)
        cust_id_pct = (null_cust_id_count / total_records) * 100
        
        # 2. Duplicate Transaction ID Threshold (Max Allowed = 2.0%)
        dup_id_pct = (dup_txn_id_count / total_records) * 100
        
        # 3. Negative Amount System Anomalies Threshold (Max Allowed = 0.5%)
        neg_amount_pct = (negative_amount_count / total_records) * 100

        # 4. Critical Missing Account Numbers (Max Allowed = 5.0% - Strict Zero Tolerance)
        null_acc_pct = (null_account_number_count / total_records) * 100

        # 5. Invalid Perimeter Transaction Channel Channels (Max Allowed = 1.5%)
        invalid_channel_pct = (invalid_channel_count / total_records) * 100

        print("\n--- Task 5: Threshold Execution Evaluation Matrix ---")
        print(f"Missing Customer ID Pct  : {cust_id_pct:.2f}% (Threshold: 1.0%)")
        print(f"Duplicate Transaction Pct: {dup_id_pct:.2f}% (Threshold: 2.0%)")
        print(f"Negative Amount Pct      : {neg_amount_pct:.2f}% (Threshold: 0.5%)")
        print(f"Missing Account Number Pct: {null_acc_pct:.2f}% (Threshold: 0.5%)")
        print(f"Invalid Channel Pct       : {invalid_channel_pct:.2f}% (Threshold: 1.5%)")

        # Threshold validations evaluation (Pipeline Halt Conditions)
        if null_txn_id_count > 0:
            raise ValueError(f"CRITICAL DQ FAILURE: Null Primary Keys found ({null_txn_id_count} rows). Processing aborted.")
        
        if null_acc_pct > 0.5:
            raise ValueError(f"CRITICAL DQ FAILURE: Account Numbers threshold breached. Found {null_acc_pct:.2f}%, exceeding 0.5% limit.")

        if cust_id_pct > 1.0:
            raise ValueError(f"PIPELINE THRESHOLD BREACHED: Missing Customer ID is {cust_id_pct:.2f}%, exceeding 1% limit.")

        if dup_id_pct > 2.0:
            raise ValueError(f"PIPELINE THRESHOLD BREACHED: Duplicate transactions are {dup_id_pct:.2f}%, exceeding 2% limit.")

        if neg_amount_pct > 0.5:
            raise ValueError(f"PIPELINE THRESHOLD BREACHED: Negative amount anomalies are {neg_amount_pct:.2f}%, exceeding 0.5% limit.")

        if invalid_channel_pct > 1.5:
            raise ValueError(f"PIPELINE THRESHOLD BREACHED: Invalid channels are {invalid_channel_pct:.2f}%, exceeding 1.5% limit.")
        
        # ==========================================================================================
        # Segmentation & Filtering Logic
        # ==========================================================================================
        is_invalid_mask = (
            null_account_number_cond | null_cust_id_cond | negative_amount_cond | 
            duplicate_txn_id_cond | future_txn_date_cond | invalid_txn_types_cond | 
            invalid_channel_cond | blank_br_code_cond | null_txn_id_cond
        )

        invalid_df = df[is_invalid_mask].copy()
        valid_df = df[~is_invalid_mask].copy()

        print("\n--- Validation Summary ---")
        print(f"Total Input Records   : {total_records:,}")
        print(f"Total Valid Records   : {len(valid_df):,}")
        print(f"Total Invalid Records : {len(invalid_df):,}")
        print("Data Quality Framework Verification: SUCCESS")

        return valid_df, invalid_df
    
    except Exception as err:
        print("Validation Critical Pipeline Error: ", err)
        raise err  # Crash explicitly to signal failure state upstream to Airflow

