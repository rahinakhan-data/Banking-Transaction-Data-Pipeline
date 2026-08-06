import os 
import pandas as pd
import numpy as np

# Current working folder 
parent_dir = os.getcwd()
quarantine_dir = os.path.join(parent_dir, 'quarantine')
os.makedirs(quarantine_dir, exist_ok=True)

quarantine_file_path = os.path.join(quarantine_dir, 'quarantine_records.csv')

def quarantine_records(invalid_data):

    df = invalid_data.copy()
    try:
        print("\n","*="*50)
        print("--- Quarantine Processing Started ---")
        print("Total Invalid Records",len(df))
        # Pre-requisite conversion for safe processing
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df['transaction_datetime'] = pd.to_datetime(df['transaction_datetime'], errors='coerce')

        # 1. Individual Boolean Conditions
        null_acc_num_cond = (df['account_number'].isnull() | (df['account_number'].astype(str).str.strip() == ""))

        null_cust_id_cond = (df['customer_id'].isnull() | (df['account_number'].astype(str).str.strip() == ""))

        negative_amount_cond = (df['amount'] < 0)

        duplicate_txn_id_cond = df.duplicated(subset = ['transaction_id'], keep= 'first')

        future_txn_date_cond = (df['transaction_datetime'].isnull() | (df['transaction_datetime'] > pd.Timestamp.now() ))

        valid_txn_types = ['withdrawal', 'transfer', 'deposit']
        invalid_txn_types_cond = (df['transaction_type'].isnull() | ~(df['transaction_type'].astype(str).str.strip().str.lower().isin(valid_txn_types)))

        valid_channels = ['rtgs', 'neft', 'atm', 'upi', 'imps', 'branch']
        invalid_channel_cond = (df['channel'].isnull() | ~(df['channel'].astype(str).str.strip().str.lower().isin(valid_channels)))

        blank_branch_code_cond = (df['branch_code'].isnull() | (df['channel'].astype(str).str.strip() == ""))

        # 2. All conditions for invalid checks
        is_invalid = (
                        null_acc_num_cond | null_cust_id_cond | negative_amount_cond |
                        duplicate_txn_id_cond | future_txn_date_cond | invalid_txn_types_cond |
                        invalid_channel_cond | blank_branch_code_cond
                    )

        invalid_df = df[is_invalid].copy()

        # 3. Generate the reason for invalid records
        if not invalid_df.empty:

            print("Generating reasons for quarantine records...")

            conditions_df = pd.DataFrame(
                            {
                                "Missing Account Number" : null_acc_num_cond, 
                                "Missing Customer ID" : null_cust_id_cond, 
                                "Negative Amount" : negative_amount_cond,
                                "Duplicate Transaction" : duplicate_txn_id_cond, 
                                "Future Date" : future_txn_date_cond, 
                                "Invalid Transaction Type" : invalid_txn_types_cond,
                                "Invalid Channel" : invalid_channel_cond,
                                "Blank Branch Code" : blank_branch_code_cond
                            }
                        )

            reasons_series = conditions_df.dot(conditions_df.columns + ", ")
            invalid_df['quarantine_reason'] = (reasons_series.str.rstrip().replace('', 'Validation Reason'))

            # 4. Save to quarantine/ folder 
            invalid_df.to_csv(quarantine_file_path, index=False)

            print(f"Quarantine Dataset Saved Successfully at: {quarantine_file_path}")

            print(f"Total Quarantine Records Saved: {len(invalid_df)}")
            print("--- Quarantine Processing Ended ---")
            
            return invalid_df
        else:
            print("No Invalid records are found")
            return pd.DataFrame()

    except Exception as err:
        raise RuntimeError(f"ERROR during quarantine processing: {err}")
