import pandas as pd

def validate_records(extracted_df):
    df = extracted_df.copy()
    try:
        print("\n","*="*50)
        print("Validation Started")

        if df.empty:
            print("DataFrame is empty. Validation Completed.")
            return df, pd.DataFrame()

        # 1. Check Missing Account Number
        null_account_number_cond = (df['account_number'].isnull() | (df['account_number'].astype(str).str.strip() == ""))
        null_account_number_count = null_account_number_cond.sum()
    
        if null_account_number_count > 0:
            print("Total Missing Account Number Count ", null_account_number_count)
        else:
            print("Not any account number is missing")

        # 2. Check Missing Customer ID 
        null_cust_id_cond = (df['customer_id'].isnull() | (df['customer_id'].astype(str).str.strip() == ""))
        null_cust_id_count = null_cust_id_cond.sum()
        if null_cust_id_count > 0:
            print("Total Missing Customer ID count ", null_cust_id_count)
        else:
            print("Not Any Customer ID is missing")

        # 3. Check Negative Transaction Amount
        negative_amount_cond = (df['amount'] < 0)
        negative_amount_count = negative_amount_cond.sum()
        if negative_amount_count > 0:
            print("Total Negative Amount ", negative_amount_count)
        else:
            print("No Any Negative amount is found")

        # 4. Duplicate Transaction ID
        duplicate_txn_id_cond = df.duplicated(subset ='transaction_id')
        dup_txn_id_count = duplicate_txn_id_cond.sum()
        if dup_txn_id_count > 0:
            print("Total Duplicated transaction id : ", dup_txn_id_count)
        else:
            print("No duplicated transaction id found")
        
        # 5. Check Future Transaction Date
        today = pd.Timestamp.now()
        future_txn_date_cond = pd.to_datetime(df['transaction_datetime'], errors='coerce') > today
        if future_txn_date_cond.sum() > 0:
            print("Total Future Transaction Date ",future_txn_date_cond.sum())
        else:
            print("No Future transaction date")

        # 6. Invalid Transaction Type
        valid_txn_types = ['Withdrawal', 'Transfer', 'Deposit']
        invalid_txn_types_cond = ((df['transaction_type'].isnull()) | ~df['transaction_type'].astype(str).str.strip().str.title().isin(valid_txn_types))
        invalid_txn_types_counts = invalid_txn_types_cond.sum()
        if invalid_txn_types_counts > 0:
            print("Total Invalid transaction type : ", invalid_txn_types_cond.sum())
        else:
            print("Transaction types are valid")

        # 7. Invalid Channel
        valid_channel = ['Rtgs', 'Neft', 'Atm', 'Upi', 'Imps', 'Branch']
        invalid_channel_cond = ((df['channel'].isnull() ) | ~df['channel'].astype(str).str.strip().str.title().isin(valid_channel))
        invalid_channel_count = invalid_channel_cond.sum()
        if invalid_channel_count > 0:
            print("Total InValid Channel Type : ", invalid_channel_cond.sum())
        else:
            print("All channel types are valid")

        # 8. Blank Branch Code
        blank_br_code_cond = (df['branch_code'].isnull() | (df['branch_code'].astype(str).str.strip() == ""))
        blank_br_code_count = blank_br_code_cond.sum()
        if blank_br_code_count > 0:
            print("Total Blank Branch code : ", blank_br_code_count)
        else:
            print("No Missing or null branch code")

        # Filtering invalid records
        is_invalid_mask = null_account_number_cond | null_cust_id_cond | negative_amount_cond | duplicate_txn_id_cond | future_txn_date_cond | invalid_txn_types_cond | invalid_channel_cond | blank_br_code_cond

        # filter invalid records
        invalid_df = df[is_invalid_mask].copy()

        # Filter Valid records
        valid_df = df[~is_invalid_mask].copy()

        print("\nValidation Summary")
        print(f"Total Input Records : {len(df)}")
        print(f"Total Valid Records : {len(valid_df)}")
        print(f"Total Invalid Records : {len(invalid_df)}")
        print("Validation Completed")

        # return both dataframe 
        return valid_df, invalid_df
    
    except Exception as err:
        print("Error: ",err)
        
        return pd.DataFrame(), pd.DataFrame()

