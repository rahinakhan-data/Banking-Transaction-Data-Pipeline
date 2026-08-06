import os
import pandas as pd

def transform_records(valid_data):
    df = valid_data.copy()

    try:
        print("\n","*="*50)
        print("--- Transformation of Data Started ---")

        print("Initial Records : ", len(df))
        # 1. Convert transaction date to proper datetime format
        print("Convert transaction date to proper datetime format")
        df['transaction_datetime'] = pd.to_datetime(df['transaction_datetime'],errors='coerce')

        # 2. Convert account number to string format
        print("Convert acoount number to string format")
        df['account_number'] = df['account_number'].astype(str)        

        # 3. Remove duplicate records
        initial_records_count = len(df)
        print("Removing duplicate records")
        df = df.drop_duplicates()
        final_records_count = len(df)
        print(f"Final records after removing {df.duplicated().sum()} duplicated rows ", final_records_count)

        # 4. Trim leading/trailing spaces
        cat_col = df.select_dtypes('object')

        for col in cat_col:
            df[col] = df[col].str.strip()

        # 5. Convert transaction type to Proper Case (e.g., 'withdrawal' -> 'Withdrawal')
        if 'transaction_type' in df.columns:
            df['transaction_type'] = df['transaction_type'].str.title()

        # 6. Convert channel names to uppercase
        if 'channel' in df.columns:
            df['channel'] = df['channel'].str.upper()

        # 7. Sort records by transaction date
        print("Sorting records by transaction date...")
        df = df.sort_values(by ='transaction_datetime', ascending = True, ignore_index = True)

        print("Final records after transformation: ", len(df))
        print("--- Transformation of Data Completed ---")

        return df

    except Exception as err:
        print("Error: ",err)