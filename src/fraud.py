import os 
import pandas as pd
import numpy as np
from config import FRAUD_CSV_PATH

def fraud_transactions(transformed_data):

    # Create a shallow copy of the input DataFrame to protect the original data
    df = transformed_data.copy()

    # Check if the input DataFrame is null
    if df is None or df.empty:
        print("No records exist for fraud detection")

        # return an empty DataFrame to prevent breaking the downstream workflow
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    try:
        print("\n","*="*50)
        print("--- Fraud Detection Started ---")

        # extract only date from transaction_datetime column
        df['txn_date'] = df['transaction_datetime'].dt.date

        # Define a boolean mask selecting rows where the txn amount > 10000
        high_amount_cond = (df['amount']>10000)

        # Group by account and date, then broadcast the total daily transaction count back to every row
        freq_count = df.groupby(['account_number', 'txn_date'])['account_number'].transform('count')
        high_freq_cond = freq_count > 5

        # Initialize a new column named fraud_flag with a default value of No for all rows
        df['fraud_flag'] = 'NO'

        # Update the fraud_flag to YES for any row meeting either the high amount or frequency threshold
        df.loc[(high_amount_cond) | (high_freq_cond), 'fraud_flag' ] = 'YES'

        # Separate the fraud records and clean records to save 

        fraud_txn_df = df[df['fraud_flag'] == 'YES'].copy()
        clean_df = df[df['fraud_flag'] == 'NO'].copy()
        
        fraud_txn_df.to_csv(FRAUD_CSV_PATH, index = False)

        # to print summary
        print(f"Total Scanned Rows : {len(df)}")
        print(f"Total Fraud Transactions Found  : {len(fraud_txn_df)}")
        print(f"Total Clean Records Found : {len(clean_df)}")
        print(f"Fraud Transactions Saved At      : {FRAUD_CSV_PATH}")
        print("--- Fraud Detection Ended ---")        

        return fraud_txn_df, clean_df, df

    except Exception as err:
        raise RuntimeError(f"CRITICAL ERROR during fraud detection logic: {err}")
