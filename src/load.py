import os
import pandas as pd 

# Current Working Directory
parent_dir = os.getcwd()
processed_dir = os.path.join(parent_dir, 'processed')

# Safely creates the 'processed' folder if it not exist (exist_ok=True prevents crashes if it already exists)
os.makedirs(processed_dir, exist_ok= True)

# Defines the full file path where the final clean CSV will be saved
file_load_path = os.path.join(processed_dir, 'clean_transactions.csv')

def load_records(clean_data):
    print("\n","*="*50)
    print("--- Clean Data Loading Started ---")

    # Create the copy of incoming DataFrame to ensure the original dataframe is not modified
    df = clean_data.copy()

    # Save the clean data to the specified file path
    df.to_csv(file_load_path, index = False)

    print(f"Successfully Loaded {len(df)} Clean Records to csv")
    print("--- Data Loading Ended ---")

