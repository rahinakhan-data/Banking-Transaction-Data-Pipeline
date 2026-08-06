import pandas as pd
import os 

parent_dir = os.getcwd()
raw_data_dir = os.path.join(parent_dir, 'raw_data')

north_file_path = os.path.join(raw_data_dir, 'north_transactions.csv')
south_file_path = os.path.join(raw_data_dir, 'south_transactions.csv')
west_file_path = os.path.join(raw_data_dir, 'west_transactions.csv')

def extract_records(north_file_path, south_file_path, west_file_path):
    print("\n","*="*50)
    print("Extraction Started")

    if not os.path.exists(north_file_path):
        raise FileNotFoundError(f"Missing file path : {north_file_path}")

    if not os.path.exists(south_file_path):
        raise FileNotFoundError(f"Missing File Path : {south_file_path}")

    if not os.path.exists(west_file_path):
        raise FileNotFoundError(f"Missing File Path : {west_file_path}")

    try:
        north_df = pd.read_csv(north_file_path)
        south_df = pd.read_csv(south_file_path)
        west_df = pd.read_csv(west_file_path)
    except Exception as err:
        raise RuntimeError(f"Error while reading files {err}")

    print ("Total Files Loaded : 3")
    print("North Records : ", north_df.shape[0])
    print("South Records : ",south_df.shape[0])
    print("West Records : ",west_df.shape[0])
    print("Total Records Extracted : ", len(north_df) + len(south_df) + len(west_df))

    try:
        merged_df  = pd.concat([north_df, south_df, west_df], ignore_index=True)
        print(f"Total records extracted successfully: {merged_df.shape[0]}")
        print("--- Extraction completed ---")
        return merged_df
    
    except Exception as err:
        print(f"Error While merging Dataframes {err}")

