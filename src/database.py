from sqlalchemy import create_engine, text
from config import DB_URL

# Create the main connection engine
engine = create_engine(DB_URL)

# =========================================================
# TASK 15: REUSABLE DATABASE MODULE FUNCTIONS
# =========================================================

#i) Define function to open a manual connection to the database
def create_connection():
    return engine.connect()

#ii) Define function to securely close an active connection
def close_connection(connection):
    if connection:
        connection.close()

#iii) Define function to run structural SQL commands
def execute_query(query_string, params=None):
    with engine.begin() as connection:
        connection.execute(text(query_string), params or {})

#iv) Define function to insert exactly one single row of data into a table
def insert_data(query_string, param_dict):
    with engine.begin() as connection:
        connection.execute(text(query_string),param_dict)

#v) Define function to load an entire pandas dataframe into a database table at once

def bulk_insert(table_name, schema_name, dataframe):
    if dataframe.empty:
        print("Dataframe is empty. Skipping bulk insertion step.")
        return
    print(f"Executing batch generation... Preparing {len(dataframe):,} rows.")

    dataframe.to_sql(
        name = table_name,
        con = engine,
        schema = schema_name,
        if_exists = 'append',
        index = False,
        chunksize = 10000,
        method = 'multi'
    )    
    print(f"Successfully loaded {len(dataframe):,} records into {schema_name}.{table_name} via Optimized multi-chunks.")
