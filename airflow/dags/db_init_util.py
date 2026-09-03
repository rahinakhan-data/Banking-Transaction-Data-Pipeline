# airflow/dags/db_init_util.py
from sqlalchemy import text
from src.database import engine

def initialize_database_structures():

    print("--- Live Database Infrastructure Initialization Started ---")
    try:
        # Establish a synchronous connection with the SQLAlchemy Engine
        with engine.connect() as conn:

            # 1. CREATE all required Schemas (Staging, Warehouse, Audit)
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse;"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS audit;"))
            
            # 2. Create 'staging.transactions_staging'  table
            conn.execute(text("""
                
                CREATE TABLE IF NOT EXISTS staging.transactions_staging(
                    transaction_id VARCHAR(50),
                    account_number VARCHAR(50),
                    customer_id VARCHAR(50),
                    transaction_datetime TIMESTAMP,
                    transaction_type VARCHAR(50),
                    amount NUMERIC(18,2),
                    branch_code VARCHAR(20),
                    region VARCHAR(20),
                    channel VARCHAR(20),
                    status VARCHAR(20),
                    fraud_flag VARCHAR(5)
                );
            """))
            print("Successfully initialized precise staging table definitions.")

            # 3. 'warehouse.dim_customer'
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
                    customer_key SERIAL PRIMARY KEY,
                    customer_id VARCHAR(100) UNIQUE,
                    account_number VARCHAR(100)
                );
            """))

            # 4. 'warehouse.dim_branch' 
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_branch (
                    branch_key SERIAL PRIMARY KEY,
                    branch_code VARCHAR(50) UNIQUE,
                    region VARCHAR(50)
                );
            """))
            
            # 5. 'warehouse.dim_date'
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.dim_date (
                    date_key INT PRIMARY KEY,
                    full_date DATE,
                    year INT,
                    quarter INT,
                    month INT,
                    month_name VARCHAR(50),
                    day INT,
                    day_of_week VARCHAR(50)
                );
            """))
            
            # 6. 'warehouse.fact_transactions'
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warehouse.fact_transactions(
                    transaction_key SERIAL PRIMARY KEY,
                    transaction_id VARCHAR(50)UNIQUE,
                    customer_key INT REFERENCES warehouse.dim_customer(customer_key) ,
                    branch_key INT REFERENCES warehouse.dim_branch(branch_key),
                    date_key INT REFERENCES warehouse.dim_date(date_key),
                    transaction_type VARCHAR(50),
                    channel VARCHAR(50),
                    amount NUMERIC(18,2),
                    status VARCHAR(50),
                    fraud_flag VARCHAR(5)
                );
            """))
            
            # 7. 'audit.etl_run_log' 
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS audit.etl_run_log(
                    run_id SERIAL PRIMARY KEY,
                    pipeline_name VARCHAR(100),
                    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    records_extracted INT DEFAULT 0,
                    records_valid INT DEFAULT 0,
                    records_rejected INT DEFAULT 0,
                    records_loaded INT DEFAULT 0,
                    fraud_records INT DEFAULT 0,
                    status VARCHAR(50),
                    error_message TEXT
                );
            """))
    
            if hasattr(conn, 'commit'):
                conn.commit()
    except Exception as err:
        print(f"Database structure setup pass-through warning: {err}")
