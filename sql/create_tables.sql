-- create table transactions_staging inside staging schema 
CREATE TABLE staging.transactions_staging(
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
SELECT * FROM staging.transactions_staging
SELECT COUNT(*) FROM staging.transactions_staging

-- CREATE DIMENSION TABLES  
-- i) Customer Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
	customer_key SERIAL PRIMARY KEY,
	customer_id VARCHAR(50) UNIQUE,
	account_number VARCHAR(50)
);
SELECT * FROM warehouse.dim_customer

-- ii) Branch Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_branch(
	branch_key SERIAL PRIMARY KEY,
	branch_code VARCHAR(20) UNIQUE,
	region VARCHAR(50)
);
SELECT * FROM warehouse.dim_branch

-- iii) Date Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_date(
	date_key INT PRIMARY KEY, 
	full_date DATE UNIQUE ,
	year INT, 
	quarter INT,
	month INT,
	month_name VARCHAR(20),
	day INT,
	day_of_week VARCHAR(20)
);
select * from warehouse.dim_date;

-- Create Fact Table
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
SELECT * FROM warehouse.fact_transactions

-- Create Audit Table
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

SELECT * FROM audit.etl_run_log

-- ===================================================================
-- Week 5 Task 3 – Create ETL Control Table
-- ===================================================================
CREATE TABLE IF NOT EXISTS audit.pipeline_control (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_successful_run TIMESTAMP,
    last_processed_timestamp TIMESTAMP,
    last_run_status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO audit.pipeline_control (pipeline_name, last_run_status, last_successful_run)
VALUES ('banking_dw_load_pipeline', 'INIT', '1970-01-01 00:00:00')
ON CONFLICT (pipeline_name) DO NOTHING;

SELECT * FROM audit.pipeline_control;

-- ===================================================================
-- Week 5 Task 9 – Database Indexing Implementation Script
-- ===================================================================

-- 1. Index for Quick Search & Duplicate Detection (transaction_id)
CREATE INDEX IF NOT EXISTS idx_fact_transaction_id 
ON warehouse.fact_transactions (transaction_id);

-- 2. Indexes for High-Velocity Relational Dimension Tables Joins
CREATE INDEX IF NOT EXISTS idx_fact_customer_key 
ON warehouse.fact_transactions (customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_branch_key 
ON warehouse.fact_transactions (branch_key);

-- 3. Index for Analytical Warehouse Time-Series & Operational Reporting
CREATE INDEX IF NOT EXISTS idx_fact_date_key 
ON warehouse.fact_transactions (date_key);

-- 4. Staging Area Optimization Index

CREATE INDEX IF NOT EXISTS idx_stg_txn_datetime 
ON staging.transactions_staging (transaction_datetime);

-- ===================================================================
-- Verification Dashboard (Run this to monitor execution status plans)
-- ===================================================================
-- EXPLAIN ANALYZE SELECT * FROM warehouse.fact_transactions WHERE transaction_id = 'WTX1062467';
