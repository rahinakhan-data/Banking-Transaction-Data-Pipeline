

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
	fraud_records INT DEFAULT 0,
	status VARCHAR(50),
	error_message TEXT
);

SELECT * FROM audit.etl_run_log








