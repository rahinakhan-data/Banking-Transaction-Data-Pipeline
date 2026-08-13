CREATE SCHEMA IF NOT EXISTS staging;

CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE SCHEMA IF NOT EXISTS audit;












SELECT 'staging.transactions_staging' AS tbl, COUNT(*) FROM staging.transactions_staging
UNION ALL
SELECT 'warehouse.fact_transactions', COUNT(*) FROM warehouse.fact_transactions
UNION ALL
SELECT 'warehouse.dim_customer', COUNT(*) FROM warehouse.dim_customer
UNION ALL
SELECT 'warehouse.dim_branch', COUNT(*) FROM warehouse.dim_branch
UNION ALL
SELECT 'warehouse.dim_date', COUNT(*) FROM warehouse.dim_date;






