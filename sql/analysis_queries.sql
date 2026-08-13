
-- 1. Total transaction amount by region.
SELECT 
b.region,
SUM(f.amount) as total_transaction_amount
FROM warehouse.dim_branch b JOIN warehouse.fact_transactions f
ON b.branch_key = f.branch_key
GROUP BY b.region
ORDER BY total_transaction_amount DESC;


-- 2. Total number of transactions by channel.
SELECT channel,
COUNT(*) as total_transactions
FROM warehouse.fact_transactions
GROUP BY channel
ORDER BY total_transactions DESC;






-- 3. Successful vs failed transactions.
SELECT status,
COUNT(*) AS total_transactions
FROM warehouse.fact_transactions
GROUP BY status
ORDER BY total_transactions DESC;

-- 4. Top 10 branches by transaction value.
SELECT 
b.branch_code,
SUM(f.amount) AS total_value
FROM warehouse.dim_branch b JOIN warehouse.fact_transactions f
ON b.branch_key = f.branch_key
GROUP BY b.branch_code
ORDER BY total_value DESC
LIMIT 10;

-- 5. Daily transaction volume.
SELECT dt.full_date,
COUNT(f.transaction_id) AS transaction_total_volume
FROM warehouse.dim_date as dt JOIN warehouse.fact_transactions as f
ON dt.date_key = f.date_key
GROUP BY dt.full_date
ORDER BY transaction_total_volume DESC;

-- 6. Monthly transaction amount.
SELECT dt.year,
dt.month_name,
SUM(f.amount) AS monthly_transaction_amount
FROM warehouse.dim_date dt JOIN warehouse.fact_transactions f
ON dt.date_key = f.date_key
GROUP BY dt.year, dt.month_name
ORDER BY monthly_transaction_amount DESC;

-- 7. Top 10 customers by transaction value.
SELECT c.customer_id,
SUM(f.amount) as transaction_value
FROM warehouse.dim_customer c JOIN warehouse.fact_transactions f
ON c.customer_key = f.customer_key
GROUP BY c.customer_id
ORDER BY transaction_value DESC
LIMIT 10;

-- 8. Number of fraud-flagged transactions by region.
SELECT 
b.region,
COUNT(*) as total_fraud_flagged
FROM warehouse.dim_branch b JOIN warehouse.fact_transactions f
ON b.branch_key = f.branch_key
WHERE f.status = 'YES'
GROUP BY b.region
ORDER BY total_fraud_flagged DESC;

-- 9. Average transaction amount by transaction type.
SELECT transaction_type,
ROUND(AVG(amount), 2) AS average_transaction_amount
FROM warehouse.fact_transactions
GROUP BY transaction_type;

-- 10. Highest-value transactions.
SELECT 
f.transaction_id,
f.amount,
b.region
FROM warehouse.dim_branch b JOIN warehouse.fact_transactions f
ON b.branch_key = f.branch_key
ORDER BY f.amount DESC
LIMIT 10;


