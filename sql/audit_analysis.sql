-- ===================================================================
-- WEEK 5: Task 15 – Historical Audit Analysis Framework Scripts
-- ===================================================================

-- 1. How many times has the pipeline run, how many runs succeeded, and how many failed?
SELECT 
    COUNT(*) AS total_pipeline_runs,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS successful_runs,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_runs
FROM audit.etl_run_log;


-- 2. What was the average execution time across all recorded sessions?
SELECT 
    ROUND(AVG(EXTRACT(EPOCH FROM (end_time - start_time)))::numeric, 2) AS average_execution_time_seconds
FROM audit.etl_run_log
WHERE end_time IS NOT NULL;



-- 3. Which historical pipeline run processed the absolute most records?
SELECT 
    run_id, 
    pipeline_name, 
    records_extracted, 
    status 
FROM audit.etl_run_log 
ORDER BY records_extracted DESC 
LIMIT 1;


-- 4. Which run had the highest data rejection rate metric (Highest DQ Failure %)?
SELECT 
    run_id, 
    records_extracted, 
    records_rejected,
    ROUND(((records_rejected::numeric / NULLIF(records_extracted, 0)) * 100), 2) AS highest_rejection_percentage
FROM audit.etl_run_log 
WHERE records_extracted > 0
ORDER BY highest_rejection_percentage DESC 
LIMIT 1;


-- 5. How many total security fraud transactions were intercepted across pipeline lifetimes?
SELECT 
    SUM(fraud_records) AS aggregate_fraud_transactions_detected 
FROM audit.etl_run_log;

