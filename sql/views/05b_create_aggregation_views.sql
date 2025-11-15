-- ============================================================================
-- Varo Feature Store - Intermediate Aggregation Views
-- ============================================================================
-- Purpose: Create views to pre-aggregate data for Feature Store.
-- This is required because Dynamic Tables have limitations with GROUP BY.
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA FEATURE_STORE;
USE WAREHOUSE VARO_FEATURE_WH;

-- Customer Profile Aggregations
CREATE OR REPLACE VIEW V_CUSTOMER_PROFILE_AGGS AS
SELECT
    c.customer_id,
    COUNT(DISTINCT a.account_id) as num_accounts
FROM VARO_INTELLIGENCE.RAW.CUSTOMERS c
LEFT JOIN VARO_INTELLIGENCE.RAW.ACCOUNTS a ON c.customer_id = a.customer_id
GROUP BY c.customer_id;

-- Transaction Pattern Aggregations
CREATE OR REPLACE VIEW V_TRANSACTION_PATTERN_AGGS AS
SELECT
    customer_id,
    COUNT(CASE WHEN transaction_date >= DATEADD('day', -7, CURRENT_DATE()) THEN 1 END) as txn_count_7d,
    SUM(CASE WHEN transaction_date >= DATEADD('day', -7, CURRENT_DATE()) THEN ABS(amount) END) as txn_volume_7d,
    COUNT(CASE WHEN transaction_timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP()) THEN 1 END) as txn_count_1h
FROM VARO_INTELLIGENCE.RAW.TRANSACTIONS
WHERE status = 'COMPLETED'
GROUP BY customer_id;

-- Advance Risk Aggregations
CREATE OR REPLACE VIEW V_ADVANCE_RISK_AGGS AS
SELECT
    c.customer_id,
    COUNT(ca.advance_id) as total_advances_taken,
    AVG(ca.advance_amount) as avg_advance_amount,
    COUNT(CASE WHEN ca.advance_status = 'DEFAULTED' THEN 1 END) as num_defaults
FROM VARO_INTELLIGENCE.RAW.CUSTOMERS c
LEFT JOIN VARO_INTELLIGENCE.RAW.CASH_ADVANCES ca ON c.customer_id = ca.customer_id
GROUP BY c.customer_id;

-- Fraud Detection Aggregations
CREATE OR REPLACE VIEW V_FRAUD_DETECTION_AGGS AS
WITH recent_transactions AS (
    SELECT 
        t.*,
        LAG(transaction_timestamp) OVER (PARTITION BY customer_id ORDER BY transaction_timestamp) as prev_txn_time,
        LAG(merchant_city) OVER (PARTITION BY customer_id ORDER BY transaction_timestamp) as prev_merchant_city
    FROM VARO_INTELLIGENCE.RAW.TRANSACTIONS t
    WHERE transaction_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP()) AND status = 'COMPLETED'
)
SELECT
    t.customer_id,
    MAX(CASE WHEN ABS(t.amount) > avg_txn.avg_amount * 3 AND ABS(t.amount) > 100 THEN 1 ELSE 0 END) as has_unusual_amount,
    MAX(CASE WHEN DATEDIFF('minute', t.prev_txn_time, t.transaction_timestamp) < 5 AND t.merchant_city != t.prev_merchant_city THEN 1 ELSE 0 END) as impossible_travel_flag
FROM recent_transactions t
LEFT JOIN (
    SELECT customer_id, AVG(ABS(amount)) as avg_amount
    FROM VARO_INTELLIGENCE.RAW.TRANSACTIONS
    WHERE transaction_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY customer_id
) avg_txn ON t.customer_id = avg_txn.customer_id
GROUP BY t.customer_id;
