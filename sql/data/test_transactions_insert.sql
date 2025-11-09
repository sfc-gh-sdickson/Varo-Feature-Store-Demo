-- Test script to verify TRANSACTIONS insert works
-- Run this AFTER tables are created and ACCOUNTS/CUSTOMERS are populated

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA RAW;

-- First create the temp merchants table
CREATE OR REPLACE TEMPORARY TABLE temp_merchants AS
SELECT 
    merchant_name,
    mcc_code,
    ARRAY_CONSTRUCT('New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix')[UNIFORM(0, 4, RANDOM())] AS merchant_city,
    ARRAY_CONSTRUCT('NY', 'CA', 'IL', 'TX', 'AZ')[UNIFORM(0, 4, RANDOM())] AS merchant_state
FROM (
    VALUES
    ('Walmart', '5411'), ('Target', '5411'), ('Kroger', '5411')
) AS merchants(merchant_name, mcc_code);

-- Test with just 10 transactions to verify it works
INSERT INTO TRANSACTIONS
SELECT
    'TXN' || LPAD(SEQ4(), 12, '0') AS transaction_id,
    base_txn.account_id,
    base_txn.customer_id,
    base_txn.transaction_type,
    CASE 
        WHEN base_txn.transaction_type = 'DEBIT' THEN 
            CASE 
                WHEN m.mcc_code = '6011' THEN 'ATM'
                WHEN m.mcc_code IN ('6010') THEN 'P2P'
                ELSE 'POS'
            END
        WHEN base_txn.transaction_type = 'CREDIT' THEN 'ACH'
        WHEN base_txn.transaction_type = 'TRANSFER' THEN 'TRANSFER'
        ELSE 'OTHER'
    END AS transaction_category,
    
    base_txn.transaction_date,
    DATEADD('second', UNIFORM(0, 86399, RANDOM()), base_txn.transaction_date::TIMESTAMP_NTZ) AS transaction_timestamp,
    
    CASE 
        WHEN base_txn.transaction_type = 'DEBIT' THEN -ABS(base_txn.amount)
        ELSE ABS(base_txn.amount)
    END AS amount,
    
    -- Running balance (simplified)
    base_txn.current_balance AS running_balance,
    
    m.merchant_name,
    m.mcc_code AS merchant_category,
    m.merchant_city,
    m.merchant_state,
    
    CASE base_txn.transaction_type
        WHEN 'DEBIT' THEN m.merchant_name || ' Purchase'
        WHEN 'CREDIT' THEN 'Direct Deposit - ' || COALESCE(base_txn.employer_name, 'Transfer')
        WHEN 'TRANSFER' THEN 'Transfer ' || CASE WHEN base_txn.amount > 0 THEN 'In' ELSE 'Out' END
        ELSE 'Fee/Adjustment'
    END AS description,
    
    m.mcc_code IN ('4814', '4900', '7832') AS is_recurring,
    FALSE AS is_international,
    0.05 AS fraud_score,
    'COMPLETED' AS status,
    CURRENT_TIMESTAMP() AS created_at
FROM (
    SELECT 
        a.*,
        CURRENT_DATE() AS transaction_date,
        'DEBIT' AS transaction_type,
        100.00 AS amount,
        NULL AS employer_name,
        NULL AS deposit_date
    FROM ACCOUNTS a
    WHERE a.account_type = 'CHECKING'
    LIMIT 10
) AS base_txn
CROSS JOIN (
    SELECT * FROM temp_merchants LIMIT 1
) AS m;

-- Check results
SELECT COUNT(*) AS transaction_count FROM TRANSACTIONS;
SELECT * FROM TRANSACTIONS LIMIT 5;
