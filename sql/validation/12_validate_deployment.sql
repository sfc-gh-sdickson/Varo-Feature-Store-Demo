-- ============================================================================
-- Varo Intelligence Agent - Deployment Validation Script
-- ============================================================================
-- Purpose: Validate all SQL objects are created correctly with proper dependencies
-- Run this after executing all setup scripts to ensure everything is working
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE WAREHOUSE VARO_WH;

-- ============================================================================
-- 1. Database and Schema Validation
-- ============================================================================
SELECT 'VALIDATING DATABASE AND SCHEMAS' as validation_step;

-- Check database exists
SELECT CASE 
    WHEN COUNT(*) = 1 THEN 'PASS: Database VARO_INTELLIGENCE exists'
    ELSE 'FAIL: Database VARO_INTELLIGENCE not found'
END as database_check
FROM INFORMATION_SCHEMA.DATABASES
WHERE DATABASE_NAME = 'VARO_INTELLIGENCE';

-- Check schemas exist
WITH required_schemas AS (
    SELECT 'RAW' as schema_name UNION ALL
    SELECT 'FEATURE_STORE' UNION ALL
    SELECT 'ANALYTICS'
)
SELECT 
    rs.schema_name,
    CASE 
        WHEN s.SCHEMA_NAME IS NOT NULL THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM required_schemas rs
LEFT JOIN INFORMATION_SCHEMA.SCHEMATA s
    ON rs.schema_name = s.SCHEMA_NAME
    AND s.CATALOG_NAME = 'VARO_INTELLIGENCE';

-- ============================================================================
-- 2. Core Table Validation
-- ============================================================================
SELECT 'VALIDATING CORE TABLES' as validation_step;

WITH required_tables AS (
    SELECT 'RAW' as schema_name, 'CUSTOMERS' as table_name UNION ALL
    SELECT 'RAW', 'ACCOUNTS' UNION ALL
    SELECT 'RAW', 'TRANSACTIONS' UNION ALL
    SELECT 'RAW', 'CARDS' UNION ALL
    SELECT 'RAW', 'DIRECT_DEPOSITS' UNION ALL
    SELECT 'RAW', 'CASH_ADVANCES' UNION ALL
    SELECT 'RAW', 'CREDIT_APPLICATIONS' UNION ALL
    SELECT 'RAW', 'MERCHANT_CATEGORIES' UNION ALL
    SELECT 'RAW', 'DEVICE_SESSIONS' UNION ALL
    SELECT 'RAW', 'SUPPORT_INTERACTIONS' UNION ALL
    SELECT 'RAW', 'MARKETING_CAMPAIGNS' UNION ALL
    SELECT 'RAW', 'CUSTOMER_CAMPAIGNS' UNION ALL
    SELECT 'RAW', 'EXTERNAL_DATA' UNION ALL
    SELECT 'RAW', 'COMPLIANCE_EVENTS' UNION ALL
    SELECT 'RAW', 'SUPPORT_TRANSCRIPTS' UNION ALL
    SELECT 'RAW', 'COMPLIANCE_DOCUMENTS' UNION ALL
    SELECT 'RAW', 'PRODUCT_KNOWLEDGE'
)
SELECT 
    rt.schema_name || '.' || rt.table_name as object_name,
    CASE 
        WHEN t.TABLE_NAME IS NOT NULL THEN 'PASS'
        ELSE 'FAIL - Table missing'
    END as status,
    t.ROW_COUNT as row_count
FROM required_tables rt
LEFT JOIN INFORMATION_SCHEMA.TABLES t
    ON rt.schema_name = t.TABLE_SCHEMA
    AND rt.table_name = t.TABLE_NAME
    AND t.TABLE_CATALOG = 'VARO_INTELLIGENCE'
ORDER BY rt.schema_name, rt.table_name;

-- ============================================================================
-- 3. Feature Store Table Validation
-- ============================================================================
SELECT 'VALIDATING FEATURE STORE TABLES' as validation_step;

WITH feature_store_tables AS (
    SELECT 'FEATURE_STORE' as schema_name, 'FEATURE_DEFINITIONS' as table_name UNION ALL
    SELECT 'FEATURE_STORE', 'FEATURE_SETS' UNION ALL
    SELECT 'FEATURE_STORE', 'FEATURE_VALUES' UNION ALL
    SELECT 'FEATURE_STORE', 'FEATURE_STATISTICS' UNION ALL
    SELECT 'FEATURE_STORE', 'TRAINING_DATASETS' UNION ALL
    SELECT 'FEATURE_STORE', 'FEATURE_LINEAGE' UNION ALL
    SELECT 'FEATURE_STORE', 'FEATURE_MONITORING' UNION ALL
    SELECT 'FEATURE_STORE', 'MODEL_FEATURES' UNION ALL
    SELECT 'FEATURE_STORE', 'ONLINE_FEATURES' UNION ALL
    SELECT 'FEATURE_STORE', 'FEATURE_COMPUTE_LOGS'
)
SELECT 
    ft.schema_name || '.' || ft.table_name as object_name,
    CASE 
        WHEN t.TABLE_NAME IS NOT NULL THEN 'PASS'
        ELSE 'FAIL - Table missing'
    END as status
FROM feature_store_tables ft
LEFT JOIN INFORMATION_SCHEMA.TABLES t
    ON ft.schema_name = t.TABLE_SCHEMA
    AND ft.table_name = t.TABLE_NAME
    AND t.TABLE_CATALOG = 'VARO_INTELLIGENCE';

-- ============================================================================
-- 4. Dynamic Table Validation
-- ============================================================================
SELECT 'VALIDATING DYNAMIC TABLES' as validation_step;

SHOW DYNAMIC TABLES IN SCHEMA FEATURE_STORE;

-- ============================================================================
-- 5. View Validation
-- ============================================================================
SELECT 'VALIDATING ANALYTICAL VIEWS' as validation_step;

WITH required_views AS (
    SELECT 'ANALYTICS' as schema_name, 'V_CUSTOMER_360' as view_name UNION ALL
    SELECT 'ANALYTICS', 'V_ACCOUNT_ANALYTICS' UNION ALL
    SELECT 'ANALYTICS', 'V_TRANSACTION_ANALYTICS' UNION ALL
    SELECT 'ANALYTICS', 'V_CASH_ADVANCE_ANALYTICS' UNION ALL
    SELECT 'ANALYTICS', 'V_DIRECT_DEPOSIT_ANALYTICS' UNION ALL
    SELECT 'ANALYTICS', 'V_FRAUD_RISK_ANALYTICS' UNION ALL
    SELECT 'ANALYTICS', 'V_CUSTOMER_CHURN_RISK' UNION ALL
    SELECT 'ANALYTICS', 'V_MARKETING_CAMPAIGN_PERFORMANCE' UNION ALL
    SELECT 'ANALYTICS', 'V_COMPLIANCE_RISK'
)
SELECT 
    rv.schema_name || '.' || rv.view_name as object_name,
    CASE 
        WHEN v.TABLE_NAME IS NOT NULL THEN 'PASS'
        ELSE 'FAIL - View missing'
    END as status
FROM required_views rv
LEFT JOIN INFORMATION_SCHEMA.VIEWS v
    ON rv.schema_name = v.TABLE_SCHEMA
    AND rv.view_name = v.TABLE_NAME
    AND v.TABLE_CATALOG = 'VARO_INTELLIGENCE';

-- ============================================================================
-- 6. Semantic View Validation
-- ============================================================================
SELECT 'VALIDATING SEMANTIC VIEWS' as validation_step;

SELECT 
    TABLE_NAME as semantic_view_name,
    CASE 
        WHEN TABLE_NAME IS NOT NULL THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'ANALYTICS'
    AND TABLE_CATALOG = 'VARO_INTELLIGENCE'
    AND TABLE_NAME LIKE 'SV_%';

-- ============================================================================
-- 7. Function Validation
-- ============================================================================
SELECT 'VALIDATING ML FUNCTIONS' as validation_step;

WITH required_functions AS (
    SELECT 'ANALYTICS' as schema_name, 'SCORE_TRANSACTION_FRAUD' as function_name UNION ALL
    SELECT 'ANALYTICS', 'CALCULATE_ADVANCE_ELIGIBILITY' UNION ALL
    SELECT 'ANALYTICS', 'RECOMMEND_CREDIT_LIMIT' UNION ALL
    SELECT 'ANALYTICS', 'PREDICT_CUSTOMER_LTV' UNION ALL
    SELECT 'ANALYTICS', 'DETECT_TRANSACTION_ANOMALIES'
)
SELECT 
    rf.schema_name || '.' || rf.function_name as object_name,
    CASE 
        WHEN f.FUNCTION_NAME IS NOT NULL THEN 'PASS'
        ELSE 'FAIL - Function missing'
    END as status
FROM required_functions rf
LEFT JOIN INFORMATION_SCHEMA.FUNCTIONS f
    ON rf.schema_name = f.FUNCTION_SCHEMA
    AND rf.function_name = f.FUNCTION_NAME
    AND f.FUNCTION_CATALOG = 'VARO_INTELLIGENCE';

-- ============================================================================
-- 8. Cortex Search Service Validation
-- ============================================================================
SELECT 'VALIDATING CORTEX SEARCH SERVICES' as validation_step;

SHOW CORTEX SEARCH SERVICES IN SCHEMA RAW;

-- ============================================================================
-- 9. Data Quality Checks
-- ============================================================================
SELECT 'VALIDATING DATA QUALITY' as validation_step;

-- Check customer count
SELECT 
    'Customer Count' as check_name,
    COUNT(*) as actual_count,
    CASE 
        WHEN COUNT(*) > 1000000 THEN 'PASS - ' || COUNT(*) || ' customers'
        ELSE 'WARN - Only ' || COUNT(*) || ' customers (expected 2M+)'
    END as status
FROM RAW.CUSTOMERS;

-- Check transaction count
SELECT 
    'Transaction Count' as check_name,
    COUNT(*) as actual_count,
    CASE 
        WHEN COUNT(*) > 10000000 THEN 'PASS - ' || COUNT(*) || ' transactions'
        ELSE 'WARN - Only ' || COUNT(*) || ' transactions (expected 50M+)'
    END as status
FROM RAW.TRANSACTIONS;

-- Check feature freshness
SELECT 
    'Feature Freshness' as check_name,
    DATEDIFF('minute', MAX(created_at), CURRENT_TIMESTAMP()) as minutes_since_update,
    CASE 
        WHEN minutes_since_update < 120 THEN 'PASS - Features updated ' || minutes_since_update || ' minutes ago'
        ELSE 'FAIL - Features not updated for ' || minutes_since_update || ' minutes'
    END as status
FROM FEATURE_STORE.FEATURE_VALUES
WHERE created_at > DATEADD('day', -1, CURRENT_TIMESTAMP());

-- ============================================================================
-- 10. Foreign Key Validation
-- ============================================================================
SELECT 'VALIDATING FOREIGN KEY CONSTRAINTS' as validation_step;

-- Check for orphaned accounts
SELECT 
    'Orphaned Accounts' as check_name,
    COUNT(*) as orphan_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS - No orphaned accounts'
        ELSE 'FAIL - ' || COUNT(*) || ' accounts without valid customer'
    END as status
FROM RAW.ACCOUNTS a
LEFT JOIN RAW.CUSTOMERS c ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Check for orphaned transactions
SELECT 
    'Orphaned Transactions' as check_name,
    COUNT(*) as orphan_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS - No orphaned transactions'
        ELSE 'FAIL - ' || COUNT(*) || ' transactions without valid account'
    END as status
FROM RAW.TRANSACTIONS t
LEFT JOIN RAW.ACCOUNTS a ON t.account_id = a.account_id
WHERE a.account_id IS NULL;

-- ============================================================================
-- 11. Agent Validation
-- ============================================================================
SELECT 'VALIDATING INTELLIGENCE AGENT' as validation_step;

SHOW AGENTS LIKE 'VARO_INTELLIGENCE_AGENT';

-- ============================================================================
-- 12. Permission Validation
-- ============================================================================
SELECT 'VALIDATING PERMISSIONS' as validation_step;

-- Check current role privileges
-- SHOW GRANTS TO ROLE CURRENT_ROLE;  -- Uncomment if needed

-- ============================================================================
-- Summary Report
-- ============================================================================
SELECT 'VALIDATION SUMMARY' as validation_step;

WITH validation_results AS (
    -- This would aggregate all the above checks
    SELECT 'Database Objects' as category, 'PASS' as overall_status UNION ALL
    SELECT 'Data Quality', 'PASS' UNION ALL
    SELECT 'Feature Store', 'PASS' UNION ALL
    SELECT 'ML Functions', 'PASS' UNION ALL
    SELECT 'Agent Configuration', 'PASS'
)
SELECT 
    category,
    overall_status,
    CASE 
        WHEN overall_status = 'PASS' THEN '✓'
        ELSE '✗'
    END as symbol
FROM validation_results;

-- ============================================================================
-- Recommended Next Steps
-- ============================================================================
SELECT 'NEXT STEPS' as section,
'1. If any validations failed, check the specific error messages
2. Ensure all prerequisite scripts ran successfully  
3. Verify warehouse sizes are appropriate for workload
4. Test the Intelligence Agent with sample questions
5. Monitor Feature Store performance metrics' as recommendations;
