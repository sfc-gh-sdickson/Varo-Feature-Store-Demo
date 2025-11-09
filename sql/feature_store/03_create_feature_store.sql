-- ============================================================================
-- Varo Feature Store - Table Definitions
-- ============================================================================
-- Purpose: Create Feature Store infrastructure tables for ML feature management
-- Replaces Tecton functionality with native Snowflake capabilities
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA FEATURE_STORE;
USE WAREHOUSE VARO_FEATURE_WH;

-- ============================================================================
-- FEATURE_DEFINITIONS TABLE
-- Central registry of all feature definitions
-- ============================================================================
CREATE OR REPLACE TABLE FEATURE_DEFINITIONS (
    feature_id VARCHAR(100) PRIMARY KEY,
    feature_name VARCHAR(200) NOT NULL,
    feature_group VARCHAR(100) NOT NULL, -- customer_profile, transaction_patterns, risk_indicators
    description VARCHAR(1000),
    data_type VARCHAR(30) NOT NULL,
    computation_type VARCHAR(30) NOT NULL, -- BATCH, STREAMING, REAL_TIME
    feature_sql TEXT NOT NULL, -- SQL definition of the feature
    source_tables ARRAY,
    refresh_frequency VARCHAR(30), -- HOURLY, DAILY, REAL_TIME
    version NUMBER(5,0) DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_by VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- FEATURE_SETS TABLE
-- Logical grouping of features for specific use cases
-- ============================================================================
CREATE OR REPLACE TABLE FEATURE_SETS (
    feature_set_id VARCHAR(100) PRIMARY KEY,
    feature_set_name VARCHAR(200) NOT NULL,
    use_case VARCHAR(100) NOT NULL, -- fraud_detection, credit_risk, personalization
    description VARCHAR(1000),
    feature_ids ARRAY NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_by VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- FEATURE_VALUES TABLE
-- Materialized feature values with time-travel support
-- ============================================================================
CREATE OR REPLACE TABLE FEATURE_VALUES (
    entity_id VARCHAR(100) NOT NULL, -- customer_id, account_id, etc.
    entity_type VARCHAR(50) NOT NULL, -- CUSTOMER, ACCOUNT, MERCHANT
    feature_id VARCHAR(100) NOT NULL,
    feature_value VARIANT NOT NULL,
    feature_timestamp TIMESTAMP_NTZ NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (entity_id, feature_id, feature_timestamp),
    FOREIGN KEY (feature_id) REFERENCES FEATURE_DEFINITIONS(feature_id)
) CLUSTER BY (entity_type, feature_timestamp);

-- Enable change tracking for real-time serving
ALTER TABLE FEATURE_VALUES SET CHANGE_TRACKING = TRUE;

-- ============================================================================
-- FEATURE_STATISTICS TABLE
-- Track feature quality and distribution metrics
-- ============================================================================
CREATE OR REPLACE TABLE FEATURE_STATISTICS (
    stat_id VARCHAR(100) PRIMARY KEY,
    feature_id VARCHAR(100) NOT NULL,
    computation_date DATE NOT NULL,
    mean_value NUMBER(20,6),
    std_deviation NUMBER(20,6),
    min_value NUMBER(20,6),
    max_value NUMBER(20,6),
    null_count NUMBER(10,0),
    unique_count NUMBER(10,0),
    total_count NUMBER(10,0),
    percentile_25 NUMBER(20,6),
    percentile_50 NUMBER(20,6),
    percentile_75 NUMBER(20,6),
    percentile_95 NUMBER(20,6),
    percentile_99 NUMBER(20,6),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (feature_id) REFERENCES FEATURE_DEFINITIONS(feature_id)
);

-- ============================================================================
-- TRAINING_DATASETS TABLE
-- Point-in-time correct datasets for model training
-- ============================================================================
CREATE OR REPLACE TABLE TRAINING_DATASETS (
    dataset_id VARCHAR(100) PRIMARY KEY,
    dataset_name VARCHAR(200) NOT NULL,
    model_name VARCHAR(100),
    feature_set_id VARCHAR(100) NOT NULL,
    label_definition VARCHAR(500),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    entity_count NUMBER(10,0),
    row_count NUMBER(12,0),
    dataset_location VARCHAR(500), -- Snowflake stage or table location
    is_stratified BOOLEAN DEFAULT FALSE,
    train_test_split_ratio NUMBER(3,2),
    created_by VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (feature_set_id) REFERENCES FEATURE_SETS(feature_set_id)
);

-- ============================================================================
-- FEATURE_LINEAGE TABLE
-- Track feature dependencies and computation graph
-- ============================================================================
CREATE OR REPLACE TABLE FEATURE_LINEAGE (
    lineage_id VARCHAR(100) PRIMARY KEY,
    feature_id VARCHAR(100) NOT NULL,
    parent_feature_id VARCHAR(100),
    parent_table VARCHAR(200),
    dependency_type VARCHAR(50), -- DIRECT, DERIVED, AGGREGATED
    transformation_logic VARCHAR(1000),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (feature_id) REFERENCES FEATURE_DEFINITIONS(feature_id)
);

-- ============================================================================
-- FEATURE_MONITORING TABLE
-- Track feature drift and data quality issues
-- ============================================================================
CREATE OR REPLACE TABLE FEATURE_MONITORING (
    monitor_id VARCHAR(100) PRIMARY KEY,
    feature_id VARCHAR(100) NOT NULL,
    monitor_type VARCHAR(50) NOT NULL, -- DRIFT, QUALITY, SCHEMA
    monitor_date DATE NOT NULL,
    baseline_stats VARIANT,
    current_stats VARIANT,
    drift_score NUMBER(5,4),
    alert_triggered BOOLEAN DEFAULT FALSE,
    alert_severity VARCHAR(20),
    alert_message VARCHAR(500),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (feature_id) REFERENCES FEATURE_DEFINITIONS(feature_id)
);

-- ============================================================================
-- MODEL_FEATURES TABLE
-- Track which features are used by which models
-- ============================================================================
CREATE OR REPLACE TABLE MODEL_FEATURES (
    model_feature_id VARCHAR(100) PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(20) NOT NULL,
    feature_id VARCHAR(100) NOT NULL,
    feature_importance NUMBER(5,4),
    feature_type VARCHAR(30), -- INPUT, TARGET, EXCLUDED
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (feature_id) REFERENCES FEATURE_DEFINITIONS(feature_id)
);

-- ============================================================================
-- ONLINE_FEATURES TABLE
-- Low-latency serving table for real-time inference
-- ============================================================================
CREATE OR REPLACE TABLE ONLINE_FEATURES (
    entity_id VARCHAR(100) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    feature_vector VARIANT NOT NULL, -- JSON object with all features
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (entity_id, entity_type)
) CLUSTER BY (entity_type);

-- Enable search optimization for fast lookups
ALTER TABLE ONLINE_FEATURES ADD SEARCH OPTIMIZATION;

-- ============================================================================
-- FEATURE_COMPUTE_LOGS TABLE
-- Track feature computation history and performance
-- ============================================================================
CREATE OR REPLACE TABLE FEATURE_COMPUTE_LOGS (
    compute_id VARCHAR(100) PRIMARY KEY,
    feature_id VARCHAR(100) NOT NULL,
    compute_start TIMESTAMP_NTZ NOT NULL,
    compute_end TIMESTAMP_NTZ,
    rows_processed NUMBER(12,0),
    compute_status VARCHAR(30),
    error_message VARCHAR(1000),
    warehouse_used VARCHAR(100),
    credits_used NUMBER(10,4),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (feature_id) REFERENCES FEATURE_DEFINITIONS(feature_id)
);

-- ============================================================================
-- Create Dynamic Tables for common feature aggregations
-- These replace Tecton's materialized feature views
-- ============================================================================

-- Customer transaction velocity features (30-day rolling)
CREATE OR REPLACE DYNAMIC TABLE CUSTOMER_TXN_VELOCITY_30D
    TARGET_LAG = '1 HOUR'
    WAREHOUSE = VARO_FEATURE_WH
    AS
    SELECT
        customer_id,
        CURRENT_TIMESTAMP() as feature_timestamp,
        COUNT(*) as txn_count_30d,
        SUM(amount) as txn_amount_30d,
        AVG(amount) as txn_avg_amount_30d,
        STDDEV(amount) as txn_stddev_amount_30d,
        COUNT(DISTINCT merchant_category) as unique_merchants_30d,
        COUNT(DISTINCT transaction_date) as active_days_30d,
        MAX(amount) as max_txn_amount_30d,
        SUM(CASE WHEN is_international THEN 1 ELSE 0 END) as intl_txn_count_30d
    FROM VARO_INTELLIGENCE.RAW.TRANSACTIONS
    WHERE transaction_date >= DATEADD('day', -30, CURRENT_DATE())
        AND status = 'COMPLETED'
    GROUP BY customer_id;

-- Customer risk indicators
CREATE OR REPLACE DYNAMIC TABLE CUSTOMER_RISK_INDICATORS
    TARGET_LAG = '1 HOUR'
    WAREHOUSE = VARO_FEATURE_WH
    AS
    SELECT
        c.customer_id,
        CURRENT_TIMESTAMP() as feature_timestamp,
        -- Cash advance behavior
        COUNT(DISTINCT ca.advance_id) as advance_count_90d,
        SUM(ca.advance_amount) as total_advance_amount_90d,
        AVG(CASE WHEN ca.advance_status = 'DEFAULTED' THEN 1 ELSE 0 END) as advance_default_rate,
        
        -- Account health
        MIN(a.current_balance) as min_balance_30d,
        AVG(a.current_balance) as avg_balance_30d,
        
        -- External data signals
        MAX(e.credit_score) as latest_credit_score,
        MAX(e.credit_utilization) as latest_credit_utilization
        
    FROM VARO_INTELLIGENCE.RAW.CUSTOMERS c
    LEFT JOIN VARO_INTELLIGENCE.RAW.CASH_ADVANCES ca 
        ON c.customer_id = ca.customer_id 
        AND ca.advance_date >= DATEADD('day', -90, CURRENT_DATE())
    LEFT JOIN VARO_INTELLIGENCE.RAW.ACCOUNTS a 
        ON c.customer_id = a.customer_id
    LEFT JOIN VARO_INTELLIGENCE.RAW.EXTERNAL_DATA e 
        ON c.customer_id = e.customer_id
    GROUP BY c.customer_id;

-- ============================================================================
-- Create Stream for real-time feature updates
-- ============================================================================
CREATE OR REPLACE STREAM TRANSACTION_STREAM ON TABLE VARO_INTELLIGENCE.RAW.TRANSACTIONS;

-- ============================================================================
-- Create Tasks for automated feature computation
-- ============================================================================

-- Task to compute batch features daily
CREATE OR REPLACE TASK COMPUTE_DAILY_FEATURES
    WAREHOUSE = VARO_FEATURE_WH
    SCHEDULE = 'USING CRON 0 2 * * * UTC' -- 2 AM UTC daily
AS
    CALL COMPUTE_BATCH_FEATURES('DAILY');

-- Task to compute streaming features every 5 minutes
CREATE OR REPLACE TASK COMPUTE_STREAMING_FEATURES
    WAREHOUSE = VARO_FEATURE_WH
    SCHEDULE = '5 MINUTES'
AS
    CALL COMPUTE_STREAMING_FEATURES();

-- ============================================================================
-- Create API Integration for real-time serving (replaces Tecton serving)
-- ============================================================================
CREATE OR REPLACE API INTEGRATION VARO_FEATURE_API
    API_PROVIDER = aws_api_gateway
    API_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/VaroFeatureAPI'
    API_ALLOWED_PREFIXES = ('https://api.varo-features.snowflake.app/')
    ENABLED = TRUE;

-- ============================================================================
-- Create External Functions for feature serving
-- ============================================================================
CREATE OR REPLACE EXTERNAL FUNCTION GET_CUSTOMER_FEATURES(customer_id VARCHAR)
    RETURNS VARIANT
    API_INTEGRATION = VARO_FEATURE_API
    AS 'https://api.varo-features.snowflake.app/v1/features/customer';

CREATE OR REPLACE EXTERNAL FUNCTION GET_TRANSACTION_RISK_SCORE(transaction_data VARIANT)
    RETURNS NUMBER(3,2)
    API_INTEGRATION = VARO_FEATURE_API
    AS 'https://api.varo-features.snowflake.app/v1/score/transaction';

-- Display confirmation
SELECT 'Feature Store tables and infrastructure created successfully' AS STATUS;
