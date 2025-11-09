-- ============================================================================
-- Varo Feature Store - Feature Engineering Definitions
-- ============================================================================
-- Purpose: Create SQL-based feature engineering pipelines
-- Demonstrates migration from Tecton's Python/Spark to Snowflake's SQL-first approach
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA FEATURE_STORE;
USE WAREHOUSE VARO_FEATURE_WH;

-- ============================================================================
-- Create procedures for feature computation
-- ============================================================================

-- Procedure to compute batch features
CREATE OR REPLACE PROCEDURE COMPUTE_BATCH_FEATURES(feature_frequency VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Log start
    INSERT INTO FEATURE_COMPUTE_LOGS (compute_id, feature_id, compute_start, warehouse_used, compute_status)
    SELECT 
        'COMP' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS') || '_' || feature_id,
        feature_id,
        CURRENT_TIMESTAMP(),
        CURRENT_WAREHOUSE(),
        'RUNNING'
    FROM FEATURE_DEFINITIONS
    WHERE refresh_frequency = feature_frequency
        AND is_active = TRUE
        AND computation_type = 'BATCH';
    
    -- Execute feature computations
    -- In production, this would dynamically execute the SQL from feature_definitions
    RETURN 'Batch features computed for frequency: ' || feature_frequency;
END;
$$;

-- Procedure for streaming features  
CREATE OR REPLACE PROCEDURE COMPUTE_STREAMING_FEATURES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_processed INTEGER;
BEGIN
    -- Process new transactions from stream
    MERGE INTO FEATURE_VALUES fv
    USING (
        SELECT 
            customer_id,
            'CUSTOMER' as entity_type,
            feature_id,
            feature_value,
            CURRENT_TIMESTAMP() as feature_timestamp
        FROM (
            -- Real-time transaction velocity
            SELECT 
                customer_id,
                'customer_txn_velocity_1h' as feature_id,
                OBJECT_CONSTRUCT(
                    'count', COUNT(*),
                    'sum', SUM(amount),
                    'max', MAX(amount),
                    'unique_merchants', COUNT(DISTINCT merchant_name)
                ) as feature_value
            FROM TRANSACTION_STREAM
            WHERE METADATA$ACTION = 'INSERT'
                AND transaction_timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
            GROUP BY customer_id
        )
    ) src
    ON fv.entity_id = src.customer_id 
        AND fv.feature_id = src.feature_id
        AND fv.entity_type = src.entity_type
    WHEN MATCHED THEN
        UPDATE SET 
            fv.feature_value = src.feature_value,
            fv.feature_timestamp = src.feature_timestamp
    WHEN NOT MATCHED THEN
        INSERT (entity_id, entity_type, feature_id, feature_value, feature_timestamp)
        VALUES (src.customer_id, src.entity_type, src.feature_id, src.feature_value, src.feature_timestamp);
    
    -- Update online features for real-time serving
    MERGE INTO ONLINE_FEATURES
    USING (
        SELECT 
            entity_id,
            entity_type,
            OBJECT_AGG(feature_id, feature_value) as feature_vector
        FROM FEATURE_VALUES
        WHERE feature_timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        GROUP BY entity_id, entity_type
    ) src
    ON ONLINE_FEATURES.entity_id = src.entity_id AND ONLINE_FEATURES.entity_type = src.entity_type
    WHEN MATCHED THEN
        UPDATE SET 
            feature_vector = src.feature_vector,
            last_updated = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (entity_id, entity_type, feature_vector)
        VALUES (src.entity_id, src.entity_type, src.feature_vector);
    
    RETURN 'Streaming features computed successfully';
END;
$$;

-- ============================================================================
-- Create Dynamic Tables for Common Feature Aggregations
-- These replace Tecton's Feature Views with SQL-first approach
-- ============================================================================

-- Customer Profile Features (refreshed hourly)
CREATE OR REPLACE DYNAMIC TABLE CUSTOMER_PROFILE_FEATURES
    TARGET_LAG = '1 HOUR'
    WAREHOUSE = VARO_FEATURE_WH
    AS
    SELECT
        c.customer_id,
        c.customer_id as entity_id,
        'CUSTOMER' as entity_type,
        CURRENT_TIMESTAMP() as feature_timestamp,
        
        -- Demographic features
        DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) as age_years,
        DATEDIFF('day', c.acquisition_date, CURRENT_DATE()) as customer_tenure_days,
        c.credit_score as current_credit_score,
        c.income_verified as verified_income,
        
        -- Account features
        COUNT(DISTINCT a.account_id) as num_accounts,
        COUNT(DISTINCT CASE WHEN a.account_type = 'CHECKING' THEN a.account_id END) as num_checking_accounts,
        COUNT(DISTINCT CASE WHEN a.account_type = 'SAVINGS' THEN a.account_id END) as num_savings_accounts,
        MAX(CASE WHEN a.account_type = 'BELIEVE_CARD' THEN 1 ELSE 0 END) as has_credit_card,
        MAX(CASE WHEN a.account_type = 'LINE_OF_CREDIT' THEN 1 ELSE 0 END) as has_line_of_credit,
        
        -- Balance features
        SUM(CASE WHEN a.account_type IN ('CHECKING', 'SAVINGS') THEN a.current_balance ELSE 0 END) as total_deposit_balance,
        SUM(CASE WHEN a.account_type IN ('BELIEVE_CARD', 'LINE_OF_CREDIT') THEN ABS(a.current_balance) ELSE 0 END) as total_credit_balance,
        AVG(CASE WHEN a.account_type = 'SAVINGS' THEN a.current_balance ELSE NULL END) as avg_savings_balance,
        
        -- Direct deposit features
        COUNT(DISTINCT dd.deposit_id) as total_direct_deposits,
        MAX(dd.amount) as max_direct_deposit,
        AVG(dd.amount) as avg_direct_deposit,
        DATEDIFF('day', MAX(dd.deposit_date), CURRENT_DATE()) as days_since_last_deposit,
        
        -- Feature vector for online serving
        OBJECT_CONSTRUCT(
            'age_years', DATEDIFF('year', c.date_of_birth, CURRENT_DATE()),
            'tenure_days', DATEDIFF('day', c.acquisition_date, CURRENT_DATE()),
            'credit_score', c.credit_score,
            'num_accounts', COUNT(DISTINCT a.account_id),
            'total_balance', SUM(CASE WHEN a.account_type IN ('CHECKING', 'SAVINGS') THEN a.current_balance ELSE 0 END),
            'has_direct_deposit', CASE WHEN COUNT(dd.deposit_id) > 0 THEN 1 ELSE 0 END
        ) as profile_features
        
    FROM VARO_INTELLIGENCE.RAW.CUSTOMERS c
    LEFT JOIN VARO_INTELLIGENCE.RAW.ACCOUNTS a ON c.customer_id = a.customer_id
    LEFT JOIN VARO_INTELLIGENCE.RAW.DIRECT_DEPOSITS dd 
        ON c.customer_id = dd.customer_id 
        AND dd.deposit_date >= DATEADD('day', -90, CURRENT_DATE())
    WHERE c.customer_status = 'ACTIVE'
    GROUP BY c.customer_id, c.date_of_birth, c.acquisition_date, c.credit_score, c.income_verified;

-- Transaction Pattern Features (refreshed every 30 minutes)
CREATE OR REPLACE DYNAMIC TABLE TRANSACTION_PATTERN_FEATURES
    TARGET_LAG = '30 MINUTES'
    WAREHOUSE = VARO_FEATURE_WH
    AS
    WITH transaction_windows AS (
        SELECT
            customer_id,
            -- 7-day window features
            COUNT(CASE WHEN transaction_date >= DATEADD('day', -7, CURRENT_DATE()) THEN 1 END) as txn_count_7d,
            SUM(CASE WHEN transaction_date >= DATEADD('day', -7, CURRENT_DATE()) THEN ABS(amount) END) as txn_volume_7d,
            COUNT(DISTINCT CASE WHEN transaction_date >= DATEADD('day', -7, CURRENT_DATE()) THEN merchant_category END) as unique_mcc_7d,
            AVG(CASE WHEN transaction_date >= DATEADD('day', -7, CURRENT_DATE()) THEN ABS(amount) END) as avg_txn_amount_7d,
            
            -- 30-day window features
            COUNT(CASE WHEN transaction_date >= DATEADD('day', -30, CURRENT_DATE()) THEN 1 END) as txn_count_30d,
            SUM(CASE WHEN transaction_date >= DATEADD('day', -30, CURRENT_DATE()) THEN ABS(amount) END) as txn_volume_30d,
            COUNT(DISTINCT CASE WHEN transaction_date >= DATEADD('day', -30, CURRENT_DATE()) THEN merchant_name END) as unique_merchants_30d,
            STDDEV(CASE WHEN transaction_date >= DATEADD('day', -30, CURRENT_DATE()) THEN ABS(amount) END) as txn_amount_stddev_30d,
            
            -- 90-day window features
            COUNT(CASE WHEN transaction_date >= DATEADD('day', -90, CURRENT_DATE()) THEN 1 END) as txn_count_90d,
            SUM(CASE WHEN transaction_date >= DATEADD('day', -90, CURRENT_DATE()) THEN ABS(amount) END) as txn_volume_90d,
            MAX(CASE WHEN transaction_date >= DATEADD('day', -90, CURRENT_DATE()) THEN ABS(amount) END) as max_txn_amount_90d,
            
            -- Time-based patterns
            COUNT(CASE WHEN EXTRACT(hour FROM transaction_timestamp) BETWEEN 0 AND 6 THEN 1 END) as night_txn_count,
            COUNT(CASE WHEN DAYOFWEEK(transaction_date) IN (1, 7) THEN 1 END) as weekend_txn_count,
            COUNT(DISTINCT DATE_TRUNC('day', transaction_date)) as active_days_total,
            
            -- Category-specific features
            SUM(CASE WHEN merchant_category IN ('5411', '5541', '5912') THEN ABS(amount) ELSE 0 END) as essential_spend,
            SUM(CASE WHEN merchant_category IN ('5812', '5814', '7832') THEN ABS(amount) ELSE 0 END) as discretionary_spend,
            SUM(CASE WHEN merchant_category IN ('7995', '5933') THEN ABS(amount) ELSE 0 END) as risky_spend,
            
            -- International and ATM usage
            COUNT(CASE WHEN is_international = TRUE THEN 1 END) as intl_txn_count,
            SUM(CASE WHEN transaction_category = 'ATM' THEN ABS(amount) ELSE 0 END) as total_atm_withdrawals,
            
            -- Velocity features
            COUNT(CASE WHEN transaction_timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP()) THEN 1 END) as txn_count_1h,
            COUNT(CASE WHEN transaction_timestamp >= DATEADD('day', -1, CURRENT_TIMESTAMP()) THEN 1 END) as txn_count_24h
            
        FROM VARO_INTELLIGENCE.RAW.TRANSACTIONS
        WHERE status = 'COMPLETED'
        GROUP BY customer_id
    )
    SELECT
        customer_id,
        customer_id as entity_id,
        'CUSTOMER' as entity_type,
        CURRENT_TIMESTAMP() as feature_timestamp,
        
        -- All computed features
        *,
        
        -- Derived ratios
        DIV0NULL(essential_spend, txn_volume_30d) as essential_spend_ratio,
        DIV0NULL(risky_spend, txn_volume_30d) as risky_spend_ratio,
        DIV0NULL(weekend_txn_count, txn_count_30d) as weekend_txn_ratio,
        DIV0NULL(night_txn_count, txn_count_30d) as night_txn_ratio,
        
        -- Feature vector for online serving
        OBJECT_CONSTRUCT(
            'txn_count_7d', txn_count_7d,
            'txn_volume_30d', txn_volume_30d,
            'unique_merchants_30d', unique_merchants_30d,
            'velocity_1h', txn_count_1h,
            'velocity_24h', txn_count_24h,
            'essential_ratio', DIV0NULL(essential_spend, txn_volume_30d),
            'risk_indicator', DIV0NULL(risky_spend, txn_volume_30d)
        ) as transaction_features
        
    FROM transaction_windows;

-- Cash Advance Risk Features (refreshed every hour)
CREATE OR REPLACE DYNAMIC TABLE ADVANCE_RISK_FEATURES
    TARGET_LAG = '1 HOUR'
    WAREHOUSE = VARO_FEATURE_WH
    AS
    SELECT
        c.customer_id,
        c.customer_id as entity_id,
        'CUSTOMER' as entity_type,
        CURRENT_TIMESTAMP() as feature_timestamp,
        
        -- Advance history
        COUNT(ca.advance_id) as total_advances_taken,
        SUM(ca.advance_amount) as total_advance_amount,
        AVG(ca.advance_amount) as avg_advance_amount,
        MAX(ca.advance_amount) as max_advance_amount,
        
        -- Repayment behavior  
        AVG(DATEDIFF('day', ca.advance_date, ca.repayment_date)) as avg_repayment_days,
        COUNT(CASE WHEN ca.advance_status = 'DEFAULTED' THEN 1 END) as num_defaults,
        COUNT(CASE WHEN ca.repayment_date > ca.due_date THEN 1 END) as num_late_repayments,
        DIV0NULL(
            COUNT(CASE WHEN ca.advance_status = 'REPAID' AND ca.repayment_date <= ca.due_date THEN 1 END),
            COUNT(ca.advance_id)
        ) as on_time_repayment_rate,
        
        -- Current exposure
        SUM(CASE WHEN ca.advance_status = 'ACTIVE' THEN ca.advance_amount + ca.fee_amount ELSE 0 END) as current_advance_balance,
        MAX(CASE WHEN ca.advance_status = 'ACTIVE' THEN 
            DATEDIFF('day', CURRENT_DATE(), ca.due_date) 
        END) as days_until_due,
        
        -- Eligibility indicators
        MAX(dd.total_deposits_monthly) as monthly_direct_deposits,
        AVG(a.avg_balance_30d) as avg_account_balance_30d,
        DATEDIFF('day', MAX(ca.advance_date), CURRENT_DATE()) as days_since_last_advance,
        
        -- Risk scores
        CASE 
            WHEN COUNT(CASE WHEN ca.advance_status = 'DEFAULTED' THEN 1 END) > 0 THEN 0.9
            WHEN COUNT(CASE WHEN ca.repayment_date > ca.due_date THEN 1 END) > 2 THEN 0.7
            WHEN DIV0NULL(
                COUNT(CASE WHEN ca.advance_status = 'REPAID' AND ca.repayment_date <= ca.due_date THEN 1 END),
                COUNT(ca.advance_id)
            ) < 0.8 THEN 0.5
            ELSE 0.2
        END as advance_risk_score,
        
        -- Recommended advance limit
        CASE 
            WHEN COUNT(CASE WHEN ca.advance_status = 'DEFAULTED' THEN 1 END) > 0 THEN 0
            WHEN DIV0NULL(
                COUNT(CASE WHEN ca.advance_status = 'REPAID' AND ca.repayment_date <= ca.due_date THEN 1 END),
                COUNT(ca.advance_id)
            ) >= 0.95 AND COUNT(ca.advance_id) >= 5 THEN 500
            WHEN DIV0NULL(
                COUNT(CASE WHEN ca.advance_status = 'REPAID' AND ca.repayment_date <= ca.due_date THEN 1 END),
                COUNT(ca.advance_id)
            ) >= 0.90 AND COUNT(ca.advance_id) >= 3 THEN 250
            WHEN DIV0NULL(
                COUNT(CASE WHEN ca.advance_status = 'REPAID' AND ca.repayment_date <= ca.due_date THEN 1 END),
                COUNT(ca.advance_id)
            ) >= 0.85 THEN 100
            ELSE 50
        END as recommended_advance_limit,
        
        -- Feature vector
        OBJECT_CONSTRUCT(
            'total_advances', COUNT(ca.advance_id),
            'repayment_rate', DIV0NULL(
                COUNT(CASE WHEN ca.advance_status = 'REPAID' AND ca.repayment_date <= ca.due_date THEN 1 END),
                COUNT(ca.advance_id)
            ),
            'current_balance', SUM(CASE WHEN ca.advance_status = 'ACTIVE' THEN ca.advance_amount + ca.fee_amount ELSE 0 END),
            'risk_score', CASE 
                WHEN COUNT(CASE WHEN ca.advance_status = 'DEFAULTED' THEN 1 END) > 0 THEN 0.9
                WHEN COUNT(CASE WHEN ca.repayment_date > ca.due_date THEN 1 END) > 2 THEN 0.7
                WHEN DIV0NULL(
                    COUNT(CASE WHEN ca.advance_status = 'REPAID' AND ca.repayment_date <= ca.due_date THEN 1 END),
                    COUNT(ca.advance_id)
                ) < 0.8 THEN 0.5
                ELSE 0.2
            END,
            'recommended_limit', CASE 
                WHEN COUNT(CASE WHEN ca.advance_status = 'DEFAULTED' THEN 1 END) > 0 THEN 0
                WHEN DIV0NULL(
                    COUNT(CASE WHEN ca.advance_status = 'REPAID' AND ca.repayment_date <= ca.due_date THEN 1 END),
                    COUNT(ca.advance_id)
                ) >= 0.95 AND COUNT(ca.advance_id) >= 5 THEN 500
                WHEN DIV0NULL(
                    COUNT(CASE WHEN ca.advance_status = 'REPAID' AND ca.repayment_date <= ca.due_date THEN 1 END),
                    COUNT(ca.advance_id)
                ) >= 0.90 AND COUNT(ca.advance_id) >= 3 THEN 250
                WHEN DIV0NULL(
                    COUNT(CASE WHEN ca.advance_status = 'REPAID' AND ca.repayment_date <= ca.due_date THEN 1 END),
                    COUNT(ca.advance_id)
                ) >= 0.85 THEN 100
                ELSE 50
            END
        ) as advance_features
        
    FROM VARO_INTELLIGENCE.RAW.CUSTOMERS c
    LEFT JOIN VARO_INTELLIGENCE.RAW.CASH_ADVANCES ca ON c.customer_id = ca.customer_id
    LEFT JOIN (
        SELECT 
            customer_id, 
            SUM(amount) as total_deposits_monthly
        FROM VARO_INTELLIGENCE.RAW.DIRECT_DEPOSITS
        WHERE deposit_date >= DATEADD('month', -1, CURRENT_DATE())
        GROUP BY customer_id
    ) dd ON c.customer_id = dd.customer_id
    LEFT JOIN (
        SELECT 
            customer_id,
            AVG(current_balance) as avg_balance_30d
        FROM VARO_INTELLIGENCE.RAW.ACCOUNTS
        WHERE account_type = 'CHECKING'
        GROUP BY customer_id
    ) a ON c.customer_id = a.customer_id
    WHERE c.customer_status = 'ACTIVE'
    GROUP BY c.customer_id;

-- Fraud Detection Features (refreshed every 15 minutes)
CREATE OR REPLACE DYNAMIC TABLE FRAUD_DETECTION_FEATURES
    TARGET_LAG = '15 MINUTES'
    WAREHOUSE = VARO_FEATURE_WH
    AS
    WITH recent_transactions AS (
        SELECT 
            t.*,
            LAG(transaction_timestamp) OVER (PARTITION BY customer_id ORDER BY transaction_timestamp) as prev_txn_time,
            LAG(merchant_city) OVER (PARTITION BY customer_id ORDER BY transaction_timestamp) as prev_merchant_city,
            LAG(merchant_state) OVER (PARTITION BY customer_id ORDER BY transaction_timestamp) as prev_merchant_state,
            LAG(amount) OVER (PARTITION BY customer_id ORDER BY transaction_timestamp) as prev_amount
        FROM VARO_INTELLIGENCE.RAW.TRANSACTIONS t
        WHERE transaction_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
            AND status = 'COMPLETED'
    )
    SELECT
        customer_id,
        customer_id as entity_id,
        'CUSTOMER' as entity_type,
        CURRENT_TIMESTAMP() as feature_timestamp,
        
        -- Unusual amount patterns
        MAX(CASE 
            WHEN ABS(amount) > avg_amount * 3 AND ABS(amount) > 100 THEN 1 
            ELSE 0 
        END) as has_unusual_amount,
        
        -- Velocity indicators
        MAX(CASE 
            WHEN DATEDIFF('minute', prev_txn_time, transaction_timestamp) < 5 
                AND merchant_city != prev_merchant_city THEN 1 
            ELSE 0 
        END) as impossible_travel_flag,
        
        COUNT(CASE 
            WHEN DATEDIFF('minute', prev_txn_time, transaction_timestamp) < 1 THEN 1 
        END) as rapid_fire_txn_count,
        
        -- High-risk merchant usage
        SUM(CASE WHEN merchant_category IN ('7995', '5933', '6010', '6011') THEN 1 ELSE 0 END) as risky_merchant_txn_count,
        SUM(CASE WHEN is_international = TRUE THEN 1 ELSE 0 END) as international_txn_count_7d,
        
        -- Time pattern anomalies
        COUNT(CASE 
            WHEN EXTRACT(hour FROM transaction_timestamp) BETWEEN 2 AND 5 
                AND ABS(amount) > 200 THEN 1 
        END) as late_night_high_value_count,
        
        -- Geographic diversity
        COUNT(DISTINCT merchant_state) as unique_states_7d,
        COUNT(DISTINCT merchant_city) as unique_cities_7d,
        
        -- Device and channel patterns
        COUNT(DISTINCT d.device_id) as unique_devices_7d,
        MAX(d.suspicious_activity_flag) as device_flagged,
        
        -- Aggregated risk score
        LEAST(1.0, (
            MAX(CASE 
                WHEN ABS(amount) > avg_amount * 3 AND ABS(amount) > 100 THEN 1 
                ELSE 0 
            END) * 0.3 +
            MAX(CASE 
                WHEN DATEDIFF('minute', prev_txn_time, transaction_timestamp) < 5 
                    AND merchant_city != prev_merchant_city THEN 1 
                ELSE 0 
            END) * 0.4 +
            (COUNT(CASE 
                WHEN DATEDIFF('minute', prev_txn_time, transaction_timestamp) < 1 THEN 1 
            END) > 3) * 0.2 +
            (SUM(CASE WHEN merchant_category IN ('7995', '5933', '6010', '6011') THEN 1 ELSE 0 END) > 5) * 0.1
        )) as fraud_risk_score,
        
        -- Feature vector
        OBJECT_CONSTRUCT(
            'unusual_amount', MAX(CASE 
                WHEN ABS(amount) > avg_amount * 3 AND ABS(amount) > 100 THEN 1 
                ELSE 0 
            END),
            'impossible_travel', MAX(CASE 
                WHEN DATEDIFF('minute', prev_txn_time, transaction_timestamp) < 5 
                    AND merchant_city != prev_merchant_city THEN 1 
                ELSE 0 
            END),
            'rapid_fire_count', COUNT(CASE 
                WHEN DATEDIFF('minute', prev_txn_time, transaction_timestamp) < 1 THEN 1 
            END),
            'risky_merchants', SUM(CASE WHEN merchant_category IN ('7995', '5933', '6010', '6011') THEN 1 ELSE 0 END),
            'risk_score', LEAST(1.0, (
                MAX(CASE 
                    WHEN ABS(amount) > avg_amount * 3 AND ABS(amount) > 100 THEN 1 
                    ELSE 0 
                END) * 0.3 +
                MAX(CASE 
                    WHEN DATEDIFF('minute', prev_txn_time, transaction_timestamp) < 5 
                        AND merchant_city != prev_merchant_city THEN 1 
                    ELSE 0 
                END) * 0.4 +
                (COUNT(CASE 
                    WHEN DATEDIFF('minute', prev_txn_time, transaction_timestamp) < 1 THEN 1 
                END) > 3) * 0.2 +
                (SUM(CASE WHEN merchant_category IN ('7995', '5933', '6010', '6011') THEN 1 ELSE 0 END) > 5) * 0.1
            ))
        ) as fraud_features
        
    FROM recent_transactions t
    LEFT JOIN (
        SELECT customer_id, AVG(ABS(amount)) as avg_amount
        FROM VARO_INTELLIGENCE.RAW.TRANSACTIONS
        WHERE transaction_date >= DATEADD('day', -30, CURRENT_DATE())
        GROUP BY customer_id
    ) avg_txn ON t.customer_id = avg_txn.customer_id
    LEFT JOIN VARO_INTELLIGENCE.RAW.DEVICE_SESSIONS d 
        ON t.customer_id = d.customer_id 
        AND DATE(d.session_start) = t.transaction_date
    GROUP BY t.customer_id;

-- ============================================================================
-- Create Point-in-Time Feature Retrieval Function
-- Replaces Tecton's get_historical_features()
-- ============================================================================
CREATE OR REPLACE FUNCTION GET_POINT_IN_TIME_FEATURES(
    entity_ids ARRAY,
    feature_names ARRAY,
    timestamp_column TIMESTAMP_NTZ
)
RETURNS TABLE (
    entity_id VARCHAR,
    timestamp TIMESTAMP_NTZ,
    features VARIANT
)
AS
$$
    WITH pit_features AS (
        SELECT 
            fv.entity_id,
            fv.feature_timestamp,
            OBJECT_AGG(fv.feature_id, fv.feature_value) as features
        FROM FEATURE_VALUES fv
        WHERE fv.entity_id IN (SELECT VALUE FROM TABLE(FLATTEN(INPUT => entity_ids)))
            AND fv.feature_id IN (SELECT VALUE FROM TABLE(FLATTEN(INPUT => feature_names)))
            AND fv.feature_timestamp <= timestamp_column
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY fv.entity_id, fv.feature_id 
            ORDER BY fv.feature_timestamp DESC
        ) = 1
        GROUP BY fv.entity_id, fv.feature_timestamp
    )
    SELECT 
        entity_id,
        timestamp_column as timestamp,
        features
    FROM pit_features
$$;

-- ============================================================================
-- Create Training Dataset Generation Procedure
-- ============================================================================
CREATE OR REPLACE PROCEDURE CREATE_TRAINING_DATASET(
    dataset_name VARCHAR,
    feature_set_id VARCHAR,
    start_date DATE,
    end_date DATE,
    label_sql VARCHAR,
    stratify_column VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    dataset_id VARCHAR;
    table_name VARCHAR;
    row_count INTEGER;
BEGIN
    -- Generate unique dataset ID
    dataset_id := 'DS_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    table_name := 'TRAINING_' || dataset_id;
    
    -- Create training dataset table
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || table_name || ' AS
        WITH labels AS (' || label_sql || '),
        features AS (
            SELECT 
                entity_id,
                timestamp,
                features
            FROM TABLE(GET_POINT_IN_TIME_FEATURES(
                ARRAY_AGG(entity_id),
                (SELECT feature_ids FROM FEATURE_SETS WHERE feature_set_id = ''' || feature_set_id || '''),
                timestamp
            ))
        )
        SELECT 
            l.*,
            f.features
        FROM labels l
        JOIN features f ON l.entity_id = f.entity_id AND l.timestamp = f.timestamp
        WHERE l.timestamp BETWEEN ''' || start_date || ''' AND ''' || end_date || '''';
    
    -- Get row count from created table
    LET row_count_result RESULTSET := (SELECT COUNT(*) as cnt FROM IDENTIFIER(:table_name));
    LET cursor1 CURSOR FOR row_count_result;
    OPEN cursor1;
    FETCH cursor1 INTO row_count;
    CLOSE cursor1;
    
    -- Log dataset creation
    INSERT INTO TRAINING_DATASETS VALUES (
        dataset_id,
        dataset_name,
        NULL, -- model_name to be updated later
        feature_set_id,
        label_sql,
        start_date,
        end_date,
        (SELECT COUNT(DISTINCT entity_id) FROM IDENTIFIER(:table_name)),
        row_count,
        table_name,
        stratify_column IS NOT NULL,
        0.8, -- default train/test split
        CURRENT_USER(),
        CURRENT_TIMESTAMP()
    );
    
    RETURN 'Training dataset created: ' || table_name || ' with ' || row_count || ' rows';
END;
$$;

-- ============================================================================
-- Create Feature Importance Tracking
-- ============================================================================
CREATE OR REPLACE PROCEDURE UPDATE_FEATURE_IMPORTANCE(
    model_name VARCHAR,
    model_version VARCHAR,
    importance_data VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    feature_count INTEGER := 0;
BEGIN
    -- Parse importance data and update MODEL_FEATURES table
    INSERT INTO MODEL_FEATURES 
    SELECT
        'MF_' || model_name || '_' || feature_name || '_' || CURRENT_TIMESTAMP(),
        model_name,
        model_version,
        feature_name,
        importance_value,
        'INPUT',
        CURRENT_TIMESTAMP()
    FROM (
        SELECT 
            KEY as feature_name,
            VALUE::NUMBER(5,4) as importance_value
        FROM TABLE(FLATTEN(input => importance_data))
    );
    
    -- Get count from FLATTEN result
    SELECT COUNT(*) INTO feature_count
    FROM TABLE(FLATTEN(input => importance_data));
    
    RETURN 'Updated importance for ' || feature_count || ' features';
END;
$$;

-- ============================================================================
-- Enable Tasks
-- ============================================================================
ALTER TASK COMPUTE_DAILY_FEATURES RESUME;
ALTER TASK COMPUTE_STREAMING_FEATURES RESUME;

-- Display confirmation
SELECT 'Feature engineering infrastructure created successfully' AS STATUS;
