# Varo Feature Store Migration Guide

## From Tecton to Snowflake Native Feature Store

This guide provides detailed patterns for migrating from Tecton to Snowflake's SQL-first Feature Store, addressing all requirements from the Varo consultation.

---

## Executive Summary

Snowflake's native Feature Store provides:
- **100% SQL-based feature definitions** (no Python/Scala required)
- **Automatic scaling** with serverless compute
- **Built-in streaming** via Snowflake Streams
- **Time-travel backfill** capabilities
- **Lower TCO** than Tecton/Databricks solutions

---

## Migration Patterns

### 1. Feature Definition Migration

#### Tecton Pattern (Python/Spark)
```python
from tecton import batch_feature_view, Aggregation
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[transactions_source],
    entities=[customer],
    mode='spark_sql',
    aggregations=[
        Aggregation(column='amount', function='sum', time_window=timedelta(days=30)),
        Aggregation(column='amount', function='count', time_window=timedelta(days=30))
    ],
    ttl=timedelta(days=90)
)
def customer_transaction_features(transactions):
    return f"""
    SELECT 
        customer_id,
        timestamp,
        amount,
        merchant_category
    FROM {transactions}
    """
```

#### Snowflake Pattern (Pure SQL)
```sql
-- Create as Dynamic Table for automatic refresh
CREATE OR REPLACE DYNAMIC TABLE CUSTOMER_TRANSACTION_FEATURES
    TARGET_LAG = '1 HOUR'  -- Equivalent to Tecton's freshness
    WAREHOUSE = VARO_FEATURE_WH
    AS
    SELECT
        customer_id,
        CURRENT_TIMESTAMP() as feature_timestamp,
        -- 30-day aggregations
        SUM(amount) FILTER (WHERE transaction_date >= DATEADD('day', -30, CURRENT_DATE())) 
            as amount_sum_30d,
        COUNT(*) FILTER (WHERE transaction_date >= DATEADD('day', -30, CURRENT_DATE())) 
            as transaction_count_30d,
        -- Additional aggregations
        AVG(amount) FILTER (WHERE transaction_date >= DATEADD('day', -30, CURRENT_DATE())) 
            as amount_avg_30d,
        COUNT(DISTINCT merchant_category) FILTER (WHERE transaction_date >= DATEADD('day', -30, CURRENT_DATE())) 
            as unique_merchants_30d
    FROM RAW.TRANSACTIONS
    WHERE transaction_date >= DATEADD('day', -90, CURRENT_DATE())  -- TTL equivalent
    GROUP BY customer_id;
```

### 2. Streaming Feature Migration

#### Tecton Pattern
```python
@stream_feature_view(
    source=transactions_stream,
    entities=[customer],
    ttl=timedelta(hours=1)
)
def real_time_transaction_counts(transactions):
    return f"""
    SELECT
        customer_id,
        COUNT(*) as transaction_count_1h,
        MAX(amount) as max_amount_1h,
        timestamp
    FROM {transactions}
    GROUP BY customer_id, TUMBLE(timestamp, INTERVAL 1 HOUR)
    """
```

#### Snowflake Pattern
```sql
-- Create Stream on source table
CREATE OR REPLACE STREAM TRANSACTIONS_STREAM ON TABLE RAW.TRANSACTIONS;

-- Create Task for streaming computation
CREATE OR REPLACE TASK COMPUTE_REALTIME_FEATURES
    WAREHOUSE = VARO_FEATURE_WH
    SCHEDULE = '1 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('TRANSACTIONS_STREAM')
AS
    MERGE INTO ONLINE_FEATURES t
    USING (
        SELECT 
            customer_id,
            OBJECT_CONSTRUCT(
                'transaction_count_1h', COUNT(*),
                'max_amount_1h', MAX(amount),
                'last_transaction_time', MAX(transaction_timestamp)
            ) as feature_vector
        FROM TRANSACTIONS_STREAM
        WHERE METADATA$ACTION = 'INSERT'
            AND transaction_timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        GROUP BY customer_id
    ) s
    ON t.entity_id = s.customer_id AND t.entity_type = 'CUSTOMER'
    WHEN MATCHED THEN UPDATE SET
        t.feature_vector = OBJECT_INSERT(t.feature_vector, 
            'transaction_count_1h', s.feature_vector:transaction_count_1h,
            'max_amount_1h', s.feature_vector:max_amount_1h,
            TRUE),
        t.last_updated = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (entity_id, entity_type, feature_vector, last_updated)
        VALUES (s.customer_id, 'CUSTOMER', s.feature_vector, CURRENT_TIMESTAMP());
```

### 3. Point-in-Time Feature Retrieval

#### Tecton Pattern
```python
# Get historical features for training
training_data = customer_transaction_features.get_historical_features(
    entities=training_labels,
    timestamp_key='event_timestamp',
    start_time=datetime(2024, 1, 1),
    end_time=datetime(2024, 6, 30)
)
```

#### Snowflake Pattern
```sql
-- Using Time Travel for point-in-time correctness
CREATE OR REPLACE FUNCTION GET_POINT_IN_TIME_FEATURES(
    entity_ids ARRAY,
    feature_names ARRAY,
    as_of_timestamp TIMESTAMP_NTZ
)
RETURNS TABLE (
    entity_id VARCHAR,
    features VARIANT
)
AS
$$
    SELECT 
        fv.entity_id,
        OBJECT_AGG(fv.feature_id, fv.feature_value) as features
    FROM FEATURE_VALUES AS fv
    WHERE fv.entity_id IN (SELECT VALUE FROM TABLE(FLATTEN(INPUT => entity_ids)))
        AND fv.feature_id IN (SELECT VALUE FROM TABLE(FLATTEN(INPUT => feature_names)))
        AND fv.feature_timestamp <= as_of_timestamp
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fv.entity_id, fv.feature_id 
        ORDER BY fv.feature_timestamp DESC
    ) = 1
    GROUP BY fv.entity_id
$$;
```

### 4. Online Serving Migration

#### Tecton Pattern
```python
# Tecton Feature Server
feature_service = FeatureService(
    name='fraud_detection_v1',
    features=[
        customer_transaction_features,
        customer_profile_features
    ]
)

# Client code
features = feature_service.get_online_features(
    customer_id='CUST123'
)
```

#### Snowflake Pattern
```sql
-- Create External Function for REST API
CREATE OR REPLACE API INTEGRATION FEATURE_SERVING_API
    API_PROVIDER = aws_api_gateway
    API_AWS_ROLE_ARN = 'arn:aws:iam::123456:role/FeatureServingRole'
    API_ALLOWED_PREFIXES = ('https://api.varo-features.com/')
    ENABLED = TRUE;

CREATE OR REPLACE EXTERNAL FUNCTION GET_CUSTOMER_FEATURES_RT(customer_id VARCHAR)
    RETURNS VARIANT
    API_INTEGRATION = FEATURE_SERVING_API
    AS 'https://api.varo-features.com/v1/features';

-- Or direct query for lower latency
CREATE OR REPLACE FUNCTION GET_CUSTOMER_FEATURES_DIRECT(customer_id VARCHAR)
RETURNS VARIANT
AS
$$
    SELECT feature_vector
    FROM ONLINE_FEATURES
    WHERE entity_id = customer_id 
        AND entity_type = 'CUSTOMER'
$$;
```

---

## Performance Optimization

### 1. Feature Computation

**Dynamic Tables** for batch features:
```sql
-- Optimize refresh frequency based on use case
ALTER DYNAMIC TABLE CUSTOMER_FEATURES SET TARGET_LAG = '30 MINUTES';

-- Monitor refresh performance
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    TABLE_NAME => 'CUSTOMER_FEATURES'
));
```

**Clustering** for query performance:
```sql
-- Cluster by common query patterns
ALTER TABLE FEATURE_VALUES CLUSTER BY (entity_type, entity_id, feature_timestamp);
```

### 2. Real-Time Serving

**Search Optimization** for point lookups:
```sql
ALTER TABLE ONLINE_FEATURES ADD SEARCH OPTIMIZATION ON (entity_id);
```

**Result Caching**:
```sql
-- Enable automatic caching
ALTER SESSION SET USE_CACHED_RESULT = TRUE;
```

---

## Cost Optimization

### 1. Warehouse Management

```sql
-- Create dedicated warehouses by workload
CREATE WAREHOUSE FEATURE_BATCH_WH 
    WAREHOUSE_SIZE = 'MEDIUM'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    AUTO_SUSPEND = 60;

CREATE WAREHOUSE FEATURE_SERVING_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    MIN_CLUSTER_COUNT = 2
    MAX_CLUSTER_COUNT = 5
    AUTO_SUSPEND = 300;  -- Keep warm for serving
```

### 2. Data Retention

```sql
-- Set retention based on feature usage
ALTER TABLE FEATURE_VALUES SET DATA_RETENTION_TIME_IN_DAYS = 90;

-- Archive old features
CREATE TABLE FEATURE_VALUES_ARCHIVE CLONE FEATURE_VALUES;
DELETE FROM FEATURE_VALUES WHERE feature_timestamp < DATEADD('day', -90, CURRENT_DATE());
```

---

## Monitoring and Alerting

### 1. Feature Freshness Monitoring

```sql
CREATE OR REPLACE VIEW FEATURE_FRESHNESS_MONITOR AS
SELECT 
    feature_id,
    MAX(feature_timestamp) as last_computed,
    DATEDIFF('minute', MAX(feature_timestamp), CURRENT_TIMESTAMP()) as minutes_stale,
    CASE 
        WHEN minutes_stale > 120 THEN 'CRITICAL'
        WHEN minutes_stale > 60 THEN 'WARNING'
        ELSE 'OK'
    END as freshness_status
FROM FEATURE_VALUES
GROUP BY feature_id;

-- Create alert
CREATE OR REPLACE ALERT FEATURE_STALENESS_ALERT
    WAREHOUSE = VARO_WH
    SCHEDULE = '30 MINUTES'
    IF (EXISTS (
        SELECT 1 FROM FEATURE_FRESHNESS_MONITOR 
        WHERE freshness_status = 'CRITICAL'
    ))
    THEN CALL SYSTEM$SEND_EMAIL(
        'ml-team@varo.com',
        'Feature Freshness Alert',
        'Critical features are stale. Check Feature Store dashboard.'
    );
```

### 2. Drift Detection

```sql
CREATE OR REPLACE PROCEDURE DETECT_FEATURE_DRIFT(
    feature_id VARCHAR,
    lookback_days INTEGER DEFAULT 7
)
RETURNS TABLE (drift_score FLOAT, alert_required BOOLEAN)
AS
$$
DECLARE
    baseline_stats VARIANT;
    current_stats VARIANT;
    drift_threshold FLOAT := 0.15;
BEGIN
    -- Get baseline statistics
    SELECT OBJECT_CONSTRUCT(
        'mean', AVG(feature_value),
        'stddev', STDDEV(feature_value),
        'min', MIN(feature_value),
        'max', MAX(feature_value)
    ) INTO baseline_stats
    FROM FEATURE_VALUES
    WHERE feature_id = :feature_id
        AND feature_timestamp BETWEEN 
            DATEADD('day', -2 * :lookback_days, CURRENT_DATE())
            AND DATEADD('day', -:lookback_days, CURRENT_DATE());
    
    -- Get current statistics
    SELECT OBJECT_CONSTRUCT(
        'mean', AVG(feature_value),
        'stddev', STDDEV(feature_value),
        'min', MIN(feature_value),
        'max', MAX(feature_value)
    ) INTO current_stats
    FROM FEATURE_VALUES
    WHERE feature_id = :feature_id
        AND feature_timestamp >= DATEADD('day', -:lookback_days, CURRENT_DATE());
    
    -- Calculate drift score (simplified KS statistic)
    RETURN TABLE(
        SELECT 
            ABS(baseline_stats:mean - current_stats:mean) / baseline_stats:stddev as drift_score,
            drift_score > drift_threshold as alert_required
    );
END;
$$;
```

---

## Best Practices

### 1. Feature Naming Conventions

```sql
-- Use consistent naming: {entity}_{metric}_{window}
-- Examples:
customer_transaction_count_30d
customer_revenue_sum_7d
merchant_fraud_rate_24h
account_balance_avg_90d
```

### 2. Feature Documentation

```sql
-- Document features in FEATURE_DEFINITIONS
INSERT INTO FEATURE_DEFINITIONS VALUES (
    'customer_transaction_velocity',
    'Customer Transaction Velocity',
    'risk_indicators',
    'Number of transactions per hour, used for fraud detection',
    'NUMBER',
    'STREAMING',
    'SELECT customer_id, COUNT(*) as value FROM transactions 
     WHERE timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP()) 
     GROUP BY customer_id',
    ARRAY_CONSTRUCT('transactions'),
    'REAL_TIME',
    1,
    TRUE,
    CURRENT_USER(),
    CURRENT_TIMESTAMP()
);
```

### 3. Testing Framework

```sql
-- Create test cases for features
CREATE OR REPLACE PROCEDURE TEST_FEATURE_COMPUTATION(feature_id VARCHAR)
RETURNS VARCHAR
AS
$$
DECLARE
    expected_count INTEGER;
    actual_count INTEGER;
    test_result VARCHAR;
BEGIN
    -- Test data completeness
    SELECT COUNT(*) INTO expected_count
    FROM (SELECT DISTINCT entity_id FROM RAW.CUSTOMERS WHERE customer_status = 'ACTIVE');
    
    SELECT COUNT(DISTINCT entity_id) INTO actual_count
    FROM FEATURE_VALUES
    WHERE feature_id = :feature_id
        AND feature_timestamp >= DATEADD('hour', -2, CURRENT_TIMESTAMP());
    
    IF actual_count >= expected_count * 0.95 THEN
        test_result := 'PASSED: ' || actual_count || ' of ' || expected_count || ' entities have features';
    ELSE
        test_result := 'FAILED: Only ' || actual_count || ' of ' || expected_count || ' entities have features';
    END IF;
    
    RETURN test_result;
END;
$$;
```

---

## Migration Checklist

- [ ] Map all Tecton feature views to Snowflake Dynamic Tables
- [ ] Convert streaming features to Streams + Tasks
- [ ] Implement point-in-time retrieval functions
- [ ] Set up online serving (External Functions or direct queries)
- [ ] Configure monitoring and alerting
- [ ] Test feature computation performance
- [ ] Validate feature values against Tecton baseline
- [ ] Set up CI/CD for feature deployment
- [ ] Train team on SQL-based feature engineering
- [ ] Document all features and dependencies

---

## Support Resources

- Snowflake Dynamic Tables: https://docs.snowflake.com/en/user-guide/dynamic-tables
- Streams and Tasks: https://docs.snowflake.com/en/user-guide/streams-intro
- External Functions: https://docs.snowflake.com/en/sql-reference/external-functions
- Model Registry: https://docs.snowflake.com/en/developer-guide/snowpark-ml/model-registry
