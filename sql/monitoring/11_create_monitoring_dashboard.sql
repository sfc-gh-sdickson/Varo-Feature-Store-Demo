-- ============================================================================
-- Varo Feature Store & Intelligence Agent - Monitoring Dashboard
-- ============================================================================
-- Purpose: Create monitoring views and dashboards for system health,
--          feature quality, and ML model performance
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE VARO_WH;

-- ============================================================================
-- 1. Feature Store Health Dashboard
-- ============================================================================
CREATE OR REPLACE VIEW V_FEATURE_STORE_HEALTH AS
WITH feature_metrics AS (
    SELECT 
        fd.feature_id,
        fd.feature_name,
        fd.feature_group,
        fd.computation_type,
        fd.refresh_frequency,
        -- Get latest computation stats
        fcl.compute_end as last_computed,
        fcl.rows_processed as last_rows_processed,
        fcl.compute_status as last_status,
        DATEDIFF('minute', fcl.compute_end, CURRENT_TIMESTAMP()) as minutes_since_update,
        -- Get feature statistics
        fs.mean_value as feature_mean,
        fs.std_deviation as feature_stddev,
        fs.null_count,
        fs.total_count,
        DIV0NULL(fs.null_count, fs.total_count) as null_rate
    FROM FEATURE_STORE.FEATURE_DEFINITIONS fd
    LEFT JOIN (
        SELECT * FROM FEATURE_STORE.FEATURE_COMPUTE_LOGS
        QUALIFY ROW_NUMBER() OVER (PARTITION BY feature_id ORDER BY compute_end DESC) = 1
    ) fcl ON fd.feature_id = fcl.feature_id
    LEFT JOIN (
        SELECT * FROM FEATURE_STORE.FEATURE_STATISTICS
        QUALIFY ROW_NUMBER() OVER (PARTITION BY feature_id ORDER BY computation_date DESC) = 1
    ) fs ON fd.feature_id = fs.feature_id
    WHERE fd.is_active = TRUE
)
SELECT 
    feature_id,
    feature_name,
    feature_group,
    computation_type,
    refresh_frequency,
    last_computed,
    minutes_since_update,
    -- Feature health status
    CASE 
        WHEN last_status != 'SUCCESS' THEN 'ERROR'
        WHEN computation_type = 'REAL_TIME' AND minutes_since_update > 5 THEN 'CRITICAL'
        WHEN computation_type = 'STREAMING' AND minutes_since_update > 30 THEN 'WARNING'
        WHEN computation_type = 'BATCH' AND refresh_frequency = 'HOURLY' AND minutes_since_update > 90 THEN 'WARNING'
        WHEN computation_type = 'BATCH' AND refresh_frequency = 'DAILY' AND minutes_since_update > 1500 THEN 'WARNING'
        ELSE 'HEALTHY'
    END as health_status,
    -- Data quality status
    CASE
        WHEN null_rate > 0.5 THEN 'HIGH_NULLS'
        WHEN null_rate > 0.1 THEN 'MODERATE_NULLS'
        ELSE 'GOOD'
    END as data_quality,
    last_rows_processed,
    feature_mean,
    feature_stddev,
    null_rate
FROM feature_metrics
ORDER BY 
    CASE health_status 
        WHEN 'ERROR' THEN 1
        WHEN 'CRITICAL' THEN 2
        WHEN 'WARNING' THEN 3
        ELSE 4
    END,
    minutes_since_update DESC;

-- ============================================================================
-- 2. Feature Drift Monitoring Dashboard
-- ============================================================================
CREATE OR REPLACE VIEW V_FEATURE_DRIFT_MONITOR AS
WITH feature_baseline AS (
    -- Baseline statistics from 7-30 days ago
    SELECT 
        feature_id,
        AVG(mean_value) as baseline_mean,
        AVG(std_deviation) as baseline_stddev,
        AVG(percentile_50) as baseline_median,
        COUNT(*) as baseline_days
    FROM FEATURE_STORE.FEATURE_STATISTICS
    WHERE computation_date BETWEEN DATEADD('day', -30, CURRENT_DATE()) 
        AND DATEADD('day', -7, CURRENT_DATE())
    GROUP BY feature_id
),
recent_stats AS (
    -- Recent statistics from last 7 days
    SELECT 
        feature_id,
        AVG(mean_value) as recent_mean,
        AVG(std_deviation) as recent_stddev,
        AVG(percentile_50) as recent_median,
        MAX(mean_value) - MIN(mean_value) as recent_range,
        COUNT(*) as recent_days
    FROM FEATURE_STORE.FEATURE_STATISTICS
    WHERE computation_date >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY feature_id
)
SELECT 
    fd.feature_id,
    fd.feature_name,
    fd.feature_group,
    -- Drift metrics
    b.baseline_mean,
    r.recent_mean,
    ABS(r.recent_mean - b.baseline_mean) as mean_shift,
    DIV0NULL(ABS(r.recent_mean - b.baseline_mean), b.baseline_stddev) as normalized_shift,
    -- Drift detection
    CASE
        WHEN normalized_shift > 3 THEN 'HIGH_DRIFT'
        WHEN normalized_shift > 2 THEN 'MODERATE_DRIFT'
        WHEN normalized_shift > 1 THEN 'MINOR_DRIFT'
        ELSE 'STABLE'
    END as drift_status,
    -- Stability metrics
    DIV0NULL(r.recent_stddev, b.baseline_stddev) as variance_ratio,
    r.recent_range as recent_volatility,
    -- Sample counts
    b.baseline_days,
    r.recent_days
FROM FEATURE_STORE.FEATURE_DEFINITIONS fd
JOIN feature_baseline b ON fd.feature_id = b.feature_id
JOIN recent_stats r ON fd.feature_id = r.feature_id
WHERE fd.is_active = TRUE
ORDER BY normalized_shift DESC;

-- ============================================================================
-- 3. ML Model Performance Dashboard
-- ============================================================================
CREATE OR REPLACE VIEW V_ML_MODEL_PERFORMANCE AS
WITH model_predictions AS (
    -- Aggregate predictions by model and date
    SELECT 
        mf.model_name,
        mf.model_version,
        DATE(t.transaction_date) as prediction_date,
        COUNT(*) as prediction_count,
        -- Fraud detection metrics (based on fraud_score)
        SUM(CASE WHEN t.fraud_score > 0.7 THEN 1 ELSE 0 END) as high_risk_flagged,
        SUM(CASE WHEN t.fraud_score > 0.7 AND t.status = 'DECLINED' THEN 1 ELSE 0 END) as high_risk_declined,
        SUM(CASE WHEN t.fraud_score <= 0.3 AND t.status = 'DECLINED' THEN 1 ELSE 0 END) as low_risk_declined,
        AVG(t.fraud_score) as avg_risk_score
    FROM RAW.TRANSACTIONS t
    CROSS JOIN FEATURE_STORE.MODEL_FEATURES mf
    WHERE mf.model_name = 'FRAUD_DETECTION_MODEL'
      AND t.transaction_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY mf.model_name, mf.model_version, DATE(t.transaction_date)
)
SELECT 
    model_name,
    model_version,
    prediction_date,
    prediction_count,
    high_risk_flagged,
    DIV0NULL(high_risk_flagged, prediction_count) as flag_rate,
    high_risk_declined,
    DIV0NULL(high_risk_declined, high_risk_flagged) as precision,
    low_risk_declined,
    avg_risk_score,
    -- Performance trend
    avg_risk_score - LAG(avg_risk_score, 7) OVER (PARTITION BY model_name ORDER BY prediction_date) as score_change_7d
FROM model_predictions
ORDER BY model_name, prediction_date DESC;

-- ============================================================================
-- 4. Real-Time Feature Serving Dashboard
-- ============================================================================
CREATE OR REPLACE VIEW V_FEATURE_SERVING_METRICS AS
WITH serving_logs AS (
    SELECT 
        DATE_TRUNC('hour', last_updated) as serving_hour,
        entity_type,
        COUNT(*) as requests_served,
        AVG(DATEDIFF('millisecond', last_updated, CURRENT_TIMESTAMP())) as avg_latency_ms,
        MIN(last_updated) as first_request,
        MAX(last_updated) as last_request
    FROM FEATURE_STORE.ONLINE_FEATURES
    WHERE last_updated >= DATEADD('day', -1, CURRENT_TIMESTAMP())
    GROUP BY DATE_TRUNC('hour', last_updated), entity_type
)
SELECT 
    serving_hour,
    entity_type,
    requests_served,
    avg_latency_ms,
    CASE
        WHEN avg_latency_ms < 10 THEN 'EXCELLENT'
        WHEN avg_latency_ms < 50 THEN 'GOOD'
        WHEN avg_latency_ms < 100 THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END as latency_rating,
    requests_served / 3600.0 as requests_per_second,
    DATEDIFF('minute', last_request, CURRENT_TIMESTAMP()) as minutes_since_last_request
FROM serving_logs
ORDER BY serving_hour DESC;

-- ============================================================================
-- 5. System Resource Usage Dashboard
-- ============================================================================
CREATE OR REPLACE VIEW V_SYSTEM_RESOURCE_USAGE AS
WITH warehouse_usage AS (
    SELECT 
        warehouse_name,
        DATE(start_time) as usage_date,
        SUM(credits_used) as daily_credits,
        COUNT(DISTINCT query_id) as query_count,
        AVG(execution_time) / 1000 as avg_query_time_seconds,
        MAX(execution_time) / 1000 as max_query_time_seconds
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE warehouse_name IN ('VARO_WH', 'VARO_FEATURE_WH')
        AND start_time >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY warehouse_name, DATE(start_time)
),
table_storage AS (
    SELECT 
        table_schema,
        SUM(bytes) / POWER(1024, 3) as storage_gb,
        SUM(row_count) as total_rows,
        COUNT(*) as table_count
    FROM INFORMATION_SCHEMA.TABLES
    WHERE table_catalog = 'VARO_INTELLIGENCE'
        AND table_type = 'BASE TABLE'
    GROUP BY table_schema
)
SELECT 
    'Warehouse Usage' as metric_type,
    w.warehouse_name as resource_name,
    w.usage_date::VARCHAR as time_period,
    w.daily_credits as value,
    'credits' as unit,
    w.query_count::VARCHAR as additional_info
FROM warehouse_usage w
UNION ALL
SELECT 
    'Storage Usage' as metric_type,
    t.table_schema as resource_name,
    'Current' as time_period,
    t.storage_gb as value,
    'GB' as unit,
    t.total_rows || ' rows in ' || t.table_count || ' tables' as additional_info
FROM table_storage t
ORDER BY metric_type, time_period DESC;

-- ============================================================================
-- 6. Data Quality Alerts Dashboard
-- ============================================================================
CREATE OR REPLACE VIEW V_DATA_QUALITY_ALERTS AS
WITH quality_checks AS (
    -- Check for stale features
    SELECT 
        'Stale Features' as alert_type,
        'WARNING' as severity,
        COUNT(*) as issue_count,
        'Features not updated within expected timeframe' as description,
        ARRAY_AGG(feature_name) as affected_items
    FROM V_FEATURE_STORE_HEALTH
    WHERE health_status IN ('WARNING', 'CRITICAL', 'ERROR')
    
    UNION ALL
    
    -- Check for high drift
    SELECT 
        'Feature Drift' as alert_type,
        'WARNING' as severity,
        COUNT(*) as issue_count,
        'Features showing significant distribution drift' as description,
        ARRAY_AGG(feature_name) as affected_items
    FROM V_FEATURE_DRIFT_MONITOR
    WHERE drift_status IN ('HIGH_DRIFT', 'MODERATE_DRIFT')
    
    UNION ALL
    
    -- Check for data completeness
    SELECT 
        'Data Completeness' as alert_type,
        'ERROR' as severity,
        COUNT(*) as issue_count,
        'Tables with unexpected low row counts' as description,
        ARRAY_AGG(table_name) as affected_items
    FROM (
        SELECT 
            table_name,
            row_count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_catalog = 'VARO_INTELLIGENCE'
            AND table_schema = 'RAW'
            AND table_type = 'BASE TABLE'
            AND row_count < 1000
            AND table_name IN ('CUSTOMERS', 'ACCOUNTS', 'TRANSACTIONS')
    )
)
SELECT 
    alert_type,
    severity,
    issue_count,
    description,
    ARRAY_TO_STRING(affected_items, ', ') as affected_items_list,
    CURRENT_TIMESTAMP() as alert_timestamp
FROM quality_checks
WHERE issue_count > 0
ORDER BY 
    CASE severity 
        WHEN 'ERROR' THEN 1
        WHEN 'WARNING' THEN 2
        ELSE 3
    END;

-- ============================================================================
-- 7. Business Metrics Dashboard
-- ============================================================================
CREATE OR REPLACE VIEW V_BUSINESS_METRICS_DASHBOARD AS
WITH daily_metrics AS (
    SELECT 
        DATE(transaction_date) as metric_date,
        -- Transaction metrics
        COUNT(DISTINCT t.customer_id) as daily_active_users,
        COUNT(*) as transaction_count,
        SUM(ABS(amount)) as transaction_volume,
        AVG(ABS(amount)) as avg_transaction_size,
        -- Fraud metrics
        SUM(CASE WHEN fraud_score > 0.7 THEN 1 ELSE 0 END) as high_risk_transactions,
        SUM(CASE WHEN fraud_score > 0.7 THEN ABS(amount) ELSE 0 END) as high_risk_volume,
        -- Cash advance metrics
        COUNT(DISTINCT ca.customer_id) as advance_users,
        SUM(ca.advance_amount) as advance_volume,
        AVG(ca.eligibility_score) as avg_eligibility_score
    FROM RAW.TRANSACTIONS t
    LEFT JOIN RAW.CASH_ADVANCES ca 
        ON DATE(t.transaction_date) = DATE(ca.advance_date)
    WHERE t.transaction_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY DATE(transaction_date)
)
SELECT 
    metric_date,
    daily_active_users,
    transaction_count,
    transaction_volume,
    avg_transaction_size,
    -- Risk metrics
    DIV0NULL(high_risk_transactions, transaction_count) * 100 as high_risk_transaction_pct,
    DIV0NULL(high_risk_volume, transaction_volume) * 100 as high_risk_volume_pct,
    -- Growth metrics
    daily_active_users - LAG(daily_active_users, 7) OVER (ORDER BY metric_date) as dau_change_wow,
    transaction_volume - LAG(transaction_volume, 7) OVER (ORDER BY metric_date) as volume_change_wow,
    -- Advance metrics
    advance_users,
    advance_volume,
    avg_eligibility_score
FROM daily_metrics
ORDER BY metric_date DESC;

-- ============================================================================
-- 8. Create Master Health Score
-- ============================================================================
CREATE OR REPLACE VIEW V_SYSTEM_HEALTH_SCORE AS
WITH health_components AS (
    -- Feature health (40% weight)
    SELECT 
        'Feature Store' as component,
        0.4 as weight,
        100 - (COUNT(CASE WHEN health_status != 'HEALTHY' THEN 1 END) * 5) as score
    FROM V_FEATURE_STORE_HEALTH
    
    UNION ALL
    
    -- Model performance (30% weight)
    SELECT 
        'ML Models' as component,
        0.3 as weight,
        AVG(CASE WHEN precision > 0.9 THEN 100 
             WHEN precision > 0.8 THEN 85
             WHEN precision > 0.7 THEN 70
             ELSE 50 END) as score
    FROM V_ML_MODEL_PERFORMANCE
    WHERE prediction_date = CURRENT_DATE() - 1
    
    UNION ALL
    
    -- System performance (20% weight)
    SELECT 
        'System Performance' as component,
        0.2 as weight,
        CASE 
            WHEN AVG(avg_latency_ms) < 50 THEN 100
            WHEN AVG(avg_latency_ms) < 100 THEN 85
            WHEN AVG(avg_latency_ms) < 200 THEN 70
            ELSE 50
        END as score
    FROM V_FEATURE_SERVING_METRICS
    WHERE serving_hour >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
    
    UNION ALL
    
    -- Data quality (10% weight)
    SELECT 
        'Data Quality' as component,
        0.1 as weight,
        100 - (COUNT(*) * 10) as score
    FROM V_DATA_QUALITY_ALERTS
)
SELECT 
    ROUND(SUM(score * weight)) as overall_health_score,
    CASE 
        WHEN overall_health_score >= 90 THEN 'EXCELLENT'
        WHEN overall_health_score >= 80 THEN 'GOOD'
        WHEN overall_health_score >= 70 THEN 'FAIR'
        ELSE 'POOR'
    END as health_status,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'component', component,
            'score', ROUND(score),
            'weight', weight
        )
    ) as component_scores,
    CURRENT_TIMESTAMP() as assessment_time
FROM health_components
GROUP BY ALL;

-- ============================================================================
-- Create Alert Procedure for Automated Monitoring
-- ============================================================================
CREATE OR REPLACE PROCEDURE MONITOR_SYSTEM_HEALTH()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    health_score NUMBER;
    critical_alerts NUMBER;
    message VARCHAR;
BEGIN
    -- Get overall health score
    SELECT overall_health_score INTO health_score
    FROM V_SYSTEM_HEALTH_SCORE;
    
    -- Count critical alerts
    SELECT COUNT(*) INTO critical_alerts
    FROM V_DATA_QUALITY_ALERTS
    WHERE severity = 'ERROR';
    
    -- Generate alert message
    IF health_score < 70 OR critical_alerts > 0 THEN
        message := 'ALERT: System health score is ' || health_score || 
                   ' with ' || critical_alerts || ' critical issues.';
        
        -- In production, this would send alerts via email/Slack
        -- CALL SYSTEM$SEND_EMAIL(...);
        
        RETURN message;
    ELSE
        RETURN 'System health is good. Score: ' || health_score;
    END IF;
END;
$$;

-- ============================================================================
-- Schedule Monitoring Task
-- ============================================================================
CREATE OR REPLACE TASK HOURLY_HEALTH_MONITORING
    WAREHOUSE = VARO_WH
    SCHEDULE = '60 MINUTES'
AS
    CALL MONITOR_SYSTEM_HEALTH();

-- Start the monitoring task
ALTER TASK HOURLY_HEALTH_MONITORING RESUME;

-- Display confirmation
SELECT 'Monitoring dashboard views and tasks created successfully' AS STATUS;
