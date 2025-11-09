-- ============================================================================
-- Varo Intelligence Agent - ML Model Wrapper Procedures
-- ============================================================================
-- Purpose: Create Python procedures that wrap ML models for banking predictions
--          These models integrate with the Feature Store for real-time inference
-- Pattern based on: Axon template Python procedures
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE VARO_FEATURE_WH;

-- ============================================================================
-- Procedure 1: Fraud Risk Scoring
-- ============================================================================

-- Drop if exists (in case it was created as FUNCTION before)
DROP FUNCTION IF EXISTS SCORE_TRANSACTION_FRAUD(VARCHAR, NUMBER, VARCHAR, BOOLEAN);

CREATE OR REPLACE PROCEDURE SCORE_TRANSACTION_FRAUD(
    customer_id VARCHAR,
    amount NUMBER,
    merchant_category VARCHAR,
    is_international BOOLEAN
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'score_fraud'
COMMENT = 'Scores transaction fraud risk using real-time features from Feature Store'
AS
$$
def score_fraud(session, customer_id, amount, merchant_category, is_international):
    import json
    
    # Get real-time customer features from Feature Store
    feature_query = f"""
        SELECT 
            feature_vector:txn_count_1h::NUMBER as velocity_1h,
            feature_vector:txn_volume_30d::NUMBER as monthly_volume,
            feature_vector:unique_merchants_30d::NUMBER as merchant_diversity,
            feature_vector:risk_indicator::NUMBER as historical_risk
        FROM FEATURE_STORE.ONLINE_FEATURES
    WHERE entity_id = '{customer_id}' AND entity_type = 'CUSTOMER'
    LIMIT 1
    """
    
    features = session.sql(feature_query).collect()
    
    if not features:
        return json.dumps({
            "fraud_probability": 0.5,
            "risk_level": "UNKNOWN",
            "reason_codes": ["NO_FEATURE_DATA"]
        })
    
    row = features[0]
    velocity_1h = row['VELOCITY_1H'] or 0
    monthly_volume = row['MONTHLY_VOLUME'] or 1000
    merchant_diversity = row['MERCHANT_DIVERSITY'] or 5
    historical_risk = row['HISTORICAL_RISK'] or 0
    
    # Calculate fraud probability
    fraud_prob = 0.1  # Base risk
    reason_codes = []
    
    if amount > monthly_volume * 0.5:
        fraud_prob += 0.3
        reason_codes.append('UNUSUAL_AMOUNT')
    
    if velocity_1h > 5:
        fraud_prob += 0.2
        reason_codes.append('HIGH_VELOCITY')
    
    if is_international:
        fraud_prob += 0.15
        reason_codes.append('INTERNATIONAL_TXN')
    
    if merchant_category in ['7995', '5933', '6010']:
        fraud_prob += 0.2
        reason_codes.append('RISKY_MERCHANT')
    
    fraud_prob += historical_risk * 0.15
    fraud_prob = min(1.0, max(0.0, fraud_prob))
    
    # Determine risk level
    if fraud_prob >= 0.7:
        risk_level = 'HIGH'
    elif fraud_prob >= 0.4:
        risk_level = 'MEDIUM'
    elif fraud_prob >= 0.2:
        risk_level = 'LOW'
    else:
        risk_level = 'MINIMAL'
    
    return json.dumps({
        "fraud_probability": round(fraud_prob, 4),
        "risk_level": risk_level,
        "reason_codes": reason_codes
    })
$$;

-- ============================================================================
-- Procedure 2: Cash Advance Eligibility
-- ============================================================================

-- Drop if exists
DROP FUNCTION IF EXISTS CALCULATE_ADVANCE_ELIGIBILITY(VARCHAR);

CREATE OR REPLACE PROCEDURE CALCULATE_ADVANCE_ELIGIBILITY(
    customer_id VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'calculate_eligibility'
COMMENT = 'Determines cash advance eligibility and limit using Feature Store data'
AS
$$
def calculate_eligibility(session, customer_id):
    import json
    
    # Get customer data
    query = f"""
        SELECT
            c.customer_id,
            c.credit_score,
            c.customer_status,
            c.risk_tier,
            COALESCE(dd.monthly_deposits, 0) as monthly_deposits,
            COALESCE(af.total_advances::NUMBER, 0) as total_advances,
            COALESCE(af.repayment_rate::NUMBER, 1.0) as repayment_rate,
            COALESCE(af.current_balance::NUMBER, 0) as current_advance_balance
        FROM RAW.CUSTOMERS c
        LEFT JOIN (
            SELECT 
                customer_id,
            SUM(amount) as monthly_deposits
            FROM RAW.DIRECT_DEPOSITS
            WHERE deposit_date >= DATEADD('month', -1, CURRENT_DATE())
            GROUP BY customer_id
        ) dd ON c.customer_id = dd.customer_id
        LEFT JOIN (
            SELECT 
                entity_id,
                feature_vector:total_advances as total_advances,
                feature_vector:repayment_rate as repayment_rate,
                feature_vector:current_balance as current_balance
            FROM FEATURE_STORE.ONLINE_FEATURES
        WHERE entity_type = 'CUSTOMER'
        ) af ON c.customer_id = af.entity_id
    WHERE c.customer_id = '{customer_id}'
    """
    
    result = session.sql(query).collect()
    
    if not result:
        return json.dumps({
            "is_eligible": False,
            "max_advance_amount": 0,
            "eligibility_score": 0.0,
            "decline_reasons": ["CUSTOMER_NOT_FOUND"]
        })
    
    row = result[0]
    customer_status = row['CUSTOMER_STATUS']
    current_advance_balance = row['CURRENT_ADVANCE_BALANCE']
    monthly_deposits = row['MONTHLY_DEPOSITS']
    credit_score = row['CREDIT_SCORE'] or 0
    risk_tier = row['RISK_TIER']
    total_advances = row['TOTAL_ADVANCES']
    repayment_rate = row['REPAYMENT_RATE']
    
    # Check eligibility
    decline_reasons = []
    is_eligible = True
    
    if customer_status != 'ACTIVE':
        is_eligible = False
        decline_reasons.append('ACCOUNT_NOT_ACTIVE')
    
    if current_advance_balance > 0:
        is_eligible = False
        decline_reasons.append('EXISTING_ADVANCE_ACTIVE')
    
    if monthly_deposits < 1000:
        is_eligible = False
        decline_reasons.append('INSUFFICIENT_DIRECT_DEPOSITS')
    
    if credit_score < 600:
        is_eligible = False
        decline_reasons.append('LOW_CREDIT_SCORE')
    
    if risk_tier == 'HIGH':
        is_eligible = False
        decline_reasons.append('HIGH_RISK_PROFILE')
    
    # Calculate eligibility score
    eligibility_score = 0.2  # Base score
    eligibility_score += 0.2 if credit_score >= 700 else credit_score / 3500.0
    eligibility_score += 0.2 if monthly_deposits >= 2000 else monthly_deposits / 10000.0
    eligibility_score += repayment_rate * 0.2
    eligibility_score += 0.2 if total_advances >= 5 and repayment_rate == 1.0 else 0.0
    eligibility_score = min(1.0, max(0.0, eligibility_score))
    
    # Calculate max advance amount
    if not is_eligible or repayment_rate < 0.8:
        max_advance_amount = 0
    elif total_advances == 0:
        max_advance_amount = min(100, monthly_deposits * 0.1)
    elif total_advances < 3:
        max_advance_amount = min(250, monthly_deposits * 0.15)
    elif repayment_rate == 1.0:
        max_advance_amount = min(500, monthly_deposits * 0.25)
    else:
        max_advance_amount = min(250, monthly_deposits * 0.15)
    
    return json.dumps({
        "is_eligible": is_eligible,
        "max_advance_amount": float(max_advance_amount),
        "eligibility_score": round(eligibility_score, 2),
        "decline_reasons": decline_reasons
    })
$$;

-- ============================================================================
-- Procedure 3: Credit Limit Recommendation
-- ============================================================================

-- Drop if exists
DROP FUNCTION IF EXISTS RECOMMEND_CREDIT_LIMIT(VARCHAR, VARCHAR);

CREATE OR REPLACE PROCEDURE RECOMMEND_CREDIT_LIMIT(
    customer_id VARCHAR,
    product_type VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'recommend_limit'
COMMENT = 'Recommends credit limit based on customer profile and behavior'
AS
$$
def recommend_limit(session, customer_id, product_type):
    import json
    
    query = f"""
        SELECT
            c.customer_id,
            c.credit_score,
            c.income_verified,
            c.employment_status,
            COALESCE(t.monthly_spend, 0) as monthly_spend,
            COALESCE(t.transaction_count, 0) as transaction_count,
            COALESCE(cc.current_utilization, 0) as current_utilization,
        COALESCE(cc.payment_history, 1.0) as payment_history
        FROM RAW.CUSTOMERS c
        LEFT JOIN (
            SELECT 
                customer_id,
                SUM(ABS(amount)) as monthly_spend,
                COUNT(*) as transaction_count
            FROM RAW.TRANSACTIONS
            WHERE transaction_type = 'DEBIT'
                AND transaction_date >= DATEADD('month', -3, CURRENT_DATE())
            GROUP BY customer_id
        ) t ON c.customer_id = t.customer_id
        LEFT JOIN (
            SELECT 
                customer_id,
                AVG(CASE WHEN credit_limit > 0 THEN ABS(current_balance) / credit_limit ELSE 0 END) as current_utilization,
            1.0 as payment_history
            FROM RAW.ACCOUNTS
            WHERE account_type IN ('BELIEVE_CARD', 'LINE_OF_CREDIT')
            GROUP BY customer_id
        ) cc ON c.customer_id = cc.customer_id
    WHERE c.customer_id = '{customer_id}'
    """
    
    result = session.sql(query).collect()
    
    if not result:
        return json.dumps({
            "recommended_limit": 0,
            "risk_adjusted_limit": 0,
            "utilization_forecast": 0.0,
            "confidence_score": 0.0
        })
    
    row = result[0]
    credit_score = row['CREDIT_SCORE'] or 600
    income_verified = row['INCOME_VERIFIED'] or 0
    monthly_spend = row['MONTHLY_SPEND']
    transaction_count = row['TRANSACTION_COUNT']
    payment_history = row['PAYMENT_HISTORY']
    
    # Calculate base limit
    if credit_score < 600:
        recommended_limit = 500
    elif credit_score < 650:
        recommended_limit = income_verified * 0.02
    elif credit_score < 700:
        recommended_limit = income_verified * 0.05
    elif credit_score < 750:
        recommended_limit = income_verified * 0.08
    else:
        recommended_limit = income_verified * 0.10
    
    # Risk-adjusted limit
    if product_type == 'BELIEVE_CARD':
        if monthly_spend > 0:
            risk_adjusted_limit = min(recommended_limit, monthly_spend * 2.5)
        else:
            risk_adjusted_limit = min(recommended_limit, 1000)
    elif product_type == 'LINE_OF_CREDIT':
        risk_adjusted_limit = min(recommended_limit * 2, 10000)
    else:
        risk_adjusted_limit = recommended_limit
    
    # Utilization forecast
    if monthly_spend > 0 and risk_adjusted_limit > 0:
        utilization_forecast = min(0.95, monthly_spend / risk_adjusted_limit)
    else:
        utilization_forecast = 0.30
    
    # Confidence score
    confidence_score = 0.0
    confidence_score += 0.25 if income_verified > 0 else 0
    confidence_score += 0.25 if transaction_count > 50 else transaction_count / 200.0
    confidence_score += (credit_score / 850.0) * 0.25
    confidence_score += payment_history * 0.25
    confidence_score = min(1.0, confidence_score)
    
    return json.dumps({
        "recommended_limit": float(recommended_limit),
        "risk_adjusted_limit": float(risk_adjusted_limit),
        "utilization_forecast": round(utilization_forecast, 2),
        "confidence_score": round(confidence_score, 2)
    })
$$;

-- ============================================================================
-- Procedure 4: Customer Lifetime Value Prediction
-- ============================================================================

-- Drop if exists
DROP FUNCTION IF EXISTS PREDICT_CUSTOMER_LTV(VARCHAR);

CREATE OR REPLACE PROCEDURE PREDICT_CUSTOMER_LTV(
    customer_id VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'predict_ltv'
COMMENT = 'Predicts customer lifetime value based on behavior patterns'
AS
$$
def predict_ltv(session, customer_id):
    import json
    from datetime import datetime
    
    query = f"""
        SELECT
            c.customer_id,
            DATEDIFF('month', c.acquisition_date, CURRENT_DATE()) as tenure_months,
            c.lifetime_value as current_ltv,
            COALESCE(r.total_revenue, 0) as total_revenue,
            COALESCE(r.monthly_revenue, 0) as monthly_revenue,
            COALESCE(e.product_count, 0) as product_count,
            COALESCE(e.transaction_frequency, 0) as transaction_frequency,
        COALESCE(e.has_direct_deposit, 0) as has_direct_deposit
        FROM RAW.CUSTOMERS c
        LEFT JOIN (
            SELECT 
                customer_id,
                SUM(CASE 
                    WHEN transaction_type = 'FEE' THEN amount
                WHEN merchant_category IS NOT NULL THEN ABS(amount) * 0.015
                    ELSE 0
                END) as total_revenue,
                AVG(CASE 
                    WHEN transaction_type = 'FEE' THEN amount
                    WHEN merchant_category IS NOT NULL THEN ABS(amount) * 0.015
                    ELSE 0
                END) * 30 as monthly_revenue
            FROM RAW.TRANSACTIONS
        WHERE customer_id = '{customer_id}'
            GROUP BY customer_id
        ) r ON c.customer_id = r.customer_id
        LEFT JOIN (
            SELECT 
                c.customer_id,
                COUNT(DISTINCT a.account_type) as product_count,
            0.5 as transaction_frequency,
                MAX(CASE WHEN dd.customer_id IS NOT NULL THEN 1 ELSE 0 END) as has_direct_deposit
            FROM RAW.CUSTOMERS c
            LEFT JOIN RAW.ACCOUNTS a ON c.customer_id = a.customer_id
            LEFT JOIN RAW.DIRECT_DEPOSITS dd ON c.customer_id = dd.customer_id
        WHERE c.customer_id = '{customer_id}'
            GROUP BY c.customer_id
        ) e ON c.customer_id = e.customer_id
    WHERE c.customer_id = '{customer_id}'
    """
    
    result = session.sql(query).collect()
    
    if not result:
        return json.dumps({
            "predicted_ltv": 0.0,
            "ltv_segment": "UNKNOWN",
            "retention_probability": 0.5,
            "growth_potential": "UNKNOWN"
        })
    
    row = result[0]
    tenure_months = row['TENURE_MONTHS']
    current_ltv = row['CURRENT_LTV'] or 0
    monthly_revenue = row['MONTHLY_REVENUE'] or 0
    product_count = row['PRODUCT_COUNT']
    transaction_frequency = row['TRANSACTION_FREQUENCY']
    has_direct_deposit = row['HAS_DIRECT_DEPOSIT']
    
    # Calculate retention probability
    retention_prob = 0.5  # Base retention
    retention_prob += 0.2 if has_direct_deposit == 1 else 0
    retention_prob += 0.15 if product_count >= 3 else product_count * 0.05
    retention_prob += transaction_frequency * 0.1
    retention_prob += 0.05 if tenure_months >= 12 else 0
    retention_prob = min(0.95, max(0.1, retention_prob))
    
    # Predict LTV
    if tenure_months < 6:
        predicted_ltv = monthly_revenue * 36 * retention_prob
    else:
        revenue_trend = 1.1
        predicted_ltv = current_ltv + (monthly_revenue * 24 * retention_prob * revenue_trend)
    
    # LTV Segment
    if predicted_ltv >= 5000:
        ltv_segment = 'PREMIUM'
    elif predicted_ltv >= 1000:
        ltv_segment = 'HIGH_VALUE'
    elif predicted_ltv >= 250:
        ltv_segment = 'STANDARD'
    else:
        ltv_segment = 'DEVELOPING'
    
    # Growth potential
    if product_count < 2 and has_direct_deposit == 0:
        growth_potential = 'HIGH'
    elif product_count < 3:
        growth_potential = 'MEDIUM'
    else:
        growth_potential = 'LOW'
    
    return json.dumps({
        "predicted_ltv": round(predicted_ltv, 2),
        "ltv_segment": ltv_segment,
        "retention_probability": round(retention_prob, 2),
        "growth_potential": growth_potential
    })
$$;

-- ============================================================================
-- Procedure 5: Anomaly Detection for Transactions
-- ============================================================================

-- Drop if exists
DROP FUNCTION IF EXISTS DETECT_TRANSACTION_ANOMALIES(NUMBER);

CREATE OR REPLACE PROCEDURE DETECT_TRANSACTION_ANOMALIES(
    lookback_hours NUMBER
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'detect_anomalies'
COMMENT = 'Detects unusual transaction patterns across all customers'
AS
$$
def detect_anomalies(session, lookback_hours):
    import json
    
    if lookback_hours is None:
        lookback_hours = 24
    
    query = f"""
    WITH recent_activity AS (
        SELECT
            t.customer_id,
            COUNT(*) as transaction_count,
            SUM(ABS(amount)) as total_amount,
            COUNT(DISTINCT merchant_category) as unique_categories,
            COUNT(DISTINCT merchant_state) as unique_states,
            MAX(ABS(amount)) as max_amount,
            COUNT(CASE WHEN is_international THEN 1 END) as international_count,
            AVG(fraud_score) as avg_fraud_score
        FROM RAW.TRANSACTIONS t
        WHERE t.transaction_timestamp >= DATEADD('hour', -{lookback_hours}, CURRENT_TIMESTAMP())
            AND t.status = 'COMPLETED'
        GROUP BY t.customer_id
    ),
    historical_baseline AS (
        SELECT
            t.customer_id,
            AVG(daily_count) as avg_daily_transactions,
            AVG(daily_amount) as avg_daily_amount,
            STDDEV(daily_count) as stddev_transactions,
            STDDEV(daily_amount) as stddev_amount,
            MAX(max_historical_amount) as max_historical_amount
        FROM (
            SELECT 
                customer_id,
                DATE(transaction_date) as transaction_date,
                COUNT(*) as daily_count,
                SUM(ABS(amount)) as daily_amount,
                MAX(ABS(amount)) as max_historical_amount
            FROM RAW.TRANSACTIONS
            WHERE transaction_date BETWEEN DATEADD('day', -30, CURRENT_DATE()) 
                AND DATEADD('day', -1, CURRENT_DATE())
            GROUP BY customer_id, DATE(transaction_date)
        ) t
        GROUP BY t.customer_id
    ),
    anomalies AS (
    SELECT
        r.customer_id,
        CASE
            WHEN r.transaction_count > h.avg_daily_transactions + (3 * COALESCE(h.stddev_transactions, 1)) 
                THEN 'VELOCITY_SPIKE'
            WHEN r.total_amount > h.avg_daily_amount + (3 * COALESCE(h.stddev_amount, h.avg_daily_amount))
                THEN 'AMOUNT_SPIKE'
            WHEN r.max_amount > COALESCE(h.max_historical_amount, 0) * 2 
                AND r.max_amount > 500
                THEN 'UNUSUAL_LARGE_TRANSACTION'
            WHEN r.international_count > 3
                THEN 'MULTIPLE_INTERNATIONAL'
            WHEN r.unique_states > 5
                THEN 'GEOGRAPHIC_DISPERSION'
            WHEN r.avg_fraud_score > 0.6
                THEN 'HIGH_RISK_PATTERN'
            ELSE 'BEHAVIORAL_ANOMALY'
        END as anomaly_type,
            (r.transaction_count - h.avg_daily_transactions) / GREATEST(h.avg_daily_transactions, 1) * 0.3 +
            (r.total_amount - h.avg_daily_amount) / GREATEST(h.avg_daily_amount, 1) * 0.3 +
            r.avg_fraud_score * 0.4 as anomaly_score,
        r.transaction_count,
            r.total_amount
    FROM recent_activity r
    JOIN historical_baseline h ON r.customer_id = h.customer_id
    WHERE 
        r.transaction_count > h.avg_daily_transactions + (2 * COALESCE(h.stddev_transactions, 1))
        OR r.total_amount > h.avg_daily_amount + (2 * COALESCE(h.stddev_amount, h.avg_daily_amount))
        OR r.avg_fraud_score > 0.5
        OR r.international_count > 3
        OR r.unique_states > 5
    ORDER BY anomaly_score DESC
        LIMIT 50
    )
    SELECT
        customer_id,
        anomaly_type,
        LEAST(1.0, GREATEST(0.0, anomaly_score)) as anomaly_score,
        transaction_count,
        total_amount,
        CURRENT_TIMESTAMP() as detection_timestamp
    FROM anomalies
    """
    
    result = session.sql(query).collect()
    
    anomalies_list = []
    for row in result:
        anomalies_list.append({
            "customer_id": row['CUSTOMER_ID'],
            "anomaly_type": row['ANOMALY_TYPE'],
            "anomaly_score": round(row['ANOMALY_SCORE'], 2),
            "transaction_count": int(row['TRANSACTION_COUNT']),
            "total_amount": float(row['TOTAL_AMOUNT']),
            "detection_timestamp": row['DETECTION_TIMESTAMP'].isoformat()
        })
    
    return json.dumps({
        "lookback_hours": lookback_hours,
        "anomalies_detected": len(anomalies_list),
        "anomalies": anomalies_list
    })
$$;

-- ============================================================================
-- Display confirmation
-- ============================================================================
SELECT 'ML model wrapper procedures created successfully' AS STATUS;

-- ============================================================================
-- Test the wrapper procedures (examples)
-- ============================================================================
/*
CALL SCORE_TRANSACTION_FRAUD('CUST00001234', 500, '5411', TRUE);
CALL CALCULATE_ADVANCE_ELIGIBILITY('CUST00001234');
CALL RECOMMEND_CREDIT_LIMIT('CUST00001234', 'BELIEVE_CARD');
CALL PREDICT_CUSTOMER_LTV('CUST00001234');
CALL DETECT_TRANSACTION_ANOMALIES(24);
*/
