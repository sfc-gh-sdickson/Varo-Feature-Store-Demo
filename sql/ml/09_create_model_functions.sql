-- ============================================================================
-- Varo Intelligence Agent - ML Model Wrapper Procedures
-- ============================================================================
-- Purpose: Create Python procedures that wrap Model Registry models
--          These procedures call the registered models from the notebook
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
    CUSTOMER_ID VARCHAR,
    AMOUNT NUMBER,
    MERCHANT_CATEGORY VARCHAR,
    IS_INTERNATIONAL BOOLEAN
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-ml-python', 'scikit-learn')
HANDLER = 'score_fraud'
COMMENT = 'Calls FRAUD_DETECTION_MODEL from Model Registry to score transaction fraud risk'
AS
$$
def score_fraud(session, customer_id, amount, merchant_category, is_international):
    from snowflake.ml.registry import Registry
    import json
    
    # Get model from registry
    reg = Registry(session)
    model = reg.get_model("FRAUD_DETECTION_MODEL").default
    
    # Get customer data for prediction
    input_query = f"""
    SELECT
        {amount}::FLOAT AS amount,
        '{merchant_category}' AS merchant_category,
        '{is_international}'::BOOLEAN AS is_international,
        c.credit_score::FLOAT AS credit_score,
        c.risk_tier,
        COALESCE(a.current_balance, 0)::BIGINT AS account_balance,
        'DEBIT' AS transaction_type
    FROM RAW.CUSTOMERS c
    LEFT JOIN RAW.ACCOUNTS a ON c.customer_id = a.customer_id
    WHERE c.customer_id = '{customer_id}'
    LIMIT 1
    """
    
    input_df = session.sql(input_query)
    
    # Get predictions
    predictions = model.run(input_df, function_name="predict")
    
    # Extract result
    result = predictions.select("FRAUD_PREDICTION").collect()
    
    if not result:
        return json.dumps({
            "fraud_probability": 0.5,
            "risk_level": "UNKNOWN",
            "prediction": "NO_DATA"
        })
    
    fraud_prediction = int(result[0]['FRAUD_PREDICTION'])
    
    return json.dumps({
        "customer_id": customer_id,
        "amount": float(amount),
        "is_fraud_predicted": fraud_prediction == 1,
        "model_version": "FRAUD_DETECTION_MODEL"
    })
$$;

-- ============================================================================
-- Procedure 2: Cash Advance Eligibility
-- ============================================================================

-- Drop if exists
DROP FUNCTION IF EXISTS CALCULATE_ADVANCE_ELIGIBILITY(VARCHAR);

CREATE OR REPLACE PROCEDURE CALCULATE_ADVANCE_ELIGIBILITY(
    CUSTOMER_ID VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-ml-python', 'scikit-learn')
HANDLER = 'calculate_eligibility'
COMMENT = 'Calls ADVANCE_ELIGIBILITY_MODEL from Model Registry to predict advance repayment'
AS
$$
def calculate_eligibility(session, customer_id):
    from snowflake.ml.registry import Registry
    import json
    
    # Get model
    reg = Registry(session)
    model = reg.get_model("ADVANCE_ELIGIBILITY_MODEL").default
    
    # Get customer data for prediction
    input_query = f"""
    SELECT
        100.0::FLOAT AS advance_amount,
        5.0::FLOAT AS fee_amount,
        COALESCE(ca.eligibility_score, 0.5)::FLOAT AS eligibility_score,
        COALESCE(c.credit_score, 650)::FLOAT AS credit_score,
        c.risk_tier,
        c.employment_status,
        COUNT(DISTINCT dd.deposit_id)::FLOAT AS deposit_count,
        COALESCE(AVG(dd.amount), 1000)::FLOAT AS avg_deposit_amount
    FROM RAW.CUSTOMERS c
    LEFT JOIN RAW.CASH_ADVANCES ca ON c.customer_id = ca.customer_id AND ca.advance_status = 'ACTIVE'
    LEFT JOIN RAW.DIRECT_DEPOSITS dd ON c.customer_id = dd.customer_id
    WHERE c.customer_id = '{customer_id}'
    GROUP BY ca.eligibility_score, c.credit_score, c.risk_tier, c.employment_status
    LIMIT 1
    """
    
    input_df = session.sql(input_query)
    
    # Get predictions
    predictions = model.run(input_df, function_name="predict")
    
    # Extract result
    result = predictions.select("REPAYMENT_PREDICTION").collect()
    
    if not result:
        return json.dumps({
            "is_eligible": False,
            "reason": "NO_DATA"
        })
    
    will_repay = int(result[0]['REPAYMENT_PREDICTION'])
    
    return json.dumps({
        "customer_id": customer_id,
        "is_eligible": will_repay == 1,
        "predicted_repayment_success": will_repay == 1,
        "model_version": "ADVANCE_ELIGIBILITY_MODEL"
    })
$$;

-- ============================================================================
-- Procedure 3: Credit Limit Recommendation (Placeholder - uses advance model)
-- ============================================================================

-- Drop if exists
DROP FUNCTION IF EXISTS RECOMMEND_CREDIT_LIMIT(VARCHAR, VARCHAR);

CREATE OR REPLACE PROCEDURE RECOMMEND_CREDIT_LIMIT(
    CUSTOMER_ID VARCHAR,
    PRODUCT_TYPE VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-ml-python', 'scikit-learn')
HANDLER = 'recommend_limit'
COMMENT = 'Recommends credit limit based on customer profile'
AS
$$
def recommend_limit(session, customer_id, product_type):
    import json
    
    # Simplified recommendation based on customer data
    query = f"""
    SELECT
        c.credit_score,
        c.risk_tier,
        COALESCE(AVG(a.current_balance), 0) as avg_balance
    FROM RAW.CUSTOMERS c
    LEFT JOIN RAW.ACCOUNTS a ON c.customer_id = a.customer_id
    WHERE c.customer_id = '{customer_id}'
    GROUP BY c.credit_score, c.risk_tier
    """
    
    result = session.sql(query).collect()
    
    if not result:
        return json.dumps({"recommended_limit": 0, "reason": "CUSTOMER_NOT_FOUND"})
    
    row = result[0]
    credit_score = row['CREDIT_SCORE'] or 600
    
    # Simple rule-based limit
    if credit_score >= 750:
        limit = 5000
    elif credit_score >= 700:
        limit = 2500
    elif credit_score >= 650:
        limit = 1000
    else:
        limit = 500
    
    if product_type == 'LINE_OF_CREDIT':
        limit = limit * 2
    
    return json.dumps({
        "customer_id": customer_id,
        "product_type": product_type,
        "recommended_limit": limit,
        "credit_score": int(credit_score)
    })
$$;

-- ============================================================================
-- Procedure 4: Customer Lifetime Value Prediction
-- ============================================================================

-- Drop if exists
DROP FUNCTION IF EXISTS PREDICT_CUSTOMER_LTV(VARCHAR);

CREATE OR REPLACE PROCEDURE PREDICT_CUSTOMER_LTV(
    CUSTOMER_ID VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-ml-python', 'scikit-learn')
HANDLER = 'predict_ltv'
COMMENT = 'Calls CUSTOMER_LTV_MODEL from Model Registry to predict lifetime value'
AS
$$
def predict_ltv(session, customer_id):
    from snowflake.ml.registry import Registry
    import json
    
    # Get model
    reg = Registry(session)
    model = reg.get_model("CUSTOMER_LTV_MODEL").default
    
    # Get customer data for prediction
    input_query = f"""
        SELECT
        DATEDIFF('month', c.acquisition_date, CURRENT_DATE())::FLOAT AS tenure_months,
        COALESCE(c.credit_score, 650)::FLOAT AS credit_score,
        c.risk_tier,
        c.acquisition_channel,
        COUNT(DISTINCT a.account_id)::FLOAT AS product_count,
        COALESCE(AVG(a.current_balance), 0)::FLOAT AS avg_account_balance,
        COUNT(DISTINCT CASE WHEN t.transaction_date >= DATEADD('day', -90, CURRENT_DATE())
                       THEN t.transaction_id END)::FLOAT AS recent_transaction_count,
        (COUNT(DISTINCT dd.deposit_id) > 0)::BOOLEAN AS has_direct_deposit
            FROM RAW.CUSTOMERS c
            LEFT JOIN RAW.ACCOUNTS a ON c.customer_id = a.customer_id
            LEFT JOIN RAW.TRANSACTIONS t ON c.customer_id = t.customer_id
            LEFT JOIN RAW.DIRECT_DEPOSITS dd ON c.customer_id = dd.customer_id
    WHERE c.customer_id = '{customer_id}'
    GROUP BY c.acquisition_date, c.credit_score, c.risk_tier, c.acquisition_channel
    LIMIT 1
    """
    
    input_df = session.sql(input_query)
    
    # Get predictions
    predictions = model.run(input_df, function_name="predict")
    
    # Extract result
    result = predictions.select("PREDICTED_LTV").collect()
    
    if not result:
        return json.dumps({
            "predicted_ltv": 0,
            "reason": "NO_DATA"
        })
    
    predicted_ltv = float(result[0]['PREDICTED_LTV'])
    
    return json.dumps({
        "customer_id": customer_id,
        "predicted_ltv": round(predicted_ltv, 2),
        "model_version": "CUSTOMER_LTV_MODEL"
    })
$$;

-- ============================================================================
-- Procedure 5: Anomaly Detection (Simplified - no model)
-- ============================================================================

-- Drop if exists
DROP FUNCTION IF EXISTS DETECT_TRANSACTION_ANOMALIES(NUMBER);

CREATE OR REPLACE PROCEDURE DETECT_TRANSACTION_ANOMALIES(
    LOOKBACK_HOURS NUMBER
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'detect_anomalies'
COMMENT = 'Detects unusual transaction patterns using rule-based logic'
AS
$$
def detect_anomalies(session, lookback_hours):
    import json
    
    if lookback_hours is None:
        lookback_hours = 24
    
    query = f"""
        SELECT
            t.customer_id,
            COUNT(*) as transaction_count,
        SUM(ABS(t.amount)) as total_amount,
        MAX(ABS(t.amount)) as max_amount,
        AVG(t.fraud_score) as avg_fraud_score
        FROM RAW.TRANSACTIONS t
    WHERE t.transaction_timestamp >= DATEADD('hour', -{lookback_hours}, CURRENT_TIMESTAMP())
            AND t.status = 'COMPLETED'
        GROUP BY t.customer_id
    HAVING COUNT(*) > 10 OR AVG(t.fraud_score) > 0.6
    ORDER BY avg_fraud_score DESC
    LIMIT 50
    """
    
    result = session.sql(query).collect()
    
    anomalies_list = []
    for row in result:
        anomalies_list.append({
            "customer_id": row['CUSTOMER_ID'],
            "transaction_count": int(row['TRANSACTION_COUNT']),
            "total_amount": float(row['TOTAL_AMOUNT']),
            "max_amount": float(row['MAX_AMOUNT']),
            "avg_fraud_score": round(float(row['AVG_FRAUD_SCORE']), 2)
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
