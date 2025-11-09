-- ============================================================================
-- Varo Intelligence Agent - ML Model Wrapper Functions
-- ============================================================================
-- Purpose: Create SQL functions that wrap ML models for banking predictions
--          These models integrate with the Feature Store for real-time inference
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE VARO_FEATURE_WH;

-- ============================================================================
-- Function 1: Fraud Risk Scoring
-- ============================================================================
CREATE OR REPLACE FUNCTION SCORE_TRANSACTION_FRAUD(
    customer_id VARCHAR,
    amount NUMBER,
    merchant_category VARCHAR,
    is_international BOOLEAN
)
RETURNS TABLE (
    fraud_probability NUMBER(5,4),
    risk_level VARCHAR,
    reason_codes ARRAY
)
COMMENT = 'Scores transaction fraud risk using real-time features from Feature Store'
AS
$$
    WITH customer_features AS (
        -- Get real-time customer features from Feature Store
        SELECT 
            entity_id,
            feature_vector:txn_count_1h::NUMBER as velocity_1h,
            feature_vector:txn_volume_30d::NUMBER as monthly_volume,
            feature_vector:unique_merchants_30d::NUMBER as merchant_diversity,
            feature_vector:risk_indicator::NUMBER as historical_risk
        FROM FEATURE_STORE.ONLINE_FEATURES
        WHERE entity_id = customer_id AND entity_type = 'CUSTOMER'
    ),
    risk_calculation AS (
        SELECT
            -- Simple risk scoring model (in production, would call Snowpark ML model)
            LEAST(1.0, GREATEST(0.0,
                0.1 +  -- Base risk
                (CASE WHEN amount > cf.monthly_volume * 0.5 THEN 0.3 ELSE 0.0 END) +  -- Unusual amount
                (CASE WHEN cf.velocity_1h > 5 THEN 0.2 ELSE 0.0 END) +  -- High velocity
                (CASE WHEN is_international THEN 0.15 ELSE 0.0 END) +  -- International
                (CASE WHEN merchant_category IN ('7995', '5933', '6010') THEN 0.2 ELSE 0.0 END) +  -- Risky MCC
                (cf.historical_risk * 0.15)  -- Historical behavior
            )) as fraud_probability
        FROM customer_features cf
    )
    SELECT
        fraud_probability,
        CASE
            WHEN fraud_probability >= 0.7 THEN 'HIGH'
            WHEN fraud_probability >= 0.4 THEN 'MEDIUM'
            WHEN fraud_probability >= 0.2 THEN 'LOW'
            ELSE 'MINIMAL'
        END as risk_level,
        -- Generate reason codes
        ARRAY_AGG(reason) FILTER (WHERE reason IS NOT NULL) as reason_codes
    FROM (
        SELECT 
            rc.fraud_probability,
            CASE
                WHEN amount > (SELECT monthly_volume * 0.5 FROM customer_features) THEN 'UNUSUAL_AMOUNT'
                WHEN (SELECT velocity_1h FROM customer_features) > 5 THEN 'HIGH_VELOCITY'
                WHEN is_international THEN 'INTERNATIONAL_TXN'
                WHEN merchant_category IN ('7995', '5933', '6010') THEN 'RISKY_MERCHANT'
                ELSE NULL
            END as reason
        FROM risk_calculation rc
    )
    GROUP BY fraud_probability
$$;

-- ============================================================================
-- Function 2: Cash Advance Eligibility
-- ============================================================================
CREATE OR REPLACE FUNCTION CALCULATE_ADVANCE_ELIGIBILITY(
    customer_id VARCHAR
)
RETURNS TABLE (
    is_eligible BOOLEAN,
    max_advance_amount NUMBER,
    eligibility_score NUMBER(3,2),
    decline_reasons ARRAY
)
COMMENT = 'Determines cash advance eligibility and limit using Feature Store data'
AS
$$
    WITH customer_data AS (
        SELECT
            c.customer_id,
            c.credit_score,
            c.customer_status,
            c.risk_tier,
            -- Get direct deposit data
            COALESCE(dd.monthly_deposits, 0) as monthly_deposits,
            COALESCE(dd.deposit_count, 0) as deposit_count,
            -- Get account balance
            COALESCE(a.avg_balance, 0) as avg_balance,
            -- Get advance history from Feature Store
            COALESCE(af.total_advances::NUMBER, 0) as total_advances,
            COALESCE(af.repayment_rate::NUMBER, 1.0) as repayment_rate,
            COALESCE(af.current_balance::NUMBER, 0) as current_advance_balance
        FROM RAW.CUSTOMERS c
        LEFT JOIN (
            SELECT 
                customer_id,
                SUM(amount) as monthly_deposits,
                COUNT(*) as deposit_count
            FROM RAW.DIRECT_DEPOSITS
            WHERE deposit_date >= DATEADD('month', -1, CURRENT_DATE())
            GROUP BY customer_id
        ) dd ON c.customer_id = dd.customer_id
        LEFT JOIN (
            SELECT 
                customer_id,
                AVG(current_balance) as avg_balance
            FROM RAW.ACCOUNTS
            WHERE account_type = 'CHECKING'
            GROUP BY customer_id
        ) a ON c.customer_id = a.customer_id
        LEFT JOIN (
            SELECT 
                entity_id,
                feature_vector:total_advances as total_advances,
                feature_vector:repayment_rate as repayment_rate,
                feature_vector:current_balance as current_balance
            FROM FEATURE_STORE.ONLINE_FEATURES
            WHERE entity_id = customer_id AND entity_type = 'CUSTOMER'
        ) af ON c.customer_id = af.entity_id
        WHERE c.customer_id = customer_id
    ),
    eligibility_check AS (
        SELECT
            customer_id,
            -- Check eligibility criteria
            CASE
                WHEN customer_status != 'ACTIVE' THEN FALSE
                WHEN current_advance_balance > 0 THEN FALSE  -- Already has active advance
                WHEN monthly_deposits < 1000 THEN FALSE
                WHEN credit_score < 600 THEN FALSE
                WHEN risk_tier = 'HIGH' THEN FALSE
                ELSE TRUE
            END as is_eligible,
            -- Calculate eligibility score (0-1)
            LEAST(1.0, GREATEST(0.0,
                0.2 +  -- Base score
                (CASE WHEN credit_score >= 700 THEN 0.2 ELSE credit_score / 3500.0 END) +
                (CASE WHEN monthly_deposits >= 2000 THEN 0.2 ELSE monthly_deposits / 10000.0 END) +
                (repayment_rate * 0.2) +
                (CASE WHEN total_advances >= 5 AND repayment_rate = 1.0 THEN 0.2 ELSE 0.0 END)
            )) as eligibility_score,
            -- Calculate max advance amount
            CASE
                WHEN customer_status != 'ACTIVE' OR current_advance_balance > 0 THEN 0
                WHEN repayment_rate < 0.8 THEN 0
                WHEN total_advances = 0 THEN LEAST(100, monthly_deposits * 0.1)
                WHEN total_advances < 3 THEN LEAST(250, monthly_deposits * 0.15)
                WHEN repayment_rate = 1.0 THEN LEAST(500, monthly_deposits * 0.25)
                ELSE LEAST(250, monthly_deposits * 0.15)
            END as max_advance_amount,
            -- Collect decline reasons
            customer_status,
            current_advance_balance,
            monthly_deposits,
            credit_score,
            risk_tier
        FROM customer_data
    )
    SELECT
        is_eligible,
        max_advance_amount,
        eligibility_score,
        ARRAY_AGG(reason) FILTER (WHERE reason IS NOT NULL) as decline_reasons
    FROM (
        SELECT 
            is_eligible,
            max_advance_amount,
            eligibility_score,
            CASE
                WHEN customer_status != 'ACTIVE' THEN 'ACCOUNT_NOT_ACTIVE'
                WHEN current_advance_balance > 0 THEN 'EXISTING_ADVANCE_ACTIVE'
                WHEN monthly_deposits < 1000 THEN 'INSUFFICIENT_DIRECT_DEPOSITS'
                WHEN credit_score < 600 THEN 'LOW_CREDIT_SCORE'
                WHEN risk_tier = 'HIGH' THEN 'HIGH_RISK_PROFILE'
                ELSE NULL
            END as reason
        FROM eligibility_check
    )
    GROUP BY is_eligible, max_advance_amount, eligibility_score
$$;

-- ============================================================================
-- Function 3: Credit Limit Recommendation
-- ============================================================================
CREATE OR REPLACE FUNCTION RECOMMEND_CREDIT_LIMIT(
    customer_id VARCHAR,
    product_type VARCHAR  -- BELIEVE_CARD or LINE_OF_CREDIT
)
RETURNS TABLE (
    recommended_limit NUMBER,
    risk_adjusted_limit NUMBER,
    utilization_forecast NUMBER(5,2),
    confidence_score NUMBER(3,2)
)
COMMENT = 'Recommends credit limit based on customer profile and behavior'
AS
$$
    WITH customer_profile AS (
        SELECT
            c.customer_id,
            c.credit_score,
            c.income_verified,
            c.employment_status,
            -- Get spending patterns
            COALESCE(t.monthly_spend, 0) as monthly_spend,
            COALESCE(t.transaction_count, 0) as transaction_count,
            -- Get existing credit utilization
            COALESCE(cc.current_utilization, 0) as current_utilization,
            COALESCE(cc.payment_history, 1.0) as payment_history,
            -- Get savings behavior
            COALESCE(s.avg_savings_balance, 0) as avg_savings_balance
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
                1.0 as payment_history  -- Simplified - would calculate from payment data
            FROM RAW.ACCOUNTS
            WHERE account_type IN ('BELIEVE_CARD', 'LINE_OF_CREDIT')
            GROUP BY customer_id
        ) cc ON c.customer_id = cc.customer_id
        LEFT JOIN (
            SELECT 
                customer_id,
                AVG(current_balance) as avg_savings_balance
            FROM RAW.ACCOUNTS
            WHERE account_type = 'SAVINGS'
            GROUP BY customer_id
        ) s ON c.customer_id = s.customer_id
        WHERE c.customer_id = customer_id
    )
    SELECT
        -- Base limit calculation
        CASE
            WHEN credit_score < 600 THEN 500
            WHEN credit_score < 650 THEN income_verified * 0.02
            WHEN credit_score < 700 THEN income_verified * 0.05
            WHEN credit_score < 750 THEN income_verified * 0.08
            ELSE income_verified * 0.10
        END as recommended_limit,
        -- Risk-adjusted limit
        CASE product_type
            WHEN 'BELIEVE_CARD' THEN 
                LEAST(recommended_limit, 
                      CASE 
                          WHEN monthly_spend > 0 THEN monthly_spend * 2.5
                          ELSE 1000
                      END)
            WHEN 'LINE_OF_CREDIT' THEN 
                LEAST(recommended_limit * 2, 10000)
        END as risk_adjusted_limit,
        -- Forecast utilization
        CASE 
            WHEN monthly_spend > 0 AND risk_adjusted_limit > 0 
            THEN LEAST(0.95, monthly_spend / risk_adjusted_limit)
            ELSE 0.30
        END as utilization_forecast,
        -- Confidence score
        LEAST(1.0, 
            (CASE WHEN income_verified > 0 THEN 0.25 ELSE 0 END) +
            (CASE WHEN transaction_count > 50 THEN 0.25 ELSE transaction_count / 200.0 END) +
            (credit_score / 850.0 * 0.25) +
            (payment_history * 0.25)
        ) as confidence_score
    FROM customer_profile
$$;

-- ============================================================================
-- Function 4: Customer Lifetime Value Prediction
-- ============================================================================
CREATE OR REPLACE FUNCTION PREDICT_CUSTOMER_LTV(
    customer_id VARCHAR
)
RETURNS TABLE (
    predicted_ltv NUMBER(12,2),
    ltv_segment VARCHAR,
    retention_probability NUMBER(3,2),
    growth_potential VARCHAR
)
COMMENT = 'Predicts customer lifetime value based on behavior patterns'
AS
$$
    WITH customer_metrics AS (
        SELECT
            c.customer_id,
            DATEDIFF('month', c.acquisition_date, CURRENT_DATE()) as tenure_months,
            c.lifetime_value as current_ltv,
            -- Revenue metrics
            COALESCE(r.total_revenue, 0) as total_revenue,
            COALESCE(r.monthly_revenue, 0) as monthly_revenue,
            -- Engagement metrics
            COALESCE(e.product_count, 0) as product_count,
            COALESCE(e.transaction_frequency, 0) as transaction_frequency,
            COALESCE(e.has_direct_deposit, 0) as has_direct_deposit,
            -- Growth indicators
            COALESCE(g.revenue_trend, 0) as revenue_trend,
            COALESCE(g.product_adoption_rate, 0) as product_adoption_rate
        FROM RAW.CUSTOMERS c
        LEFT JOIN (
            -- Calculate revenue (fees, interchange, etc.)
            SELECT 
                customer_id,
                -- Simplified revenue calculation
                SUM(CASE 
                    WHEN transaction_type = 'FEE' THEN amount
                    WHEN merchant_category IS NOT NULL THEN ABS(amount) * 0.015  -- Interchange estimate
                    ELSE 0
                END) as total_revenue,
                AVG(CASE 
                    WHEN transaction_type = 'FEE' THEN amount
                    WHEN merchant_category IS NOT NULL THEN ABS(amount) * 0.015
                    ELSE 0
                END) * 30 as monthly_revenue
            FROM RAW.TRANSACTIONS
            WHERE customer_id = customer_id
            GROUP BY customer_id
        ) r ON c.customer_id = r.customer_id
        LEFT JOIN (
            -- Engagement metrics
            SELECT 
                c.customer_id,
                COUNT(DISTINCT a.account_type) as product_count,
                COUNT(DISTINCT DATE_TRUNC('week', t.transaction_date)) / 
                    GREATEST(1, DATEDIFF('week', MIN(t.transaction_date), CURRENT_DATE())) as transaction_frequency,
                MAX(CASE WHEN dd.customer_id IS NOT NULL THEN 1 ELSE 0 END) as has_direct_deposit
            FROM RAW.CUSTOMERS c
            LEFT JOIN RAW.ACCOUNTS a ON c.customer_id = a.customer_id
            LEFT JOIN RAW.TRANSACTIONS t ON c.customer_id = t.customer_id
            LEFT JOIN RAW.DIRECT_DEPOSITS dd ON c.customer_id = dd.customer_id
            WHERE c.customer_id = customer_id
            GROUP BY c.customer_id
        ) e ON c.customer_id = e.customer_id
        LEFT JOIN (
            -- Growth indicators
            SELECT 
                customer_id,
                -- Revenue growth trend (simplified)
                1.1 as revenue_trend,
                -- Product adoption rate
                0.7 as product_adoption_rate
            FROM RAW.CUSTOMERS
            WHERE customer_id = customer_id
        ) g ON c.customer_id = g.customer_id
        WHERE c.customer_id = customer_id
    )
    SELECT
        -- Predict LTV (simplified model)
        CASE
            WHEN tenure_months < 6 THEN monthly_revenue * 36 * retention_probability
            ELSE current_ltv + (monthly_revenue * 24 * retention_probability * revenue_trend)
        END as predicted_ltv,
        -- LTV Segment
        CASE
            WHEN predicted_ltv >= 5000 THEN 'PREMIUM'
            WHEN predicted_ltv >= 1000 THEN 'HIGH_VALUE'
            WHEN predicted_ltv >= 250 THEN 'STANDARD'
            ELSE 'DEVELOPING'
        END as ltv_segment,
        -- Retention probability
        LEAST(0.95, GREATEST(0.1,
            0.5 +  -- Base retention
            (CASE WHEN has_direct_deposit = 1 THEN 0.2 ELSE 0 END) +
            (CASE WHEN product_count >= 3 THEN 0.15 ELSE product_count * 0.05 END) +
            (transaction_frequency * 0.1) +
            (CASE WHEN tenure_months >= 12 THEN 0.05 ELSE 0 END)
        )) as retention_probability,
        -- Growth potential
        CASE
            WHEN product_count < 2 AND has_direct_deposit = 0 THEN 'HIGH'
            WHEN product_count < 3 THEN 'MEDIUM'
            ELSE 'LOW'
        END as growth_potential
    FROM customer_metrics
$$;

-- ============================================================================
-- Function 5: Anomaly Detection for Transactions
-- ============================================================================
CREATE OR REPLACE FUNCTION DETECT_TRANSACTION_ANOMALIES(
    lookback_hours NUMBER DEFAULT 24
)
RETURNS TABLE (
    customer_id VARCHAR,
    anomaly_type VARCHAR,
    anomaly_score NUMBER(3,2),
    transaction_count NUMBER,
    total_amount NUMBER(12,2),
    detection_timestamp TIMESTAMP_NTZ
)
COMMENT = 'Detects unusual transaction patterns across all customers'
AS
$$
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
        WHERE t.transaction_timestamp >= DATEADD('hour', -lookback_hours, CURRENT_TIMESTAMP())
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
    )
    SELECT
        r.customer_id,
        -- Determine anomaly type
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
        -- Calculate anomaly score
        LEAST(1.0, GREATEST(
            (r.transaction_count - h.avg_daily_transactions) / GREATEST(h.avg_daily_transactions, 1) * 0.3 +
            (r.total_amount - h.avg_daily_amount) / GREATEST(h.avg_daily_amount, 1) * 0.3 +
            r.avg_fraud_score * 0.4
        , 0)) as anomaly_score,
        r.transaction_count,
        r.total_amount,
        CURRENT_TIMESTAMP() as detection_timestamp
    FROM recent_activity r
    JOIN historical_baseline h ON r.customer_id = h.customer_id
    WHERE 
        -- Only flag significant anomalies
        r.transaction_count > h.avg_daily_transactions + (2 * COALESCE(h.stddev_transactions, 1))
        OR r.total_amount > h.avg_daily_amount + (2 * COALESCE(h.stddev_amount, h.avg_daily_amount))
        OR r.avg_fraud_score > 0.5
        OR r.international_count > 3
        OR r.unique_states > 5
    ORDER BY anomaly_score DESC
$$;

-- Display confirmation
SELECT 'ML model wrapper functions created successfully' AS STATUS;
