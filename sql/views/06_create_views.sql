-- ============================================================================
-- Varo Intelligence Agent - Analytical Views
-- ============================================================================
-- Purpose: Create curated analytical views for banking intelligence
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE VARO_WH;

-- ============================================================================
-- Customer 360 View
-- ============================================================================
CREATE OR REPLACE VIEW V_CUSTOMER_360 AS
WITH customer_metrics AS (
    SELECT
        c.customer_id AS cust_id,
        -- Account metrics
        COUNT(DISTINCT a.account_id) AS total_accounts,
        COUNT(DISTINCT CASE WHEN a.account_type = 'CHECKING' THEN a.account_id END) AS checking_accounts,
        COUNT(DISTINCT CASE WHEN a.account_type = 'SAVINGS' THEN a.account_id END) AS savings_accounts,
        COUNT(DISTINCT CASE WHEN a.account_type = 'BELIEVE_CARD' THEN a.account_id END) AS credit_cards,
        COUNT(DISTINCT CASE WHEN a.account_type = 'LINE_OF_CREDIT' THEN a.account_id END) AS lines_of_credit,
        
        -- Balance metrics
        SUM(CASE WHEN a.account_type IN ('CHECKING', 'SAVINGS') THEN a.current_balance ELSE 0 END) AS total_deposit_balance,
        SUM(CASE WHEN a.account_type IN ('BELIEVE_CARD', 'LINE_OF_CREDIT') THEN ABS(a.current_balance) ELSE 0 END) AS total_credit_balance,
        MAX(CASE WHEN a.account_type = 'SAVINGS' THEN a.apy_rate END) AS savings_apy_rate,
        
        -- Transaction metrics (30 days)
        COUNT(DISTINCT t.transaction_id) AS transactions_30d,
        SUM(CASE WHEN t.transaction_type = 'DEBIT' THEN ABS(t.amount) ELSE 0 END) AS total_spend_30d,
        SUM(CASE WHEN t.transaction_type = 'CREDIT' THEN t.amount ELSE 0 END) AS total_deposits_30d,
        COUNT(DISTINCT t.merchant_category) AS unique_merchant_categories_30d,
        
        -- Direct deposit metrics
        COUNT(DISTINCT dd.deposit_id) AS total_direct_deposits,
        MAX(dd.deposit_date) AS last_direct_deposit_date,
        AVG(CASE WHEN dd.deposit_date >= DATEADD('month', -3, CURRENT_DATE()) THEN dd.amount END) AS avg_monthly_direct_deposit,
        
        -- Cash advance metrics
        COUNT(DISTINCT ca.advance_id) AS total_advances_taken,
        SUM(CASE WHEN ca.advance_status = 'ACTIVE' THEN ca.advance_amount + ca.fee_amount ELSE 0 END) AS outstanding_advance_balance,
        AVG(CASE WHEN ca.advance_status = 'REPAID' THEN 
            DATEDIFF('day', ca.advance_date, ca.repayment_date) 
        END) AS avg_advance_repayment_days,
        
        -- Support metrics
        COUNT(DISTINCT si.interaction_id) AS total_support_contacts,
        AVG(si.satisfaction_score) AS avg_satisfaction_score,
        MAX(si.interaction_date) AS last_support_contact_date
        
    FROM VARO_INTELLIGENCE.RAW.CUSTOMERS c
    LEFT JOIN VARO_INTELLIGENCE.RAW.ACCOUNTS a ON c.customer_id = a.customer_id
    LEFT JOIN VARO_INTELLIGENCE.RAW.TRANSACTIONS t ON c.customer_id = t.customer_id 
        AND t.transaction_date >= DATEADD('day', -30, CURRENT_DATE())
    LEFT JOIN VARO_INTELLIGENCE.RAW.DIRECT_DEPOSITS dd ON c.customer_id = dd.customer_id
    LEFT JOIN VARO_INTELLIGENCE.RAW.CASH_ADVANCES ca ON c.customer_id = ca.customer_id
    LEFT JOIN VARO_INTELLIGENCE.RAW.SUPPORT_INTERACTIONS si ON c.customer_id = si.customer_id
    GROUP BY c.customer_id
)
SELECT
    c.*,
    cm.total_accounts,
    cm.checking_accounts,
    cm.savings_accounts,
    cm.credit_cards,
    cm.lines_of_credit,
    cm.total_deposit_balance,
    cm.total_credit_balance,
    cm.savings_apy_rate,
    cm.transactions_30d,
    cm.total_spend_30d,
    cm.total_deposits_30d,
    cm.unique_merchant_categories_30d,
    cm.total_direct_deposits,
    cm.last_direct_deposit_date,
    cm.avg_monthly_direct_deposit,
    cm.total_advances_taken,
    cm.outstanding_advance_balance,
    cm.avg_advance_repayment_days,
    cm.total_support_contacts,
    cm.avg_satisfaction_score,
    cm.last_support_contact_date,
    -- Calculated metrics
    DATEDIFF('day', c.acquisition_date, CURRENT_DATE()) AS customer_tenure_days,
    DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) AS customer_age,
    CASE 
        WHEN cm.total_deposit_balance >= 10000 THEN 'HIGH_VALUE'
        WHEN cm.total_deposit_balance >= 1000 THEN 'MEDIUM_VALUE'
        WHEN cm.total_deposit_balance >= 100 THEN 'LOW_VALUE'
        ELSE 'MINIMAL_VALUE'
    END AS value_segment,
    CASE
        WHEN cm.transactions_30d >= 50 AND cm.total_direct_deposits > 0 THEN 'HIGHLY_ENGAGED'
        WHEN cm.transactions_30d >= 20 THEN 'ENGAGED'
        WHEN cm.transactions_30d >= 5 THEN 'OCCASIONAL'
        ELSE 'DORMANT'
    END AS engagement_level,
    CASE
        WHEN cm.outstanding_advance_balance > 0 THEN 'ADVANCE_ACTIVE'
        WHEN cm.total_advances_taken > 0 THEN 'ADVANCE_USER'
        ELSE 'ADVANCE_NEVER'
    END AS advance_user_type
FROM VARO_INTELLIGENCE.RAW.CUSTOMERS c
JOIN customer_metrics cm ON c.customer_id = cm.customer_id;

-- ============================================================================
-- Account Analytics View
-- ============================================================================
CREATE OR REPLACE VIEW V_ACCOUNT_ANALYTICS AS
SELECT
    a.account_id,
    a.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    a.account_type,
    a.account_status,
    a.opening_date,
    a.closing_date,
    DATEDIFF('day', a.opening_date, COALESCE(a.closing_date, CURRENT_DATE())) AS account_age_days,
    a.current_balance,
    a.available_balance,
    a.credit_limit,
    a.apy_rate,
    -- Transaction summary
    COUNT(DISTINCT t.transaction_id) AS total_transactions,
    SUM(CASE WHEN t.transaction_type = 'DEBIT' THEN 1 ELSE 0 END) AS debit_count,
    SUM(CASE WHEN t.transaction_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count,
    SUM(CASE WHEN t.transaction_type = 'DEBIT' THEN ABS(t.amount) ELSE 0 END) AS total_debits,
    SUM(CASE WHEN t.transaction_type = 'CREDIT' THEN t.amount ELSE 0 END) AS total_credits,
    MAX(t.transaction_date) AS last_transaction_date,
    -- Balance trends
    AVG(a.current_balance) AS avg_balance_lifetime,
    MIN(a.current_balance) AS min_balance_lifetime,
    MAX(a.current_balance) AS max_balance_lifetime,
    -- Direct deposit info
    COUNT(DISTINCT dd.deposit_id) AS direct_deposit_count,
    MAX(dd.deposit_date) AS last_direct_deposit,
    -- Card info
    MAX(cd.card_status) AS primary_card_status,
    MAX(cd.activation_date) AS card_activation_date
FROM VARO_INTELLIGENCE.RAW.ACCOUNTS a
JOIN VARO_INTELLIGENCE.RAW.CUSTOMERS c ON a.customer_id = c.customer_id
LEFT JOIN VARO_INTELLIGENCE.RAW.TRANSACTIONS t ON a.account_id = t.account_id
LEFT JOIN VARO_INTELLIGENCE.RAW.DIRECT_DEPOSITS dd ON a.account_id = dd.account_id
LEFT JOIN VARO_INTELLIGENCE.RAW.CARDS cd ON a.account_id = cd.account_id
GROUP BY
    a.account_id, a.customer_id, c.first_name, c.last_name, a.account_type,
    a.account_status, a.opening_date, a.closing_date, a.current_balance,
    a.available_balance, a.credit_limit, a.apy_rate;

-- ============================================================================
-- Transaction Analytics View
-- ============================================================================
CREATE OR REPLACE VIEW V_TRANSACTION_ANALYTICS AS
SELECT
    t.transaction_id,
    t.customer_id,
    t.account_id,
    t.transaction_type,
    t.transaction_category,
    t.transaction_date,
    t.transaction_timestamp,
    t.amount,
    t.running_balance,
    t.merchant_name,
    t.merchant_category,
    mc.category_name AS merchant_category_name,
    mc.category_group AS merchant_category_group,
    mc.cashback_eligible,
    mc.cashback_percentage,
    mc.high_risk_category,
    t.merchant_city,
    t.merchant_state,
    t.description,
    t.is_recurring,
    t.is_international,
    t.fraud_score,
    t.status,
    -- Time-based features
    EXTRACT(HOUR FROM t.transaction_timestamp) AS transaction_hour,
    DAYNAME(t.transaction_date) AS transaction_day_of_week,
    CASE 
        WHEN DAYOFWEEK(t.transaction_date) IN (1, 7) THEN 'WEEKEND'
        ELSE 'WEEKDAY'
    END AS day_type,
    CASE
        WHEN EXTRACT(HOUR FROM t.transaction_timestamp) BETWEEN 6 AND 11 THEN 'MORNING'
        WHEN EXTRACT(HOUR FROM t.transaction_timestamp) BETWEEN 12 AND 17 THEN 'AFTERNOON'
        WHEN EXTRACT(HOUR FROM t.transaction_timestamp) BETWEEN 18 AND 23 THEN 'EVENING'
        ELSE 'NIGHT'
    END AS time_of_day,
    -- Amount categorization
    CASE
        WHEN ABS(t.amount) < 10 THEN 'MICRO'
        WHEN ABS(t.amount) < 50 THEN 'SMALL'
        WHEN ABS(t.amount) < 200 THEN 'MEDIUM'
        WHEN ABS(t.amount) < 1000 THEN 'LARGE'
        ELSE 'VERY_LARGE'
    END AS amount_category
FROM VARO_INTELLIGENCE.RAW.TRANSACTIONS t
LEFT JOIN VARO_INTELLIGENCE.RAW.MERCHANT_CATEGORIES mc ON t.merchant_category = mc.mcc_code;

-- ============================================================================
-- Cash Advance Analytics View
-- ============================================================================
CREATE OR REPLACE VIEW V_CASH_ADVANCE_ANALYTICS AS
SELECT
    ca.advance_id,
    ca.customer_id,
    ca.account_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.credit_score,
    c.income_verified,
    ca.advance_amount,
    ca.fee_amount,
    ca.advance_date,
    ca.due_date,
    ca.repayment_date,
    ca.repayment_amount,
    ca.advance_status,
    ca.eligibility_score,
    ca.default_risk_score,
    -- Repayment metrics
    DATEDIFF('day', ca.advance_date, COALESCE(ca.repayment_date, CURRENT_DATE())) AS days_outstanding,
    DATEDIFF('day', ca.due_date, COALESCE(ca.repayment_date, CURRENT_DATE())) AS days_past_due,
    CASE
        WHEN ca.advance_status = 'REPAID' AND ca.repayment_date <= ca.due_date THEN 'ON_TIME'
        WHEN ca.advance_status = 'REPAID' AND ca.repayment_date > ca.due_date THEN 'LATE'
        WHEN ca.advance_status = 'ACTIVE' AND CURRENT_DATE() > ca.due_date THEN 'OVERDUE'
        WHEN ca.advance_status = 'ACTIVE' THEN 'CURRENT'
        ELSE 'DEFAULTED'
    END AS repayment_status,
    -- Customer advance history
    COUNT(*) OVER (PARTITION BY ca.customer_id) AS total_advances_by_customer,
    ROW_NUMBER() OVER (PARTITION BY ca.customer_id ORDER BY ca.advance_date) AS advance_sequence_number,
    LAG(ca.advance_date) OVER (PARTITION BY ca.customer_id ORDER BY ca.advance_date) AS previous_advance_date,
    DATEDIFF('day', 
        LAG(ca.advance_date) OVER (PARTITION BY ca.customer_id ORDER BY ca.advance_date),
        ca.advance_date
    ) AS days_since_last_advance
FROM VARO_INTELLIGENCE.RAW.CASH_ADVANCES ca
JOIN VARO_INTELLIGENCE.RAW.CUSTOMERS c ON ca.customer_id = c.customer_id;

-- ============================================================================
-- Direct Deposit Analytics View
-- ============================================================================
CREATE OR REPLACE VIEW V_DIRECT_DEPOSIT_ANALYTICS AS
SELECT
    dd.deposit_id,
    dd.customer_id,
    dd.account_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    dd.employer_name,
    dd.deposit_type,
    dd.deposit_date,
    dd.expected_date,
    dd.amount,
    dd.is_recurring,
    dd.frequency,
    dd.early_access_eligible,
    dd.early_access_used,
    -- Early access metrics
    DATEDIFF('day', dd.expected_date, dd.deposit_date) AS days_early,
    CASE WHEN dd.early_access_used THEN dd.amount * 0.02 ELSE 0 END AS early_access_value,
    -- Deposit patterns
    COUNT(*) OVER (
        PARTITION BY dd.customer_id, dd.employer_name 
        ORDER BY dd.deposit_date 
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS deposits_last_12,
    AVG(dd.amount) OVER (
        PARTITION BY dd.customer_id, dd.employer_name 
        ORDER BY dd.deposit_date 
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS avg_amount_last_6,
    -- Consistency metrics
    STDDEV(dd.amount) OVER (
        PARTITION BY dd.customer_id, dd.employer_name
    ) AS amount_volatility,
    DATEDIFF('day',
        LAG(dd.deposit_date) OVER (PARTITION BY dd.customer_id, dd.employer_name ORDER BY dd.deposit_date),
        dd.deposit_date
    ) AS days_since_last_deposit
FROM VARO_INTELLIGENCE.RAW.DIRECT_DEPOSITS dd
JOIN VARO_INTELLIGENCE.RAW.CUSTOMERS c ON dd.customer_id = c.customer_id;

-- ============================================================================
-- Fraud Risk Analytics View
-- ============================================================================
CREATE OR REPLACE VIEW V_FRAUD_RISK_ANALYTICS AS
WITH transaction_patterns AS (
    SELECT
        t.customer_id,
        t.transaction_date,
        -- Daily transaction patterns
        COUNT(*) AS daily_transaction_count,
        COUNT(DISTINCT t.merchant_category) AS unique_merchants_daily,
        COUNT(DISTINCT t.merchant_state) AS unique_states_daily,
        SUM(CASE WHEN t.is_international THEN 1 ELSE 0 END) AS international_transactions,
        MAX(t.fraud_score) AS max_fraud_score_daily,
        -- Unusual patterns
        SUM(CASE WHEN ABS(t.amount) > 1000 THEN 1 ELSE 0 END) AS high_value_transactions,
        SUM(CASE WHEN mc.high_risk_category THEN 1 ELSE 0 END) AS risky_merchant_transactions,
        -- Time patterns
        SUM(CASE WHEN EXTRACT(HOUR FROM t.transaction_timestamp) BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS night_transactions
    FROM VARO_INTELLIGENCE.RAW.TRANSACTIONS t
    LEFT JOIN VARO_INTELLIGENCE.RAW.MERCHANT_CATEGORIES mc ON t.merchant_category = mc.mcc_code
    WHERE t.transaction_date >= DATEADD('day', -90, CURRENT_DATE())
    GROUP BY t.customer_id, t.transaction_date
)
SELECT
    tp.*,
    c.risk_tier AS customer_risk_tier,
    c.kyc_status,
    -- Risk indicators
    CASE
        WHEN tp.max_fraud_score_daily > 0.7 THEN 'HIGH'
        WHEN tp.max_fraud_score_daily > 0.4 THEN 'MEDIUM'
        WHEN tp.max_fraud_score_daily > 0.2 THEN 'LOW'
        ELSE 'MINIMAL'
    END AS daily_risk_level,
    -- Behavioral anomalies
    tp.daily_transaction_count > AVG(tp.daily_transaction_count) OVER (
        PARTITION BY tp.customer_id 
        ORDER BY tp.transaction_date 
        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) * 3 AS volume_anomaly,
    tp.unique_states_daily > 3 AS geographic_anomaly,
    tp.risky_merchant_transactions > 5 AS merchant_risk_anomaly
FROM transaction_patterns tp
JOIN VARO_INTELLIGENCE.RAW.CUSTOMERS c ON tp.customer_id = c.customer_id;

-- ============================================================================
-- Customer Churn Risk View
-- ============================================================================
CREATE OR REPLACE VIEW V_CUSTOMER_CHURN_RISK AS
WITH customer_activity AS (
    SELECT
        c.customer_id,
        c.acquisition_date,
        c.customer_status,
        -- Activity metrics
        DATEDIFF('day', MAX(t.transaction_date), CURRENT_DATE()) AS days_since_last_transaction,
        DATEDIFF('day', MAX(dd.deposit_date), CURRENT_DATE()) AS days_since_last_direct_deposit,
        DATEDIFF('day', MAX(si.interaction_date), CURRENT_DATE()) AS days_since_last_support_contact,
        -- Volume trends (comparing last 30 days to previous 30 days)
        COUNT(CASE WHEN t.transaction_date >= DATEADD('day', -30, CURRENT_DATE()) THEN t.transaction_id END) AS transactions_last_30d,
        COUNT(CASE WHEN t.transaction_date BETWEEN DATEADD('day', -60, CURRENT_DATE()) 
            AND DATEADD('day', -31, CURRENT_DATE()) THEN t.transaction_id END) AS transactions_prev_30d,
        -- Balance trends
        AVG(CASE WHEN a.account_type = 'CHECKING' THEN a.current_balance END) AS avg_checking_balance,
        MIN(CASE WHEN a.account_type = 'CHECKING' THEN a.current_balance END) AS min_checking_balance
    FROM VARO_INTELLIGENCE.RAW.CUSTOMERS c
    LEFT JOIN VARO_INTELLIGENCE.RAW.TRANSACTIONS t ON c.customer_id = t.customer_id
    LEFT JOIN VARO_INTELLIGENCE.RAW.DIRECT_DEPOSITS dd ON c.customer_id = dd.customer_id
    LEFT JOIN VARO_INTELLIGENCE.RAW.SUPPORT_INTERACTIONS si ON c.customer_id = si.customer_id
    LEFT JOIN VARO_INTELLIGENCE.RAW.ACCOUNTS a ON c.customer_id = a.customer_id
    GROUP BY c.customer_id, c.acquisition_date, c.customer_status
)
SELECT
    ca.*,
    -- Churn risk indicators
    CASE
        WHEN ca.customer_status != 'ACTIVE' THEN 'CHURNED'
        WHEN ca.days_since_last_transaction > 90 THEN 'HIGH_RISK'
        WHEN ca.days_since_last_transaction > 60 THEN 'MEDIUM_RISK'
        WHEN ca.days_since_last_transaction > 30 THEN 'LOW_RISK'
        ELSE 'ACTIVE'
    END AS churn_risk_level,
    -- Activity decline flag
    CASE
        WHEN ca.transactions_last_30d < ca.transactions_prev_30d * 0.5 THEN TRUE
        ELSE FALSE
    END AS significant_activity_decline,
    -- Direct deposit loss flag
    CASE
        WHEN ca.days_since_last_direct_deposit > 45 THEN TRUE
        ELSE FALSE
    END AS direct_deposit_at_risk,
    -- Low balance flag
    CASE
        WHEN ca.avg_checking_balance < 50 THEN TRUE
        ELSE FALSE
    END AS low_balance_risk
FROM customer_activity ca;

-- ============================================================================
-- Marketing Campaign Performance View
-- ============================================================================
CREATE OR REPLACE VIEW V_MARKETING_CAMPAIGN_PERFORMANCE AS
SELECT
    mc.campaign_id,
    mc.campaign_name,
    mc.campaign_type,
    mc.target_segment,
    mc.start_date,
    mc.end_date,
    mc.budget,
    mc.expected_roi,
    mc.actual_roi,
    mc.campaign_status,
    -- Campaign metrics
    COUNT(DISTINCT cc.customer_id) AS customers_targeted,
    SUM(CASE WHEN cc.sent_date IS NOT NULL THEN 1 ELSE 0 END) AS messages_sent,
    SUM(CASE WHEN cc.opened THEN 1 ELSE 0 END) AS messages_opened,
    SUM(CASE WHEN cc.clicked THEN 1 ELSE 0 END) AS messages_clicked,
    SUM(CASE WHEN cc.converted THEN 1 ELSE 0 END) AS conversions,
    SUM(cc.conversion_value) AS total_conversion_value,
    -- Performance rates
    DIV0NULL(SUM(CASE WHEN cc.opened THEN 1 ELSE 0 END), SUM(CASE WHEN cc.sent_date IS NOT NULL THEN 1 ELSE 0 END)) AS open_rate,
    DIV0NULL(SUM(CASE WHEN cc.clicked THEN 1 ELSE 0 END), SUM(CASE WHEN cc.opened THEN 1 ELSE 0 END)) AS click_through_rate,
    DIV0NULL(SUM(CASE WHEN cc.converted THEN 1 ELSE 0 END), SUM(CASE WHEN cc.sent_date IS NOT NULL THEN 1 ELSE 0 END)) AS conversion_rate,
    -- Cost efficiency
    DIV0NULL(mc.budget, COUNT(DISTINCT cc.customer_id)) AS cost_per_customer,
    DIV0NULL(mc.budget, SUM(CASE WHEN cc.converted THEN 1 ELSE 0 END)) AS cost_per_conversion,
    DIV0NULL(SUM(cc.conversion_value), mc.budget) AS return_on_spend
FROM VARO_INTELLIGENCE.RAW.MARKETING_CAMPAIGNS mc
LEFT JOIN VARO_INTELLIGENCE.RAW.CUSTOMER_CAMPAIGNS cc ON mc.campaign_id = cc.campaign_id
GROUP BY
    mc.campaign_id, mc.campaign_name, mc.campaign_type, mc.target_segment,
    mc.start_date, mc.end_date, mc.budget, mc.expected_roi, mc.actual_roi, mc.campaign_status;

-- ============================================================================
-- Compliance Risk View
-- ============================================================================
CREATE OR REPLACE VIEW V_COMPLIANCE_RISK AS
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.kyc_status,
    c.risk_tier,
    -- AML indicators
    COUNT(DISTINCT ce.event_id) AS total_compliance_events,
    COUNT(DISTINCT CASE WHEN ce.event_type = 'SAR' THEN ce.event_id END) AS sar_count,
    COUNT(DISTINCT CASE WHEN ce.event_type = 'CTR' THEN ce.event_id END) AS ctr_count,
    MAX(ce.event_date) AS last_compliance_event_date,
    -- Transaction risk patterns
    SUM(CASE WHEN t.amount > 10000 THEN 1 ELSE 0 END) AS large_transactions_count,
    SUM(CASE WHEN t.is_international THEN 1 ELSE 0 END) AS international_transactions_count,
    SUM(CASE WHEN mc.high_risk_category THEN 1 ELSE 0 END) AS high_risk_merchant_transactions,
    -- Cash patterns
    SUM(CASE WHEN t.transaction_category = 'ATM' THEN ABS(t.amount) ELSE 0 END) AS total_cash_withdrawals,
    COUNT(DISTINCT CASE WHEN t.transaction_category = 'ATM' AND ABS(t.amount) > 1000 THEN t.transaction_date END) AS days_with_large_cash_withdrawals,
    -- Risk score
    CASE
        WHEN COUNT(CASE WHEN ce.event_type IN ('SAR', 'CTR') THEN 1 END) > 0 THEN 'HIGH'
        WHEN c.risk_tier = 'HIGH' OR SUM(CASE WHEN mc.high_risk_category THEN 1 ELSE 0 END) > 10 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS compliance_risk_level
FROM VARO_INTELLIGENCE.RAW.CUSTOMERS c
LEFT JOIN VARO_INTELLIGENCE.RAW.COMPLIANCE_EVENTS ce ON c.customer_id = ce.customer_id
LEFT JOIN VARO_INTELLIGENCE.RAW.TRANSACTIONS t ON c.customer_id = t.customer_id
    AND t.transaction_date >= DATEADD('day', -90, CURRENT_DATE())
LEFT JOIN VARO_INTELLIGENCE.RAW.MERCHANT_CATEGORIES mc ON t.merchant_category = mc.mcc_code
GROUP BY c.customer_id, c.first_name, c.last_name, c.kyc_status, c.risk_tier;

-- Display confirmation
SELECT 'Analytical views created successfully' AS STATUS;
