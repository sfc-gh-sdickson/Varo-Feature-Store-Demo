-- ============================================================================
-- Varo Intelligence Agent - Core Banking Table Definitions
-- ============================================================================
-- Purpose: Create all necessary tables for the Varo banking data model
-- Includes tables for customers, accounts, transactions, and ML features
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA RAW;
USE WAREHOUSE VARO_WH;

-- ============================================================================
-- CUSTOMERS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE CUSTOMERS (
    customer_id VARCHAR(20) PRIMARY KEY,
    email VARCHAR(200) NOT NULL,
    phone_number VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    ssn_last4 VARCHAR(4),
    address_street VARCHAR(200),
    address_city VARCHAR(100),
    address_state VARCHAR(2),
    address_zip VARCHAR(10),
    customer_status VARCHAR(20) DEFAULT 'ACTIVE',
    kyc_status VARCHAR(20) DEFAULT 'PENDING',
    risk_tier VARCHAR(20),
    acquisition_channel VARCHAR(50),
    acquisition_date DATE NOT NULL,
    first_deposit_date DATE,
    churn_date DATE,
    lifetime_value NUMBER(12,2) DEFAULT 0.00,
    credit_score NUMBER(3,0),
    income_verified NUMBER(12,2),
    employment_status VARCHAR(50),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- ACCOUNTS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE ACCOUNTS (
    account_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    account_type VARCHAR(30) NOT NULL, -- CHECKING, SAVINGS, BELIEVE_CARD, LINE_OF_CREDIT
    account_number VARCHAR(20) NOT NULL,
    routing_number VARCHAR(9) DEFAULT '103112036', -- Varo's routing number
    account_status VARCHAR(20) DEFAULT 'ACTIVE',
    opening_date DATE NOT NULL,
    closing_date DATE,
    current_balance NUMBER(12,2) DEFAULT 0.00,
    available_balance NUMBER(12,2) DEFAULT 0.00,
    credit_limit NUMBER(12,2),
    apy_rate NUMBER(5,4), -- For savings accounts
    overdraft_protection BOOLEAN DEFAULT FALSE,
    auto_save_enabled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- TRANSACTIONS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE TRANSACTIONS (
    transaction_id VARCHAR(30) PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    transaction_type VARCHAR(30) NOT NULL, -- DEBIT, CREDIT, TRANSFER, FEE, INTEREST
    transaction_category VARCHAR(50), -- ATM, POS, ONLINE, WIRE, ACH, etc.
    transaction_date DATE NOT NULL,
    transaction_timestamp TIMESTAMP_NTZ NOT NULL,
    amount NUMBER(12,2) NOT NULL,
    running_balance NUMBER(12,2),
    merchant_name VARCHAR(200),
    merchant_category VARCHAR(4), -- MCC code
    merchant_city VARCHAR(100),
    merchant_state VARCHAR(2),
    description VARCHAR(500),
    is_recurring BOOLEAN DEFAULT FALSE,
    is_international BOOLEAN DEFAULT FALSE,
    fraud_score NUMBER(3,2),
    status VARCHAR(20) DEFAULT 'COMPLETED',
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (account_id) REFERENCES ACCOUNTS(account_id),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- CARDS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE CARDS (
    card_id VARCHAR(20) PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    card_type VARCHAR(20) NOT NULL, -- DEBIT, BELIEVE_CREDIT
    card_number_last4 VARCHAR(4) NOT NULL,
    card_status VARCHAR(20) DEFAULT 'ACTIVE',
    issue_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    activation_date DATE,
    pin_set BOOLEAN DEFAULT FALSE,
    contactless_enabled BOOLEAN DEFAULT TRUE,
    international_enabled BOOLEAN DEFAULT TRUE,
    atm_enabled BOOLEAN DEFAULT TRUE,
    online_enabled BOOLEAN DEFAULT TRUE,
    daily_limit NUMBER(10,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (account_id) REFERENCES ACCOUNTS(account_id),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- DIRECT_DEPOSITS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE DIRECT_DEPOSITS (
    deposit_id VARCHAR(30) PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    employer_name VARCHAR(200),
    deposit_type VARCHAR(30), -- PAYROLL, GOVERNMENT, OTHER
    deposit_date DATE NOT NULL,
    expected_date DATE,
    amount NUMBER(12,2) NOT NULL,
    is_recurring BOOLEAN DEFAULT TRUE,
    frequency VARCHAR(20), -- WEEKLY, BIWEEKLY, MONTHLY
    early_access_eligible BOOLEAN DEFAULT TRUE,
    early_access_used BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (account_id) REFERENCES ACCOUNTS(account_id),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- CASH_ADVANCES TABLE
-- ============================================================================
CREATE OR REPLACE TABLE CASH_ADVANCES (
    advance_id VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    account_id VARCHAR(20) NOT NULL,
    advance_amount NUMBER(10,2) NOT NULL,
    fee_amount NUMBER(10,2) NOT NULL,
    advance_date DATE NOT NULL,
    due_date DATE NOT NULL,
    repayment_date DATE,
    repayment_amount NUMBER(10,2),
    advance_status VARCHAR(20) DEFAULT 'ACTIVE',
    eligibility_score NUMBER(3,2),
    default_risk_score NUMBER(3,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id),
    FOREIGN KEY (account_id) REFERENCES ACCOUNTS(account_id)
);

-- ============================================================================
-- CREDIT_APPLICATIONS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE CREDIT_APPLICATIONS (
    application_id VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    product_type VARCHAR(30) NOT NULL, -- BELIEVE_CARD, LINE_OF_CREDIT
    application_date DATE NOT NULL,
    application_status VARCHAR(30) DEFAULT 'PENDING',
    decision_date DATE,
    approved_amount NUMBER(10,2),
    requested_amount NUMBER(10,2),
    credit_score_at_application NUMBER(3,0),
    income_stated NUMBER(12,2),
    debt_to_income_ratio NUMBER(5,2),
    decision_reason_codes VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- MERCHANT_CATEGORIES TABLE
-- ============================================================================
CREATE OR REPLACE TABLE MERCHANT_CATEGORIES (
    mcc_code VARCHAR(4) PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    category_group VARCHAR(50),
    cashback_eligible BOOLEAN DEFAULT FALSE,
    cashback_percentage NUMBER(3,2),
    high_risk_category BOOLEAN DEFAULT FALSE,
    typical_transaction_amount NUMBER(10,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- DEVICE_SESSIONS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE DEVICE_SESSIONS (
    session_id VARCHAR(40) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    device_id VARCHAR(100),
    device_type VARCHAR(20), -- IOS, ANDROID, WEB
    app_version VARCHAR(20),
    session_start TIMESTAMP_NTZ NOT NULL,
    session_end TIMESTAMP_NTZ,
    ip_address VARCHAR(45),
    geolocation VARCHAR(100),
    actions_performed VARCHAR(500),
    biometric_used BOOLEAN DEFAULT FALSE,
    suspicious_activity_flag BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- SUPPORT_INTERACTIONS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE SUPPORT_INTERACTIONS (
    interaction_id VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    interaction_type VARCHAR(30), -- CHAT, PHONE, EMAIL
    interaction_date DATE NOT NULL,
    category VARCHAR(50),
    subcategory VARCHAR(50),
    issue_resolved BOOLEAN,
    resolution_time_minutes NUMBER(10,0),
    agent_id VARCHAR(20),
    satisfaction_score NUMBER(1,0), -- 1-5
    transcript_available BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- MARKETING_CAMPAIGNS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE MARKETING_CAMPAIGNS (
    campaign_id VARCHAR(30) PRIMARY KEY,
    campaign_name VARCHAR(200) NOT NULL,
    campaign_type VARCHAR(50), -- EMAIL, PUSH, IN_APP, SMS
    target_segment VARCHAR(100),
    start_date DATE NOT NULL,
    end_date DATE,
    budget NUMBER(10,2),
    expected_roi NUMBER(5,2),
    actual_roi NUMBER(5,2),
    campaign_status VARCHAR(20) DEFAULT 'PLANNED',
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- CUSTOMER_CAMPAIGNS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE CUSTOMER_CAMPAIGNS (
    customer_campaign_id VARCHAR(40) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    campaign_id VARCHAR(30) NOT NULL,
    sent_date DATE NOT NULL,
    opened BOOLEAN DEFAULT FALSE,
    clicked BOOLEAN DEFAULT FALSE,
    converted BOOLEAN DEFAULT FALSE,
    conversion_value NUMBER(10,2),
    unsubscribed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id),
    FOREIGN KEY (campaign_id) REFERENCES MARKETING_CAMPAIGNS(campaign_id)
);

-- ============================================================================
-- EXTERNAL_DATA TABLE (Credit Bureau and Bank Verification)
-- ============================================================================
CREATE OR REPLACE TABLE EXTERNAL_DATA (
    external_data_id VARCHAR(40) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    data_source VARCHAR(50) NOT NULL, -- EQUIFAX, EXPERIAN, PLAID, YODLEE
    data_type VARCHAR(50), -- CREDIT_REPORT, BANK_VERIFICATION, INCOME_VERIFICATION
    pull_date DATE NOT NULL,
    credit_score NUMBER(3,0),
    credit_utilization NUMBER(5,2),
    total_debt NUMBER(12,2),
    monthly_income NUMBER(12,2),
    number_of_accounts NUMBER(3,0),
    oldest_account_age_months NUMBER(4,0),
    recent_inquiries NUMBER(2,0),
    data_json VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- COMPLIANCE_EVENTS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE COMPLIANCE_EVENTS (
    event_id VARCHAR(40) PRIMARY KEY,
    customer_id VARCHAR(20),
    account_id VARCHAR(20),
    event_type VARCHAR(50) NOT NULL, -- SAR, CTR, KYC_UPDATE, OFAC_HIT
    event_date DATE NOT NULL,
    severity VARCHAR(20),
    description VARCHAR(1000),
    action_taken VARCHAR(500),
    resolved BOOLEAN DEFAULT FALSE,
    resolution_date DATE,
    reported_to_regulator BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id),
    FOREIGN KEY (account_id) REFERENCES ACCOUNTS(account_id)
);

-- ============================================================================
-- Optimize tables for performance using clustering
-- ============================================================================
-- In Snowflake, we use clustering instead of indexes for query optimization
ALTER TABLE TRANSACTIONS CLUSTER BY (customer_id, transaction_date);
ALTER TABLE ACCOUNTS CLUSTER BY (customer_id);
ALTER TABLE CARDS CLUSTER BY (customer_id);
ALTER TABLE CASH_ADVANCES CLUSTER BY (customer_id);

-- Note: Clustering keys automatically optimize query performance
-- Snowflake's micro-partitions and pruning eliminate the need for traditional indexes

-- Display confirmation
SELECT 'Core banking tables created successfully' AS STATUS;
