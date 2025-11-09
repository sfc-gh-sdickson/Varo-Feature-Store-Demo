-- ============================================================================
-- Varo Intelligence Agent - Synthetic Banking Data Generation
-- ============================================================================
-- Purpose: Generate realistic sample data for Varo banking operations
-- Volume: ~2M customers, 500M transactions, comprehensive feature data
-- Runtime: ~15-20 minutes
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA RAW;
USE WAREHOUSE VARO_FEATURE_WH; -- Use larger warehouse for data generation

-- ============================================================================
-- Truncate all tables to ensure clean data load
-- ============================================================================
TRUNCATE TABLE IF EXISTS VARO_INTELLIGENCE.RAW.SUPPORT_TRANSCRIPTS;
TRUNCATE TABLE IF EXISTS VARO_INTELLIGENCE.RAW.COMPLIANCE_DOCUMENTS;
TRUNCATE TABLE IF EXISTS VARO_INTELLIGENCE.RAW.PRODUCT_KNOWLEDGE;
TRUNCATE TABLE IF EXISTS VARO_INTELLIGENCE.FEATURE_STORE.FEATURE_VALUES;
TRUNCATE TABLE IF EXISTS VARO_INTELLIGENCE.FEATURE_STORE.FEATURE_SETS;
TRUNCATE TABLE IF EXISTS VARO_INTELLIGENCE.FEATURE_STORE.FEATURE_DEFINITIONS;
TRUNCATE TABLE IF EXISTS SUPPORT_INTERACTIONS;
TRUNCATE TABLE IF EXISTS CASH_ADVANCES;
TRUNCATE TABLE IF EXISTS DIRECT_DEPOSITS;
TRUNCATE TABLE IF EXISTS TRANSACTIONS;
TRUNCATE TABLE IF EXISTS CARDS;
TRUNCATE TABLE IF EXISTS CUSTOMER_CAMPAIGNS;
TRUNCATE TABLE IF EXISTS MARKETING_CAMPAIGNS;
TRUNCATE TABLE IF EXISTS ACCOUNTS;
TRUNCATE TABLE IF EXISTS CUSTOMERS;
TRUNCATE TABLE IF EXISTS COMPLIANCE_EVENTS;
TRUNCATE TABLE IF EXISTS EXTERNAL_DATA;
TRUNCATE TABLE IF EXISTS CREDIT_APPLICATIONS;
TRUNCATE TABLE IF EXISTS DEVICE_SESSIONS;
TRUNCATE TABLE IF EXISTS MERCHANT_CATEGORIES;

-- ============================================================================
-- Step 1: Generate Merchant Categories
-- ============================================================================
INSERT INTO MERCHANT_CATEGORIES VALUES
('5411', 'Grocery Stores, Supermarkets', 'GROCERIES', TRUE, 2.00, FALSE, 75.00, CURRENT_TIMESTAMP()),
('5814', 'Fast Food Restaurants', 'DINING', TRUE, 3.00, FALSE, 15.00, CURRENT_TIMESTAMP()),
('5541', 'Service Stations', 'GAS', TRUE, 2.00, FALSE, 50.00, CURRENT_TIMESTAMP()),
('5912', 'Drug Stores and Pharmacies', 'HEALTHCARE', TRUE, 2.00, FALSE, 35.00, CURRENT_TIMESTAMP()),
('4111', 'Transportation-Suburban and Local Commuter Passenger', 'TRANSPORT', TRUE, 1.00, FALSE, 20.00, CURRENT_TIMESTAMP()),
('5812', 'Eating Places, Restaurants', 'DINING', TRUE, 3.00, FALSE, 45.00, CURRENT_TIMESTAMP()),
('5999', 'Miscellaneous and Specialty Retail Stores', 'SHOPPING', TRUE, 1.00, FALSE, 50.00, CURRENT_TIMESTAMP()),
('5311', 'Department Stores', 'SHOPPING', TRUE, 1.00, FALSE, 100.00, CURRENT_TIMESTAMP()),
('5732', 'Electronic Sales', 'ELECTRONICS', TRUE, 1.00, FALSE, 200.00, CURRENT_TIMESTAMP()),
('7832', 'Motion Picture Theaters', 'ENTERTAINMENT', TRUE, 3.00, FALSE, 25.00, CURRENT_TIMESTAMP()),
('5045', 'Computers, Peripherals, and Software', 'ELECTRONICS', TRUE, 1.00, FALSE, 500.00, CURRENT_TIMESTAMP()),
('5691', 'Mens and Womens Clothing Stores', 'SHOPPING', TRUE, 1.00, FALSE, 80.00, CURRENT_TIMESTAMP()),
('7995', 'Betting, Casino Gambling', 'GAMBLING', FALSE, 0.00, TRUE, 100.00, CURRENT_TIMESTAMP()),
('6010', 'Financial Institutions-Manual Cash Disbursements', 'CASH', FALSE, 0.00, TRUE, 300.00, CURRENT_TIMESTAMP()),
('6011', 'Financial Institutions-Automated Cash Disbursements', 'ATM', FALSE, 0.00, FALSE, 200.00, CURRENT_TIMESTAMP()),
('5933', 'Pawn Shops', 'PAWN', FALSE, 0.00, TRUE, 150.00, CURRENT_TIMESTAMP()),
('7399', 'Business Services-Not Elsewhere Classified', 'SERVICES', FALSE, 0.00, FALSE, 150.00, CURRENT_TIMESTAMP()),
('5122', 'Drugs, Drug Proprietaries, and Druggist Sundries', 'HEALTHCARE', TRUE, 2.00, FALSE, 50.00, CURRENT_TIMESTAMP()),
('4814', 'Telecommunication Services', 'UTILITIES', FALSE, 0.00, FALSE, 80.00, CURRENT_TIMESTAMP()),
('4900', 'Utilities-Electric, Gas, Water, Sanitary', 'UTILITIES', FALSE, 0.00, FALSE, 150.00, CURRENT_TIMESTAMP());

-- ============================================================================
-- Step 2: Generate Customers (2M)
-- ============================================================================
INSERT INTO CUSTOMERS
SELECT
    'CUST' || LPAD(SEQ4(), 8, '0') AS customer_id,
    LOWER(
        ARRAY_CONSTRUCT('john', 'sarah', 'michael', 'emma', 'david', 'olivia', 'james', 'sophia', 
                       'robert', 'isabella', 'william', 'mia', 'joseph', 'charlotte', 'charles')[UNIFORM(0, 14, RANDOM())]
    ) || '.' ||
    LOWER(
        ARRAY_CONSTRUCT('smith', 'johnson', 'williams', 'brown', 'jones', 'garcia', 'miller', 'davis',
                       'rodriguez', 'martinez', 'hernandez', 'lopez', 'gonzalez', 'wilson', 'anderson')[UNIFORM(0, 14, RANDOM())]
    ) || UNIFORM(1000, 9999, RANDOM()) || '@' ||
    ARRAY_CONSTRUCT('gmail.com', 'yahoo.com', 'outlook.com', 'icloud.com', 'protonmail.com')[UNIFORM(0, 4, RANDOM())] AS email,
    
    '+1' || LPAD(UNIFORM(200, 999, RANDOM()), 3, '0') || 
    LPAD(UNIFORM(200, 999, RANDOM()), 3, '0') || 
    LPAD(UNIFORM(1000, 9999, RANDOM()), 4, '0') AS phone_number,
    
    ARRAY_CONSTRUCT('John', 'Sarah', 'Michael', 'Emma', 'David', 'Olivia', 'James', 'Sophia', 
                   'Robert', 'Isabella', 'William', 'Mia', 'Joseph', 'Charlotte', 'Charles')[UNIFORM(0, 14, RANDOM())] AS first_name,
                   
    ARRAY_CONSTRUCT('Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                   'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson')[UNIFORM(0, 14, RANDOM())] AS last_name,
                   
    DATEADD('year', -1 * UNIFORM(18, 75, RANDOM()), 
            DATEADD('day', -1 * UNIFORM(0, 365, RANDOM()), CURRENT_DATE())) AS date_of_birth,
            
    LPAD(UNIFORM(1000, 9999, RANDOM()), 4, '0') AS ssn_last4,
    
    UNIFORM(100, 9999, RANDOM()) || ' ' ||
    ARRAY_CONSTRUCT('Main', 'Oak', 'Elm', 'Maple', 'Pine', 'Cedar', 'Birch', 'Walnut',
                   'Cherry', 'Ash', 'Spruce', 'Willow', 'Poplar', 'Magnolia')[UNIFORM(0, 13, RANDOM())] || ' ' ||
    ARRAY_CONSTRUCT('St', 'Ave', 'Blvd', 'Ln', 'Dr', 'Way', 'Rd', 'Ct')[UNIFORM(0, 7, RANDOM())] AS address_street,
    
    ARRAY_CONSTRUCT('New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
                   'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
                   'Fort Worth', 'Columbus', 'Charlotte', 'San Francisco', 'Indianapolis',
                   'Seattle', 'Denver', 'Washington')[UNIFORM(0, 19, RANDOM())] AS address_city,
                   
    ARRAY_CONSTRUCT('NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA', 
                   'TX', 'FL', 'TX', 'OH', 'NC', 'CA', 'IN', 'WA', 'CO', 'DC')[UNIFORM(0, 19, RANDOM())] AS address_state,
                   
    LPAD(UNIFORM(10000, 99999, RANDOM()), 5, '0') AS address_zip,
    
    CASE 
        WHEN UNIFORM(0, 100, RANDOM()) < 95 THEN 'ACTIVE'
        WHEN UNIFORM(0, 100, RANDOM()) < 98 THEN 'SUSPENDED'
        ELSE 'CLOSED'
    END AS customer_status,
    
    CASE 
        WHEN UNIFORM(0, 100, RANDOM()) < 85 THEN 'VERIFIED'
        WHEN UNIFORM(0, 100, RANDOM()) < 95 THEN 'PENDING'
        ELSE 'FAILED'
    END AS kyc_status,
    
    ARRAY_CONSTRUCT('LOW', 'MEDIUM', 'HIGH')[UNIFORM(0, 2, RANDOM())] AS risk_tier,
    
    ARRAY_CONSTRUCT('MOBILE_APP', 'WEB', 'REFERRAL', 'PAID_AD', 'ORGANIC', 'PARTNER')[UNIFORM(0, 5, RANDOM())] AS acquisition_channel,
    
    DATEADD('day', -1 * UNIFORM(1, 1825, RANDOM()), CURRENT_DATE()) AS acquisition_date,
    DATEADD('day', UNIFORM(1, 30, RANDOM()), DATEADD('day', -1 * UNIFORM(1, 1825, RANDOM()), CURRENT_DATE())) AS first_deposit_date,
    
    CASE WHEN customer_status = 'CLOSED' 
         THEN DATEADD('day', UNIFORM(30, 365, RANDOM()), DATEADD('day', -1 * UNIFORM(1, 1825, RANDOM()), CURRENT_DATE()))
         ELSE NULL 
    END AS churn_date,
    
    ROUND(UNIFORM(0, 50000, RANDOM()) * (1.0 + UNIFORM(0, 10, RANDOM()) / 10.0), 2) AS lifetime_value,
    
    -- Credit score distribution (realistic)
    CASE 
        WHEN UNIFORM(0, 100, RANDOM()) < 5 THEN UNIFORM(300, 549, RANDOM())   -- 5% Poor
        WHEN UNIFORM(0, 100, RANDOM()) < 20 THEN UNIFORM(550, 649, RANDOM())  -- 15% Fair  
        WHEN UNIFORM(0, 100, RANDOM()) < 60 THEN UNIFORM(650, 749, RANDOM())  -- 40% Good
        WHEN UNIFORM(0, 100, RANDOM()) < 90 THEN UNIFORM(750, 799, RANDOM())  -- 30% Very Good
        ELSE UNIFORM(800, 850, RANDOM())                                       -- 10% Excellent
    END AS credit_score,
    
    ROUND(UNIFORM(20000, 150000, RANDOM()), -3) AS income_verified,
    
    ARRAY_CONSTRUCT('EMPLOYED_FULL_TIME', 'EMPLOYED_PART_TIME', 'SELF_EMPLOYED', 
                   'UNEMPLOYED', 'RETIRED', 'STUDENT')[UNIFORM(0, 5, RANDOM())] AS employment_status,
                   
    DATEADD('second', -1 * UNIFORM(0, 86400, RANDOM()), CURRENT_TIMESTAMP()) AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM TABLE(GENERATOR(ROWCOUNT => 2000000));

-- ============================================================================
-- Step 3: Generate Accounts (~3.5M - multiple per customer)
-- ============================================================================
INSERT INTO ACCOUNTS
SELECT
    'ACC' || LPAD(SEQ4(), 10, '0') AS account_id,
    customer_id,
    account_type,
    LPAD(UNIFORM(1000000000, 9999999999, RANDOM())::VARCHAR, 10, '0') AS account_number,
    '103112036' AS routing_number, -- Varo's actual routing number
    CASE 
        WHEN c.customer_status = 'CLOSED' THEN 'CLOSED'
        WHEN UNIFORM(0, 100, RANDOM()) < 95 THEN 'ACTIVE'
        ELSE 'SUSPENDED'
    END AS account_status,
    
    DATEADD('day', UNIFORM(0, 30, RANDOM()), c.acquisition_date) AS opening_date,
    
    CASE 
        WHEN c.customer_status = 'CLOSED' THEN DATEADD('day', UNIFORM(30, 365, RANDOM()), DATEADD('day', UNIFORM(0, 30, RANDOM()), c.acquisition_date))
        ELSE NULL
    END AS closing_date,
    
    -- Credit limits based on credit score (calculated first)
    CASE 
        WHEN account_type = 'BELIEVE_CARD' THEN 
            CASE
                WHEN c.credit_score < 600 THEN 500.00
                WHEN c.credit_score < 700 THEN 1000.00
                WHEN c.credit_score < 750 THEN 2500.00
                ELSE 5000.00
            END
        WHEN account_type = 'LINE_OF_CREDIT' THEN 
            CASE
                WHEN c.credit_score < 650 THEN 1000.00
                WHEN c.credit_score < 700 THEN 2000.00
                WHEN c.credit_score < 750 THEN 5000.00
                ELSE 10000.00
            END
        ELSE NULL
    END AS credit_limit,
    
    -- Balance based on account type and customer profile (using credit_limit)
    CASE 
        WHEN account_type = 'CHECKING' THEN ROUND(UNIFORM(0, 5000, RANDOM()), 2)
        WHEN account_type = 'SAVINGS' THEN ROUND(UNIFORM(0, 25000, RANDOM()), 2)
        WHEN account_type = 'BELIEVE_CARD' THEN 
            -CASE
                WHEN c.credit_score < 600 THEN ROUND(UNIFORM(0, 350, RANDOM()), 2)
                WHEN c.credit_score < 700 THEN ROUND(UNIFORM(0, 700, RANDOM()), 2)
                WHEN c.credit_score < 750 THEN ROUND(UNIFORM(0, 1750, RANDOM()), 2)
                ELSE ROUND(UNIFORM(0, 3500, RANDOM()), 2)
            END
        WHEN account_type = 'LINE_OF_CREDIT' THEN 
            -CASE
                WHEN c.credit_score < 650 THEN ROUND(UNIFORM(0, 500, RANDOM()), 2)
                WHEN c.credit_score < 700 THEN ROUND(UNIFORM(0, 1000, RANDOM()), 2)
                WHEN c.credit_score < 750 THEN ROUND(UNIFORM(0, 2500, RANDOM()), 2)
                ELSE ROUND(UNIFORM(0, 5000, RANDOM()), 2)
            END
        ELSE 0.00
    END AS current_balance,
    
    -- Available balance (calculated last)
    CASE 
        WHEN account_type IN ('CHECKING', 'SAVINGS') THEN 
            -- For deposit accounts, available = current
            CASE 
                WHEN account_type = 'CHECKING' THEN ROUND(UNIFORM(0, 5000, RANDOM()), 2)
                WHEN account_type = 'SAVINGS' THEN ROUND(UNIFORM(0, 25000, RANDOM()), 2)
                ELSE 0.00
            END
        WHEN account_type = 'BELIEVE_CARD' THEN 
            -- For credit accounts, available = limit - used (limit + negative balance)
            CASE
                WHEN c.credit_score < 600 THEN ROUND(500.00 - UNIFORM(0, 350, RANDOM()), 2)
                WHEN c.credit_score < 700 THEN ROUND(1000.00 - UNIFORM(0, 700, RANDOM()), 2)
                WHEN c.credit_score < 750 THEN ROUND(2500.00 - UNIFORM(0, 1750, RANDOM()), 2)
                ELSE ROUND(5000.00 - UNIFORM(0, 3500, RANDOM()), 2)
            END
        WHEN account_type = 'LINE_OF_CREDIT' THEN 
            CASE
                WHEN c.credit_score < 650 THEN ROUND(1000.00 - UNIFORM(0, 500, RANDOM()), 2)
                WHEN c.credit_score < 700 THEN ROUND(2000.00 - UNIFORM(0, 1000, RANDOM()), 2)
                WHEN c.credit_score < 750 THEN ROUND(5000.00 - UNIFORM(0, 2500, RANDOM()), 2)
                ELSE ROUND(10000.00 - UNIFORM(0, 5000, RANDOM()), 2)
            END
        ELSE 0.00
    END AS available_balance,
    
    -- APY for savings accounts
    CASE 
        WHEN account_type = 'SAVINGS' THEN 
            CASE 
                WHEN ROUND(UNIFORM(0, 25000, RANDOM()), 2) >= 5000 THEN 5.00
                ELSE 2.50
            END
        ELSE NULL
    END AS apy_rate,
    
    CASE WHEN account_type = 'CHECKING' THEN UNIFORM(0, 1, RANDOM()) = 1 ELSE FALSE END AS overdraft_protection,
    CASE WHEN account_type = 'SAVINGS' THEN UNIFORM(0, 1, RANDOM()) = 1 ELSE FALSE END AS auto_save_enabled,
    
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM (
    -- Generate 1-3 accounts per customer
    SELECT 
        c.*,
        CASE rn
            WHEN 1 THEN 'CHECKING'
            WHEN 2 THEN ARRAY_CONSTRUCT('SAVINGS', 'BELIEVE_CARD')[UNIFORM(0, 1, RANDOM())]
            WHEN 3 THEN ARRAY_CONSTRUCT('SAVINGS', 'LINE_OF_CREDIT')[UNIFORM(0, 1, RANDOM())]
        END AS account_type
    FROM (
        SELECT 
            c.*,
            ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY RANDOM()) as rn
        FROM CUSTOMERS c
        CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 3))
    ) c
    WHERE rn <= 
        CASE 
            WHEN UNIFORM(0, 100, RANDOM()) < 40 THEN 1  -- 40% have 1 account
            WHEN UNIFORM(0, 100, RANDOM()) < 80 THEN 2  -- 40% have 2 accounts  
            ELSE 3                                       -- 20% have 3 accounts
        END
) c;

-- ============================================================================
-- Step 4: Generate Cards
-- ============================================================================
INSERT INTO CARDS
SELECT
    'CARD' || LPAD(SEQ4(), 10, '0') AS card_id,
    a.account_id,
    a.customer_id,
    CASE 
        WHEN a.account_type IN ('CHECKING', 'SAVINGS') THEN 'DEBIT'
        ELSE 'BELIEVE_CREDIT'
    END AS card_type,
    LPAD(UNIFORM(1000, 9999, RANDOM()), 4, '0') AS card_number_last4,
    CASE 
        WHEN a.account_status = 'ACTIVE' THEN 
            CASE UNIFORM(0, 100, RANDOM())
                WHEN 0 THEN 'LOST'
                WHEN 1 THEN 'STOLEN'
                WHEN 2 THEN 'DAMAGED'
                ELSE 'ACTIVE'
            END
        ELSE 'INACTIVE'
    END AS card_status,
    a.opening_date AS issue_date,
    DATEADD('year', 3, a.opening_date) AS expiration_date,
    DATEADD('day', UNIFORM(0, 7, RANDOM()), a.opening_date) AS activation_date,
    TRUE AS pin_set,
    TRUE AS contactless_enabled,
    UNIFORM(0, 1, RANDOM()) = 1 AS international_enabled,
    TRUE AS atm_enabled,
    TRUE AS online_enabled,
    CASE 
        WHEN a.account_type IN ('CHECKING', 'SAVINGS') THEN 1000.00
        ELSE 5000.00
    END AS daily_limit,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM ACCOUNTS a
WHERE a.account_type IN ('CHECKING', 'SAVINGS', 'BELIEVE_CARD');

-- ============================================================================
-- Step 5: Generate Marketing Campaigns
-- ============================================================================
INSERT INTO MARKETING_CAMPAIGNS
SELECT
    'CAMP' || LPAD(SEQ4(), 6, '0') AS campaign_id,
    campaign_name,
    campaign_type,
    target_segment,
    start_date,
    DATEADD('day', duration_days, start_date) AS end_date,
    budget,
    expected_roi,
    CASE 
        WHEN DATEADD('day', duration_days, start_date) < CURRENT_DATE() THEN ROUND(expected_roi * UNIFORM(70, 130, RANDOM()) / 100.0, 2)
        ELSE NULL
    END AS actual_roi,
    CASE 
        WHEN start_date > CURRENT_DATE() THEN 'PLANNED'
        WHEN DATEADD('day', duration_days, start_date) < CURRENT_DATE() THEN 'COMPLETED'
        ELSE 'ACTIVE'
    END AS campaign_status,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM (
    VALUES
    ('Welcome Series - New Customers', 'EMAIL', 'NEW_CUSTOMERS', '2024-01-01'::DATE, 365, 50000, 3.5),
    ('Cash Advance Awareness Q1', 'PUSH', 'ELIGIBLE_ADVANCE', '2024-01-15'::DATE, 75, 25000, 4.2),
    ('Believe Card Launch', 'EMAIL', 'GOOD_CREDIT', '2024-02-01'::DATE, 90, 100000, 5.0),
    ('Tax Refund Savings', 'IN_APP', 'ALL_CUSTOMERS', '2024-02-15'::DATE, 60, 30000, 2.8),
    ('Spring Cashback Boost', 'PUSH', 'ACTIVE_SPENDERS', '2024-03-01'::DATE, 30, 20000, 3.2),
    ('Credit Building Tips', 'EMAIL', 'LOW_CREDIT', '2024-03-15'::DATE, 45, 15000, 2.5),
    ('Summer Travel Rewards', 'IN_APP', 'HIGH_SPENDERS', '2024-06-01'::DATE, 90, 40000, 4.5),
    ('Back to School Advance', 'SMS', 'PARENTS', '2024-08-01'::DATE, 30, 35000, 3.8),
    ('Holiday Shopping Cashback', 'EMAIL', 'ALL_CUSTOMERS', '2024-11-01'::DATE, 60, 80000, 6.2),
    ('Year End Savings Challenge', 'PUSH', 'SAVERS', '2024-11-15'::DATE, 45, 25000, 3.0),
    ('New Year Financial Goals', 'EMAIL', 'ALL_CUSTOMERS', '2025-01-01'::DATE, 30, 40000, 3.5),
    ('Valentine Dining Rewards', 'IN_APP', 'COUPLES', '2025-02-01'::DATE, 14, 15000, 2.8)
) AS campaigns(campaign_name, campaign_type, target_segment, start_date, duration_days, budget, expected_roi);

-- ============================================================================
-- Step 6: Generate Direct Deposits (Simplified)
-- ============================================================================
INSERT INTO DIRECT_DEPOSITS
WITH account_periods AS (
    SELECT 
        a.account_id,
        a.customer_id,
        DATEADD('day', -1 * UNIFORM(0, 730, RANDOM()), CURRENT_DATE()) AS base_date,
        ARRAY_CONSTRUCT('WEEKLY', 'BIWEEKLY', 'MONTHLY')[UNIFORM(0, 2, RANDOM())] AS frequency,
        ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS period_num
    FROM ACCOUNTS a
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 24))
    WHERE a.account_type = 'CHECKING'
        AND a.customer_id IN (
            SELECT customer_id 
            FROM CUSTOMERS 
            WHERE employment_status IN ('EMPLOYED_FULL_TIME', 'EMPLOYED_PART_TIME', 'SELF_EMPLOYED', 'RETIRED')
            AND UNIFORM(0, 100, RANDOM()) < 80
        )
),
deposits_with_dates AS (
    SELECT 
        account_id,
        customer_id,
        frequency,
        CASE frequency
            WHEN 'WEEKLY' THEN DATEADD('week', period_num, base_date)
            WHEN 'BIWEEKLY' THEN DATEADD('week', period_num * 2, base_date)
            WHEN 'MONTHLY' THEN DATEADD('month', period_num, base_date)
        END AS deposit_date
    FROM account_periods
),
deposits_with_employer AS (
    SELECT 
        account_id,
        customer_id,
        frequency,
        deposit_date,
        ARRAY_CONSTRUCT(
            'Amazon', 'Google', 'Apple', 'Microsoft', 'Facebook', 'Netflix', 'Uber', 'Lyft',
            'DoorDash', 'Instacart', 'Walmart', 'Target', 'Costco', 'Starbucks', 'McDonalds',
            'US Government', 'State of California', 'City of New York', 'Veterans Affairs',
            'Social Security Admin', 'Unemployment Insurance', NULL
        )[UNIFORM(0, 21, RANDOM())] AS employer_name
    FROM deposits_with_dates
    WHERE deposit_date <= CURRENT_DATE()
),
deposits_with_type AS (
    SELECT 
        account_id,
        customer_id,
        frequency,
        deposit_date,
        employer_name,
        CASE 
            WHEN employer_name LIKE '%Government%' OR employer_name LIKE '%Security%' 
                 OR employer_name LIKE '%Veterans%' OR employer_name LIKE '%Unemployment%'
            THEN 'GOVERNMENT'
            WHEN employer_name IS NULL THEN 'OTHER'
            ELSE 'PAYROLL'
        END AS deposit_type
    FROM deposits_with_employer
)
SELECT
    'DD' || LPAD(SEQ4(), 10, '0') AS deposit_id,
    account_id,
    customer_id,
    employer_name,
    deposit_type,
    deposit_date,
    CASE 
        WHEN deposit_type = 'PAYROLL' AND frequency = 'BIWEEKLY' 
        THEN DATEADD('day', -2, deposit_date)
        ELSE deposit_date
    END AS expected_date,
    CASE 
        WHEN deposit_type = 'PAYROLL' THEN ROUND(UNIFORM(1000, 5000, RANDOM()), -1)
        WHEN deposit_type = 'GOVERNMENT' THEN ROUND(UNIFORM(500, 2000, RANDOM()), -1)
        ELSE ROUND(UNIFORM(100, 1000, RANDOM()), -1)
    END AS amount,
    TRUE AS is_recurring,
    frequency,
    deposit_type = 'PAYROLL' AS early_access_eligible,
    CASE 
        WHEN deposit_type = 'PAYROLL' AND UNIFORM(0, 100, RANDOM()) < 70 
        THEN TRUE 
        ELSE FALSE 
    END AS early_access_used,
    CURRENT_TIMESTAMP() AS created_at
FROM deposits_with_type;

-- ============================================================================
-- Step 7: Generate Transactions (500M - this will take time)
-- Split into smaller batches for performance
-- ============================================================================

-- First, create a temporary table with merchant data
CREATE OR REPLACE TEMPORARY TABLE temp_merchants AS
SELECT 
    merchant_name,
    mcc_code,
    ARRAY_CONSTRUCT('New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
                   'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose')[UNIFORM(0, 9, RANDOM())] AS merchant_city,
    ARRAY_CONSTRUCT('NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA')[UNIFORM(0, 9, RANDOM())] AS merchant_state
FROM (
    VALUES
    ('Walmart', '5411'), ('Target', '5411'), ('Kroger', '5411'), ('Whole Foods', '5411'),
    ('McDonalds', '5814'), ('Starbucks', '5814'), ('Subway', '5814'), ('Chipotle', '5814'),
    ('Shell', '5541'), ('Exxon', '5541'), ('BP', '5541'), ('Chevron', '5541'),
    ('CVS Pharmacy', '5912'), ('Walgreens', '5912'), ('Rite Aid', '5912'),
    ('Uber', '4111'), ('Lyft', '4111'), ('Yellow Cab', '4111'),
    ('Amazon.com', '5999'), ('Best Buy', '5732'), ('Apple Store', '5045'),
    ('Netflix', '7832'), ('Spotify', '7832'), ('Hulu', '7832'),
    ('ATM Withdrawal', '6011'), ('Cash App', '6010'), ('Venmo', '6010'),
    ('T-Mobile', '4814'), ('Verizon', '4814'), ('AT&T', '4814'),
    ('Con Edison', '4900'), ('PG&E', '4900'), ('ComEd', '4900')
) AS merchants(merchant_name, mcc_code);

-- Generate transactions in batches (adjust ROWCOUNT for performance)
INSERT INTO TRANSACTIONS
SELECT
    'TXN' || LPAD(SEQ4(), 12, '0') AS transaction_id,
    base_txn.account_id,
    base_txn.customer_id,
    base_txn.transaction_type,
    CASE 
        WHEN base_txn.transaction_type = 'DEBIT' THEN 
            CASE 
                WHEN m.mcc_code = '6011' THEN 'ATM'
                WHEN m.mcc_code IN ('6010') THEN 'P2P'
                ELSE 'POS'
            END
        WHEN base_txn.transaction_type = 'CREDIT' THEN 'ACH'
        WHEN base_txn.transaction_type = 'TRANSFER' THEN 'TRANSFER'
        ELSE 'OTHER'
    END AS transaction_category,
    
    base_txn.transaction_date,
    DATEADD('second', UNIFORM(0, 86399, RANDOM()), base_txn.transaction_date::TIMESTAMP_NTZ) AS transaction_timestamp,
    
    CASE 
        WHEN base_txn.transaction_type = 'DEBIT' THEN -ABS(base_txn.amount)
        ELSE ABS(base_txn.amount)
    END AS amount,
    
    -- Running balance (simplified - in production would be calculated properly)
    base_txn.current_balance + SUM(CASE 
        WHEN base_txn.transaction_type = 'DEBIT' THEN -ABS(base_txn.amount)
        ELSE ABS(base_txn.amount)
    END) OVER (
        PARTITION BY base_txn.account_id 
        ORDER BY DATEADD('second', UNIFORM(0, 86399, RANDOM()), base_txn.transaction_date::TIMESTAMP_NTZ)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_balance,
    
    m.merchant_name,
    m.mcc_code AS merchant_category,
    m.merchant_city,
    m.merchant_state,
    
    CASE base_txn.transaction_type
        WHEN 'DEBIT' THEN m.merchant_name || ' Purchase'
        WHEN 'CREDIT' THEN 'Direct Deposit - ' || COALESCE(base_txn.employer_name, 'Transfer')
        WHEN 'TRANSFER' THEN 'Transfer ' || CASE WHEN base_txn.amount > 0 THEN 'In' ELSE 'Out' END
        ELSE 'Fee/Adjustment'
    END AS description,
    
    -- Recurring if it's a utility or subscription
    m.mcc_code IN ('4814', '4900', '7832') AS is_recurring,
    
    -- International (5% of transactions)
    UNIFORM(0, 100, RANDOM()) < 5 AS is_international,
    
    -- Fraud score (higher for certain MCCs and amounts)
    LEAST(99, GREATEST(1, 
        CASE 
            WHEN m.mcc_code IN ('7995', '5933', '6010') THEN UNIFORM(20, 80, RANDOM())
            WHEN ABS(base_txn.amount) > 1000 THEN UNIFORM(10, 50, RANDOM())
            WHEN UNIFORM(0, 100, RANDOM()) < 5 THEN UNIFORM(5, 30, RANDOM()) -- 5% international
            ELSE UNIFORM(1, 10, RANDOM())
        END
    )) / 100.0 AS fraud_score,
    
    CASE 
        WHEN UNIFORM(0, 10000, RANDOM()) = 0 THEN 'DECLINED'
        WHEN UNIFORM(0, 10000, RANDOM()) = 1 THEN 'PENDING'
        ELSE 'COMPLETED'
    END AS status,
    
    CURRENT_TIMESTAMP() AS created_at
FROM (
    -- Generate multiple transactions per account
    SELECT 
        a.*,
        DATEADD('day', -1 * day_offset, CURRENT_DATE()) AS transaction_date,
        CASE 
            WHEN UNIFORM(0, 100, RANDOM()) < 80 THEN 'DEBIT'
            WHEN UNIFORM(0, 100, RANDOM()) < 95 THEN 'CREDIT'
            ELSE 'TRANSFER'
        END AS transaction_type,
        -- Realistic amount distribution
        CASE 
            WHEN UNIFORM(0, 100, RANDOM()) < 50 THEN UNIFORM(5, 50, RANDOM())      -- 50% small
            WHEN UNIFORM(0, 100, RANDOM()) < 80 THEN UNIFORM(50, 200, RANDOM())    -- 30% medium
            WHEN UNIFORM(0, 100, RANDOM()) < 95 THEN UNIFORM(200, 500, RANDOM())   -- 15% large
            ELSE UNIFORM(500, 2000, RANDOM())                                       -- 5% very large
        END AS amount,
        dd.employer_name,
        dd.deposit_date
    FROM ACCOUNTS a
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS day_offset
        FROM TABLE(GENERATOR(ROWCOUNT => 730)) -- 2 years of days
    ) AS day_offsets
    LEFT JOIN DIRECT_DEPOSITS dd 
        ON a.account_id = dd.account_id
        AND dd.deposit_date = DATEADD('day', -1 * day_offset, CURRENT_DATE())
    WHERE a.account_type IN ('CHECKING', 'BELIEVE_CARD')
        AND a.opening_date <= DATEADD('day', -1 * day_offset, CURRENT_DATE())
        AND (a.closing_date IS NULL OR a.closing_date >= DATEADD('day', -1 * day_offset, CURRENT_DATE()))
) AS base_txn
CROSS JOIN (
    -- Generate 1-10 transactions per account per day
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS txn_num
    FROM TABLE(GENERATOR(ROWCOUNT => 10))
) AS txn_gen
CROSS JOIN (
    SELECT 
        merchant_name,
        mcc_code,
        merchant_city,
        merchant_state,
        ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
    FROM temp_merchants
    QUALIFY rn = 1
) AS m
WHERE txn_num <= 
    CASE 
        WHEN UNIFORM(0, 100, RANDOM()) < 30 THEN 0  -- 30% no transactions
        WHEN UNIFORM(0, 100, RANDOM()) < 60 THEN 1  -- 30% one transaction
        WHEN UNIFORM(0, 100, RANDOM()) < 80 THEN 2  -- 20% two transactions
        WHEN UNIFORM(0, 100, RANDOM()) < 90 THEN 3  -- 10% three transactions
        ELSE UNIFORM(4, 10, RANDOM())               -- 10% many transactions
    END
LIMIT 50000000; -- Generate 50M transactions first (adjust as needed)

-- ============================================================================
-- Step 8: Generate Cash Advances
-- ============================================================================
INSERT INTO CASH_ADVANCES
SELECT
    'ADV' || LPAD(SEQ4(), 8, '0') AS advance_id,
    c.customer_id,
    a.account_id,
    advance_amount,
    CASE 
        WHEN advance_amount <= 50 THEN 3.00
        WHEN advance_amount <= 100 THEN 5.00
        WHEN advance_amount <= 250 THEN 10.00
        ELSE 20.00
    END AS fee_amount,
    advance_date,
    DATEADD('day', 30, advance_date) AS due_date,
    CASE 
        WHEN advance_status IN ('REPAID', 'DEFAULTED') 
        THEN DATEADD('day', UNIFORM(1, 35, RANDOM()), advance_date)
        ELSE NULL
    END AS repayment_date,
    CASE 
        WHEN advance_status = 'REPAID' THEN advance_amount + 
            CASE 
                WHEN advance_amount <= 50 THEN 3.00
                WHEN advance_amount <= 100 THEN 5.00
                WHEN advance_amount <= 250 THEN 10.00
                ELSE 20.00
            END
        WHEN advance_status = 'DEFAULTED' THEN 0.00
        ELSE NULL
    END AS repayment_amount,
    advance_status,
    -- Eligibility score based on customer profile
    LEAST(99, GREATEST(1,
        50 + 
        (c.credit_score - 650) / 10 +
        CASE WHEN dd_count > 0 THEN 20 ELSE 0 END +
        CASE WHEN c.lifetime_value > 1000 THEN 10 ELSE 0 END
    )) / 100.0 AS eligibility_score,
    -- Default risk score
    CASE 
        WHEN advance_status = 'DEFAULTED' THEN UNIFORM(70, 99, RANDOM()) / 100.0
        WHEN advance_status = 'ACTIVE' AND CURRENT_DATE() > DATEADD('day', 30, advance_date) THEN UNIFORM(50, 80, RANDOM()) / 100.0
        ELSE UNIFORM(1, 30, RANDOM()) / 100.0
    END AS default_risk_score,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM (
    SELECT 
        c.*,
        a.account_id,
        dd.dd_count,
        DATEADD('day', -1 * UNIFORM(1, 365, RANDOM()), CURRENT_DATE()) AS advance_date,
        ARRAY_CONSTRUCT(20, 50, 100, 250, 500)[UNIFORM(0, 4, RANDOM())] AS advance_amount,
        CASE 
            WHEN UNIFORM(0, 100, RANDOM()) < 80 THEN 'REPAID'
            WHEN UNIFORM(0, 100, RANDOM()) < 95 THEN 'ACTIVE'
            ELSE 'DEFAULTED'
        END AS advance_status
    FROM CUSTOMERS c
    JOIN ACCOUNTS a ON c.customer_id = a.customer_id AND a.account_type = 'CHECKING'
    LEFT JOIN (
        SELECT customer_id, COUNT(*) as dd_count 
        FROM DIRECT_DEPOSITS 
        GROUP BY customer_id
    ) dd ON c.customer_id = dd.customer_id
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 5)) -- Up to 5 advances per customer
    WHERE c.customer_status = 'ACTIVE'
        AND c.credit_score >= 600
        AND UNIFORM(0, 100, RANDOM()) < 30 -- 30% of eligible customers use advances
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY RANDOM()) <= 
        CASE 
            WHEN UNIFORM(0, 100, RANDOM()) < 50 THEN 1
            WHEN UNIFORM(0, 100, RANDOM()) < 80 THEN 2
            ELSE 3
        END
);

-- ============================================================================
-- Step 9: Generate Support Interactions
-- ============================================================================
INSERT INTO SUPPORT_INTERACTIONS
SELECT
    'SUP' || LPAD(SEQ4(), 10, '0') AS interaction_id,
    customer_id,
    ARRAY_CONSTRUCT('CHAT', 'PHONE', 'EMAIL')[UNIFORM(0, 2, RANDOM())] AS interaction_type,
    interaction_date,
    category,
    CASE category
        WHEN 'ACCOUNT' THEN ARRAY_CONSTRUCT('Login Issues', 'Password Reset', 'Account Locked', 'Update Info')[UNIFORM(0, 3, RANDOM())]
        WHEN 'TRANSACTION' THEN ARRAY_CONSTRUCT('Disputed Charge', 'Missing Transaction', 'Wrong Amount', 'Refund Request')[UNIFORM(0, 3, RANDOM())]
        WHEN 'CARD' THEN ARRAY_CONSTRUCT('Lost Card', 'Stolen Card', 'Card Not Working', 'PIN Issues')[UNIFORM(0, 3, RANDOM())]
        WHEN 'ADVANCE' THEN ARRAY_CONSTRUCT('Eligibility', 'Repayment', 'Increase Limit', 'Fee Question')[UNIFORM(0, 3, RANDOM())]
        ELSE 'Other'
    END AS subcategory,
    UNIFORM(0, 100, RANDOM()) < 85 AS issue_resolved,
    CASE ARRAY_CONSTRUCT('CHAT', 'PHONE', 'EMAIL')[UNIFORM(0, 2, RANDOM())]
        WHEN 'CHAT' THEN UNIFORM(5, 30, RANDOM())
        WHEN 'PHONE' THEN UNIFORM(10, 45, RANDOM())
        WHEN 'EMAIL' THEN UNIFORM(60, 480, RANDOM())
    END AS resolution_time_minutes,
    'AGENT' || LPAD(UNIFORM(1, 100, RANDOM()), 3, '0') AS agent_id,
    CASE 
        WHEN UNIFORM(0, 100, RANDOM()) < 85 THEN UNIFORM(3, 5, RANDOM())
        ELSE UNIFORM(1, 3, RANDOM())
    END AS satisfaction_score,
    ARRAY_CONSTRUCT('CHAT', 'PHONE', 'EMAIL')[UNIFORM(0, 2, RANDOM())] IN ('CHAT', 'PHONE') AS transcript_available,
    CURRENT_TIMESTAMP() AS created_at
FROM (
    SELECT 
        c.customer_id,
        DATEADD('day', -1 * UNIFORM(1, 730, RANDOM()), CURRENT_DATE()) AS interaction_date,
        ARRAY_CONSTRUCT('ACCOUNT', 'TRANSACTION', 'CARD', 'ADVANCE', 'GENERAL')[UNIFORM(0, 4, RANDOM())] AS category
    FROM CUSTOMERS c
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 5))
    WHERE UNIFORM(0, 100, RANDOM()) < 40 -- 40% of customers contact support
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY RANDOM()) <= 
        CASE 
            WHEN UNIFORM(0, 100, RANDOM()) < 60 THEN 1
            WHEN UNIFORM(0, 100, RANDOM()) < 85 THEN 2
            ELSE UNIFORM(3, 5, RANDOM())
        END
);

-- ============================================================================
-- Step 10: Generate Feature Store Sample Data
-- ============================================================================

-- Insert feature definitions
INSERT INTO VARO_INTELLIGENCE.FEATURE_STORE.FEATURE_DEFINITIONS VALUES
('customer_txn_count_30d', 'Customer Transaction Count 30 Days', 'transaction_patterns', 'Number of transactions in last 30 days', 'NUMBER', 'BATCH', 
'SELECT customer_id, COUNT(*) as value FROM transactions WHERE transaction_date >= DATEADD(day, -30, CURRENT_DATE()) GROUP BY customer_id',
ARRAY_CONSTRUCT('transactions'), 'DAILY', 1, TRUE, 'system', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

('customer_txn_amount_30d', 'Customer Transaction Amount 30 Days', 'transaction_patterns', 'Total transaction amount in last 30 days', 'NUMBER', 'BATCH',
'SELECT customer_id, SUM(ABS(amount)) as value FROM transactions WHERE transaction_date >= DATEADD(day, -30, CURRENT_DATE()) GROUP BY customer_id',
ARRAY_CONSTRUCT('transactions'), 'DAILY', 1, TRUE, 'system', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

('customer_unique_merchants_30d', 'Customer Unique Merchants 30 Days', 'transaction_patterns', 'Number of unique merchants in last 30 days', 'NUMBER', 'BATCH',
'SELECT customer_id, COUNT(DISTINCT merchant_name) as value FROM transactions WHERE transaction_date >= DATEADD(day, -30, CURRENT_DATE()) GROUP BY customer_id',
ARRAY_CONSTRUCT('transactions'), 'DAILY', 1, TRUE, 'system', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

('customer_avg_daily_balance', 'Customer Average Daily Balance', 'customer_profile', 'Average daily balance across all accounts', 'NUMBER', 'STREAMING',
'SELECT customer_id, AVG(current_balance) as value FROM accounts GROUP BY customer_id',
ARRAY_CONSTRUCT('accounts'), 'REAL_TIME', 1, TRUE, 'system', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

('customer_advance_utilization', 'Customer Cash Advance Utilization', 'risk_indicators', 'Percentage of advances used vs eligible', 'NUMBER', 'BATCH',
'SELECT customer_id, COUNT(CASE WHEN advance_status != ''DEFAULTED'' THEN 1 END) / NULLIF(COUNT(*), 0) as value FROM cash_advances GROUP BY customer_id',
ARRAY_CONSTRUCT('cash_advances'), 'DAILY', 1, TRUE, 'system', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

('customer_fraud_transaction_ratio', 'Customer Fraud Transaction Ratio', 'risk_indicators', 'Ratio of high fraud score transactions', 'NUMBER', 'STREAMING',
'SELECT customer_id, AVG(CASE WHEN fraud_score > 0.5 THEN 1 ELSE 0 END) as value FROM transactions WHERE transaction_date >= DATEADD(day, -90, CURRENT_DATE()) GROUP BY customer_id',
ARRAY_CONSTRUCT('transactions'), 'HOURLY', 1, TRUE, 'system', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Insert feature sets
INSERT INTO VARO_INTELLIGENCE.FEATURE_STORE.FEATURE_SETS VALUES
('fraud_detection_v1', 'Fraud Detection Model V1', 'fraud_detection', 'Features for real-time fraud detection',
ARRAY_CONSTRUCT('customer_txn_count_30d', 'customer_txn_amount_30d', 'customer_unique_merchants_30d', 'customer_fraud_transaction_ratio'),
TRUE, 'ml_team', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

('credit_risk_v1', 'Credit Risk Assessment V1', 'credit_risk', 'Features for credit limit and advance eligibility',
ARRAY_CONSTRUCT('customer_avg_daily_balance', 'customer_advance_utilization', 'customer_txn_amount_30d'),
TRUE, 'ml_team', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- ============================================================================
-- Step 11: Create sample unstructured data tables for Cortex Search
-- ============================================================================

-- Support transcripts table
CREATE OR REPLACE TABLE VARO_INTELLIGENCE.RAW.SUPPORT_TRANSCRIPTS (
    transcript_id VARCHAR(40) PRIMARY KEY,
    interaction_id VARCHAR(30),
    customer_id VARCHAR(20),
    agent_id VARCHAR(20),
    interaction_date DATE,
    interaction_type VARCHAR(30),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    transcript_text TEXT,
    sentiment_score NUMBER(3,2),
    resolution_achieved BOOLEAN,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Generate sample support transcripts
INSERT INTO VARO_INTELLIGENCE.RAW.SUPPORT_TRANSCRIPTS
SELECT
    'TRANS' || LPAD(SEQ4(), 10, '0') AS transcript_id,
    si.interaction_id,
    si.customer_id,
    si.agent_id,
    si.interaction_date,
    si.interaction_type,
    si.category,
    si.subcategory,
    'Customer: ' || 
    CASE si.category
        WHEN 'ACCOUNT' THEN 'I am having trouble logging into my account. I have tried resetting my password but the email is not coming through. Can you help me access my account?'
        WHEN 'TRANSACTION' THEN 'I see a charge on my account that I do not recognize. The amount is $' || ROUND(UNIFORM(50, 500, RANDOM()), 2) || ' from ' || m.merchant_name || '. I did not make this purchase.'
        WHEN 'CARD' THEN 'My card was declined at the store today even though I have sufficient funds. The card is not expired. What could be the issue?'
        WHEN 'ADVANCE' THEN 'I need to request a cash advance but the app says I am not eligible. I have been a customer for over a year and have direct deposit. Why am I not eligible?'
        ELSE 'I have a general question about my account and the fees associated with international transactions.'
    END || '\n\nAgent: ' ||
    CASE si.category
        WHEN 'ACCOUNT' THEN 'I understand your frustration. Let me help you regain access to your account. First, I will verify your identity and then manually send a password reset link to your registered email.'
        WHEN 'TRANSACTION' THEN 'I can help you with that disputed transaction. I have initiated a dispute claim for the charge. You should see a provisional credit within 2 business days while we investigate.'
        WHEN 'CARD' THEN 'I apologize for the inconvenience. I can see that your card was flagged for unusual activity. I have removed the block and your card should work now. For security, we recommend setting up transaction alerts.'
        WHEN 'ADVANCE' THEN 'Let me check your eligibility criteria. I see that you need to have at least $1000 in monthly direct deposits. Currently, your deposits are slightly below this threshold. Once you meet this requirement, you will be eligible.'
        ELSE 'Happy to help! International transactions with your Varo debit card have no foreign transaction fees. However, the ATM operator may charge their own fees.'
    END AS transcript_text,
    CASE 
        WHEN si.issue_resolved THEN UNIFORM(7, 10, RANDOM()) / 10.0
        ELSE UNIFORM(3, 6, RANDOM()) / 10.0
    END AS sentiment_score,
    si.issue_resolved AS resolution_achieved,
    CURRENT_TIMESTAMP() AS created_at
FROM SUPPORT_INTERACTIONS si
CROSS JOIN (
    SELECT merchant_name FROM temp_merchants ORDER BY RANDOM() LIMIT 1
) m
WHERE si.transcript_available = TRUE
LIMIT 25000; -- Generate 25K transcripts

-- Enable change tracking for Cortex Search
ALTER TABLE SUPPORT_TRANSCRIPTS SET CHANGE_TRACKING = TRUE;

-- Compliance documents table
CREATE OR REPLACE TABLE VARO_INTELLIGENCE.RAW.COMPLIANCE_DOCUMENTS (
    document_id VARCHAR(40) PRIMARY KEY,
    document_type VARCHAR(50),
    title VARCHAR(200),
    content TEXT,
    effective_date DATE,
    tags ARRAY,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample compliance documents
INSERT INTO VARO_INTELLIGENCE.RAW.COMPLIANCE_DOCUMENTS VALUES
('DOC001', 'POLICY', 'Anti-Money Laundering (AML) Policy', 
'Varo Bank Anti-Money Laundering Policy\n\n1. Purpose\nThis policy establishes Varo Bank''s framework for preventing, detecting, and reporting money laundering activities.\n\n2. Customer Due Diligence\n- Identity verification required for all new accounts\n- Enhanced due diligence for high-risk customers\n- Ongoing monitoring of customer transactions\n\n3. Transaction Monitoring\n- Automated systems flag suspicious patterns\n- Daily reports reviewed by compliance team\n- Thresholds: Cash deposits over $10,000, Wire transfers over $3,000\n\n4. Reporting Requirements\n- SARs filed within 30 days of detection\n- CTRs for cash transactions over $10,000\n- Quarterly reports to board of directors',
'2024-01-01', ARRAY_CONSTRUCT('AML', 'compliance', 'policy', 'monitoring'), CURRENT_TIMESTAMP()),

('DOC002', 'PROCEDURE', 'Know Your Customer (KYC) Procedures',
'KYC Verification Procedures\n\n1. Initial Verification\n- Government-issued ID required\n- SSN verification through credit bureaus\n- Address verification via utility bills or bank statements\n\n2. Risk Assessment\n- Low Risk: Standard verification\n- Medium Risk: Additional income verification\n- High Risk: Enhanced due diligence including source of funds\n\n3. Periodic Reviews\n- Low Risk: Every 3 years\n- Medium Risk: Annually\n- High Risk: Quarterly\n\n4. Red Flags\n- Multiple accounts with different addresses\n- Unusual transaction patterns\n- Connections to high-risk jurisdictions',
'2024-01-15', ARRAY_CONSTRUCT('KYC', 'verification', 'risk', 'procedures'), CURRENT_TIMESTAMP()),

('DOC003', 'REGULATION', 'Regulation E Compliance Guide',
'Electronic Fund Transfer Act (Regulation E) Compliance\n\n1. Error Resolution\n- Customer has 60 days to report errors\n- Bank must investigate within 10 business days\n- Provisional credit required if investigation exceeds 10 days\n\n2. Disclosure Requirements\n- Fee schedule must be provided at account opening\n- Change notifications sent 21 days in advance\n- Monthly statements required for accounts with EFT activity\n\n3. Unauthorized Transactions\n- Customer liability limited to $50 if reported within 2 days\n- Up to $500 if reported within 60 days\n- Unlimited liability after 60 days',
'2024-02-01', ARRAY_CONSTRUCT('RegE', 'EFT', 'disputes', 'liability'), CURRENT_TIMESTAMP());

-- Enable change tracking
ALTER TABLE COMPLIANCE_DOCUMENTS SET CHANGE_TRACKING = TRUE;

-- Product knowledge base table
CREATE OR REPLACE TABLE VARO_INTELLIGENCE.RAW.PRODUCT_KNOWLEDGE (
    knowledge_id VARCHAR(40) PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    title VARCHAR(200),
    content TEXT,
    version VARCHAR(10),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert product knowledge articles
INSERT INTO VARO_INTELLIGENCE.RAW.PRODUCT_KNOWLEDGE VALUES
('KB001', 'Varo Advance', 'FEATURES', 'How Varo Advance Works',
'Varo Advance Overview\n\nEligibility Requirements:\n- Active Varo Bank Account for 30+ days\n- $1,000+ in qualifying direct deposits per month\n- Regular account activity\n- Positive account balance\n\nAdvance Limits:\n- Start: $20-$100\n- Grow up to: $500\n- Based on: Direct deposit history, account usage, repayment history\n\nFees:\n- $20 advance: $1.60 fee\n- $50 advance: $3.95 fee\n- $100 advance: $5.95 fee\n- $250 advance: $10.00 fee\n- $500 advance: $20.00 fee\n\nRepayment:\n- Automatic from next direct deposit\n- No interest charges\n- Must repay in full to get next advance',
'2.0', CURRENT_TIMESTAMP()),

('KB002', 'Varo Believe Card', 'CREDIT', 'Building Credit with Believe',
'Varo Believe Credit Builder Card\n\nHow It Works:\n1. Secured credit card - you fund your spending limit\n2. Funds held in Varo Believe Secured Account\n3. Monthly automatic payment from secured funds\n4. Payment history reported to all 3 credit bureaus\n\nBenefits:\n- No interest charges\n- No annual fee\n- No credit check to apply\n- Average 40+ point credit score increase in 3 months\n\nBest Practices:\n- Use for regular purchases\n- Keep utilization under 30%\n- Never miss the automatic payment\n- Gradually increase your limit',
'1.5', CURRENT_TIMESTAMP()),

('KB003', 'Savings Account', 'ACCOUNTS', 'High Yield Savings Features',
'Varo Savings Account\n\nBase APY: 2.50% on all balances\n\nQualify for 5.00% APY:\n- Receive $1,000+ in qualifying direct deposits\n- End month with positive balance in all Varo accounts\n- Applies to balances up to $5,000\n\nAuto-Save Tools:\n- Round-ups: Round debit purchases to nearest dollar\n- Save Your Pay: Automatic % of direct deposits\n- Save Your Change: Fixed amount daily/weekly\n\nNo Fees:\n- No minimum balance\n- No monthly maintenance\n- Unlimited transfers to Varo Bank Account',
'3.0', CURRENT_TIMESTAMP());

-- Enable change tracking
ALTER TABLE PRODUCT_KNOWLEDGE SET CHANGE_TRACKING = TRUE;

-- ============================================================================
-- Final Steps
-- ============================================================================

-- Switch back to smaller warehouse
USE WAREHOUSE VARO_WH;

-- Analyze tables for optimizer
ANALYZE TABLE CUSTOMERS;
ANALYZE TABLE ACCOUNTS;
ANALYZE TABLE TRANSACTIONS;
ANALYZE TABLE CASH_ADVANCES;
ANALYZE TABLE DIRECT_DEPOSITS;

-- Display summary
SELECT 'Data generation completed successfully!' AS status,
       (SELECT COUNT(*) FROM CUSTOMERS) AS customer_count,
       (SELECT COUNT(*) FROM ACCOUNTS) AS account_count,
       (SELECT COUNT(*) FROM TRANSACTIONS) AS transaction_count,
       (SELECT COUNT(*) FROM CASH_ADVANCES) AS advance_count,
       (SELECT COUNT(*) FROM SUPPORT_TRANSCRIPTS) AS transcript_count;

-- Note: Full 500M transactions would require multiple batches or background task
-- This script generates a representative sample for demo purposes
