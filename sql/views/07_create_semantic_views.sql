-- ============================================================================
-- Varo Intelligence Agent - Semantic Views
-- ============================================================================
-- Purpose: Create semantic views for Snowflake Intelligence agents
-- All syntax VERIFIED against official documentation:
-- https://docs.snowflake.com/en/sql-reference/sql/create-semantic-view
-- 
-- Banking-specific semantic views for:
-- 1. Customer Banking Intelligence
-- 2. Transaction & Payment Intelligence  
-- 3. Credit & Risk Intelligence
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE VARO_WH;

-- ============================================================================
-- Semantic View 1: Customer Banking Intelligence
-- ============================================================================
CREATE OR REPLACE SEMANTIC VIEW SV_CUSTOMER_BANKING_INTELLIGENCE
  TABLES (
    RAW.CUSTOMERS AS customers
      PRIMARY KEY (customer_id)
      WITH SYNONYMS = ('bank customers', 'account holders', 'varo users')
      COMMENT = 'Varo bank customers and account holders',
    RAW.ACCOUNTS AS accounts
      PRIMARY KEY (account_id)
      WITH SYNONYMS = ('bank accounts', 'customer accounts', 'financial accounts')
      COMMENT = 'Customer bank accounts including checking, savings, credit',
    RAW.CARDS AS cards
      PRIMARY KEY (card_id)
      WITH SYNONYMS = ('debit cards', 'credit cards', 'payment cards')
      COMMENT = 'Customer debit and credit cards',
    RAW.DIRECT_DEPOSITS AS direct_deposits
      PRIMARY KEY (deposit_id)
      WITH SYNONYMS = ('payroll deposits', 'recurring deposits', 'employer deposits')
      COMMENT = 'Direct deposit transactions from employers and government',
    RAW.CASH_ADVANCES AS cash_advances
      PRIMARY KEY (advance_id)
      WITH SYNONYMS = ('payday advances', 'varo advance', 'short term loans')
      COMMENT = 'Cash advance transactions and repayments',
    RAW.SUPPORT_INTERACTIONS AS support_interactions
      PRIMARY KEY (interaction_id)
      WITH SYNONYMS = ('customer support', 'service interactions', 'help requests')
      COMMENT = 'Customer support interactions and tickets'
  )
  RELATIONSHIPS (
    accounts(customer_id) REFERENCES customers(customer_id),
    cards(customer_id) REFERENCES customers(customer_id),
    cards(account_id) REFERENCES accounts(account_id),
    direct_deposits(customer_id) REFERENCES customers(customer_id),
    direct_deposits(account_id) REFERENCES accounts(account_id),
    cash_advances(customer_id) REFERENCES customers(customer_id),
    cash_advances(account_id) REFERENCES accounts(account_id),
    support_interactions(customer_id) REFERENCES customers(customer_id)
  )
  DIMENSIONS (
    customers.customer_id AS customer_id
      WITH SYNONYMS = ('user id banking', 'account holder id')
      COMMENT = 'Unique customer identifier',
    customers.email AS email
      WITH SYNONYMS = ('customer email banking', 'user email address')
      COMMENT = 'Customer email address',
    customers.customer_status AS customer_status
      WITH SYNONYMS = ('account holder status', 'user status banking')
      COMMENT = 'Customer status: ACTIVE, SUSPENDED, CLOSED',
    customers.kyc_status AS kyc_status
      WITH SYNONYMS = ('verification status', 'identity verification')
      COMMENT = 'KYC verification status: VERIFIED, PENDING, FAILED',
    customers.risk_tier AS risk_tier
      WITH SYNONYMS = ('customer risk level', 'risk classification')
      COMMENT = 'Customer risk tier: LOW, MEDIUM, HIGH',
    customers.acquisition_channel AS acquisition_channel
      WITH SYNONYMS = ('signup channel', 'customer source')
      COMMENT = 'How customer joined: MOBILE_APP, WEB, REFERRAL, PAID_AD',
    customers.employment_status AS employment_status
      WITH SYNONYMS = ('work status', 'job status')
      COMMENT = 'Employment status: EMPLOYED_FULL_TIME, EMPLOYED_PART_TIME, SELF_EMPLOYED, UNEMPLOYED, RETIRED, STUDENT',
    customers.address_state AS address_state
      WITH SYNONYMS = ('customer state banking', 'residence state', 'state')
      COMMENT = 'Customer state of residence',
    accounts.account_type AS account_type
      WITH SYNONYMS = ('banking product type', 'account category')
      COMMENT = 'Account type: CHECKING, SAVINGS, BELIEVE_CARD, LINE_OF_CREDIT',
    accounts.account_status AS account_status
      WITH SYNONYMS = ('account state', 'product status')
      COMMENT = 'Account status: ACTIVE, SUSPENDED, CLOSED',
    accounts.apy_rate AS apy_rate
      WITH SYNONYMS = ('savings rate', 'interest rate')
      COMMENT = 'Annual percentage yield for savings accounts',
    cards.card_type AS card_type
      WITH SYNONYMS = ('payment card type', 'plastic type')
      COMMENT = 'Card type: DEBIT, BELIEVE_CREDIT',
    cards.card_status AS card_status
      WITH SYNONYMS = ('card state', 'plastic status')
      COMMENT = 'Card status: ACTIVE, LOST, STOLEN, DAMAGED, INACTIVE',
    direct_deposits.deposit_type AS deposit_type
      WITH SYNONYMS = ('income type', 'deposit source type')
      COMMENT = 'Deposit type: PAYROLL, GOVERNMENT, OTHER',
    direct_deposits.frequency AS frequency
      WITH SYNONYMS = ('deposit frequency', 'income frequency')
      COMMENT = 'Deposit frequency: WEEKLY, BIWEEKLY, MONTHLY',
    cash_advances.advance_status AS advance_status
      WITH SYNONYMS = ('loan status', 'advance state')
      COMMENT = 'Advance status: ACTIVE, REPAID, DEFAULTED',
    support_interactions.interaction_type AS interaction_type
      WITH SYNONYMS = ('contact method', 'support channel')
      COMMENT = 'Support interaction type: CHAT, PHONE, EMAIL',
    support_interactions.category AS category
      WITH SYNONYMS = ('issue category', 'support topic', 'interaction category')
      COMMENT = 'Support category: ACCOUNT, TRANSACTION, CARD, ADVANCE, GENERAL'
  )
  METRICS (
    customers.total_customers AS COUNT(DISTINCT customer_id)
      WITH SYNONYMS = ('customer count banking', 'number of account holders')
      COMMENT = 'Total number of customers',
    customers.active_customers AS COUNT(DISTINCT CASE WHEN customer_status = 'ACTIVE' THEN customer_id END)
      WITH SYNONYMS = ('active user count', 'current customers')
      COMMENT = 'Number of active customers',
    customers.avg_credit_score AS AVG(credit_score)
      WITH SYNONYMS = ('average fico score', 'mean credit rating')
      COMMENT = 'Average customer credit score',
    customers.avg_income AS AVG(income_verified)
      WITH SYNONYMS = ('average verified income', 'mean earnings')
      COMMENT = 'Average verified income',
    accounts.total_accounts AS COUNT(DISTINCT account_id)
      WITH SYNONYMS = ('account count', 'number of products')
      COMMENT = 'Total number of accounts',
    accounts.checking_accounts AS COUNT(DISTINCT CASE WHEN account_type = 'CHECKING' THEN account_id END)
      WITH SYNONYMS = ('checking count', 'transaction accounts')
      COMMENT = 'Number of checking accounts',
    accounts.savings_accounts AS COUNT(DISTINCT CASE WHEN account_type = 'SAVINGS' THEN account_id END)
      WITH SYNONYMS = ('savings count', 'deposit accounts')
      COMMENT = 'Number of savings accounts',
    accounts.credit_accounts AS COUNT(DISTINCT CASE WHEN account_type IN ('BELIEVE_CARD', 'LINE_OF_CREDIT') THEN account_id END)
      WITH SYNONYMS = ('credit product count', 'lending accounts')
      COMMENT = 'Number of credit accounts',
    accounts.avg_checking_balance AS AVG(CASE WHEN account_type = 'CHECKING' THEN current_balance END)
      WITH SYNONYMS = ('average checking funds', 'mean checking balance')
      COMMENT = 'Average checking account balance',
    accounts.avg_savings_balance AS AVG(CASE WHEN account_type = 'SAVINGS' THEN current_balance END)
      WITH SYNONYMS = ('average savings funds', 'mean savings balance')
      COMMENT = 'Average savings account balance',
    accounts.total_deposits AS SUM(CASE WHEN account_type IN ('CHECKING', 'SAVINGS') THEN current_balance ELSE 0 END)
      WITH SYNONYMS = ('total customer deposits', 'aggregate balances')
      COMMENT = 'Total customer deposit balances',
    cards.active_cards AS COUNT(DISTINCT CASE WHEN card_status = 'ACTIVE' THEN card_id END)
      WITH SYNONYMS = ('active plastic count', 'usable cards')
      COMMENT = 'Number of active cards',
    direct_deposits.total_deposits AS COUNT(DISTINCT deposit_id)
      WITH SYNONYMS = ('direct deposit count', 'payroll count')
      COMMENT = 'Total number of direct deposits',
    direct_deposits.avg_deposit_amount AS AVG(amount)
      WITH SYNONYMS = ('average payroll amount', 'mean deposit size')
      COMMENT = 'Average direct deposit amount',
    direct_deposits.monthly_deposit_volume AS SUM(CASE WHEN deposit_date >= DATEADD('month', -1, CURRENT_DATE()) THEN amount END)
      WITH SYNONYMS = ('monthly payroll volume', 'recent deposit total')
      COMMENT = 'Total direct deposit volume in last month',
    cash_advances.total_advances AS COUNT(DISTINCT advance_id)
      WITH SYNONYMS = ('advance count', 'payday loan count')
      COMMENT = 'Total number of cash advances',
    cash_advances.active_advances AS COUNT(DISTINCT CASE WHEN advance_status = 'ACTIVE' THEN advance_id END)
      WITH SYNONYMS = ('outstanding advances', 'current loans')
      COMMENT = 'Number of active advances',
    cash_advances.avg_advance_amount AS AVG(advance_amount)
      WITH SYNONYMS = ('average loan size', 'mean advance amount')
      COMMENT = 'Average cash advance amount',
    cash_advances.default_rate AS COUNT(DISTINCT CASE WHEN advance_status = 'DEFAULTED' THEN advance_id END) * 1.0 / NULLIF(COUNT(DISTINCT advance_id), 0)
      WITH SYNONYMS = ('advance default percentage', 'loan loss rate')
      COMMENT = 'Percentage of advances that defaulted',
    support_interactions.total_interactions AS COUNT(DISTINCT interaction_id)
      WITH SYNONYMS = ('support contact count', 'help request count')
      COMMENT = 'Total support interactions',
    support_interactions.avg_satisfaction AS AVG(satisfaction_score)
      WITH SYNONYMS = ('average csat score', 'mean satisfaction rating')
      COMMENT = 'Average customer satisfaction score'
  )
  COMMENT = 'Comprehensive view of customer banking relationships, accounts, and product usage';

-- ============================================================================
-- Semantic View 2: Transaction & Payment Intelligence
-- ============================================================================
CREATE OR REPLACE SEMANTIC VIEW SV_TRANSACTION_PAYMENT_INTELLIGENCE
  TABLES (
    RAW.TRANSACTIONS AS transactions
      PRIMARY KEY (transaction_id)
      WITH SYNONYMS = ('payments', 'financial transactions', 'money movements')
      COMMENT = 'All customer financial transactions',
    RAW.ACCOUNTS AS accounts
      PRIMARY KEY (account_id)
      WITH SYNONYMS = ('source accounts', 'transaction accounts', 'payment accounts')
      COMMENT = 'Accounts used for transactions',
    RAW.CUSTOMERS AS customers
      PRIMARY KEY (customer_id)
      WITH SYNONYMS = ('transaction customers', 'payers', 'account owners')
      COMMENT = 'Customers making transactions',
    RAW.MERCHANT_CATEGORIES AS merchant_categories
      PRIMARY KEY (mcc_code)
      WITH SYNONYMS = ('merchant types', 'business categories', 'mcc codes')
      COMMENT = 'Merchant category classifications',
    RAW.CARDS AS cards
      PRIMARY KEY (card_id)
      WITH SYNONYMS = ('payment instruments', 'transaction cards', 'plastic cards')
      COMMENT = 'Cards used for transactions'
  )
  RELATIONSHIPS (
    transactions(account_id) REFERENCES accounts(account_id),
    transactions(customer_id) REFERENCES customers(customer_id),
    transactions(merchant_category) REFERENCES merchant_categories(mcc_code),
    accounts(customer_id) REFERENCES customers(customer_id),
    cards(account_id) REFERENCES accounts(account_id),
    cards(customer_id) REFERENCES customers(customer_id)
  )
  DIMENSIONS (
    transactions.transaction_type AS transaction_type
      WITH SYNONYMS = ('payment type', 'transaction direction')
      COMMENT = 'Transaction type: DEBIT, CREDIT, TRANSFER, FEE, INTEREST',
    transactions.transaction_category AS transaction_category
      WITH SYNONYMS = ('payment category', 'transaction channel')
      COMMENT = 'Transaction category: ATM, POS, ONLINE, WIRE, ACH, P2P',
    transactions.transaction_date AS transaction_date
      WITH SYNONYMS = ('payment date', 'transaction day')
      COMMENT = 'Date of transaction',
    transactions.merchant_name AS merchant_name
      WITH SYNONYMS = ('business name', 'payee name')
      COMMENT = 'Merchant or payee name',
    transactions.merchant_category AS merchant_category
      WITH SYNONYMS = ('mcc code payment', 'business type code')
      COMMENT = 'Merchant category code',
    transactions.merchant_state AS merchant_state
      WITH SYNONYMS = ('business state', 'merchant location')
      COMMENT = 'Merchant state location',
    transactions.is_recurring AS is_recurring
      WITH SYNONYMS = ('subscription payment', 'recurring charge')
      COMMENT = 'Whether transaction is recurring',
    transactions.is_international AS is_international
      WITH SYNONYMS = ('foreign transaction', 'international payment')
      COMMENT = 'Whether transaction is international',
    transactions.status AS status
      WITH SYNONYMS = ('transaction status', 'payment state', 'txn status')
      COMMENT = 'Transaction status: COMPLETED, DECLINED, PENDING',
    merchant_categories.category_name AS category_name
      WITH SYNONYMS = ('merchant category name', 'business type name')
      COMMENT = 'Merchant category description',
    merchant_categories.category_group AS category_group
      WITH SYNONYMS = ('merchant group', 'business sector')
      COMMENT = 'Merchant category group: GROCERIES, DINING, GAS, HEALTHCARE, etc',
    merchant_categories.cashback_eligible AS cashback_eligible
      WITH SYNONYMS = ('rewards eligible', 'cashback qualified')
      COMMENT = 'Whether category is eligible for cashback',
    merchant_categories.high_risk_category AS high_risk_category
      WITH SYNONYMS = ('risky merchant type', 'high risk business')
      COMMENT = 'Whether category is considered high risk',
    accounts.account_type AS account_type_txn
      WITH SYNONYMS = ('source account type', 'payment account type')
      COMMENT = 'Type of account used for transaction',
    customers.customer_status AS customer_status_txn
      WITH SYNONYMS = ('payer status', 'transactor status')
      COMMENT = 'Status of customer making transaction'
  )
  METRICS (
    transactions.total_transactions AS COUNT(DISTINCT transaction_id)
      WITH SYNONYMS = ('transaction count', 'payment count')
      COMMENT = 'Total number of transactions',
    transactions.transaction_volume AS SUM(ABS(amount))
      WITH SYNONYMS = ('payment volume', 'transaction value')
      COMMENT = 'Total transaction volume',
    transactions.avg_transaction_amount AS AVG(ABS(amount))
      WITH SYNONYMS = ('average payment size', 'mean transaction value')
      COMMENT = 'Average transaction amount',
    transactions.debit_volume AS SUM(CASE WHEN transaction_type = 'DEBIT' THEN ABS(amount) ELSE 0 END)
      WITH SYNONYMS = ('spending volume', 'purchase total')
      COMMENT = 'Total debit transaction volume',
    transactions.credit_volume AS SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END)
      WITH SYNONYMS = ('deposit volume', 'credit total')
      COMMENT = 'Total credit transaction volume',
    transactions.atm_volume AS SUM(CASE WHEN transaction_category = 'ATM' THEN ABS(amount) ELSE 0 END)
      WITH SYNONYMS = ('cash withdrawal volume', 'atm total')
      COMMENT = 'Total ATM withdrawal volume',
    transactions.pos_transactions AS COUNT(DISTINCT CASE WHEN transaction_category = 'POS' THEN transaction_id END)
      WITH SYNONYMS = ('point of sale count', 'card purchase count')
      COMMENT = 'Number of point-of-sale transactions',
    transactions.online_transactions AS COUNT(DISTINCT CASE WHEN transaction_category = 'ONLINE' THEN transaction_id END)
      WITH SYNONYMS = ('ecommerce count', 'online payment count')
      COMMENT = 'Number of online transactions',
    transactions.recurring_count AS COUNT(DISTINCT CASE WHEN is_recurring = TRUE THEN transaction_id END)
      WITH SYNONYMS = ('subscription count', 'recurring payment count')
      COMMENT = 'Number of recurring transactions',
    transactions.international_count AS COUNT(DISTINCT CASE WHEN is_international = TRUE THEN transaction_id END)
      WITH SYNONYMS = ('foreign transaction count', 'cross border payments')
      COMMENT = 'Number of international transactions',
    transactions.declined_count AS COUNT(DISTINCT CASE WHEN status = 'DECLINED' THEN transaction_id END)
      WITH SYNONYMS = ('rejected transactions', 'failed payments')
      COMMENT = 'Number of declined transactions',
    transactions.unique_merchants AS COUNT(DISTINCT merchant_name)
      WITH SYNONYMS = ('merchant diversity', 'unique payees')
      COMMENT = 'Number of unique merchants',
    transactions.unique_categories AS COUNT(DISTINCT merchant_category)
      WITH SYNONYMS = ('category diversity', 'spending categories')
      COMMENT = 'Number of unique merchant categories',
    transactions.cashback_eligible_volume AS SUM(ABS(amount))
      WITH SYNONYMS = ('rewards eligible spending', 'cashback qualified volume')
      COMMENT = 'Total transaction volume (cashback filtering done via merchant_categories.cashback_eligible dimension)',
    transactions.high_risk_volume AS SUM(ABS(amount))
      WITH SYNONYMS = ('risky transaction volume', 'high risk spending')
      COMMENT = 'Total transaction volume (risk filtering done via merchant_categories.high_risk_category dimension)',
    transactions.avg_fraud_score AS AVG(fraud_score)
      WITH SYNONYMS = ('average risk score', 'mean fraud probability')
      COMMENT = 'Average fraud risk score'
  )
  COMMENT = 'Detailed view of all financial transactions, payments, and spending patterns';

-- ============================================================================
-- Semantic View 3: Credit & Risk Intelligence
-- ============================================================================
CREATE OR REPLACE SEMANTIC VIEW SV_CREDIT_RISK_INTELLIGENCE
  TABLES (
    RAW.CREDIT_APPLICATIONS AS credit_applications
      PRIMARY KEY (application_id)
      WITH SYNONYMS = ('loan applications', 'credit requests', 'lending applications')
      COMMENT = 'Applications for credit products',
    RAW.CASH_ADVANCES AS cash_advances
      PRIMARY KEY (advance_id)
      WITH SYNONYMS = ('short term lending', 'payday loans risk', 'advance lending')
      COMMENT = 'Cash advance loans and repayments',
    RAW.ACCOUNTS AS accounts
      PRIMARY KEY (account_id)
      WITH SYNONYMS = ('credit accounts risk', 'lending products', 'credit lines')
      COMMENT = 'Credit accounts including cards and lines of credit',
    RAW.CUSTOMERS AS customers
      PRIMARY KEY (customer_id)
      WITH SYNONYMS = ('borrowers', 'credit customers', 'lending clients')
      COMMENT = 'Customers with credit products',
    RAW.EXTERNAL_DATA AS external_data
      PRIMARY KEY (external_data_id)
      WITH SYNONYMS = ('credit bureau data', 'third party data', 'external credit info')
      COMMENT = 'External credit and verification data',
    RAW.COMPLIANCE_EVENTS AS compliance_events
      PRIMARY KEY (event_id)
      WITH SYNONYMS = ('regulatory events', 'compliance issues', 'risk events')
      COMMENT = 'Compliance and regulatory events'
  )
  RELATIONSHIPS (
    credit_applications(customer_id) REFERENCES customers(customer_id),
    cash_advances(customer_id) REFERENCES customers(customer_id),
    cash_advances(account_id) REFERENCES accounts(account_id),
    accounts(customer_id) REFERENCES customers(customer_id),
    external_data(customer_id) REFERENCES customers(customer_id),
    compliance_events(customer_id) REFERENCES customers(customer_id)
  )
  DIMENSIONS (
    credit_applications.product_type AS product_type_applied
      WITH SYNONYMS = ('credit product applied', 'loan type requested')
      COMMENT = 'Credit product type: BELIEVE_CARD, LINE_OF_CREDIT',
    credit_applications.application_status AS application_status
      WITH SYNONYMS = ('credit decision', 'approval status')
      COMMENT = 'Application status: PENDING, APPROVED, DECLINED',
    credit_applications.decision_reason_codes AS decision_reason_codes
      WITH SYNONYMS = ('decline reasons', 'credit decision factors')
      COMMENT = 'Reason codes for credit decision',
    cash_advances.advance_status AS advance_repayment_status
      WITH SYNONYMS = ('loan repayment status', 'advance collection status')
      COMMENT = 'Cash advance status: ACTIVE, REPAID, DEFAULTED',
    accounts.account_type AS credit_account_type
      WITH SYNONYMS = ('credit product type risk', 'lending account type')
      COMMENT = 'Type of credit account',
    accounts.credit_limit AS credit_limit
      WITH SYNONYMS = ('credit line amount', 'lending limit')
      COMMENT = 'Credit limit for account',
    customers.credit_score AS customer_credit_score
      WITH SYNONYMS = ('fico score risk', 'credit rating')
      COMMENT = 'Customer credit score',
    customers.risk_tier AS customer_risk_tier
      WITH SYNONYMS = ('risk segment', 'credit risk level')
      COMMENT = 'Customer risk tier classification',
    customers.employment_status AS employment_status_risk
      WITH SYNONYMS = ('job status risk', 'income stability')
      COMMENT = 'Employment status for risk assessment',
    external_data.data_source AS external_data_source
      WITH SYNONYMS = ('bureau source', 'verification provider')
      COMMENT = 'Source of external data: EQUIFAX, EXPERIAN, PLAID',
    external_data.data_type AS external_data_type
      WITH SYNONYMS = ('verification type', 'external check type')
      COMMENT = 'Type of external data: CREDIT_REPORT, BANK_VERIFICATION, INCOME_VERIFICATION',
    compliance_events.event_type AS compliance_event_type
      WITH SYNONYMS = ('regulatory event type', 'compliance issue type')
      COMMENT = 'Compliance event type: SAR, CTR, KYC_UPDATE, OFAC_HIT',
    compliance_events.severity AS compliance_severity
      WITH SYNONYMS = ('event severity', 'risk severity level')
      COMMENT = 'Severity of compliance event'
  )
  METRICS (
    credit_applications.total_applications AS COUNT(DISTINCT application_id)
      WITH SYNONYMS = ('credit app count', 'loan application count')
      COMMENT = 'Total credit applications',
    credit_applications.approved_applications AS COUNT(DISTINCT CASE WHEN application_status = 'APPROVED' THEN application_id END)
      WITH SYNONYMS = ('approved credit count', 'successful applications')
      COMMENT = 'Number of approved applications',
    credit_applications.approval_rate AS COUNT(DISTINCT CASE WHEN application_status = 'APPROVED' THEN application_id END) * 1.0 / NULLIF(COUNT(DISTINCT application_id), 0)
      WITH SYNONYMS = ('credit approval percentage', 'acceptance rate')
      COMMENT = 'Credit application approval rate',
    credit_applications.avg_approved_amount AS AVG(approved_amount)
      WITH SYNONYMS = ('average credit limit', 'mean approved line')
      COMMENT = 'Average approved credit amount',
    cash_advances.total_advance_volume AS SUM(advance_amount)
      WITH SYNONYMS = ('total lending volume', 'advance loan volume')
      COMMENT = 'Total cash advance volume',
    cash_advances.outstanding_balance AS SUM(CASE WHEN advance_status = 'ACTIVE' THEN advance_amount + fee_amount ELSE 0 END)
      WITH SYNONYMS = ('outstanding loans', 'active advance balance')
      COMMENT = 'Total outstanding advance balance',
    cash_advances.default_volume AS SUM(CASE WHEN advance_status = 'DEFAULTED' THEN advance_amount ELSE 0 END)
      WITH SYNONYMS = ('defaulted loan volume', 'charge off amount')
      COMMENT = 'Total defaulted advance volume',
    cash_advances.collection_rate AS SUM(CASE WHEN advance_status = 'REPAID' THEN repayment_amount ELSE 0 END) * 1.0 / NULLIF(SUM(advance_amount + fee_amount), 0)
      WITH SYNONYMS = ('repayment rate', 'collection percentage')
      COMMENT = 'Percentage of advances collected',
    cash_advances.avg_days_to_repay AS AVG(CASE WHEN advance_status = 'REPAID' THEN DATEDIFF('day', advance_date, repayment_date) END)
      WITH SYNONYMS = ('average repayment days', 'mean collection time')
      COMMENT = 'Average days to repay advance',
    accounts.total_credit_exposure AS SUM(credit_limit)
      WITH SYNONYMS = ('total credit lines', 'aggregate credit limits')
      COMMENT = 'Total credit exposure across all accounts',
    accounts.credit_utilization AS AVG(CASE WHEN credit_limit > 0 THEN ABS(current_balance) / credit_limit END)
      WITH SYNONYMS = ('credit usage rate', 'utilization percentage')
      COMMENT = 'Average credit utilization rate',
    cash_advances.avg_risk_score AS AVG(default_risk_score)
      WITH SYNONYMS = ('average default risk', 'mean risk probability')
      COMMENT = 'Average default risk score for cash advances',
    external_data.verified_customers AS COUNT(DISTINCT CASE WHEN data_type = 'INCOME_VERIFICATION' THEN customer_id END)
      WITH SYNONYMS = ('income verified count', 'verified borrowers')
      COMMENT = 'Number of income-verified customers',
    compliance_events.total_events AS COUNT(DISTINCT event_id)
      WITH SYNONYMS = ('compliance issue count', 'regulatory event count')
      COMMENT = 'Total compliance events',
    compliance_events.high_severity_events AS COUNT(DISTINCT CASE WHEN severity IN ('HIGH', 'CRITICAL') THEN event_id END)
      WITH SYNONYMS = ('serious compliance issues', 'high risk events')
      COMMENT = 'Number of high severity compliance events'
  )
  COMMENT = 'Comprehensive view of credit risk, lending performance, and compliance monitoring';

-- Display confirmation
SELECT 'Semantic views created successfully' AS STATUS;
