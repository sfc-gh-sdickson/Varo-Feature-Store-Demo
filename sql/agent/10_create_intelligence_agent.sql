-- ============================================================================
-- Varo Intelligence Agent - Create Snowflake Intelligence Agent
-- ============================================================================
-- Purpose: Create and configure the Varo Intelligence Agent with:
--          - Cortex Analyst tools (Semantic Views)
--          - Cortex Search tools (Unstructured Data)
--          - ML Model tools (Functions)
--          - Feature Store Integration
-- Execution Order: Run AFTER all other scripts (01-09) are completed
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE VARO_WH;

-- ============================================================================
-- Create Varo Intelligence Agent
-- ============================================================================

CREATE OR REPLACE AGENT VARO_INTELLIGENCE_AGENT
  COMMENT = 'Varo Intelligence Agent for banking analytics and ML-powered insights'
  PROFILE = '{"display_name": "Varo Intelligence Agent", "avatar": "bank", "color": "purple"}'
  FROM SPECIFICATION
  $$
models:
  orchestration: AUTO

instructions:
  response: 'You are Varo Bank''s AI analytics assistant, specializing in digital banking insights, fraud detection, and credit risk assessment. Use semantic views for KPIs and metrics, Cortex Search for policy/support content, and ML functions for predictions. Provide actionable insights focused on customer value and risk management.'
  orchestration: 'For banking metrics and customer analytics, use semantic views. For fraud/risk scoring, use ML functions. For compliance and support queries, use Cortex Search. Access Feature Store data through online_features table when real-time features are needed.'
  system: 'Analyze Varo''s digital banking data including customer accounts, transactions, cash advances, and ML features. Focus on financial inclusion, responsible lending, and customer satisfaction.'
  sample_questions:
    # Customer Analytics (5)
    - question: 'How many active customers do we have and what is their average account balance?'
      answer: 'I will query the Customer Banking Intelligence semantic view to count active customers and calculate average balances.'
    - question: 'What percentage of customers have direct deposit set up?'
      answer: 'I will analyze direct deposit data in the Customer Banking Intelligence view.'
    - question: 'Show me customer acquisition trends by channel over the last 6 months.'
      answer: 'I will aggregate customer data by acquisition channel and date to show trends.'
    - question: 'What is our average customer lifetime value by segment?'
      answer: 'I will calculate average LTV grouped by customer segments using the semantic model.'
    - question: 'How many customers have multiple products (checking + savings/credit)?'
      answer: 'I will count customers with multiple account types in the banking intelligence view.'

    # Transaction & Spending (5)
    - question: 'What are the top 5 merchant categories by transaction volume?'
      answer: 'I will analyze transaction volumes grouped by merchant category using the Transaction Intelligence view.'
    - question: 'Show me daily transaction volumes and counts for the past week.'
      answer: 'I will aggregate transactions by date showing both volume and count metrics.'
    - question: 'What percentage of transactions are eligible for cashback rewards?'
      answer: 'I will calculate the ratio of cashback-eligible transactions using merchant category data.'
    - question: 'Analyze weekend vs weekday spending patterns.'
      answer: 'I will compare transaction patterns between weekends and weekdays.'
    - question: 'What is the average transaction amount by account type?'
      answer: 'I will calculate average transaction amounts grouped by source account type.'

    # Cash Advance & Credit (5)
    - question: 'What is our cash advance default rate and how is it trending?'
      answer: 'I will analyze advance repayment status in the Credit Risk Intelligence view to calculate default rates.'
    - question: 'Show me the distribution of cash advance amounts by credit score bands.'
      answer: 'I will group advance amounts by customer credit score ranges.'
    - question: 'What percentage of eligible customers are using cash advances?'
      answer: 'I will calculate advance utilization rate among eligible customers.'
    - question: 'Analyze the relationship between direct deposit amounts and advance repayment rates.'
      answer: 'I will correlate direct deposit data with advance repayment performance.'
    - question: 'What is the average time to repay advances by customer segment?'
      answer: 'I will calculate average repayment days grouped by customer segments.'

    # Fraud & Risk (5)
    - question: 'Score this transaction for fraud risk: $500 international online purchase.'
      answer: 'I will call the ScoreTransactionFraud function with the transaction parameters.'
    - question: 'Identify customers with anomalous transaction patterns in the last 24 hours.'
      answer: 'I will call the DetectTransactionAnomalies function to find unusual patterns.'
    - question: 'What percentage of transactions have high fraud scores (>0.7)?'
      answer: 'I will analyze fraud score distribution in the transaction data.'
    - question: 'Show me high-risk merchant categories and their transaction volumes.'
      answer: 'I will identify high-risk MCCs and sum their transaction volumes.'
    - question: 'Which customers have triggered compliance events in the last 90 days?'
      answer: 'I will query compliance events in the Credit Risk Intelligence view.'

    # ML Predictions (5)
    - question: 'Check if customer CUST00001234 is eligible for a cash advance.'
      answer: 'I will call the CalculateAdvanceEligibility function for this customer.'
    - question: 'Recommend a credit limit for customer CUST00005678 applying for a Believe Card.'
      answer: 'I will call the RecommendCreditLimit function with BELIEVE_CARD product type.'
    - question: 'Predict the lifetime value for our newest cohort of customers.'
      answer: 'I will call the PredictCustomerLTV function for recently acquired customers.'
    - question: 'Which customers are showing signs of potential churn?'
      answer: 'I will analyze churn risk indicators using activity and balance trends.'
    - question: 'Score the fraud risk for recent high-value transactions.'
      answer: 'I will call ScoreTransactionFraud for transactions above threshold amounts.'

    # Feature Store & Complex (5)
    - question: 'Show me real-time feature values for our highest-value customers.'
      answer: 'I will query the online_features table in Feature Store for top customers.'
    - question: 'Compare SQL-based feature computation performance vs our previous Tecton setup.'
      answer: 'I will analyze feature computation logs and compare latency metrics.'
    - question: 'What features have the highest correlation with advance defaults?'
      answer: 'I will analyze feature importance for advance risk using Feature Store data.'
    - question: 'Generate a training dataset for next month''s fraud model.'
      answer: 'I will describe the process to create point-in-time features for fraud labels.'
    - question: 'Monitor feature drift for critical risk indicators.'
      answer: 'I will query feature monitoring tables for drift alerts on risk features.'

    # Support & Compliance (5)
    - question: 'Search for customer issues related to card declines.'
      answer: 'I will search support transcripts for card decline troubleshooting.'
    - question: 'What are our AML transaction monitoring thresholds?'
      answer: 'I will search compliance documents for AML monitoring procedures.'
    - question: 'Find information about cash advance eligibility requirements.'
      answer: 'I will search product knowledge for advance eligibility criteria.'
    - question: 'Show me common support issues and their resolution rates.'
      answer: 'I will analyze support interaction categories and resolution status.'
    - question: 'Search for Regulation E dispute handling procedures.'
      answer: 'I will search compliance documents for Regulation E procedures.'

tools:
  # Semantic View Tools (Cortex Analyst)
  - tool_spec:
      type: 'cortex_analyst_text_to_sql'
      name: 'CustomerBankingAnalyst'
      description: 'Analyzes customers, accounts, cards, direct deposits, cash advances, support'
  - tool_spec:
      type: 'cortex_analyst_text_to_sql'
      name: 'TransactionPaymentAnalyst'
      description: 'Analyzes transactions, payments, merchants, spending patterns'
  - tool_spec:
      type: 'cortex_analyst_text_to_sql'
      name: 'CreditRiskAnalyst'
      description: 'Analyzes credit applications, cash advances, compliance, external data'
  
  # Cortex Search Tools
  - tool_spec:
      type: 'cortex_search'
      name: 'SupportTranscriptsSearch'
      description: 'Searches customer support transcripts and interactions'
  - tool_spec:
      type: 'cortex_search'
      name: 'ComplianceDocsSearch'
      description: 'Searches compliance policies and regulatory documents'
  - tool_spec:
      type: 'cortex_search'
      name: 'ProductKnowledgeSearch'
      description: 'Searches product documentation and knowledge base'
  
  # ML Function Tools
  - tool_spec:
      type: 'generic'
      name: 'ScoreTransactionFraud'
      description: 'Scores transaction fraud risk using Feature Store data'
      input_schema:
        type: 'object'
        properties:
          customer_id:
            type: 'string'
            description: 'Customer identifier'
          amount:
            type: 'number'
            description: 'Transaction amount'
          merchant_category:
            type: 'string'
            description: 'Merchant category code (MCC)'
          is_international:
            type: 'boolean'
            description: 'Whether transaction is international'
        required: ['customer_id', 'amount', 'merchant_category', 'is_international']
  
  - tool_spec:
      type: 'generic'
      name: 'CalculateAdvanceEligibility'
      description: 'Determines cash advance eligibility and limit'
      input_schema:
        type: 'object'
        properties:
          customer_id:
            type: 'string'
            description: 'Customer identifier'
        required: ['customer_id']
  
  - tool_spec:
      type: 'generic'
      name: 'RecommendCreditLimit'
      description: 'Recommends appropriate credit limit'
      input_schema:
        type: 'object'
        properties:
          customer_id:
            type: 'string'
            description: 'Customer identifier'
          product_type:
            type: 'string'
            description: 'Credit product type (BELIEVE_CARD or LINE_OF_CREDIT)'
        required: ['customer_id', 'product_type']
  
  - tool_spec:
      type: 'generic'
      name: 'PredictCustomerLTV'
      description: 'Predicts customer lifetime value'
      input_schema:
        type: 'object'
        properties:
          customer_id:
            type: 'string'
            description: 'Customer identifier'
        required: ['customer_id']
  
  - tool_spec:
      type: 'generic'
      name: 'DetectTransactionAnomalies'
      description: 'Detects unusual transaction patterns'
      input_schema:
        type: 'object'
        properties:
          lookback_hours:
            type: 'number'
            description: 'Hours to look back for anomalies (default 24)'
        required: []

tool_resources:
  # Semantic Views
  CustomerBankingAnalyst:
    semantic_view: 'VARO_INTELLIGENCE.ANALYTICS.SV_CUSTOMER_BANKING_INTELLIGENCE'
    execution_environment:
      type: 'warehouse'
      warehouse: 'VARO_WH'
      query_timeout: 60
  TransactionPaymentAnalyst:
    semantic_view: 'VARO_INTELLIGENCE.ANALYTICS.SV_TRANSACTION_PAYMENT_INTELLIGENCE'
    execution_environment:
      type: 'warehouse'
      warehouse: 'VARO_WH'
      query_timeout: 60
  CreditRiskAnalyst:
    semantic_view: 'VARO_INTELLIGENCE.ANALYTICS.SV_CREDIT_RISK_INTELLIGENCE'
    execution_environment:
      type: 'warehouse'
      warehouse: 'VARO_WH'
      query_timeout: 60
  
  # Cortex Search Services
  SupportTranscriptsSearch:
    search_service: 'VARO_INTELLIGENCE.RAW.SUPPORT_TRANSCRIPTS_SEARCH'
    max_results: 10
    title_column: 'interaction_id'
    id_column: 'transcript_id'
  ComplianceDocsSearch:
    search_service: 'VARO_INTELLIGENCE.RAW.COMPLIANCE_DOCS_SEARCH'
    max_results: 10
    title_column: 'title'
    id_column: 'document_id'
  ProductKnowledgeSearch:
    search_service: 'VARO_INTELLIGENCE.RAW.PRODUCT_KNOWLEDGE_SEARCH'
    max_results: 10
    title_column: 'title'
    id_column: 'knowledge_id'
  
  # ML Functions
  ScoreTransactionFraud:
    type: 'function'
    identifier: 'VARO_INTELLIGENCE.ANALYTICS.SCORE_TRANSACTION_FRAUD'
    execution_environment:
      type: 'warehouse'
      warehouse: 'VARO_FEATURE_WH'
      query_timeout: 30
  CalculateAdvanceEligibility:
    type: 'function'
    identifier: 'VARO_INTELLIGENCE.ANALYTICS.CALCULATE_ADVANCE_ELIGIBILITY'
    execution_environment:
      type: 'warehouse'
      warehouse: 'VARO_FEATURE_WH'
      query_timeout: 30
  RecommendCreditLimit:
    type: 'function'
    identifier: 'VARO_INTELLIGENCE.ANALYTICS.RECOMMEND_CREDIT_LIMIT'
    execution_environment:
      type: 'warehouse'
      warehouse: 'VARO_FEATURE_WH'
      query_timeout: 30
  PredictCustomerLTV:
    type: 'function'
    identifier: 'VARO_INTELLIGENCE.ANALYTICS.PREDICT_CUSTOMER_LTV'
    execution_environment:
      type: 'warehouse'
      warehouse: 'VARO_FEATURE_WH'
      query_timeout: 30
  DetectTransactionAnomalies:
    type: 'function'
    identifier: 'VARO_INTELLIGENCE.ANALYTICS.DETECT_TRANSACTION_ANOMALIES'
    execution_environment:
      type: 'warehouse'
      warehouse: 'VARO_FEATURE_WH'
      query_timeout: 60
  $$;

-- ============================================================================
-- Verify Agent Creation
-- ============================================================================
SHOW AGENTS LIKE 'VARO_INTELLIGENCE_AGENT';
DESCRIBE AGENT VARO_INTELLIGENCE_AGENT;

-- ============================================================================
-- Grant Permissions
-- ============================================================================
-- Grant usage on agent to specific roles if needed
-- GRANT USAGE ON AGENT VARO_INTELLIGENCE_AGENT TO ROLE <role_name>;

-- Display confirmation
SELECT 
    'Varo Intelligence Agent created successfully!' AS status,
    'Ready to answer questions about:' AS capabilities,
    '- Customer analytics and segmentation' AS capability_1,
    '- Transaction patterns and fraud detection' AS capability_2,
    '- Cash advance eligibility and credit risk' AS capability_3,
    '- Feature Store data and ML predictions' AS capability_4,
    '- Support transcripts and compliance docs' AS capability_5;
