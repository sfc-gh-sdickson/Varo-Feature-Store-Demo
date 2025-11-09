<img src="..\Snowflake_Logo.svg" width="200">

# Varo Intelligence Agent - Setup Guide

This guide walks through configuring a Snowflake Intelligence agent for Varo Bank's digital banking intelligence solution, featuring advanced Feature Store capabilities for ML model serving and real-time fraud detection.

---

## Key Features

- **SQL-First Feature Store**: Replaces Tecton with native Snowflake capabilities
- **Real-Time ML Serving**: Low-latency feature retrieval for fraud detection
- **Comprehensive Banking Analytics**: Customer 360, transaction analysis, credit risk
- **Unstructured Data Search**: Support transcripts, compliance docs, product knowledge
- **Point-in-Time Features**: Historical feature values for model training

---

## Prerequisites

1. **Snowflake Account** with:
   - Snowflake Intelligence (Cortex) enabled
   - Dynamic Tables enabled (for Feature Store)
   - Appropriate warehouse sizes (X-SMALL for queries, SMALL for feature engineering)
   - Permissions to create databases, schemas, tables, and semantic views

2. **Roles and Permissions**:
   - `ACCOUNTADMIN` role or equivalent for initial setup
   - `CREATE DATABASE` privilege
   - `CREATE SEMANTIC VIEW` privilege
   - `CREATE CORTEX SEARCH SERVICE` privilege
   - `CREATE DYNAMIC TABLE` privilege
   - `CREATE EXTERNAL FUNCTION` privilege (for real-time serving)
   - `USAGE` on warehouses

---

## Step 1: Execute SQL Scripts in Order

Execute the SQL files in the following sequence:

### 1.1 Database Setup
```sql
-- Execute: sql/setup/01_database_and_schema.sql
-- Creates:
--   - Database: VARO_INTELLIGENCE
--   - Schemas: RAW, FEATURE_STORE, ANALYTICS
--   - Warehouses: VARO_WH (X-SMALL), VARO_FEATURE_WH (SMALL)
-- Execution time: < 1 second
```

### 1.2 Create Core Banking Tables
```sql
-- Execute: sql/setup/02_create_tables.sql
-- Creates banking-specific tables:
--   - CUSTOMERS, ACCOUNTS, TRANSACTIONS, CARDS
--   - DIRECT_DEPOSITS, CASH_ADVANCES, CREDIT_APPLICATIONS
--   - MERCHANT_CATEGORIES, DEVICE_SESSIONS
--   - SUPPORT_INTERACTIONS, MARKETING_CAMPAIGNS
--   - EXTERNAL_DATA, COMPLIANCE_EVENTS
-- Execution time: < 5 seconds
```

### 1.3 Create Feature Store Infrastructure
```sql
-- Execute: sql/feature_store/03_create_feature_store.sql
-- Creates Feature Store components:
--   - FEATURE_DEFINITIONS, FEATURE_SETS, FEATURE_VALUES
--   - FEATURE_STATISTICS, TRAINING_DATASETS
--   - FEATURE_LINEAGE, FEATURE_MONITORING
--   - MODEL_FEATURES, ONLINE_FEATURES
--   - Dynamic tables for real-time features
--   - Streams and tasks for automation
--   - External functions for low-latency serving
-- Execution time: < 10 seconds
```

### 1.4 Generate Banking Data
```sql
-- Execute: sql/data/04_generate_synthetic_data.sql
-- Generates realistic banking data:
--   - 2,000,000 customers
--   - 3,500,000 accounts
--   - 50,000,000 transactions (initial batch)
--   - 25,000 support transcripts
--   - Cash advances, direct deposits, cards
-- WARNING: This takes 15-20 minutes on SMALL warehouse
-- TIP: Adjust transaction count if needed for faster setup
```

### 1.5 Create Feature Engineering Pipelines
```sql
-- Execute: sql/feature_store/05_create_features.sql
-- Creates SQL-based feature engineering:
--   - Customer profile features (Dynamic Table)
--   - Transaction pattern features (30-min refresh)
--   - Cash advance risk features (hourly refresh)
--   - Fraud detection features (15-min refresh)
--   - Point-in-time feature retrieval function
--   - Training dataset generation procedure
-- Execution time: < 30 seconds
```

### 1.6 Create Analytical Views
```sql
-- Execute: sql/views/06_create_views.sql
-- Creates banking analytical views:
--   - V_CUSTOMER_360
--   - V_ACCOUNT_ANALYTICS
--   - V_TRANSACTION_ANALYTICS
--   - V_CASH_ADVANCE_ANALYTICS
--   - V_DIRECT_DEPOSIT_ANALYTICS
--   - V_FRAUD_RISK_ANALYTICS
--   - V_CUSTOMER_CHURN_RISK
--   - V_MARKETING_CAMPAIGN_PERFORMANCE
--   - V_COMPLIANCE_RISK
-- Execution time: < 5 seconds
```

### 1.7 Create Semantic Views
```sql
-- Execute: sql/views/07_create_semantic_views.sql
-- Creates semantic views for AI agents:
--   - SV_CUSTOMER_BANKING_INTELLIGENCE
--   - SV_TRANSACTION_PAYMENT_INTELLIGENCE
--   - SV_CREDIT_RISK_INTELLIGENCE
-- All with banking-specific synonyms and metrics
-- Execution time: < 5 seconds
```

### 1.8 Create Cortex Search Services
```sql
-- Execute: sql/search/08_create_cortex_search.sql
-- Creates search services for:
--   - SUPPORT_TRANSCRIPTS_SEARCH (25,000 transcripts)
--   - COMPLIANCE_DOCS_SEARCH (AML, KYC, RegE docs)
--   - PRODUCT_KNOWLEDGE_SEARCH (Varo products info)
-- Includes helper views and aggregate search function
-- Execution time: 5-10 minutes (index building)
```

### 1.9 Create ML Model Functions
```sql
-- Execute: sql/ml/09_create_model_functions.sql
-- Creates ML wrapper functions:
--   - SCORE_TRANSACTION_FRAUD (real-time fraud scoring)
--   - CALCULATE_ADVANCE_ELIGIBILITY (cash advance decisioning)
--   - RECOMMEND_CREDIT_LIMIT (credit limit optimization)
--   - PREDICT_CUSTOMER_LTV (lifetime value prediction)
--   - DETECT_TRANSACTION_ANOMALIES (anomaly detection)
-- All integrated with Feature Store
-- Execution time: < 5 seconds
```

### 1.10 Create Intelligence Agent
```sql
-- Execute: sql/agent/10_create_intelligence_agent.sql
-- Creates the Varo Intelligence Agent with:
--   - 3 Cortex Analyst tools (semantic views)
--   - 3 Cortex Search tools (unstructured data)
--   - 5 ML function tools (predictions)
--   - Banking-specific instructions and examples
-- Execution time: < 5 seconds
```

---

## Step 2: Grant Permissions

### 2.1 Grant Cortex Permissions

```sql
USE ROLE ACCOUNTADMIN;

-- Grant Cortex user role
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_USER TO ROLE <your_role>;

-- Grant database usage
GRANT USAGE ON DATABASE VARO_INTELLIGENCE TO ROLE <your_role>;
GRANT USAGE ON ALL SCHEMAS IN DATABASE VARO_INTELLIGENCE TO ROLE <your_role>;

-- Grant semantic view permissions
GRANT REFERENCES, SELECT ON ALL SEMANTIC VIEWS IN SCHEMA VARO_INTELLIGENCE.ANALYTICS TO ROLE <your_role>;

-- Grant function permissions
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA VARO_INTELLIGENCE.ANALYTICS TO ROLE <your_role>;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE VARO_WH TO ROLE <your_role>;
GRANT USAGE ON WAREHOUSE VARO_FEATURE_WH TO ROLE <your_role>;

-- Grant Cortex Search usage
GRANT USAGE ON ALL CORTEX SEARCH SERVICES IN SCHEMA VARO_INTELLIGENCE.RAW TO ROLE <your_role>;

-- Grant agent usage
GRANT USAGE ON AGENT VARO_INTELLIGENCE_AGENT TO ROLE <your_role>;
```

---

## Step 3: Configure Agent in Snowsight

### Option 1: Using SQL (Recommended)

The agent is already created via SQL in script 10. To modify:

```sql
-- View agent configuration
DESCRIBE AGENT VARO_INTELLIGENCE_AGENT;

-- Update agent if needed
ALTER AGENT VARO_INTELLIGENCE_AGENT SET COMMENT = 'Updated description';
```

### Option 2: Using Snowsight UI

1. Navigate to **AI & ML** > **Agents**
2. Find `VARO_INTELLIGENCE_AGENT`
3. Click to open and review configuration
4. Tools are already configured via SQL

---

## Step 4: Test the Agent

### 4.1 Basic Customer Query
```
Question: "How many active customers do we have with direct deposit?"
Expected: Agent uses CustomerBankingAnalyst to query semantic view
```

### 4.2 Transaction Analysis
```
Question: "What are the top merchant categories by spending volume?"
Expected: Agent uses TransactionPaymentAnalyst for merchant analysis
```

### 4.3 ML Prediction
```
Question: "Is customer CUST00001234 eligible for a cash advance?"
Expected: Agent calls CalculateAdvanceEligibility function
```

### 4.4 Fraud Detection
```
Question: "Score fraud risk for a $1000 international transaction"
Expected: Agent calls ScoreTransactionFraud with parameters
```

### 4.5 Cortex Search
```
Question: "Search for procedures on handling disputed transactions"
Expected: Agent searches compliance documents and support transcripts
```

---

## Step 5: Feature Store Operations

### 5.1 View Real-Time Features
```sql
-- Check online features for a customer
SELECT * FROM FEATURE_STORE.ONLINE_FEATURES 
WHERE entity_id = 'CUST00001234';
```

### 5.2 Monitor Feature Computation
```sql
-- Check feature computation status
SELECT * FROM FEATURE_STORE.FEATURE_COMPUTE_LOGS
WHERE compute_status = 'RUNNING'
ORDER BY compute_start DESC;
```

### 5.3 Create Training Dataset
```sql
-- Generate point-in-time features for model training
CALL FEATURE_STORE.CREATE_TRAINING_DATASET(
    'fraud_model_v2',
    'fraud_detection_v1',
    '2024-01-01',
    '2024-12-31',
    'SELECT customer_id, transaction_id, fraud_label, transaction_timestamp FROM ...',
    NULL
);
```

---

## Step 6: Migration from Tecton

### 6.1 Feature Definition Migration

Tecton Python feature definition:
```python
@batch_feature_view(
    sources=[transactions],
    ttl=timedelta(days=30)
)
def transaction_features(transactions):
    return f"""
    SELECT customer_id,
           COUNT(*) as txn_count_30d,
           SUM(amount) as txn_volume_30d
    FROM {transactions}
    WHERE timestamp > current_timestamp - INTERVAL 30 DAYS
    GROUP BY customer_id
    """
```

Equivalent Snowflake Dynamic Table:
```sql
CREATE OR REPLACE DYNAMIC TABLE TRANSACTION_FEATURES_30D
    TARGET_LAG = '1 HOUR'
    WAREHOUSE = VARO_FEATURE_WH
    AS
    SELECT customer_id,
           COUNT(*) as txn_count_30d,
           SUM(amount) as txn_volume_30d
    FROM TRANSACTIONS
    WHERE transaction_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY customer_id;
```

### 6.2 Online Serving Migration

Replace Tecton's feature server with Snowflake External Functions or direct queries to ONLINE_FEATURES table.

---

## Troubleshooting

### Issue: Agent returns no results
- Check warehouse is running: `SHOW WAREHOUSES LIKE 'VARO%';`
- Verify permissions: `SHOW GRANTS TO ROLE <your_role>;`
- Test semantic views directly: `SELECT * FROM SV_CUSTOMER_BANKING_INTELLIGENCE LIMIT 1;`

### Issue: ML functions fail
- Ensure Feature Store data is populated
- Check function permissions: `SHOW GRANTS ON FUNCTION SCORE_TRANSACTION_FRAUD;`
- Test functions directly with sample data

### Issue: Search returns empty
- Verify Cortex Search services are built: `SHOW CORTEX SEARCH SERVICES;`
- Check change tracking is enabled on source tables
- Wait for TARGET_LAG period after data changes

---

## Best Practices

1. **Feature Store Management**
   - Monitor feature freshness and drift
   - Version feature definitions
   - Document feature lineage

2. **Performance Optimization**
   - Use appropriate warehouse sizes
   - Leverage Dynamic Table refresh settings
   - Cache frequently accessed features

3. **Security**
   - Implement row-level security on sensitive data
   - Audit feature access patterns
   - Encrypt PII in feature values

4. **Cost Management**
   - Schedule feature computations during off-peak
   - Use auto-suspend on warehouses
   - Monitor credit usage by feature pipeline

---

## Next Steps

1. **Customize Features**: Add domain-specific features for your use cases
2. **Train Models**: Use feature datasets to train Snowpark ML models
3. **Deploy Models**: Register models and create serving endpoints
4. **Monitor Performance**: Set up alerts for feature drift and model degradation
5. **Expand Search**: Add more unstructured data sources to Cortex Search

---

## Support Resources

- [Snowflake Intelligence Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-llm-functions)
- [Dynamic Tables Guide](https://docs.snowflake.com/en/user-guide/dynamic-tables)
- [Cortex Search Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Feature Store Guide](https://docs.snowflake.com/guides/getting-started-with-feature-store)

---

**Created**: November 2025  
**Version**: 1.0  
**Focus**: SQL-First Feature Store for Banking ML
