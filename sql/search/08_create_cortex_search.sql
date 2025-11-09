-- ============================================================================
-- Varo Intelligence Agent - Cortex Search Service Setup
-- ============================================================================
-- Purpose: Create Cortex Search services for banking unstructured data:
--          support transcripts, compliance documents, and product knowledge
-- Syntax verified against: https://docs.snowflake.com/en/sql-reference/sql/create-cortex-search
-- ============================================================================

USE DATABASE VARO_INTELLIGENCE;
USE SCHEMA RAW;
USE WAREHOUSE VARO_WH;

-- ============================================================================
-- Verify tables have change tracking enabled (from data generation script)
-- ============================================================================
-- Note: These tables were created in 04_generate_synthetic_data.sql with change tracking

-- ============================================================================
-- Step 1: Create Cortex Search Service for Support Transcripts
-- ============================================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE SUPPORT_TRANSCRIPTS_SEARCH
  ON transcript_text
  ATTRIBUTES customer_id, agent_id, interaction_type, category, subcategory
  WAREHOUSE = VARO_WH
  TARGET_LAG = '1 HOUR'
  AS SELECT
      transcript_id,
      interaction_id,
      customer_id,
      agent_id,
      interaction_date,
      interaction_type,
      category,
      subcategory,
      transcript_text,
      sentiment_score,
      resolution_achieved
  FROM SUPPORT_TRANSCRIPTS
  WHERE transcript_text IS NOT NULL;

-- ============================================================================
-- Step 2: Create Cortex Search Service for Compliance Documents
-- ============================================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE COMPLIANCE_DOCS_SEARCH
  ON content
  ATTRIBUTES document_type, title, tags
  WAREHOUSE = VARO_WH
  TARGET_LAG = '1 DAY'
  AS SELECT
      document_id,
      document_type,
      title,
      content,
      effective_date,
      tags
  FROM COMPLIANCE_DOCUMENTS
  WHERE content IS NOT NULL;

-- ============================================================================
-- Step 3: Create Cortex Search Service for Product Knowledge
-- ============================================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE PRODUCT_KNOWLEDGE_SEARCH
  ON content
  ATTRIBUTES product_name, category, title, version
  WAREHOUSE = VARO_WH
  TARGET_LAG = '1 DAY'
  AS SELECT
      knowledge_id,
      product_name,
      category,
      title,
      content,
      version
  FROM PRODUCT_KNOWLEDGE
  WHERE content IS NOT NULL;

-- ============================================================================
-- Step 4: Test Cortex Search Services
-- ============================================================================

-- Test support transcript search
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'VARO_INTELLIGENCE.RAW.SUPPORT_TRANSCRIPTS_SEARCH',
      '{
          "query": "card declined transaction",
          "columns": ["transcript_text", "category", "subcategory"],
          "limit": 5
      }'
  )
)['results'] as search_results;

-- Test compliance document search
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'VARO_INTELLIGENCE.RAW.COMPLIANCE_DOCS_SEARCH',
      '{
          "query": "money laundering detection procedures",
          "columns": ["title", "content"],
          "limit": 3
      }'
  )
)['results'] as search_results;

-- Test product knowledge search
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'VARO_INTELLIGENCE.RAW.PRODUCT_KNOWLEDGE_SEARCH',
      '{
          "query": "how to increase advance limit",
          "columns": ["title", "content", "product_name"],
          "limit": 3
      }'
  )
)['results'] as search_results;

-- Display confirmation
SELECT 
    'Cortex Search services created successfully' AS status,
    3 AS total_services_created;
