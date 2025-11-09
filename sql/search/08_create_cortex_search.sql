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

-- ============================================================================
-- Step 5: Create aggregate search function across all services
-- ============================================================================
CREATE OR REPLACE FUNCTION SEARCH_ALL_DOCUMENTS(search_query VARCHAR, max_results NUMBER)
RETURNS TABLE(
    source VARCHAR,
    document_id VARCHAR,
    title VARCHAR,
    snippet VARCHAR,
    relevance_score NUMBER(5,4)
)
AS
$$
    -- Search support transcripts
    WITH support_results AS (
        SELECT 
            'SUPPORT' as source,
            PARSE_JSON(results.value):transcript_id::VARCHAR as document_id,
            'Support: ' || PARSE_JSON(results.value):category::VARCHAR || ' - ' || 
            PARSE_JSON(results.value):subcategory::VARCHAR as title,
            SUBSTR(PARSE_JSON(results.value):transcript_text::VARCHAR, 1, 500) as snippet,
            PARSE_JSON(results.value):score::NUMBER(5,4) as relevance_score
        FROM TABLE(FLATTEN(
            input => PARSE_JSON(
                SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                    'VARO_INTELLIGENCE.RAW.SUPPORT_TRANSCRIPTS_SEARCH',
                    OBJECT_CONSTRUCT(
                        'query', search_query,
                        'columns', ARRAY_CONSTRUCT('transcript_text', 'category', 'subcategory'),
                        'limit', max_results
                    )
                )
            )['results']
        )) as results
    ),
    -- Search compliance documents
    compliance_results AS (
        SELECT 
            'COMPLIANCE' as source,
            PARSE_JSON(results.value):document_id::VARCHAR as document_id,
            PARSE_JSON(results.value):title::VARCHAR as title,
            SUBSTR(PARSE_JSON(results.value):content::VARCHAR, 1, 500) as snippet,
            PARSE_JSON(results.value):score::NUMBER(5,4) as relevance_score
        FROM TABLE(FLATTEN(
            input => PARSE_JSON(
                SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                    'VARO_INTELLIGENCE.RAW.COMPLIANCE_DOCS_SEARCH',
                    OBJECT_CONSTRUCT(
                        'query', search_query,
                        'columns', ARRAY_CONSTRUCT('title', 'content'),
                        'limit', max_results
                    )
                )
            )['results']
        )) as results
    ),
    -- Search product knowledge
    product_results AS (
        SELECT 
            'PRODUCT' as source,
            PARSE_JSON(results.value):knowledge_id::VARCHAR as document_id,
            PARSE_JSON(results.value):title::VARCHAR as title,
            SUBSTR(PARSE_JSON(results.value):content::VARCHAR, 1, 500) as snippet,
            PARSE_JSON(results.value):score::NUMBER(5,4) as relevance_score
        FROM TABLE(FLATTEN(
            input => PARSE_JSON(
                SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                    'VARO_INTELLIGENCE.RAW.PRODUCT_KNOWLEDGE_SEARCH',
                    OBJECT_CONSTRUCT(
                        'query', search_query,
                        'columns', ARRAY_CONSTRUCT('title', 'content'),
                        'limit', max_results
                    )
                )
            )['results']
        )) as results
    )
    -- Combine all results
    SELECT * FROM support_results
    UNION ALL
    SELECT * FROM compliance_results  
    UNION ALL
    SELECT * FROM product_results
    ORDER BY relevance_score DESC
    LIMIT max_results
$$;

-- ============================================================================
-- Step 6: Usage examples
-- ============================================================================

-- Example 1: Search for card decline issues
/*
SELECT * FROM TABLE(
    SEARCH_ALL_DOCUMENTS('card declined payment failed', 10)
);
*/

-- Example 2: Search for compliance procedures
/*
SELECT * FROM TABLE(
    SEARCH_ALL_DOCUMENTS('anti money laundering procedures', 5)
);
*/

-- Example 3: Search for advance eligibility information
/*
SELECT * FROM TABLE(
    SEARCH_ALL_DOCUMENTS('cash advance eligibility requirements', 5)
);
*/

-- Display confirmation
SELECT 
    'Cortex Search services created successfully' AS status,
    COUNT(*) as total_services
FROM INFORMATION_SCHEMA.CORTEX_SEARCH_SERVICES
WHERE SCHEMA_NAME = 'RAW';
