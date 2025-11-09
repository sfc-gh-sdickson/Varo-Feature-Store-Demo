-- ============================================================================
-- Varo Intelligence Agent & Feature Store - Database and Schema Setup
-- ============================================================================
-- Purpose: Initialize the database, schemas, and warehouse for the Varo
--          Intelligence Agent solution with Feature Store capabilities
-- ============================================================================

-- Create the database
CREATE DATABASE IF NOT EXISTS VARO_INTELLIGENCE;

-- Use the database
USE DATABASE VARO_INTELLIGENCE;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW COMMENT = 'Raw banking data tables';
CREATE SCHEMA IF NOT EXISTS FEATURE_STORE COMMENT = 'Feature engineering and ML features';
CREATE SCHEMA IF NOT EXISTS ANALYTICS COMMENT = 'Analytical views and semantic models';

-- Create a virtual warehouse for query processing
CREATE OR REPLACE WAREHOUSE VARO_WH WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Varo Intelligence Agent queries';

-- Create a larger warehouse for feature engineering and ML workloads
CREATE OR REPLACE WAREHOUSE VARO_FEATURE_WH WITH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for feature engineering and ML workloads';

-- Set the warehouse as active
USE WAREHOUSE VARO_WH;

-- Display confirmation
SELECT 'Database, schemas, and warehouses setup completed successfully' AS STATUS;
