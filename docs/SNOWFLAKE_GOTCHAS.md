# Snowflake SQL Gotchas and Common Mistakes

This document lists common SQL mistakes when migrating from other databases to Snowflake.

## 1. UNIFORM() Function Requires Constants

**Error**: `argument 2 to function UNIFORM needs to be constant`

### ❌ WRONG (Works in other DBs, fails in Snowflake)
```sql
-- Dynamic upper bound
UNIFORM(0, credit_limit * 0.7, RANDOM())

-- Using CASE expression  
UNIFORM(0, 
    CASE 
        WHEN score < 600 THEN 500 
        ELSE 1000 
    END, 
    RANDOM())
```

### ✅ CORRECT (Snowflake approach)
```sql
-- Use RANDOM() with multiplication
RANDOM() * (credit_limit * 0.7)

-- Use RANDOM() with CASE
RANDOM() * CASE 
    WHEN score < 600 THEN 500 
    ELSE 1000 
END
```

## 2. No CREATE INDEX on Regular Tables

**Error**: `Table 'TABLENAME' is not a hybrid table`

### ❌ WRONG
```sql
CREATE INDEX idx_customer ON transactions(customer_id);
```

### ✅ CORRECT
```sql
-- Use clustering for range queries
ALTER TABLE transactions CLUSTER BY (customer_id, transaction_date);

-- Use search optimization for point lookups
ALTER TABLE transactions ADD SEARCH OPTIMIZATION;
```

## 3. Use DIV0NULL Instead of SAFE_DIVIDE

**Error**: `Unknown function SAFE_DIVIDE`

### ❌ WRONG (BigQuery syntax)
```sql
SAFE_DIVIDE(numerator, denominator)
```

### ✅ CORRECT (Snowflake)
```sql
DIV0NULL(numerator, denominator)
```

## 4. Table Alias Scope in Subqueries

**Error**: `invalid identifier 'ALIAS.COLUMN'`

### ❌ WRONG
```sql
SELECT 
    column1,
    CASE WHEN c.status = 'ACTIVE' THEN 1 END
FROM (
    SELECT * FROM customers c
)  -- alias 'c' not available outside subquery
```

### ✅ CORRECT
```sql
SELECT 
    column1,
    CASE WHEN c.status = 'ACTIVE' THEN 1 END
FROM (
    SELECT * FROM customers c
) c  -- alias must be repeated
```

## 5. Column Reference Order Matters

**Error**: Cannot reference a column before it's defined in the same SELECT

### ❌ WRONG
```sql
SELECT 
    current_balance,
    credit_limit - current_balance AS available,
    CASE 
        WHEN type = 'CREDIT' THEN credit_limit * 0.5
        ELSE 0
    END AS credit_limit
```

### ✅ CORRECT
```sql
SELECT 
    CASE 
        WHEN type = 'CREDIT' THEN base_limit * 0.5
        ELSE 0
    END AS credit_limit,
    balance AS current_balance,
    credit_limit - balance AS available
FROM (
    SELECT base_limit, balance, type FROM accounts
)
```

## 6. External Functions Need Real Endpoints

**Error**: `URL 'https://example.com' is invalid`

### ❌ WRONG
```sql
CREATE EXTERNAL FUNCTION my_function()
    RETURNS VARIANT
    API_INTEGRATION = my_api
    AS 'https://placeholder-url.com/endpoint';
```

### ✅ CORRECT
```sql
-- For demos, use regular SQL functions
CREATE OR REPLACE FUNCTION my_function()
    RETURNS VARIANT
AS
$$
    SELECT OBJECT_CONSTRUCT('result', 'value')
$$;
```

## Summary

Always remember:
1. Test SQL in actual Snowflake environment
2. Check Snowflake documentation for function-specific requirements
3. Be aware of scoping rules and reference order
4. Use Snowflake-specific optimization features instead of traditional indexes
