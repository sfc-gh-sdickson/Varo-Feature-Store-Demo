# Snowflake External Functions Guide

## Overview

External Functions in Snowflake allow you to call external services (like AWS Lambda, Azure Functions, or Google Cloud Functions) from within Snowflake SQL. This is useful for:
- Real-time ML model serving
- Calling external APIs
- Complex computations that require external libraries

## Current Demo Implementation

For this demo, we've replaced External Functions with regular SQL functions that simulate the same behavior. This allows the demo to run without requiring AWS infrastructure setup.

### Functions Included:
1. `GET_CUSTOMER_FEATURES(customer_id)` - Retrieves feature vectors from the online store
2. `GET_TRANSACTION_RISK_SCORE(transaction_data)` - Calculates risk scores for transactions

## Setting Up Real External Functions (Production)

If you want to implement real External Functions for production use, follow these steps:

### 1. AWS Setup
```bash
# Create an AWS Lambda function
# Create an API Gateway to expose the Lambda
# Set up IAM roles for Snowflake access
```

### 2. Snowflake API Integration
```sql
CREATE OR REPLACE API INTEGRATION my_api_integration
    API_PROVIDER = aws_api_gateway
    API_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeAPIRole'
    API_ALLOWED_PREFIXES = ('https://your-api-gateway-url.amazonaws.com/')
    ENABLED = TRUE;
```

### 3. Create External Function
```sql
CREATE OR REPLACE EXTERNAL FUNCTION my_external_function(param1 VARCHAR)
    RETURNS VARIANT
    API_INTEGRATION = my_api_integration
    AS 'https://your-api-gateway-url.amazonaws.com/prod/endpoint';
```

## Benefits of External Functions for Feature Stores

1. **Low Latency**: Direct API calls for real-time predictions
2. **Scalability**: Leverage cloud provider auto-scaling
3. **Flexibility**: Use any programming language or ML framework
4. **Security**: IAM-based authentication and encryption

## Migration from Tecton

Tecton provides similar functionality through their serving infrastructure. When migrating:

1. **Tecton Feature Server** → Snowflake External Functions + AWS Lambda
2. **Tecton Python Transformations** → Snowflake UDFs or External Functions
3. **Tecton REST API** → API Gateway + External Functions

## Demo vs Production

| Aspect | Demo (Current) | Production |
|--------|---------------|------------|
| Infrastructure | None required | AWS Lambda + API Gateway |
| Latency | < 10ms (local) | 50-200ms (API call) |
| Cost | Snowflake compute only | Snowflake + AWS costs |
| ML Models | Mock calculations | Real model inference |
| Scalability | Limited by warehouse | Auto-scales with AWS |

## Next Steps

To convert the demo to production:
1. Set up AWS infrastructure
2. Deploy ML models to Lambda
3. Create API Integration in Snowflake
4. Replace SQL functions with External Functions
5. Test performance and latency

## Resources

- [Snowflake External Functions Documentation](https://docs.snowflake.com/en/sql-reference/external-functions)
- [AWS Lambda with Snowflake Tutorial](https://docs.snowflake.com/en/sql-reference/external-functions-creating-aws)
- [Snowflake Feature Store Guide](https://docs.snowflake.com/en/developer-guide/snowpark-ml/feature-store/overview)
