<img src="..\Snowflake_Logo.svg" width="200">

# Varo Intelligence Agent - Test Questions

These 25 complex questions demonstrate the Varo Intelligence Agent's ability to analyze digital banking operations, customer behavior, fraud patterns, credit risk, and Feature Store capabilities.

---

## 1. Customer Lifetime Value and Product Adoption

**Question:** "Analyze customer lifetime value by acquisition channel and product adoption. Show me average LTV for customers from each channel (MOBILE_APP, WEB, REFERRAL, PAID_AD), breakdown by number of products they use, correlation between direct deposit setup and LTV, and identify which product combinations drive highest value. Include retention rates by segment."

**Why Complex:**
- Multi-dimensional customer segmentation
- Product adoption pattern analysis
- LTV calculation with retention
- Direct deposit impact assessment
- Cross-product synergy identification

**Tools Used:** CustomerBankingAnalyst, PredictCustomerLTV

---

## 2. Cash Advance Default Risk Analysis

**Question:** "Analyze cash advance performance and default patterns. Show me default rates by credit score bands, average advance amounts and fees collected, correlation between direct deposit amounts and repayment rates, time-to-default distribution, and which customer segments have highest risk. Calculate the profitability of the advance program."

**Why Complex:**
- Risk segmentation by credit score
- Profitability calculation (fees vs. defaults)
- Behavioral pattern correlation
- Time-based risk evolution
- Multi-factor default analysis

**Tools Used:** CreditRiskAnalyst, CalculateAdvanceEligibility

---

## 3. Real-Time Fraud Detection Patterns

**Question:** "Identify transaction fraud patterns in the last 24 hours. Show me customers with anomalous transaction behavior, breakdown by anomaly type (velocity spikes, unusual amounts, geographic dispersion), merchants with highest fraud scores, and international transaction risks. Score the top 10 riskiest transactions."

**Why Complex:**
- Real-time anomaly detection
- Multiple fraud indicator types
- Merchant risk analysis
- Geographic pattern detection
- ML-based risk scoring

**Tools Used:** TransactionPaymentAnalyst, DetectTransactionAnomalies, ScoreTransactionFraud

---

## 4. Feature Store Performance vs. Tecton

**Question:** "Compare our SQL-based Feature Store performance against previous Tecton benchmarks. Show me feature computation times, data freshness metrics, cost per feature computation, number of features served per second, and point-in-time accuracy for training datasets. Which features have the highest compute costs?"

**Why Complex:**
- Performance benchmarking
- Cost analysis
- Latency measurement
- Feature freshness tracking
- Training data quality assessment

**Tools Used:** Feature Store tables direct query, FEATURE_COMPUTE_LOGS analysis

---

## 5. Direct Deposit Loss and Churn Prediction

**Question:** "Identify customers at risk of losing direct deposit and potential churn. Show me customers whose direct deposit amounts are declining, those who haven't had deposits in 45+ days, correlation with account balance trends, and support contact patterns. Predict churn probability and recommend retention actions."

**Why Complex:**
- Trend analysis over time
- Multi-signal churn indicators
- Behavioral change detection
- Predictive modeling
- Actionable recommendations

**Tools Used:** CustomerBankingAnalyst, Direct deposit analytics, Support interaction analysis

---

## 6. Credit Limit Optimization Analysis

**Question:** "Analyze credit utilization and recommend limit adjustments. For Believe Card holders, show current utilization rates, payment history, spending patterns, and income levels. Identify customers eligible for limit increases and those who should have limits reduced. Calculate revenue impact of proposed changes."

**Why Complex:**
- Credit utilization analysis
- Payment behavior assessment
- Risk-based limit recommendations
- Revenue impact modeling
- Portfolio optimization

**Tools Used:** CreditRiskAnalyst, RecommendCreditLimit, TransactionPaymentAnalyst

---

## 7. Merchant Cashback Program ROI

**Question:** "Evaluate the cashback rewards program effectiveness. Show me total cashback paid by merchant category, customer engagement rates, incremental spending driven by cashback, program costs vs. interchange revenue, and which merchant categories drive highest customer retention. Is the program profitable?"

**Why Complex:**
- Program ROI calculation
- Customer behavior attribution
- Merchant category analysis
- Retention impact measurement
- Cost-benefit analysis

**Tools Used:** TransactionPaymentAnalyst, Marketing campaign analysis

---

## 8. AML Transaction Monitoring Alerts

**Question:** "Analyze potential money laundering patterns. Show me customers with suspicious transaction patterns (rapid movement of funds, structured deposits, high-risk merchant usage), cash withdrawal patterns exceeding thresholds, accounts with multiple SAR filings, and geographic risk indicators. Prioritize cases for investigation."

**Why Complex:**
- Pattern-based AML detection
- Regulatory threshold monitoring
- Risk scoring and prioritization
- Geographic risk assessment
- Compliance event correlation

**Tools Used:** CreditRiskAnalyst, ComplianceDocsSearch, Transaction pattern analysis

---

## 9. Customer Support Efficiency and Cost

**Question:** "Analyze support interaction patterns and costs. Show me most common support issues by category, average resolution times by channel (CHAT, PHONE, EMAIL), correlation between support contacts and account closure, support costs per customer segment, and search for successful resolution procedures in transcripts."

**Why Complex:**
- Multi-channel support analysis
- Cost allocation by segment
- Churn correlation analysis
- Unstructured data search
- Operational efficiency metrics

**Tools Used:** CustomerBankingAnalyst, SupportTranscriptsSearch

---

## 10. Cross-Sell Opportunity Identification

**Question:** "Identify cross-sell opportunities using Feature Store data. Show me customers with checking but no savings, high-balance customers without credit products, customers eligible for cash advances not using them, and predict acceptance probability for each product. Calculate potential revenue from successful cross-sells."

**Why Complex:**
- Product gap analysis
- Eligibility assessment
- ML-based acceptance prediction
- Revenue potential calculation
- Feature Store integration

**Tools Used:** CustomerBankingAnalyst, ML prediction functions, Feature Store queries

---

## 11. Weekend vs. Weekday Spending Behavior

**Question:** "Compare customer spending patterns between weekdays and weekends. Show transaction volumes, average amounts, merchant category differences, fraud risk variations, and which customer segments show biggest behavioral changes. Do weekend transactions have higher default risk for cash advances?"

**Why Complex:**
- Temporal pattern analysis
- Behavioral segmentation
- Risk correlation by time
- Multi-dimensional comparison
- Default risk assessment

**Tools Used:** TransactionPaymentAnalyst, Fraud risk analysis

---

## 12. Savings Account Growth Strategies

**Question:** "Analyze savings account performance and growth opportunities. Show me customers earning 5% APY vs. 2.5%, auto-save tool adoption rates, correlation between savings balance and account longevity, and identify customers likely to increase savings based on income patterns. What drives savings growth?"

**Why Complex:**
- APY tier analysis
- Feature adoption impact
- Behavioral prediction
- Income-based targeting
- Growth driver identification

**Tools Used:** CustomerBankingAnalyst, Account analytics

---

## 13. International Transaction Risk Assessment

**Question:** "Evaluate international transaction patterns and risks. Show me customers with frequent international transactions, countries with highest transaction volumes, fraud rates for international vs. domestic, card usage patterns abroad, and customers who might benefit from travel notifications. Search for international transaction policies."

**Why Complex:**
- Geographic risk analysis
- Fraud rate comparison
- Travel pattern detection
- Policy compliance check
- Customer communication needs

**Tools Used:** TransactionPaymentAnalyst, ScoreTransactionFraud, ComplianceDocsSearch

---

## 14. Feature Drift Detection and Model Impact

**Question:** "Monitor feature drift in our ML models. Show me features with significant distribution changes over the last 30 days, impact on model predictions, which customer segments show most drift, correlation with external events, and recommend model retraining priorities. How does drift affect fraud detection accuracy?"

**Why Complex:**
- Statistical drift detection
- Model impact assessment
- Segment-specific analysis
- External factor correlation
- Retraining prioritization

**Tools Used:** Feature Store monitoring tables, ML model performance analysis

---

## 15. Marketing Campaign Attribution

**Question:** "Analyze marketing campaign effectiveness with multi-touch attribution. Show me conversion rates by campaign and channel, time from first touch to conversion, which campaigns drive highest-value customers, and ROI including downstream product adoption. Which segments respond best to which campaign types?"

**Why Complex:**
- Multi-touch attribution
- Conversion path analysis
- LTV-based ROI calculation
- Segment response analysis
- Cross-product impact

**Tools Used:** CustomerBankingAnalyst, Campaign performance views

---

## 16. Regulation E Dispute Analysis

**Question:** "Analyze Regulation E dispute patterns and outcomes. Search for dispute handling procedures, show me dispute volumes by transaction type, resolution times vs. regulatory requirements, provisional credit amounts outstanding, and customers with multiple disputes. Are we compliant with resolution timeframes?"

**Why Complex:**
- Regulatory compliance tracking
- Unstructured procedure search
- Time-based requirement analysis
- Customer dispute patterns
- Financial exposure calculation

**Tools Used:** ComplianceDocsSearch, Transaction dispute analysis

---

## 17. Mobile App Engagement and Security

**Question:** "Analyze mobile app usage patterns and security risks. Show me daily active users by device type, biometric authentication adoption, suspicious login patterns, correlation between app usage and account value, and sessions from unusual locations. Which features drive highest engagement?"

**Why Complex:**
- Device-based analysis
- Security pattern detection
- Engagement metric correlation
- Geographic anomaly detection
- Feature usage analytics

**Tools Used:** Device session analysis, Security event monitoring

---

## 18. Credit Building Success Metrics

**Question:** "Evaluate Believe Card credit building effectiveness. Show me average credit score improvements by initial score band, utilization patterns that correlate with score increases, payment history impact, and time to meaningful score improvement. Which customers are ready for credit limit increases?"

**Why Complex:**
- Credit score trajectory analysis
- Behavior-outcome correlation
- Temporal improvement tracking
- Graduation readiness assessment
- Success factor identification

**Tools Used:** CreditRiskAnalyst, External data analysis, RecommendCreditLimit

---

## 19. Real-Time Feature Serving Performance

**Question:** "Analyze real-time feature serving performance for fraud detection. Show me p50, p95, p99 latency for feature retrieval, cache hit rates, features with highest computation cost, concurrent request handling, and compare with SLA requirements. Which features should we pre-compute?"

**Why Complex:**
- Latency percentile analysis
- Cache effectiveness measurement
- Cost-performance tradeoff
- Concurrency assessment
- Optimization recommendations

**Tools Used:** Feature Store performance logs, Online serving metrics

---

## 20. Comprehensive Fraud Ring Detection

**Question:** "Detect potential fraud rings using network analysis. Show me accounts with similar transaction patterns, shared device fingerprints, rapid fund transfers between accounts, unusual account opening velocities from same IP ranges, and calculate total exposure. Generate investigation priority list."

**Why Complex:**
- Network pattern analysis
- Device fingerprint matching
- Velocity detection
- IP-based clustering
- Risk exposure calculation

**Tools Used:** Multiple fraud detection functions, Device analysis, Network queries

---

## 21. Cash Flow Prediction for Advances

**Question:** "Predict customer cash flow patterns for advance eligibility. Show me customers with stable vs. variable income, predict next month's direct deposit amounts, identify seasonal patterns, and recommend personalized advance limits. Which customers show improving financial health?"

**Why Complex:**
- Time series prediction
- Income stability analysis
- Seasonality detection
- Personalized limit calculation
- Financial health trending

**Tools Used:** CalculateAdvanceEligibility, Direct deposit analysis, ML predictions

---

## 22. Compliance Risk Scoring

**Question:** "Create comprehensive compliance risk scores. Show me customers with highest AML risk, breakdown by risk factors (transaction patterns, geographic exposure, account age), recent OFAC screening results, enhanced due diligence requirements, and search for relevant compliance procedures. Prioritize for review."

**Why Complex:**
- Multi-factor risk scoring
- Regulatory requirement mapping
- Screening result integration
- Procedure retrieval
- Review prioritization

**Tools Used:** CreditRiskAnalyst, ComplianceDocsSearch, External data integration

---

## 23. Product Cannibalization Analysis

**Question:** "Analyze product cannibalization between savings and investment products. When customers open high-yield savings, do checking balances decrease? What's the net impact on total deposits? Which customer segments show highest cannibalization, and how does it affect profitability?"

**Why Complex:**
- Cross-product balance flows
- Net impact calculation
- Segment behavior differences
- Profitability assessment
- Strategic implications

**Tools Used:** CustomerBankingAnalyst, Account balance trending

---

## 24. Support Contact Deflection ROI

**Question:** "Evaluate self-service impact on support costs. Search product knowledge for most-viewed topics, correlate with support ticket reduction, calculate cost savings from deflected contacts, and identify gaps in self-service content. Which features reduce support contacts most effectively?"

**Why Complex:**
- Content effectiveness measurement
- Cost deflection calculation
- Gap analysis
- Feature impact assessment
- ROI quantification

**Tools Used:** ProductKnowledgeSearch, Support interaction analysis

---

## 25. Holistic Customer Health Score

**Question:** "Create a comprehensive customer health score combining multiple signals. Include account balance trends, transaction frequency, direct deposit stability, support contact sentiment, product usage, payment history, and ML predictions. Identify top 100 at-risk customers with specific intervention recommendations."

**Why Complex:**
- Multi-signal integration
- Weighted scoring model
- ML prediction incorporation
- Risk stratification
- Actionable recommendations

**Tools Used:** All semantic views, ML prediction functions, Support sentiment analysis

---

## Testing Best Practices

1. **Start Simple**: Test basic queries before complex ones
2. **Verify Data**: Ensure sample data is fully loaded
3. **Check Permissions**: Confirm all tools are accessible
4. **Monitor Performance**: Note query execution times
5. **Validate Results**: Cross-check aggregations
6. **Test Edge Cases**: Try queries with no results
7. **Feature Store**: Verify real-time features are updating
8. **Search Relevance**: Assess Cortex Search result quality

---

**Note**: These questions are designed to test the full capabilities of the Varo Intelligence Agent, including structured data analysis, unstructured search, ML predictions, and Feature Store integration. Adjust complexity based on your specific testing needs.
