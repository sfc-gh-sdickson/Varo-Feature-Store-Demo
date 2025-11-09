# Snowflake Query Optimization Notes

## Key Differences from Traditional Databases

### No Traditional Indexes in Snowflake

Snowflake does NOT support CREATE INDEX syntax on regular tables. Instead, it uses:

1. **Micro-partitions**: Automatic data organization
2. **Clustering Keys**: Explicit optimization hints
3. **Search Optimization**: For point lookups
4. **Query Pruning**: Automatic partition elimination

### Correct Optimization Patterns

#### ❌ INCORRECT (Traditional Database)
```sql
CREATE INDEX idx_customer ON transactions(customer_id);
```

#### ✅ CORRECT (Snowflake)
```sql
-- Option 1: Clustering (best for range queries)
ALTER TABLE transactions CLUSTER BY (customer_id, transaction_date);

-- Option 2: Search Optimization (best for point lookups)
ALTER TABLE transactions ADD SEARCH OPTIMIZATION;
```

### When to Use Each

**Clustering Keys**:
- Range queries (date ranges, numeric ranges)
- Frequently filtered columns
- Join columns
- Columns with moderate cardinality

**Search Optimization**:
- Point lookups (WHERE id = 'specific_value')
- High cardinality columns
- Random access patterns
- Real-time serving tables

### Hybrid Tables Exception

Snowflake DOES support indexes on Hybrid Tables (OLTP-optimized):
```sql
CREATE HYBRID TABLE my_table (...);
CREATE INDEX idx_name ON my_table(column);
```

But Hybrid Tables are for OLTP workloads, not analytics.

### Performance Best Practices

1. **Let Snowflake optimize automatically** - Often no action needed
2. **Use clustering for predictable query patterns**
3. **Monitor clustering health** with SYSTEM$CLUSTERING_INFORMATION()
4. **Add Search Optimization selectively** - It has storage costs
5. **Trust the query optimizer** - It's very sophisticated

### Cost Considerations

- **Clustering**: Minimal cost, automatic maintenance
- **Search Optimization**: Additional storage cost (~10-20%)
- **Micro-partitions**: No cost, automatic

The fix applied to `02_create_tables.sql` follows these best practices by using CLUSTER BY instead of CREATE INDEX.
