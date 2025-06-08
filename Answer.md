# Olist Data Warehouse 
## 1. Requirement Gathering

## Stakeholder Questions and Answers

### Question 1: How should we track changes to seller performance tiers?
**Stakeholder Answer:**  
"Seller tiers are updated monthly based on delivery performance and review scores. We need to:  
- See the current tier for real-time dashboards  
- Analyze historical tier changes to identify trends  
- Audit when and why tiers changed"

**SCD Implication:**  
→ Type 2 SCD (full history) for `dim_sellers`

---

### Question 2: Do product categories ever change? How should we handle this?
**Stakeholder Answer:**  
"Categories are rarely updated, but when they are, it's critical for reporting consistency. We need:  
- Current categories for daily operations  
- Old categories preserved for historical sales analysis"

**SCD Implication:**  
→ Type 1 SCD (overwrite) for corrections  
→ Type 2 SCD for business reclassifications

---

### Question 3: Should we track customer location changes?
**Stakeholder Answer:**  
"Customer locations are stable, but when they change, it impacts regional sales analysis. We need:  
- Current locations for delivery logistics  
- Historical locations for accurate past sales attribution"

**SCD Implication:**  
→ Type 2 SCD for `dim_customers`

---

### Question 4: How should payment type definitions be handled?
**Stakeholder Answer:**  
"Payment types are static, but new methods may be added. We just need the current list."

**SCD Implication:**  
→ Type 0 SCD (immutable) for `dim_payment_type`

---

### Question 5: Do you need to track order status changes with timestamps?
**Stakeholder Answer:**  
"Yes! We need to measure time spent in each status to optimize logistics."

**SCD Implication:**  
→ Accumulating Snapshot Fact Table for `fact_order_fulfillment`

---

## SCD Strategy Summary

| Dimension          | SCD Type | Reason                                  |
|--------------------|----------|-----------------------------------------|
| `dim_sellers`      | Type 2   | Track tier changes for performance trends |
| `dim_products`     | Type 1/2 | Type 1 for typos, Type 2 for reclassifications |
| `dim_customers`    | Type 2   | Preserve location history               |
| `dim_payment_type` | Type 0   | Static list; no history needed          |

---

## Next Steps

1. **Implement SCD Logic**:
   - Use surrogate keys (e.g., `seller_sk`) for Type 2 dimensions
   - Add `effective_date`/`expiration_date` columns

2. **ETL Pipeline**:
   - Detect changes in source data
   - Insert new rows for Type 2 changes

**Example SQL for Type 2 SCD**:
```sql
INSERT INTO dim_sellers (seller_sk, seller_id, tier, effective_date, expiration_date)
SELECT 
    nextval('seller_sk_seq'), 
    seller_id, 
    'Gold', 
    CURRENT_DATE, 
    '9999-12-31'
FROM staging_sellers
WHERE tier_updated = TRUE;
```


## 2. Slowly Changing Dimension (SCD) 
## 3. ELT with Python & SQL


## 4. Orchestrate ELT with Luigi
