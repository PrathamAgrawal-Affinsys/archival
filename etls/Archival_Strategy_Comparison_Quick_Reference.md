# Archival Strategy Quick Reference Guide

## Two Approaches at a Glance

### 1. Hot → Warm → Cold (Traditional Two-Phase)

```
┌──────────┐     ┌─────────────────┐     ┌──────────┐
│   Hot    │ ──> │  Warm (2 years) │ ──> │   Cold   │
│ Postgres │     │  Permanent DB   │     │ Parquet  │
└──────────┘     └─────────────────┘     └──────────┘
                         ↑
                   Always queryable
                   $200/month cost
```

### 2. Warm-on-Demand (Proposed Alternative)

```
┌──────────┐                    ┌──────────┐
│   Hot    │ ───────────────> │   Cold   │
│ Postgres │                    │ Parquet  │
└──────────┘                    └────┬─────┘
                                     │
                          On retrieval request
                                     ↓
                             ┌───────────────┐
                             │ Warm Staging  │
                             │ Temporary DB  │
                             │ 30-day TTL    │
                             └───────────────┘
```

---

## Side-by-Side Comparison

| Feature | Hot→Warm→Cold | Warm-on-Demand |
|---------|---------------|----------------|
| **Cost (Low Retrieval)** | $17,400 / 7 years | $1,680 / 7 years ✅ |
| **Cost (High Retrieval)** | $17,400 / 7 years ✅ | $28,000 / 7 years |
| **Operational Complexity** | High (2 jobs, migrations) | Low (1 job, ephemeral) ✅ |
| **First Access Latency** | Instant ✅ | 15 min - 2 hours |
| **Subsequent Access** | Instant ✅ | Instant (if < 30 days) ✅ |
| **Schema Evolution** | Complex (multi-version DB) | Simple (transform on load) ✅ |
| **Scales to Zero** | No | Yes ✅ |
| **Best For** | High retrieval (>100/mo) | Low retrieval (<50/mo) |
| **Regulatory Compliance** | ✅ Easily accessible | ⚠️ Depends on interpretation |

---

## Decision Tree

```
START: Do you need to archive data?
    │
    ├─ Are you a regulated financial institution?
    │   ├─ Yes → Does "easily accessible" mean instant?
    │   │   ├─ Yes → Use Hot→Warm→Cold
    │   │   └─ No (24hr OK) → Continue
    │   └─ No → Continue
    │
    ├─ What's your expected retrieval volume?
    │   ├─ > 100 requests/month → Use Hot→Warm→Cold
    │   ├─ 50-100 requests/month → Test both, measure
    │   └─ < 50 requests/month → Continue
    │
    ├─ How often does your schema change?
    │   ├─ > 2 times/year → Warm-on-Demand ✅
    │   └─ < 1 time/year → Either works
    │
    ├─ What's your budget?
    │   ├─ Constrained → Warm-on-Demand ✅
    │   └─ Flexible → Either works
    │
    └─ RECOMMENDATION: Start with Warm-on-Demand
       Monitor for 3-6 months, migrate if needed
```

---

## Cost Break-Even Analysis

```
Monthly Unique Leads Loaded

    0  ────────────────────────────────────
              Warm-on-Demand: $5/month
              (Empty staging, cold storage only)
    
   50  ────────────────────────────────────
              Warm-on-Demand: $105/month
              Hot→Warm→Cold: $200/month
    
  100  ════════════════════════════════════  ← BREAK-EVEN POINT
              Both cost ~$200/month
    
  150  ────────────────────────────────────
              Warm-on-Demand: $305/month
              Hot→Warm→Cold: $200/month
    
  200+ ────────────────────────────────────
              Hot→Warm→Cold is cheaper
```

**Rule of Thumb:**
- **< 100 loads/month:** Warm-on-Demand saves money
- **> 100 loads/month:** Permanent warm tier is cheaper

---

## Retrieval Latency Comparison

### Scenario: Query Archived Lead

**Hot → Warm → Cold:**
```
Request → Check Hot DB → Not found
       → Check Warm DB → FOUND
       → Return data
       
Latency: < 100ms ✅
```

**Warm-on-Demand (First Access):**
```
Request → Check Hot DB → Not found
       → Check Manifest → tier='cold'
       → Check Staging → Not found
       → Trigger Load Job
           ├─ Download Parquet from MinIO (10 min)
           ├─ Transform schema (2 min)
           └─ Insert to staging (3 min)
       → Notify user
       → User queries again → FOUND in staging
       
Latency: 15 minutes ⚠️
```

**Warm-on-Demand (Second Access, same lead):**
```
Request → Check Hot DB → Not found
       → Check Staging → FOUND
       → Return data
       
Latency: < 100ms ✅
```

---

## Schema Evolution Comparison

### Scenario: Add New Column

**Hot → Warm → Cold:**
```
1. ALTER TABLE warm_db.leads ADD COLUMN new_field VARCHAR(100);
   - Touches 12,000 rows (2 years of data)
   - Takes 2-4 hours
   - Requires downtime or online migration

2. Update archival job to include new column

3. Now warm DB has:
   - 12,000 old rows with new_field = NULL
   - New rows with new_field populated
   
4. Queries need: COALESCE(new_field, 'default_value')
```

**Warm-on-Demand:**
```
1. Update archival job to include new column in Parquet

2. Update transformation logic:
   transform_old_to_new() {
       if 'new_field' not in df:
           df['new_field'] = NULL  # Or inferred value
   }

3. Update staging schema with new column

4. Done. Old Parquets unchanged, new ones have the field.
   On load: transformation handles the difference.
   
No ALTER TABLE, no migration, no downtime.
```

---

## When to Use Each Approach

### Use Warm-on-Demand If:

✅ Retrieval volume < 50 requests/month  
✅ Budget-constrained  
✅ Schema changes frequently  
✅ Small engineering team  
✅ Internal/non-customer-facing  
✅ First access latency acceptable  

**Example:** Internal compliance archival, rarely accessed

---

### Use Hot→Warm→Cold If:

✅ Retrieval volume > 100 requests/month  
✅ Customer-facing APIs  
✅ Regulatory requirement for instant access  
✅ Mature engineering org  
✅ Schema is stable  
✅ SLAs demand sub-second response  

**Example:** Customer service portal, daily lookups

---

## Migration Strategy

### Start Small, Scale If Needed

```
Month 0: Deploy Warm-on-Demand
         - Low initial cost ($5-10/month)
         - Simple to maintain
         
Month 1-6: Monitor Metrics
           - Retrieval volume
           - User complaints
           - Staging occupancy
           
Month 6: Decision Point
         ├─ Retrieval < 50/mo → Continue warm-on-demand
         ├─ Retrieval > 100/mo → Migrate to permanent warm
         └─ High user complaints → Migrate to permanent warm
```

**Advantage:** Make decision based on real data, not assumptions.

---

## Key Metrics to Monitor

### For Warm-on-Demand Success:

```
✓ Monthly unique leads loaded
  Target: < 100 (if higher, consider permanent warm)

✓ Average staging occupancy
  Target: < 30% (if higher, not scaling to zero)

✓ User complaints about latency
  Target: < 5/month (if higher, UX unacceptable)

✓ Cache hit rate (lead already in staging)
  Target: > 30% (if lower, staging TTL too short)

✓ Cost per month
  Target: < $100 (if higher, compare to permanent warm)
```

---

## Common Pitfalls

### Warm-on-Demand

❌ **Underestimating retrieval volume**  
   → Monitor for 3-6 months before committing

❌ **Not handling bulk queries**  
   → Deploy DuckDB or bulk staging DB

❌ **Staging TTL too short**  
   → 30 days balances cache hit rate vs cost

❌ **Poor user communication**  
   → Clearly explain: "First access = 15 min, then instant"

### Hot→Warm→Cold

❌ **Over-engineering for low retrieval**  
   → Warm tier empty 90% of time but costs $200/month

❌ **Ignoring schema migration complexity**  
   → Every schema change = hours of ALTER TABLE

❌ **Not planning capacity**  
   → Warm tier grows indefinitely, requires scaling

---

## Summary

**Warm-on-Demand is ideal for:**
- Projects with uncertain retrieval patterns
- Budget-conscious teams
- Rapidly evolving schemas
- Internal tooling

**Start here unless you have clear evidence of high retrieval volume.**

**Hot→Warm→Cold is ideal for:**
- Production systems with proven high retrieval
- Customer-facing applications
- Strict regulatory instant-access requirements
- Teams with resources for operational complexity

**Migrate here once data justifies the cost.**

---

## Quick Links

- **Full Warm-on-Demand Documentation:** `Warm_On_Demand_Archival_Strategy.md`
- **Original Two-Phase Design:** `Archival_Design_Document.md`

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-24  
**Purpose:** Quick reference for decision-making
