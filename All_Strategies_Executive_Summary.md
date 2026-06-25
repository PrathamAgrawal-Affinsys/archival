# Archival Strategy - Complete Comparison

## Executive Summary: Three Approaches

### 1. Hybrid Two-Phase (Hot → Warm Denormalized → Cold)

**Core Innovation:** Denormalize at archival time for read-optimized queries

```
Primary (Normalized) → Warm (Denormalized, Permanent) → Cold (Parquet)
   0-3 months              3 months - 2 years            2-7 years
   
Warm Tier: Single wide table, JSONB arrays, no JOINs needed
Retrieval: 4 modes (instant SQL, staged, bulk, analytics direct)
```

**Best For:**
- High query volume (> 80/month)
- Complex multi-table relationships
- Users need simple single-table queries
- Query performance critical (10x faster than JOINs)

**Cost:** ~$1,110/month | **Query Speed:** < 10ms | **Complexity:** Medium

---

### 2. Traditional Two-Phase (Hot → Warm Normalized → Cold)

**Core Approach:** Maintain normalized structure in warm tier

```
Primary (Normalized) → Warm (Normalized, Permanent) → Cold (Parquet)
   0-3 months            3 months - 2 years           2-7 years
   
Warm Tier: Multiple tables with FK constraints, preserves schema
Retrieval: Instant SQL with JOINs
```

**Best For:**
- Need FK accuracy to current reference data
- Stable schema (< 1 change/year)
- High query volume with complex JOINs acceptable
- SQL experts as primary users

**Cost:** ~$1,047/month | **Query Speed:** 50-100ms | **Complexity:** High

---

### 3. Warm-on-Demand (Hot → Cold + Ephemeral Staging)

**Core Innovation:** No permanent warm tier, reconstitute on demand

```
Primary (Normalized) → Cold (Parquet, Permanent)
   0-3 months              3 months - 7 years
                              ↓ (on demand)
                         Staging (Temporary, 30-day TTL)
                         
Warm Staging: Only populated when requested, auto-expires
Retrieval: 15 min first access, instant after (if < 30 days)
```

**Best For:**
- Low retrieval volume (< 50/month)
- Budget constrained
- Variable/unpredictable workload
- Can tolerate first-access latency

**Cost:** ~$60/month (low usage) | **Query Speed:** 15 min first, then < 100ms | **Complexity:** Low

---

## Side-by-Side Comparison

| Feature | Hybrid Two-Phase | Traditional Two-Phase | Warm-on-Demand |
|---------|------------------|----------------------|----------------|
| **Architecture** | | | |
| Warm tier structure | Denormalized (wide table) | Normalized (multiple tables) | Ephemeral (on-demand) |
| Warm tier lifespan | Permanent (2 years) | Permanent (2 years) | Temporary (30 days) |
| Cold tier | Parquet (denormalized) | Parquet (denormalized) | Parquet (immutable) |
| **Performance** | | | |
| Warm query speed | < 10ms (no JOINs) | 50-100ms (JOINs) | < 100ms (after load) |
| Cold first access | 15 min - 2 hours | 2-4 hours | 15 min - 2 hours |
| Query complexity | Simple (single table) | Complex (multi-table JOINs) | Simple (after load) |
| **Cost (7 years)** | | | |
| Infrastructure | $10,895 | $9,503 | $1,320 |
| Operations | $57,414 | $77,000 | $14,000 |
| Retrieval | $22,414 | $0 | $25,000 (high usage) |
| **Total** | **$93,309** | **$86,503** | **$40,320** (low usage) |
| **Schema Evolution** | | | |
| Complexity | Low (mostly additive) | High (multi-version DB) | Low (transform on load) |
| Migration effort | Low | High | None (ephemeral) |
| **Data Accuracy** | | | |
| Historical accuracy | ✅ Point-in-time frozen | ❌ Current refs only | ✅ Point-in-time frozen |
| FK to current data | ❌ Frozen at archive time | ✅ Always current | ❌ Frozen at archive time |
| **Operational** | | | |
| Archival complexity | High (denormalization) | Medium (copy) | High (denormalization) |
| Retrieval modes | 4 (multi-modal) | 1 (instant SQL) | 4 (multi-modal) |
| Scales to zero | ❌ No | ❌ No | ✅ Yes |
| On-call burden | Medium | Medium | Low |

---

## Decision Matrix

### By Query Volume

```
Monthly Archive Queries

0-20    ────────────────────────────────────────────
              Warm-on-Demand wins (95% cost savings)
              $60/month vs $1,100/month
              
20-80   ────────────────────────────────────────────
              Close call - depends on latency tolerance
              Warm-on-Demand if 15-min acceptable
              Hybrid if instant required
              
80-200  ────────────────────────────────────────────
              Hybrid wins (break-even point)
              Query simplicity + performance worth cost
              
200+    ────────────────────────────────────────────
              Hybrid or Traditional (either works)
              Choose based on query complexity needs
```

---

### By Data Complexity

**Simple Data (1-3 child tables):**
- Traditional Two-Phase ✅ (JOINs are manageable)
- Hybrid overkill (denormalization not needed)

**Moderate Complexity (4-7 child tables):**
- Hybrid Two-Phase ✅ (simplifies queries significantly)
- Traditional acceptable but slower

**High Complexity (8+ child tables, nested children):**
- Hybrid Two-Phase ✅✅ (essential for query simplicity)
- Traditional becomes painful (10+ table JOINs)

---

### By User Persona

**SQL Experts (Data Engineers, DBAs):**
- Traditional Two-Phase ✅
- Comfortable with JOINs, understand normalized schemas

**Business Analysts (Moderate SQL):**
- Hybrid Two-Phase ✅
- Need simple queries, struggle with complex JOINs

**Non-Technical Users (BI tool users):**
- Hybrid Two-Phase ✅✅
- Single-table queries much easier to understand

---

### By Budget

**Tight Budget (< $100/month for archival):**
- Warm-on-Demand ✅✅
- Only viable option at this price point

**Moderate Budget ($100-500/month):**
- Warm-on-Demand if low usage
- Hybrid if high usage justifies cost

**Flexible Budget (> $500/month):**
- Hybrid or Traditional (choose by needs)
- Cost not a constraint

---

## Use Case Recommendations

### Use Case 1: Startup SaaS Product
**Situation:** 
- 5-20 archive queries/month
- Small team, budget-conscious
- Schema evolves rapidly

**Recommendation:** Warm-on-Demand ✅
- Cost: $60/month (96% savings vs alternatives)
- Can migrate to Hybrid if usage grows

---

### Use Case 2: Customer Service Portal
**Situation:**
- 100+ archive queries/day
- Need instant access (customers on phone)
- Complex data (10+ related tables)

**Recommendation:** Hybrid Two-Phase ✅
- Single-table queries 10x faster
- Simplified for CS agents
- High volume justifies cost

---

### Use Case 3: Regulatory Banking Application
**Situation:**
- 50-80 queries/month (audits, compliance)
- Need current FK accuracy (agent names, teams)
- Stable schema

**Recommendation:** Traditional Two-Phase ✅
- FKs stay current (audit requirement)
- SQL expertise available
- Schema stability reduces migration burden

---

### Use Case 4: Internal Compliance Archival
**Situation:**
- < 10 queries/month (annual audits only)
- Need historical point-in-time accuracy
- Budget constrained

**Recommendation:** Warm-on-Demand ✅
- $60/month base, load only during audits
- Point-in-time accuracy via transformation
- 95% cost savings

---

### Use Case 5: Analytics & BI Platform
**Situation:**
- 150 queries/month
- Mix of point queries and aggregations
- Complex nested data (documents, activities, payments)

**Recommendation:** Hybrid Two-Phase ✅
- JSONB enables flexible nested queries
- GIN indexes accelerate analytics
- Multi-modal retrieval (instant SQL + DuckDB direct)

---

## Migration Paths

### Start Small → Scale Up

```
Month 0: Deploy Warm-on-Demand
         Cost: $60/month
         Risk: Low
         
Month 1-6: Monitor usage
           - Query volume
           - User complaints about latency
           - Query complexity patterns
           
Month 6: Decision point
         ├─ < 50 queries/month → Keep Warm-on-Demand
         ├─ 50-100 queries/month → Test Hybrid in staging
         └─ > 100 queries/month → Migrate to Hybrid
         
Month 9: If migrating to Hybrid:
         - Provision warm archive DB
         - Backfill cold → warm (denormalized)
         - Cutover queries
         - Cost increase: $60 → $1,100/month
         - ROI: 10x faster queries, simpler UX
```

---

### Hybrid → Traditional (Rare)

```
Scenario: Need current FK accuracy, not frozen references

Migration:
1. Provision new warm DB (normalized schema)
2. Re-archive from primary DB (if still available)
3. Or: Un-denormalize from cold Parquet
   - Parse JSONB arrays back into separate tables
   - Restore FK references (reverse mapping)
4. Cutover queries
5. Decommission denormalized warm DB

Duration: 2-4 weeks
Cost: One-time migration effort
```

---

## Quick Selection Guide

### Answer These Questions:

**1. What's your monthly archive query volume?**
- < 20: → Warm-on-Demand
- 20-80: → Depends (see Q2)
- 80-200: → Hybrid
- 200+: → Hybrid or Traditional (see Q3)

**2. Can you tolerate 15-minute first-access latency?**
- Yes: → Warm-on-Demand
- No: → Hybrid or Traditional

**3. How complex is your data model?**
- Simple (1-3 child tables): → Traditional
- Moderate (4-7 child tables): → Hybrid
- Complex (8+ child tables): → Hybrid

**4. Do you need current FK accuracy or historical frozen state?**
- Current (agent names always up-to-date): → Traditional
- Historical (frozen at archive time): → Hybrid or Warm-on-Demand

**5. What's your budget?**
- < $100/month: → Warm-on-Demand (only option)
- $100-500/month: → Depends on volume
- > $500/month: → Any approach works

**6. Who will query the archives?**
- SQL experts: → Traditional (JOINs acceptable)
- Business analysts: → Hybrid (simpler queries)
- Non-technical: → Hybrid (single-table easier)

---

## Final Recommendation

**Default Starting Point: Warm-on-Demand**

Unless you have clear evidence of:
- High query volume (> 80/month), OR
- Instant access requirement (cannot wait 15 min)

**Reason:**
- Lowest risk ($60/month vs $1,100/month)
- Can migrate to Hybrid/Traditional if needed
- Real usage data before committing to expensive infrastructure

**Monitor for 3-6 months:**
- Query volume
- User satisfaction
- Access patterns

**Migrate to Hybrid if:**
- Volume > 80/month, OR
- First-access latency unacceptable, OR
- Query complexity causing user frustration

**Migrate to Traditional if:**
- Need current FK accuracy (not frozen), AND
- Schema is stable (< 1 change/year), AND
- Users are SQL experts

---

## Summary

| Approach | Best For | Cost | Speed | Complexity |
|----------|----------|------|-------|------------|
| **Hybrid Two-Phase** | High volume, complex data, simple queries needed | High | Fastest | Medium |
| **Traditional Two-Phase** | Current FK accuracy, stable schema, SQL experts | High | Fast | High |
| **Warm-on-Demand** | Low volume, budget-conscious, variable workload | Low | Fast (after load) | Low |

**Start with Warm-on-Demand, migrate to Hybrid if usage justifies, choose Traditional only if FK accuracy is critical.**

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-25  
**All Documents:**
- `Hybrid_Two_Phase_Archival_Strategy.md` - Detailed hybrid approach
- `Two_Phase_Archival_Strategy.md` - Traditional normalized warm tier
- `Warm_On_Demand_Archival_Strategy.md` - Cold-first with ephemeral staging
- `Archival_Strategy_Comparison.md` - Detailed comparisons
- This document - Quick decision guide
