# Archival Strategy Comprehensive Comparison

**Decision Guide: Two-Phase vs Warm-on-Demand**

---

## Quick Decision Matrix

| Your Situation | Recommended Approach | Confidence |
|----------------|---------------------|------------|
| **Customer-facing APIs** | Two-Phase | High |
| **Internal tooling with low usage** | Warm-on-Demand | High |
| **Regulated financial institution** | Two-Phase | Medium (check compliance interpretation) |
| **Startup/budget-constrained** | Warm-on-Demand | High |
| **> 100 archive queries/month** | Two-Phase | High |
| **< 50 archive queries/month** | Warm-on-Demand | High |
| **Schema changes > 2/year** | Warm-on-Demand | Medium |
| **Need instant access (< 1 sec)** | Two-Phase | High |
| **Can wait 15 min for first access** | Warm-on-Demand | High |
| **Mature data platform team** | Two-Phase | Medium |
| **Small engineering team** | Warm-on-Demand | High |

---

## Architecture Comparison

### Two-Phase (Hot → Warm → Cold)

```
Primary DB (0-3 months)
    ↓ Phase 1 (Weekly)
Warm Archive DB (3 months - 2 years) ← Always queryable, $200/month
    ↓ Phase 2 (Monthly)
Cold Storage (2+ years) ← Request-based retrieval

Retrieval:
- Hot: < 100ms
- Warm: < 200ms (instant SQL)
- Cold: 2-4 hours (request-based)
```

### Warm-on-Demand (Hot → Cold + Ephemeral Staging)

```
Primary DB (0-3 months)
    ↓ Single Phase (Weekly)
Cold Storage (3 months - 7 years) ← Immutable Parquet
    ↓ On-demand (15 min load)
Warm Staging (Temporary, 30-day TTL) ← Only populated when requested

Retrieval:
- Hot: < 100ms
- Cold (first access): 15 min - 2 hours
- Staging (subsequent access < 30 days): < 100ms
```

---

## Detailed Feature Comparison

### Retrieval & Access

| Feature | Two-Phase | Warm-on-Demand | Winner |
|---------|-----------|----------------|--------|
| **First access latency** | < 200ms | 15 min - 2 hours | Two-Phase ✅ |
| **Subsequent access (same lead)** | < 200ms | < 100ms (if < 30 days) | Tie ✅✅ |
| **Bulk queries (1000+ leads)** | < 5 seconds | Minutes (DuckDB) or hours (staging load) | Two-Phase ✅ |
| **Complex SQL (joins, CTEs)** | Full PostgreSQL | Limited (DuckDB) or delayed (staging load) | Two-Phase ✅ |
| **Query federation (hot+warm+cold)** | Built-in | Requires intelligent router | Two-Phase ✅ |
| **Self-service access** | Yes | Yes (after initial load) | Tie ✅✅ |
| **Cold tier retrieval** | Request-based (same) | Request-based (same) | Tie ✅✅ |

**Summary:** Two-Phase wins on instant access. Warm-on-Demand acceptable if latency tolerance exists.

---

### Cost

| Cost Factor | Two-Phase | Warm-on-Demand | Winner |
|-------------|-----------|----------------|--------|
| **Infrastructure (7 years, 1M leads)** | $10,542 | $1,320 | Warm-on-Demand ✅ |
| **Operations (engineering, on-call)** | $77,000 | $14,000 | Warm-on-Demand ✅ |
| **Total (low retrieval < 50/month)** | $87,542 | $15,320 | Warm-on-Demand ✅ (82% savings) |
| **Total (high retrieval > 200/month)** | $87,542 | $120,000 | Two-Phase ✅ |
| **Scales to zero** | No (fixed $200/month) | Yes ($5/month base) | Warm-on-Demand ✅ |
| **Cost predictability** | High (fixed) | Variable (usage-based) | Two-Phase ✅ |

**Summary:** Warm-on-Demand wins for low/medium retrieval. Two-Phase wins for high retrieval.

---

### Schema Evolution

| Aspect | Two-Phase | Warm-on-Demand | Winner |
|--------|-----------|----------------|--------|
| **Migration complexity** | High (multi-version DB) | Low (transform on load) | Warm-on-Demand ✅ |
| **ALTER TABLE impact** | Touches thousands of rows | Not needed | Warm-on-Demand ✅ |
| **Downtime for schema changes** | Minutes to hours | Zero (ephemeral staging) | Warm-on-Demand ✅ |
| **Query version awareness** | Required | Not required | Warm-on-Demand ✅ |
| **Breaking changes** | Complex (shadow columns) | Simple (update transformer) | Warm-on-Demand ✅ |
| **Data immutability** | Compromised (backfills) | Preserved (cold never changes) | Warm-on-Demand ✅ |

**Summary:** Warm-on-Demand dominates schema evolution. Two-Phase requires careful coordination.

---

### Operational Complexity

| Aspect | Two-Phase | Warm-on-Demand | Winner |
|--------|-----------|----------------|--------|
| **Number of archival jobs** | 2 (hot→warm, warm→cold) | 1 (hot→cold) | Warm-on-Demand ✅ |
| **Backup requirements** | Warm DB + cold | Cold only | Warm-on-Demand ✅ |
| **Disaster recovery** | Complex (DB restore + manifest sync) | Simple (reload from cold) | Warm-on-Demand ✅ |
| **Monitoring surface** | Warm DB + 2 jobs | Staging cleanup + 1 job | Warm-on-Demand ✅ |
| **On-call burden** | Medium (DB outages affect users) | Low (staging is disposable) | Warm-on-Demand ✅ |
| **Capacity planning** | Required (DB grows) | Minimal (staging capped) | Warm-on-Demand ✅ |
| **Failure modes** | 3 (phase1, phase2, DB issues) | 2 (archival, cleanup) | Warm-on-Demand ✅ |

**Summary:** Warm-on-Demand significantly simpler operationally.

---

### Compliance & Audit

| Requirement | Two-Phase | Warm-on-Demand | Winner |
|-------------|-----------|----------------|--------|
| **"Easily accessible" (SEC 17a-4)** | ✅ Yes (instant SQL) | ⚠️ Depends (first access = 15 min) | Two-Phase ✅ |
| **Immutability** | ⚠️ Warm tier mutable (migrations) | ✅ Cold tier immutable | Warm-on-Demand ✅ |
| **Audit trail** | ✅ Yes | ✅ Yes | Tie ✅✅ |
| **Point-in-time consistency** | ✅ Yes | ⚠️ Transformations may infer values | Two-Phase ✅ |
| **WORM compliance** | Requires separate config | Native (Parquet immutable) | Warm-on-Demand ✅ |
| **Retention period** | ✅ 7 years | ✅ 7 years | Tie ✅✅ |

**Summary:** Two-Phase better for strict "instant access" requirements. Warm-on-Demand better for immutability.

---

### Scalability

| Aspect | Two-Phase | Warm-on-Demand | Winner |
|--------|-----------|----------------|--------|
| **Handles low usage** | Poor (wastes money) | Excellent (scales to $5/month) | Warm-on-Demand ✅ |
| **Handles high usage** | Excellent (fixed cost) | Poor (retrieval costs explode) | Two-Phase ✅ |
| **Variable workload** | Poor (can't scale down) | Excellent (auto-scales) | Warm-on-Demand ✅ |
| **Growth trajectory** | Predictable (DB size linear) | Unpredictable (depends on retrieval) | Two-Phase ✅ |
| **Burst capacity** | Limited (DB size fixed) | High (can load many to staging) | Warm-on-Demand ✅ |

**Summary:** Warm-on-Demand better for variable/low workloads. Two-Phase better for consistent high workloads.

---

## Use Case Recommendations

### Use Case 1: Customer Service Portal

**Context:**
- 500 customer service agents
- Query archived leads daily (200+ queries/day)
- Need instant access (< 1 second SLA)
- Customer-facing application

**Recommendation: Two-Phase ✅**

**Reasoning:**
- High retrieval volume justifies permanent warm tier
- Instant access required (customers on phone)
- Cost: 200 queries/day × 30 days = 6,000 queries/month
  - Two-Phase: $200/month (fixed)
  - Warm-on-Demand: $200 base + ~$5,000 retrieval = $5,200/month ❌

---

### Use Case 2: Internal Compliance Archival

**Context:**
- Rarely accessed (< 10 queries/month)
- Only during audits or investigations
- Can tolerate 2-hour retrieval
- Budget-constrained

**Recommendation: Warm-on-Demand ✅**

**Reasoning:**
- Low retrieval volume
- Cost: 10 queries/month
  - Two-Phase: $200/month (wasted 90% of capacity)
  - Warm-on-Demand: $5 base + $50 retrieval = $55/month ✅
- Savings: $145/month × 12 = $1,740/year

---

### Use Case 3: Regulatory Banking Application

**Context:**
- SEC requires "easily accessible" archives
- Auditors need self-service SQL access
- 50-100 queries/month (audits, compliance checks)
- Well-funded organization

**Recommendation: Two-Phase ✅**

**Reasoning:**
- Regulatory compliance risk > cost savings
- "Easily accessible" = instant (safest interpretation)
- Mid-range retrieval volume (break-even zone)
- Organization can afford operational complexity

---

### Use Case 4: Startup SaaS Product

**Context:**
- Early stage, limited budget
- Archive access infrequent (5-20 queries/month)
- Small engineering team (2-3 people)
- Schema changes frequently (rapid iteration)

**Recommendation: Warm-on-Demand ✅**

**Reasoning:**
- Budget constraints (can't afford $200/month fixed)
- Schema evolution simplicity critical (small team)
- Low retrieval volume
- Can always migrate to two-phase if usage grows

---

### Use Case 5: Analytics & BI Platform

**Context:**
- Data analysts run complex queries
- Need to join across years of data
- 100-200 queries/month
- Queries are often exploratory (ad-hoc)

**Recommendation: Two-Phase ✅**

**Reasoning:**
- Complex SQL requirements (joins, window functions)
- High query volume
- Exploratory queries benefit from instant feedback
- Cost: $200/month justified by analyst productivity

---

## Migration Paths

### Path 1: Start Warm-on-Demand, Upgrade to Two-Phase

**When:** Retrieval volume grows beyond 100/month

**Steps:**
1. **Month 0-6:** Run warm-on-demand, monitor metrics
2. **Month 6:** Review data
   - If retrieval > 100/month → Plan migration
   - If staging occupancy > 70% → Plan migration
3. **Month 7-8:** Provision warm archive DB
4. **Month 9:** Backfill cold storage → warm DB
5. **Month 10:** Cutover queries to warm DB
6. **Month 11:** Decommission staging, deploy Phase 2 job

**Cost:** Minimal (only migrate if usage justifies)

---

### Path 2: Start Two-Phase, Downgrade to Warm-on-Demand

**When:** Retrieval volume is lower than expected

**Steps:**
1. **Month 0-12:** Run two-phase, monitor metrics
2. **Month 12:** Review data
   - If retrieval < 50/month → Plan downgrade
   - If warm DB mostly idle → Plan downgrade
3. **Month 13-14:** Deploy warm-on-demand staging
4. **Month 15:** Export warm DB → cold storage
5. **Month 16:** Cutover queries to staging
6. **Month 17:** Decommission warm DB

**Savings:** $150+/month

---

## Cost Break-Even Analysis

### Break-Even Chart

```
Monthly Cost

$5000 |                                         ╱ Warm-on-Demand
      |                                    ╱
$4000 |                               ╱
      |                          ╱
$3000 |                     ╱
      |                ╱
$2000 |           ╱
      |      ╱
$1000 | ╱_____________________________________________ Two-Phase (fixed $200)
      |
    0 +----+----+----+----+----+----+----+----+----+----
      0   50  100  150  200  250  300  350  400  450  500
                    Monthly Unique Leads Retrieved

Break-Even: ~100 leads/month
```

### Detailed Break-Even Table

| Monthly Retrieval | Two-Phase Cost | Warm-on-Demand Cost | Cheaper Option | Savings |
|-------------------|----------------|---------------------|----------------|---------|
| 10 leads | $200 | $60 | Warm-on-Demand | $140 (70%) |
| 25 leads | $200 | $85 | Warm-on-Demand | $115 (58%) |
| 50 leads | $200 | $110 | Warm-on-Demand | $90 (45%) |
| 75 leads | $200 | $160 | Warm-on-Demand | $40 (20%) |
| **100 leads** | **$200** | **$210** | **Two-Phase** | **$10 (5%)** |
| 150 leads | $200 | $310 | Two-Phase | $110 (35%) |
| 200 leads | $200 | $410 | Two-Phase | $210 (51%) |
| 300 leads | $200 | $610 | Two-Phase | $410 (67%) |

**Assumptions:**
- Two-Phase: $200/month fixed
- Warm-on-Demand: $10 base + $2/lead retrieved

---

## Risk Assessment

### Two-Phase Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| **Warm DB outage** | Low | High (users blocked) | Read replicas, automated failover |
| **Schema migration failure** | Medium | High (data corruption) | Test in staging, rollback plan |
| **Phase 2 job failures** | Medium | Medium (warm DB bloat) | Alerting, manual intervention |
| **Over-provisioning** | High | Low (wasted cost) | Monitor usage, downgrade if low |
| **Cost overrun** | Low | Low (fixed cost) | Predictable, budgetable |

---

### Warm-on-Demand Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| **High latency UX** | High | Medium (user frustration) | Clear messaging, predictive pre-loading |
| **Retrieval cost explosion** | Medium | High (budget overrun) | Usage caps, approval workflows |
| **Staging capacity exceeded** | Low | Medium (failed loads) | Hard cap, LRU eviction |
| **Transformation bugs** | Medium | Medium (incorrect data) | Extensive testing, validation |
| **Cold storage data loss** | Low | Critical (data loss) | Versioning, cross-region replication |

---

## Final Recommendation Framework

### Decision Tree

```
START

│
├─ Is instant access a hard requirement?
│  ├─ Yes → Two-Phase
│  └─ No → Continue
│
├─ Is this customer-facing?
│  ├─ Yes → Two-Phase
│  └─ No → Continue
│
├─ Expected retrieval > 100/month?
│  ├─ Yes → Two-Phase
│  ├─ No → Continue
│  └─ Unknown → Continue (start small)
│
├─ Budget constrained?
│  ├─ Yes → Warm-on-Demand
│  └─ No → Continue
│
├─ Schema changes > 2/year?
│  ├─ Yes → Warm-on-Demand
│  └─ No → Continue
│
├─ Small engineering team?
│  ├─ Yes → Warm-on-Demand
│  └─ No → Either works
│
└─ DEFAULT: Start with Warm-on-Demand
   Monitor for 6 months, migrate if needed
```

---

### Recommendation by Organization Type

| Organization Type | Recommendation | Reasoning |
|-------------------|----------------|-----------|
| **Enterprise Bank** | Two-Phase | Regulatory compliance, high usage, resources available |
| **Fintech Startup** | Warm-on-Demand | Budget, agility, uncertain usage patterns |
| **SaaS Company** | Warm-on-Demand | Variable usage, cost optimization |
| **Healthcare Provider** | Two-Phase | HIPAA compliance, instant access for patient care |
| **E-commerce** | Warm-on-Demand | Seasonal variability, cost-conscious |
| **Consulting Firm** | Warm-on-Demand | Project-based usage spikes |
| **Government Agency** | Two-Phase | Audit requirements, predictable budget |

---

## Conclusion

**There is no one-size-fits-all answer.**

**Choose Two-Phase if:**
- You need instant access (< 1 second)
- Retrieval volume is high and consistent (> 100/month)
- Budget allows for operational complexity
- Regulatory compliance demands instant SQL access

**Choose Warm-on-Demand if:**
- Retrieval volume is low or variable (< 50/month)
- Budget constrained
- Schema evolves frequently
- Small engineering team
- Can tolerate 15-minute first-access latency

**Best Approach:**
**Start with Warm-on-Demand** unless you have clear evidence of high retrieval volume. Monitor metrics for 3-6 months. Migrate to Two-Phase if data justifies the cost and complexity.

This gives you:
- Lowest initial investment
- Real usage data before committing
- Flexibility to change course
- Clear ROI for infrastructure decisions

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-24  
**Purpose:** Comprehensive comparison for decision-making
