# Retrieval Issues and Complexities - Hybrid Two-Phase Strategy

## DuckDB in Banking: Adoption and Alternatives

### Current Banking Industry Usage

**DuckDB Adoption Status:**
- ✅ Used in some financial services for internal analytics
- ✅ Growing adoption for data exploration and ETL
- ⚠️ **Not yet widely adopted by major tier-1 banks in production**
- ⚠️ Relatively new (first stable release 2019)

**Why Banks Hesitate:**
1. **Regulatory Compliance**: Banks prefer proven, audited systems
2. **Support Contracts**: DuckDB is open-source (no enterprise support SLA)
3. **Risk Aversion**: Tier-1 banks move slowly on new technology
4. **Audit Trail**: Need certified tools for regulatory reporting

**What Big Banks Actually Use for Parquet Querying:**

| Tool | Adoption | Use Case | Support |
|------|----------|----------|---------|
| **AWS Athena** | ✅✅✅ Very High | Serverless SQL on S3 Parquet | AWS Enterprise Support |
| **Trino/Presto** | ✅✅✅ Very High | Distributed queries on data lakes | Enterprise vendors (Starburst, Ahana) |
| **Apache Spark** | ✅✅✅ Very High | Batch processing, ML pipelines | Databricks, Cloudera support |
| **Snowflake** | ✅✅ High | Data warehouse with Parquet ingestion | Enterprise support |
| **BigQuery** | ✅✅ High | Google Cloud data warehouse | Google support |
| **DuckDB** | ⚠️ Low-Medium | Internal analytics, prototyping | Community support only |

---

### Recommended Alternatives for Banking

#### Option 1: AWS Athena (Recommended for Banks on AWS)

**What it is:**
- Serverless query service built on Trino/Presto
- Queries Parquet/ORC directly in S3
- Pay-per-query pricing

**Banking Benefits:**
```
✅ AWS Enterprise Support (24/7 SLA)
✅ SOC 2, ISO 27001, PCI-DSS compliant
✅ Integrated with AWS audit tools (CloudTrail)
✅ Used by major financial institutions
✅ No infrastructure to manage
```

**Usage Pattern:**
```python
import boto3

athena = boto3.client('athena')

# Query Parquet directly on S3
query = """
SELECT * 
FROM "archive-db"."leads"
WHERE customer_id = 'CUST-500'
  AND closed_at >= DATE '2021-01-01'
"""

response = athena.start_query_execution(
    QueryString=query,
    QueryExecutionContext={'Database': 'archive-db'},
    ResultConfiguration={'OutputLocation': 's3://query-results/'}
)

# Get results
results = athena.get_query_results(QueryExecutionId=response['QueryExecutionId'])
```

**Performance:**
- Similar to DuckDB (both use Presto/Trino engine)
- Partition pruning supported
- Latency: 5-30 seconds for typical queries

**Cost:**
- $5 per TB scanned
- For 1 GB Parquet: $0.005 per query (negligible)

---

#### Option 2: Trino/Presto on EMR (Enterprise Support Available)

**What it is:**
- Open-source distributed SQL query engine
- Used by Meta, Netflix, Uber, and major banks
- Enterprise support from Starburst Data, Ahana

**Banking Benefits:**
```
✅ Battle-tested at scale (Meta uses for petabyte queries)
✅ Enterprise support contracts available
✅ Certified by financial institutions
✅ On-premises or cloud deployment
```

**Usage Pattern:**
```python
from trino.dbapi import connect
from trino.auth import BasicAuthentication

conn = connect(
    host='trino.company.com',
    port=443,
    user='analytics',
    catalog='minio',
    schema='default',
    http_scheme='https'
)

cursor = conn.cursor()
cursor.execute("""
    SELECT * 
    FROM minio.archive.leads
    WHERE customer_id = 'CUST-500'
""")

rows = cursor.fetchall()
```

**When to Use:**
- Multi-source queries (S3 + PostgreSQL + Kafka)
- Need on-premises deployment
- Want enterprise support SLA

---

#### Option 3: Apache Spark (Most Common in Banks)

**What it is:**
- Distributed processing framework
- Native Parquet support
- Used by majority of large banks

**Banking Benefits:**
```
✅ Industry standard for big data
✅ Databricks, Cloudera offer enterprise support
✅ Proven at scale (JPMorgan, Goldman Sachs use it)
✅ Mature ecosystem
```

**Usage Pattern:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("archive-query").getOrCreate()

# Read Parquet with partition pruning
df = spark.read.parquet("s3://archive/leads/**/customer_id=CUST-500/*.parquet")

# Filter and query
result = df.filter(df.closed_at >= '2021-01-01').collect()
```

**Trade-offs:**
- Slower startup (cluster initialization)
- More complex than DuckDB
- Better for batch processing than interactive queries

---

#### Option 4: Snowflake (Enterprise Data Warehouse)

**What it is:**
- Cloud data warehouse
- Can query external Parquet files

**Banking Benefits:**
```
✅ Used by major banks (Capital One, Western Union)
✅ Enterprise support + compliance
✅ Time-travel, data governance built-in
✅ Zero-copy cloning
```

**Usage Pattern:**
```sql
-- Create external table pointing to S3 Parquet
CREATE EXTERNAL TABLE archive.leads
    WITH LOCATION = 's3://archive/leads/'
    FILE_FORMAT = (TYPE = PARQUET)
    PARTITION BY (customer_id);

-- Query like normal table
SELECT * FROM archive.leads
WHERE customer_id = 'CUST-500';
```

**Trade-offs:**
- Higher cost than S3 queries
- Requires Snowflake license
- Data governance features may be overkill

---

### Comparison: DuckDB vs Banking Alternatives

| Feature | DuckDB | AWS Athena | Trino/Presto | Spark | Snowflake |
|---------|--------|------------|--------------|-------|-----------|
| **Enterprise Support** | ❌ No | ✅ AWS | ✅ Starburst | ✅ Databricks | ✅ Snowflake |
| **Banking Adoption** | ⚠️ Low | ✅✅✅ High | ✅✅✅ High | ✅✅✅ Very High | ✅✅ High |
| **Compliance Certs** | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| **Setup Complexity** | ✅ Simple | ✅ Simple | ⚠️ Medium | ⚠️ Medium | ✅ Simple |
| **Query Performance** | ✅✅ Fast | ✅ Fast | ✅ Fast | ⚠️ Slower | ✅✅ Fast |
| **Cost** | ✅✅ Free | ✅ Low | ⚠️ Medium | ⚠️ Medium | ❌ High |
| **Partition Pruning** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| **Multi-source** | ⚠️ Limited | ⚠️ Limited | ✅✅ Excellent | ✅ Good | ✅ Good |

---

### Recommendation for Banking Use Case

**For Regulated Financial Institutions:**

**Use AWS Athena** (if on AWS) or **Trino with Enterprise Support** (if multi-cloud/on-prem)

**Reasons:**
1. ✅ Enterprise support contracts (required for banks)
2. ✅ Compliance certifications (SOC 2, ISO 27001)
3. ✅ Proven in production by major banks
4. ✅ Similar performance to DuckDB
5. ✅ Audit trail and governance tools

**Migration Path from DuckDB in Design:**

Replace all DuckDB code with Athena:

```python
# Before (DuckDB):
import duckdb
con = duckdb.connect()
result = con.execute("SELECT * FROM 'minio://archive/leads/**/*.parquet'").fetchdf()

# After (AWS Athena):
import boto3
athena = boto3.client('athena')
response = athena.start_query_execution(
    QueryString="SELECT * FROM archive.leads WHERE customer_id = 'CUST-500'",
    QueryExecutionContext={'Database': 'archive-db'},
    ResultConfiguration={'OutputLocation': 's3://query-results/'}
)
results = athena.get_query_results(QueryExecutionId=response['QueryExecutionId'])
```

**No architecture changes needed** - just swap query engine!

---

## Updated Issues Without DuckDB

### Issue 1: Batch File Dilution Problem

**Problem:** Multiple customers in same Parquet batch file

**Scenario:**
```
Phase 2 runs monthly. In one month:
- Customer CUST-500 has 10 leads eligible for cold migration
- Customer CUST-501 has 5 leads eligible
- Customer CUST-502 has 8 leads eligible

Current design batches BY customer, but what if we batch ACROSS customers?
```

**If batched across customers:**
```
batch_2024-06-01.parquet contains:
  - 10 leads for CUST-500
  - 5 leads for CUST-501
  - 8 leads for CUST-502
```

**Retrieval Impact:**
```python
# User wants CUST-500's leads
# Must read entire batch file (23 leads) to get 10 leads
# Wasted I/O: 13 leads (56%)

# With 100 customers per batch file:
# Must read 1000 leads to get 10 leads for one customer
# Wasted I/O: 99%
```

**Solution (Current Design):**
```
One batch file PER customer (not across customers)

batch_2024-06-01_CUST-500.parquet (10 leads, all for CUST-500)
batch_2024-06-01_CUST-501.parquet (5 leads, all for CUST-501)
```

**Status:** ✅ Already addressed in design via customer partitioning

---

### Issue 2: Partial Batch Reading Inefficiency

**Problem:** Reading Parquet batches when only need 1-2 leads

**Scenario:**
```
batch_2024-06-01_CUST-500.parquet contains 85 leads

User queries: "Show me LEAD-1001 (which is in this batch)"

Must read entire 85-lead Parquet file to get 1 lead
Wasted I/O: 84 leads (98.8%)
```

**Why this happens:**
- Parquet is columnar (optimized for scanning columns, not filtering rows)
- No row-level indexing within Parquet
- Must read entire row group to filter

**Mitigation 1: Row Group Size Optimization**
```python
# Configure smaller row groups
df.to_parquet(
    path,
    row_group_size=10,  # Only 10 leads per row group
    # Now reading 1 lead = reading 10-lead row group (90% vs 98.8% waste)
)
```

**Mitigation 2: Staged Retrieval (Already in Design)**
```python
# For specific lead queries, load to staging first
if query_type == 'specific_lead':
    load_to_staging(lead_id)  # One-time cost
    # Subsequent queries are fast (staging has indexes)
```

**Trade-off:**
- Smaller row groups = worse compression
- Larger row groups = more wasted I/O on point queries

**Recommendation:** Keep batches customer-focused (current design), use staging for point queries

---

### Issue 3: Cross-Customer Query Performance

**Problem:** Queries spanning multiple customers are slow

**Scenario:**
```sql
-- Query: "All leads closed in 2023 for product type X"
-- Affects 1000 customers

Current design:
  minio://archive/leads/year=2023/
    customer_id=CUST-500/batch.parquet
    customer_id=CUST-501/batch.parquet
    ... (1000 files)

DuckDB must:
  - Open 1000 Parquet files
  - Read each file
  - Filter by product_type
  - Union results

Time: 30-60 seconds (acceptable for analytics)
```

**But what if:**
```sql
-- Query: "Show me lead LEAD-1001"
-- Don't know which customer it belongs to!

Must either:
  1. Scan manifest first (find customer_id) ← Extra DB query
  2. Scan ALL Parquet files ← Very slow
```

**Current Design Assumes:** Queries always include `customer_id`

**Problem:** Not all queries have customer context

**Examples of non-customer queries:**
```sql
-- Lead-centric (no customer known)
SELECT * FROM archive.leads WHERE lead_id = 'LEAD-1001';

-- Agent-centric (no customer known)
SELECT * FROM archive.leads WHERE agent_name = 'John Doe';

-- Product-centric (no customer known)
SELECT * FROM archive.leads WHERE product_type_name = 'Personal Loan';

-- Date-centric (no customer known)
SELECT * FROM archive.leads WHERE closed_at = '2023-06-15';
```

**Solution Options:**

**Option A: Always Query Manifest First (Recommended)**
```python
def get_lead(lead_id):
    # Step 1: Get customer_id from manifest
    manifest = primary_db.query("""
        SELECT customer_id, tier, cold_archive_location
        FROM archival_manifest
        WHERE archived_entity_id = ?
    """, lead_id)
    
    if manifest['tier'] == 'warm':
        return query_warm(lead_id)
    
    # Step 2: Use customer partition
    customer_id = manifest['customer_id']
    return query_cold_customer_partition(customer_id, lead_id)

# Additional query: 5-10ms (manifest lookup)
# But enables partition pruning (saves 60x on cold scan)
```

**Option B: Dual Partitioning (Complex)**
```
Partition by BOTH customer_id AND lead_id prefix:

minio://archive/leads/
  year=2024/month=06/
    customer_id=CUST-500/
      lead_prefix=LEAD-10/
        batch.parquet (LEAD-1000 to LEAD-1099)
      lead_prefix=LEAD-11/
        batch.parquet (LEAD-1100 to LEAD-1199)
```

**Complexity:** Much harder to manage, minimal benefit

**Recommendation:** Option A (manifest lookup first)

---

### Issue 4: Manifest as Single Point of Failure

**Problem:** All cold queries depend on manifest being accurate

**What if manifest is wrong?**

**Scenario 1: Manifest Out of Sync**
```
Manifest says: LEAD-1001 → customer_id=CUST-500, tier=cold
Reality: Parquet file corrupted/deleted, data lost

User queries LEAD-1001:
  1. Manifest returns customer_id=CUST-500
  2. Query partition customer_id=CUST-500/
  3. File not found or corrupted
  4. Error: "Lead not found" (but manifest says it exists!)
```

**Scenario 2: Manifest Stale During Migration**
```
Phase 2 migration in progress:
  - Parquet written ✓
  - Manifest updated ✓
  - Delete from warm... ❌ CRASH

Result:
  - Manifest says tier=cold
  - Data exists in BOTH warm and cold
  - Which is source of truth?
```

**Mitigations:**

**Mitigation 1: Verification Layer**
```python
def get_lead_with_verification(lead_id):
    manifest = get_manifest(lead_id)
    
    try:
        if manifest['tier'] == 'cold':
            # Try to read from cold
            lead = read_from_cold(manifest['customer_id'], lead_id)
            
            # Verify checksum matches manifest
            if calculate_checksum(lead) != manifest['cold_checksum']:
                raise IntegrityError("Checksum mismatch")
            
            return lead
    
    except (FileNotFoundError, IntegrityError) as e:
        # Fallback: Try warm tier
        log_warning(f"Cold retrieval failed for {lead_id}, trying warm")
        return query_warm(lead_id)
```

**Mitigation 2: Manifest Consistency Checks**
```python
# Daily job
def verify_manifest_integrity():
    """Verify manifest matches actual storage"""
    
    inconsistencies = []
    
    # Sample 1% of cold manifests
    sample = primary_db.query("""
        SELECT archived_entity_id, customer_id, cold_archive_location
        FROM archival_manifest
        WHERE tier = 'cold'
        ORDER BY RANDOM()
        LIMIT 1000
    """)
    
    for entry in sample:
        # Verify file exists
        if not file_exists(entry['cold_archive_location']):
            inconsistencies.append({
                'lead_id': entry['archived_entity_id'],
                'issue': 'file_not_found',
                'path': entry['cold_archive_location']
            })
    
    if inconsistencies:
        alert_ops_team(inconsistencies)
```

---

### Issue 5: Schema Version Mismatch in Batches

**Problem:** One Parquet batch contains multiple schema versions

**Scenario:**
```
Month 1: Schema v3
  - CUST-500 has 50 leads archived (all v3)

Month 12: Schema v4 released (new column added)
  - CUST-500 has 30 more leads archived (all v4)

Month 24: Both batches migrate to cold:
  batch_month1.parquet (50 leads, v3)
  batch_month12.parquet (30 leads, v4)

User queries CUST-500's leads:
  - Reads both files
  - Must union v3 and v4 schemas
  - v3 leads have NULL for new column
```

**Is this a problem?**

**DuckDB handles this automatically:**
```python
# DuckDB schema union (built-in)
df = duckdb.query("""
    SELECT * FROM 'minio://archive/leads/**/customer_id=CUST-500/*.parquet'
""").fetchdf()

# Result:
#   lead_id  | new_column (v4+)
#   LEAD-1   | NULL           (from v3 file)
#   LEAD-51  | 'value'        (from v4 file)
```

**Status:** ✅ Not an issue (DuckDB handles gracefully)

**But watch out for:**
- Breaking schema changes (column renamed)
- Type changes (string → int)
- These require transformation logic

---

### Issue 6: Cold Retrieval Latency for Real-Time Needs

**Problem:** 15-minute staging load is too slow for some use cases

**Scenarios where this breaks:**

**Scenario 1: Customer Support Call**
```
Customer on phone: "I need info about lead LEAD-500"
Agent queries system...
System: "Loading from cold storage, please wait 15 minutes"
Customer: *hangs up* ❌
```

**Scenario 2: API Response Time**
```
External API call: GET /leads/LEAD-500
Expected: < 2 seconds
Actual: 15 minutes (cold load)
API timeout ❌
```

**Solution Options:**

**Option 1: Predictive Pre-Loading**
```python
# Predict which leads will be accessed soon
def predictive_preload():
    """Load likely-to-be-accessed leads to staging proactively"""
    
    candidates = ml_model.predict_access_probability()
    
    for lead_id in candidates:
        if access_probability > 0.7:
            load_to_staging_async(lead_id)
```

**Option 2: Reduce Cold Tier Threshold**
```
Current: 2 years in warm → cold
Alternative: 3-5 years in warm → cold

Trade-off: Higher warm DB cost, but less cold retrieval
```

**Option 3: Fast Path for Critical Leads**
```python
# Mark certain leads as "high-priority" (never move to cold)
ALTER TABLE archive.leads ADD COLUMN priority VARCHAR(20);

# Keep VIP customers in warm tier indefinitely
UPDATE archive.leads 
SET priority = 'high'
WHERE customer_segment = 'VIP';

# Phase 2 skips high-priority leads
```

**Option 4: Synchronous Cold Read (No Staging)**
```python
def get_lead_fast(lead_id):
    """Read directly from Parquet without staging"""
    manifest = get_manifest(lead_id)
    
    if manifest['tier'] == 'cold':
        # Direct read (no staging)
        parquet_path = manifest['cold_archive_location']
        df = pd.read_parquet(parquet_path)
        lead = df[df['lead_id'] == lead_id].to_dict('records')[0]
        return lead
    
# Latency: 2-5 seconds (acceptable for some use cases)
# Trade-off: No subsequent instant access (not cached)
```

---

### Issue 7: Concurrent Access to Same Cold Lead

**Problem:** Multiple users request same cold lead simultaneously

**Scenario:**
```
Time 0:00 - User A requests LEAD-500 (cold)
            → Starts loading to staging (15 min)

Time 0:05 - User B requests LEAD-500 (cold)
            → Starts ANOTHER load to staging (duplicate work!)

Time 0:10 - User C requests LEAD-500 (cold)
            → Third load starts (wasteful!)

Time 0:15 - Three staging loads complete (2 are wasted)
```

**Impact:**
- Wasted compute (3x load for same lead)
- Wasted network (3x download)
- Staging table conflicts (3 concurrent inserts)

**Solution: Load Job Deduplication**
```python
class LoadJobCoordinator:
    def __init__(self):
        self.in_progress = {}  # {lead_id: job_id}
        self.lock = threading.Lock()
    
    def request_load(self, lead_id):
        with self.lock:
            # Check if already loading
            if lead_id in self.in_progress:
                job_id = self.in_progress[lead_id]
                return {
                    'status': 'loading',
                    'job_id': job_id,
                    'message': 'Another user already requested this lead'
                }
            
            # Start new load job
            job_id = start_load_job(lead_id)
            self.in_progress[lead_id] = job_id
            
            return {
                'status': 'started',
                'job_id': job_id,
                'estimated_time': '15 min'
            }
    
    def complete_load(self, lead_id):
        with self.lock:
            if lead_id in self.in_progress:
                del self.in_progress[lead_id]
```

**Notification System:**
```python
# Notify all waiting users when load completes
def on_load_complete(lead_id):
    # Get all users waiting for this lead
    waiters = get_waiting_users(lead_id)
    
    for user in waiters:
        send_notification(user, f"LEAD-{lead_id} is now available")
```

---

### Issue 8: Stale Staging Data

**Problem:** Staging data expires, but user expects it to be there

**Scenario:**
```
Day 0: User loads LEAD-500 to staging (expires in 30 days)
Day 15: User queries LEAD-500 (instant, from staging) ✓
Day 31: Staging expired (auto-deleted)
Day 32: User queries LEAD-500...
        System: "Loading from cold storage... 15 min" 😞
        User: "But I just used it last month!"
```

**User Expectation Mismatch:**
- Users think "I accessed it recently" = it's cached
- System thinks "30 days since LAST access" = expired

**Solution 1: Access-Based Extension**
```python
# Extend expiration on each access
def query_staging(lead_id):
    lead = staging_db.query("SELECT * FROM staging.leads WHERE lead_id = ?", lead_id)
    
    if lead:
        # Extend expiration by 30 days
        staging_db.execute("""
            UPDATE staging.leads
            SET expires_at = NOW() + INTERVAL '30 days'
            WHERE lead_id = ?
        """, lead_id)
    
    return lead
```

**Solution 2: Expiration Warning**
```python
def query_staging_with_warning(lead_id):
    lead = staging_db.query("""
        SELECT *, expires_at 
        FROM staging.leads 
        WHERE lead_id = ?
    """, lead_id)
    
    if lead:
        days_until_expiry = (lead['expires_at'] - NOW()).days
        
        if days_until_expiry < 7:
            return {
                'data': lead,
                'warning': f'This lead expires in {days_until_expiry} days. Access now to keep it cached.'
            }
    
    return lead
```

---

### Issue 9: Query Routing Complexity

**Problem:** System must decide: warm, cold-staged, cold-direct, or DuckDB

**Decision tree is complex:**
```python
def route_query(query, user):
    """Complex routing logic"""
    
    analysis = analyze_query(query)
    
    # Decision factors:
    # 1. Which tier? (hot, warm, cold)
    # 2. Query type? (point, range, aggregation)
    # 3. Result size? (< 10, 10-1000, > 1000)
    # 4. Already staged? (check staging first)
    # 5. User priority? (VIP gets fast path)
    # 6. System load? (DuckDB if staging busy)
    
    if analysis.entity_ids:
        tiers = get_tiers(analysis.entity_ids)
        
        if 'cold' in tiers:
            # Check if already staged
            staged = check_staging(analysis.entity_ids)
            
            if staged:
                return query_staging(analysis.entity_ids)
            
            # Not staged - decide: stage or direct?
            if len(analysis.entity_ids) < 10 and user.priority == 'high':
                return stage_and_query(analysis.entity_ids)
            elif len(analysis.entity_ids) > 100:
                return bulk_staging_db(analysis.entity_ids)
            elif analysis.is_aggregation:
                return duckdb_direct(query)
            else:
                return prompt_user_for_method(query)
    
    # ... many more branches
```

**Complexity Issues:**
- Hard to test (many code paths)
- Hard to debug (why did it choose X over Y?)
- Brittle (one wrong decision = slow query)

**Mitigation: Simplified Decision Tree**
```python
def route_query_simple(query):
    """Simplified routing"""
    
    # Rule 1: Check tier
    tier = get_tier(query)
    
    if tier == 'hot':
        return query_primary(query)
    
    if tier == 'warm':
        return query_warm(query)
    
    # Tier = cold
    # Rule 2: Check staging first (always)
    if in_staging(query):
        return query_staging(query)
    
    # Rule 3: Aggregation? Use DuckDB
    if is_aggregation(query):
        return duckdb_direct(query)
    
    # Rule 4: Default - stage first
    return stage_and_query(query)
```

---

### Issue 10: Manifest Query Overhead

**Problem:** Every cold query requires manifest lookup

**Overhead per query:**
```
1. User query received
2. Parse query → extract lead_id
3. Query manifest → get customer_id (5-10ms)
4. Query cold storage with customer partition
5. Return results

Extra latency: 5-10ms per query
```

**At scale:**
- 1000 cold queries/day = 10 seconds/day wasted on manifest
- Manifest DB becomes bottleneck

**Solution: Manifest Caching**
```python
class ManifestCache:
    def __init__(self):
        self.cache = {}  # {lead_id: manifest_entry}
        self.ttl = 3600  # 1 hour
    
    def get_manifest(self, lead_id):
        # Check cache first
        if lead_id in self.cache:
            entry = self.cache[lead_id]
            if not entry.is_expired():
                return entry.data
        
        # Cache miss - query DB
        manifest = primary_db.query("""
            SELECT customer_id, tier, cold_archive_location
            FROM archival_manifest
            WHERE archived_entity_id = ?
        """, lead_id)
        
        # Cache result
        self.cache[lead_id] = CacheEntry(manifest, ttl=self.ttl)
        
        return manifest

# Reduces manifest query from every request to once per hour
```

---

## Summary of Issues

| Issue | Severity | Mitigation | Status |
|-------|----------|------------|--------|
| **1. Batch dilution** | High | Customer-specific batches | ✅ Already addressed |
| **2. Partial batch reading** | Medium | Use staging for point queries | ✅ Design includes this |
| **3. Cross-customer queries** | High | Manifest lookup first | ⚠️ Needs implementation |
| **4. Manifest sync issues** | Critical | Verification + fallback | ⚠️ Needs implementation |
| **5. Schema version mix** | Low | DuckDB handles automatically | ✅ Not an issue |
| **6. Cold latency for real-time** | High | Predictive pre-load or reduce threshold | ⚠️ Design decision needed |
| **7. Concurrent load** | Medium | Job deduplication | ⚠️ Needs implementation |
| **8. Stale staging** | Low | Access-based extension | ⚠️ Needs implementation |
| **9. Routing complexity** | Medium | Simplify decision tree | ⚠️ Needs implementation |
| **10. Manifest overhead** | Low | Caching layer | ⚠️ Needs implementation |

---

## Recommended Immediate Actions

### 1. **Implement Manifest-First Lookup (Critical)**
```python
# Always query manifest before cold storage
def get_cold_lead(lead_id):
    manifest = get_manifest(lead_id)  # Gets customer_id
    return query_customer_partition(manifest['customer_id'], lead_id)
```

### 2. **Add Load Job Deduplication (High Priority)**
```python
# Prevent duplicate concurrent loads
coordinator = LoadJobCoordinator()
```

### 3. **Build Manifest Verification Job (High Priority)**
```python
# Daily: verify manifest matches actual storage
verify_manifest_integrity()
```

### 4. **Add Manifest Caching (Medium Priority)**
```python
# Reduce manifest query overhead
manifest_cache = ManifestCache(ttl=3600)
```

### 5. **Decide on Cold Latency Strategy (Design Decision)**
- Option A: Accept 15-min latency for most cases
- Option B: Predictive pre-loading for likely accesses
- Option C: Keep more data in warm tier (increase threshold)

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-25  
**Purpose:** Identify and address retrieval complexities
