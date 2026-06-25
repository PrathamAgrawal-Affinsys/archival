# Two-Phase Archival Strategy (Hot → Warm → Cold)

**PostgreSQL Archival Strategy — Permanent Warm Tier Approach**

| Field | Value |
| :---- | :---- |
| Document Type | Archival Strategy Document |
| Strategy | Two-Phase with Permanent Warm Tier |
| Archive Trigger | Phase 1: 3 months, Phase 2: 2 years |
| Retrieval Model | Instant SQL access via permanent warm database |
| Author | Engineering |
| Status | Design Proposal |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Core Concept](#2-core-concept)
3. [Architecture Overview](#3-architecture-overview)
4. [Phase 1: Hot → Warm Archival](#4-phase-1-hot--warm-archival)
5. [Phase 2: Warm → Cold Archival](#5-phase-2-warm--cold-archival)
6. [Retrieval Strategy](#6-retrieval-strategy)
7. [Schema Evolution Management](#7-schema-evolution-management)
8. [Query Federation](#8-query-federation)
9. [Implementation Details](#9-implementation-details)
10. [Merits and Demerits](#10-merits-and-demerits)
11. [Operational Considerations](#11-operational-considerations)
12. [Cost Analysis](#12-cost-analysis)
13. [Testing Strategy](#13-testing-strategy)

---

## 1. Executive Summary

**The Two-Phase Archival Approach** maintains a permanent **warm tier** database that holds 2 years of archived data in a fully queryable state, before moving older data to cold storage.

### Core Architecture

```
Primary DB (Hot) → External DB (Warm, 2 years) → Object Storage (Cold, 5+ years)
     0-3 months         3 months - 2 years              2+ years
     
     Instant SQL ←──────────────────────────────→ Request-based retrieval
```

### Key Characteristics

**Permanent Warm Tier:**
- Dedicated external PostgreSQL instance
- Always available, always queryable
- Holds 24 months of archived data
- Fully indexed, optimized for queries
- Auto-expires to cold storage after 2 years

**When to Use:**
- High retrieval volume (> 100 requests/month)
- Customer-facing applications requiring instant access
- Regulatory requirement for "easily accessible" data
- Complex queries across multiple archived records
- Mature engineering organization

**Trade-offs:**
- Higher infrastructure cost ($200+/month)
- Schema evolution complexity (multi-version database)
- More operational overhead (backups, migrations, monitoring)
- Cannot scale to zero (fixed cost regardless of usage)

---

## 2. Core Concept

### The Three-Tier Model

```
┌─────────────────────────────────────────────────────────────────┐
│  HOT TIER (Primary PostgreSQL)                                  │
│  - Active, operational data                                     │
│  - Age: 0 - 3 months after closure                             │
│  - Mutable, frequently updated                                  │
│  - Cost: High (production workload)                             │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ Phase 1 Archival (Weekly)
                 │ Trigger: closed_at < NOW() - INTERVAL '3 months'
                 │
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│  WARM TIER (External PostgreSQL - Permanent)                    │
│  - Archived but easily accessible                               │
│  - Age: 3 months - 2 years                                      │
│  - Read-only, fully indexed                                     │
│  - Instant SQL queries via Datalens                             │
│  - Cost: Medium ($200/month)                                    │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ Phase 2 Archival (Monthly)
                 │ Trigger: archived_at < NOW() - INTERVAL '2 years'
                 │
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│  COLD TIER (MinIO Parquet - Immutable)                          │
│  - Long-term compliance storage                                 │
│  - Age: 2 - 7 years                                             │
│  - Immutable, compressed                                        │
│  - Request-based retrieval only                                 │
│  - Cost: Very low ($10/month)                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Why Two Phases?

**Regulatory Compliance:**
- SEC Rule 17a-4: First 2 years must be "easily accessible"
- "Easily accessible" = queryable via SQL in seconds, not hours
- Warm tier satisfies this requirement

**User Experience:**
- 90% of archive queries are for recent data (< 2 years)
- Instant response vs 2-hour wait drives user satisfaction
- Customer service can self-serve via Datalens

**Query Complexity:**
- Complex joins across thousands of archived records
- Aggregations, pivots, window functions
- SQL is more powerful than Parquet query engines for these use cases

---

## 3. Architecture Overview

### Component Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│  PRIMARY POSTGRES (Hot Tier)                                     │
│  - Instance: Production RDS/Self-hosted                          │
│  - Size: Optimized for transactional workload                    │
│  - Data: Active leads (0-3 months after closure)                 │
└────────────────┬─────────────────────────────────────────────────┘
                 │
                 │ Phase 1 Archival Job (Weekly DAG)
                 │
                 ↓
┌──────────────────────────────────────────────────────────────────┐
│  WARM ARCHIVE DB (External PostgreSQL - Permanent)               │
│  - Instance: Dedicated RDS db.r5.xlarge (4 CPU, 32 GB RAM)      │
│  - Size: ~50-100 GB (2 years of data)                           │
│  - Schema: Denormalized archive schema                           │
│  - Indexes: Optimized for retrieval patterns                     │
│  - Replication: Read replica for high-availability               │
│  - Backup: Daily snapshots, 30-day retention                     │
│  - Access: Read-only for Datalens users                          │
└────────────────┬─────────────────────────────────────────────────┘
                 │
                 │ Phase 2 Archival Job (Monthly DAG)
                 │
                 ↓
┌──────────────────────────────────────────────────────────────────┐
│  COLD ARCHIVE (MinIO/S3 Parquet)                                 │
│  - Storage: Object storage with Hive partitioning                │
│  - Format: Parquet (columnar, compressed)                        │
│  - Size: ~1-2 GB (5 years of data)                              │
│  - Immutable: Write-once, never modified                         │
│  - Access: Request-based retrieval to staging                    │
└──────────────────────────────────────────────────────────────────┘
                 ↑
                 │ Query Access
                 │
┌──────────────────────────────────────────────────────────────────┐
│  DATALENS (Query Interface)                                      │
│  - Primary connection: Warm Archive DB                           │
│  - Secondary: Cold retrieval request portal                      │
│  - Role-based access control                                     │
│  - Pre-built dashboards for common queries                       │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│  ARCHIVAL MANIFEST (Primary Postgres)                            │
│  - Permanent index of all archived data                          │
│  - Tracks: tier (warm/cold), location, schema version            │
│  - Never archived itself                                         │
└──────────────────────────────────────────────────────────────────┘
```

### Data Flow Timeline

```
Day 0: Lead created in Primary DB
Day 90: Lead closed
Day 180: Lead qualifies for archival (3 months after closure)
        → Phase 1: Moved to Warm Archive DB
        → Queryable via Datalens instantly

Month 27: Lead in warm tier for 24 months
         → Phase 2: Moved to Cold Storage (Parquet)
         → Deleted from Warm Archive DB
         → Available via request-based retrieval

Year 7: Lead reaches retention limit
        → Deleted from Cold Storage (compliance allows)
```

---

## 4. Phase 1: Hot → Warm Archival

### Eligibility Criteria

A lead qualifies for Phase 1 archival when:

```sql
SELECT lead_id 
FROM leads 
WHERE closed_at < NOW() - INTERVAL '3 months'
  AND pii_masked = TRUE
  AND purged_at IS NOT NULL
  AND lead_id NOT IN (
      SELECT archived_entity_id 
      FROM archival_manifest 
      WHERE tier IN ('warm', 'cold')
  );
```

### Archival Pipeline (Phase 1)

#### Step 1: Denormalize Lead Envelope

Same as single-phase approach:
- Fetch lead + all child tables (documents, activities, payments)
- Resolve all FK references to human-readable values
- Stamp `schema_version` onto envelope

```python
def denormalize_lead(lead_id):
    lead = fetch_lead(lead_id)
    
    # Resolve foreign keys
    lead['closure_reason'] = lookup_closure_reason(lead['closure_reason_id'])
    lead['agent_name'] = lookup_agent_name(lead['agent_id'])
    lead['product_type'] = lookup_product_type(lead['product_type_id'])
    
    # Fetch children
    lead['documents'] = fetch_documents(lead_id)
    lead['activities'] = fetch_activities(lead_id)
    lead['payments'] = fetch_payments_with_line_items(lead_id)
    
    # Stamp version
    lead['schema_version'] = CURRENT_SCHEMA_VERSION  # e.g., 'v5'
    lead['archived_at'] = NOW()
    
    return lead
```

#### Step 2: Insert into Warm Archive DB

```python
def archive_to_warm(lead_envelope):
    """Insert denormalized lead into warm archive database"""
    
    with warm_db.transaction():
        # Insert root lead
        warm_db.execute("""
            INSERT INTO archive.leads (
                lead_id, customer_name, agent_name, closure_reason,
                closed_at, amount, schema_version, archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, lead_envelope)
        
        # Insert children
        for doc in lead_envelope['documents']:
            warm_db.execute("""
                INSERT INTO archive.lead_documents (
                    document_id, lead_id, document_type, file_path, created_at
                ) VALUES (?, ?, ?, ?, ?)
            """, doc)
        
        # ... insert activities, payments, etc.
```

#### Step 3: Verify Insertion

```python
def verify_warm_archive(lead_id, expected_row_counts):
    """Verify data integrity in warm archive"""
    
    # Check lead exists
    lead = warm_db.query("SELECT * FROM archive.leads WHERE lead_id = ?", lead_id)
    assert lead is not None, f"Lead {lead_id} not found in warm archive"
    
    # Check row counts match
    actual_counts = {
        'documents': warm_db.count("archive.lead_documents WHERE lead_id = ?", lead_id),
        'activities': warm_db.count("archive.lead_activities WHERE lead_id = ?", lead_id),
        'payments': warm_db.count("archive.payments WHERE lead_id = ?", lead_id)
    }
    
    assert actual_counts == expected_row_counts, "Row count mismatch"
    
    return True
```

#### Step 4: Write Archival Manifest

```python
archival_manifest.insert({
    'archived_entity_id': lead_id,
    'tier': 'warm',
    'export_target': 'warm_db',
    'warm_archive_location': 'warm-archive-db.internal:5432/archive',
    'warm_archived_at': NOW(),
    'schema_version': 'v5',
    'row_counts': {'lead': 1, 'documents': 4, 'activities': 10, 'payments': 2},
    'archived_by_job': job_execution_id
})
```

#### Step 5: Delete from Primary DB

Same deletion order as single-phase (leaves to root):
1. payment_line_items
2. payments
3. lead_assignments
4. lead_activities
5. lead_documents
6. leads (root)

Each table deleted in its own transaction for safety.

### Phase 1 DAG Structure

```python
dag_phase1 = DAG('archive_hot_to_warm', schedule='0 2 * * 0')  # Weekly

identify = PythonOperator(task_id='identify_eligible')
denormalize = PythonOperator(task_id='denormalize_lead_graph')
insert_warm = PythonOperator(task_id='insert_into_warm_db')
verify_warm = PythonOperator(task_id='verify_warm_insertion')
write_manifest = PythonOperator(task_id='write_manifest')
delete_hot = PythonOperator(task_id='delete_from_primary')
log_audit = PythonOperator(task_id='log_audit_events')

identify >> denormalize >> insert_warm >> verify_warm >> write_manifest >> delete_hot >> log_audit
```

---

## 5. Phase 2: Warm → Cold Archival

### Eligibility Criteria

A lead qualifies for Phase 2 archival when:

```sql
SELECT archived_entity_id 
FROM archival_manifest 
WHERE tier = 'warm'
  AND warm_archived_at < NOW() - INTERVAL '2 years';
```

### Archival Pipeline (Phase 2)

#### Step 1: Read from Warm Archive DB

```python
def read_from_warm(lead_id):
    """Read lead and all children from warm archive"""
    
    lead = warm_db.query("SELECT * FROM archive.leads WHERE lead_id = ?", lead_id)
    lead['documents'] = warm_db.query("SELECT * FROM archive.lead_documents WHERE lead_id = ?", lead_id)
    lead['activities'] = warm_db.query("SELECT * FROM archive.lead_activities WHERE lead_id = ?", lead_id)
    lead['payments'] = warm_db.query("SELECT * FROM archive.payments WHERE lead_id = ?", lead_id)
    
    return lead
```

#### Step 2: Export to Parquet (Cold Storage)

```python
def archive_to_cold(lead_envelope):
    """Export warm archive data to Parquet"""
    
    # 1. Write to staging path
    staging_path = f"minio://staging/leads/{lead_id}_v{schema_version}.parquet"
    write_parquet(lead_envelope, staging_path)
    
    # 2. Verify
    verify_parquet(staging_path, lead_envelope)
    
    # 3. Atomic move to final location
    final_path = f"minio://archive/leads/year={year}/month={month}/{lead_id}_v{schema_version}.parquet"
    move(staging_path, final_path)
    
    return final_path
```

#### Step 3: Update Archival Manifest

```python
archival_manifest.update(
    where={'archived_entity_id': lead_id},
    values={
        'tier': 'cold',
        'cold_archive_location': final_path,
        'cold_archived_at': NOW(),
        'cold_checksum': 'sha256:xyz789...',
        # Preserve warm metadata for audit trail
        'warm_archive_location': 'warm-archive-db.internal:5432/archive',
        'warm_archived_at': '2024-06-24 10:30:00'
    }
)
```

#### Step 4: Delete from Warm Archive DB

```python
def delete_from_warm(lead_id):
    """Remove lead from warm archive after successful cold export"""
    
    with warm_db.transaction():
        warm_db.execute("DELETE FROM archive.lead_documents WHERE lead_id = ?", lead_id)
        warm_db.execute("DELETE FROM archive.lead_activities WHERE lead_id = ?", lead_id)
        warm_db.execute("DELETE FROM archive.payments WHERE lead_id = ?", lead_id)
        warm_db.execute("DELETE FROM archive.leads WHERE lead_id = ?", lead_id)
```

### Phase 2 DAG Structure

```python
dag_phase2 = DAG('archive_warm_to_cold', schedule='0 3 1 * *')  # Monthly, 1st of month

identify_aged = PythonOperator(task_id='identify_aged_warm_records')
read_warm = PythonOperator(task_id='read_from_warm_db')
export_parquet = PythonOperator(task_id='export_to_parquet')
verify_parquet = PythonOperator(task_id='verify_parquet_file')
update_manifest = PythonOperator(task_id='update_manifest_to_cold')
delete_warm = PythonOperator(task_id='delete_from_warm_db')
log_audit = PythonOperator(task_id='log_cold_archival')

identify_aged >> read_warm >> export_parquet >> verify_parquet >> update_manifest >> delete_warm >> log_audit
```

---

## 6. Retrieval Strategy

### Unified Query Interface via Datalens

All archived data queries go through **Datalens**, which provides a single interface abstracting the complexity of multi-tier storage.

```
User Query in Datalens
    ↓
Query Router determines tier
    ↓
    ├─ Hot Tier (< 3 months) → Primary DB
    ├─ Warm Tier (3 months - 2 years) → Warm Archive DB (Instant)
    └─ Cold Tier (2+ years) → Request-based retrieval (Hours)
```

### Retrieval Flow by Tier

#### Scenario 1: Query Recent Lead (Hot Tier)

```
User: "Show me LEAD-1001"

Datalens:
1. Check archival_manifest
   → No entry found (not archived)

2. Query Primary DB
   → FOUND

3. Return data instantly

Latency: < 100ms
```

---

#### Scenario 2: Query Warm Archive (3 months - 2 years)

```
User: "Show me LEAD-5000"

Datalens:
1. Check archival_manifest
   → tier = 'warm'
   → warm_archive_location = 'warm-archive-db.internal:5432/archive'

2. Query Warm Archive DB
   SELECT * FROM archive.leads WHERE lead_id = 'LEAD-5000'
   → FOUND

3. Return data instantly

Latency: < 200ms (cross-database query)
Access: Self-service, no approval needed
```

**User Experience:**
- Transparent: User doesn't need to know it's archived
- Fast: Same experience as querying primary DB
- Self-service: No ops team involvement

---

#### Scenario 3: Query Cold Archive (2+ years)

```
User: "Show me LEAD-500"

Datalens:
1. Check archival_manifest
   → tier = 'cold'
   → cold_archive_location = 'minio://archive/leads/year=2023/...'

2. Show message:
   "This lead is in cold storage (archived 3 years ago).
    Retrieval requires 2-4 hours. Submit retrieval request?"
   
   [Submit Request] [Cancel]

3. User submits request with:
   - Business justification (audit, legal, investigation)
   - Department (for cost attribution)

4. Ops team receives request

5. Retrieval job:
   a. Download Parquet from MinIO
   b. Load into temporary staging schema in Warm DB
   c. Notify user: "LEAD-500 available in Datalens (Staging view)"

6. User queries staging view in Datalens

7. After 30 days, staging data purged (or when investigation closes)

Latency: 2-4 hours for retrieval, then instant
Access: Request-based, requires justification
```

---

### Query Routing Logic

```python
class QueryRouter:
    def route_query(self, entity_id, user):
        """Determine which tier to query and route accordingly"""
        
        # Check if archived
        manifest = self.get_manifest(entity_id)
        
        if not manifest:
            # Not archived - query primary DB
            return self.query_primary(entity_id)
        
        elif manifest['tier'] == 'warm':
            # In warm archive - query directly
            return self.query_warm(entity_id, manifest)
        
        elif manifest['tier'] == 'cold':
            # In cold storage - check if in staging
            if self.is_in_staging(entity_id):
                return self.query_staging(entity_id)
            else:
                return self.prompt_retrieval_request(entity_id, manifest)
    
    def query_warm(self, entity_id, manifest):
        """Query warm archive database"""
        connection_string = manifest['warm_archive_location']
        
        result = warm_db.query(f"""
            SELECT * FROM archive.leads WHERE lead_id = '{entity_id}'
        """)
        
        # Update access metrics
        self.update_manifest(entity_id, {
            'last_accessed_at': NOW(),
            'warm_access_count': manifest['warm_access_count'] + 1
        })
        
        return {
            'status': 'success',
            'tier': 'warm',
            'data': result
        }
```

---

### Complex Query Patterns

#### Cross-Tier Aggregation

**User Query:** "Total payment amount for all leads closed in 2025"

This spans both warm and cold tiers. Datalens handles this via query federation:

```sql
-- Datalens rewrites as:

-- Part 1: Warm tier (2025 leads still in warm)
SELECT SUM(amount) as warm_total
FROM warm_archive_db.archive.leads
WHERE YEAR(closed_at) = 2025;

-- Part 2: Cold tier (2025 leads moved to cold)
-- Query via DuckDB against Parquet
SELECT SUM(amount) as cold_total
FROM 'minio://archive/leads/year=2025/**/*.parquet';

-- Part 3: Union
SELECT warm_total + cold_total as total_amount;
```

**User sees:** Single aggregated result, unaware of multi-tier complexity.

---

#### Time-Range Queries

**User Query:** "All leads closed between 2023-2026"

```sql
-- Datalens automatically splits:

-- Query warm tier (2024-2026)
SELECT * FROM warm_archive_db.archive.leads
WHERE closed_at BETWEEN '2024-01-01' AND '2026-12-31'

UNION ALL

-- Query cold tier via staging or DuckDB (2023)
-- If few results: Load to staging
-- If many results: Direct DuckDB query
SELECT * FROM cold_archive_staging.leads
WHERE closed_at BETWEEN '2023-01-01' AND '2023-12-31'
```

---

### Pre-Built Datalens Views

To simplify user experience, provide pre-built views that abstract tier complexity:

```sql
-- View: all_archived_leads (hides tier logic)
CREATE VIEW datalens.all_archived_leads AS
SELECT 
    lead_id,
    customer_name,
    closure_reason,
    closed_at,
    amount,
    'warm' as storage_tier
FROM warm_archive_db.archive.leads

UNION ALL

-- Cold tier requires explicit load (cannot union directly)
-- This view only shows warm; cold requires request
```

**Documentation for Users:**
- "This view shows leads from the last 2 years (instant access)"
- "For older leads (2+ years), use the 'Request Cold Retrieval' form"

---

### Retrieval SLAs

| Tier | Latency | Access Type | Cost |
|------|---------|-------------|------|
| **Hot** | < 100ms | Self-service, instant | Included |
| **Warm** | < 200ms | Self-service, instant | Included |
| **Cold** | 2-4 hours (first access) | Request-based, approval required | $50/retrieval |

---

## 7. Schema Evolution Management

### The Challenge

Warm archive database holds 2 years of data. During this time, schema will evolve (new columns, renames, type changes). The warm DB must support **multiple schema versions simultaneously**.

---

### Strategy 1: Column Addition (Backwards Compatible)

**Scenario:** Add `closure_subcategory` column in v2.

#### Implementation

```sql
-- In Warm Archive DB
ALTER TABLE archive.leads ADD COLUMN closure_subcategory VARCHAR(100);

-- Update archival job to populate new column
-- Old rows have NULL for this column
```

#### Query Handling

```sql
-- Queries work across versions:
SELECT lead_id, closure_reason, closure_subcategory
FROM archive.leads;

-- Returns:
-- LEAD-1001 | Customer Request | NULL              (v1)
-- LEAD-5000 | Customer Request | Early Repayment   (v2)
```

**Approach:** Tolerate NULLs. Queries use `COALESCE(closure_subcategory, 'Unknown')`.

---

### Strategy 2: Column Rename (Breaking Change)

**Scenario:** Rename `closure_reason` → `closure_type` in v3.

#### Option A: Shadow Column (Recommended)

```sql
-- Keep both columns
ALTER TABLE archive.leads ADD COLUMN closure_type VARCHAR(100);

-- Populate new column from old during migration
UPDATE archive.leads 
SET closure_type = closure_reason 
WHERE schema_version IN ('v1', 'v2');

-- New archives populate closure_type only
-- Keep closure_reason for backward compatibility (v1, v2 data)
```

**Query Handling:**
```sql
-- Use COALESCE to handle both
SELECT 
    lead_id,
    COALESCE(closure_type, closure_reason) as closure_type
FROM archive.leads;
```

#### Option B: In-Place Migration (Risky)

```sql
-- Rename column (affects all rows)
ALTER TABLE archive.leads RENAME COLUMN closure_reason TO closure_type;

-- Update all rows' schema_version
UPDATE archive.leads SET schema_version = 'v3';
```

**Risk:** Mutates archived data, violates immutability principle.

---

### Strategy 3: Column Removal

**Scenario:** Remove `legacy_field` in v4.

#### Implementation

```sql
-- Do NOT drop the column
-- ALTER TABLE archive.leads DROP COLUMN legacy_field;  -- ❌ Don't do this

-- Instead: Stop populating it for new archives
-- Old rows retain the column with their historical values
```

**Rationale:** Immutability. Archived data should reflect state at time of archival.

---

### Strategy 4: Type Change

**Scenario:** Change `amount` from `VARCHAR(50)` to `DECIMAL(10,2)` in v5.

#### Implementation

```sql
-- Add new column with new type
ALTER TABLE archive.leads ADD COLUMN amount_decimal DECIMAL(10,2);

-- Migrate existing data
UPDATE archive.leads 
SET amount_decimal = CAST(amount AS DECIMAL(10,2))
WHERE amount_decimal IS NULL AND amount IS NOT NULL;

-- New archives use amount_decimal
-- Keep amount (old type) for v1-v4 data
```

**Query Handling:**
```sql
SELECT 
    lead_id,
    COALESCE(amount_decimal, CAST(amount AS DECIMAL(10,2))) as amount
FROM archive.leads;
```

---

### Schema Versioning in Warm DB

#### Tracking Schema Versions

```sql
-- Each row tracks its schema version
SELECT schema_version, COUNT(*) 
FROM archive.leads 
GROUP BY schema_version;

-- Result:
-- v1  | 5000
-- v2  | 6000
-- v3  | 7000
-- v4  | 8000
-- v5  | 9000
```

#### Version-Aware Queries

```python
def query_with_schema_awareness(sql_query):
    """
    Rewrite queries to handle multiple schema versions
    """
    
    # Example: User query
    # SELECT closure_reason FROM archive.leads
    
    # Rewrite to:
    # SELECT COALESCE(closure_type, closure_reason) as closure_reason
    # FROM archive.leads
    
    rewritten = schema_adapter.rewrite(sql_query, target_version='latest')
    return warm_db.execute(rewritten)
```

---

### Schema Migration Process

**When releasing a new schema version:**

```
1. Create migration script
   - Add new columns (with defaults or NULL)
   - Create shadow columns for renames
   - Do NOT drop columns

2. Deploy to Warm Archive DB
   - Run migration during low-traffic window
   - Expect ALTER TABLE to take minutes to hours
   - Use online migration tools (e.g., pg_repack) if possible

3. Update archival job
   - New code populates new schema version
   - Stamp schema_version = 'v6' on new archives

4. Update query adapters
   - Teach Datalens how to query across v1-v6
   - Add COALESCE logic for renamed columns

5. Document breaking changes
   - Notify users if query patterns need updates
```

**Example Migration Script:**

```sql
-- Migration: v5 → v6
-- Change: Split customer_name into first_name, last_name

BEGIN;

-- Add new columns
ALTER TABLE archive.leads ADD COLUMN first_name VARCHAR(100);
ALTER TABLE archive.leads ADD COLUMN last_name VARCHAR(100);

-- Backfill existing data (optional, can be done lazily)
UPDATE archive.leads 
SET 
    first_name = SPLIT_PART(customer_name, ' ', 1),
    last_name = SPLIT_PART(customer_name, ' ', 2)
WHERE schema_version IN ('v1', 'v2', 'v3', 'v4', 'v5')
  AND first_name IS NULL;

-- DO NOT drop customer_name column (preserve for audit)

COMMIT;
```

---

### Handling Inference and Data Loss

**Problem:** Some schema changes require inferring values.

**Example:** Adding `closure_subcategory` when it didn't exist in v1.

**Options:**

1. **Leave NULL:**
   ```sql
   -- v1 data: closure_subcategory = NULL
   -- Accept that historical data lacks granularity
   ```

2. **Infer from existing data:**
   ```sql
   UPDATE archive.leads
   SET closure_subcategory = CASE
       WHEN closure_reason LIKE '%Repaid%' THEN 'Early Repayment'
       WHEN closure_reason LIKE '%Request%' THEN 'Customer Initiated'
       ELSE 'Other'
   END
   WHERE schema_version = 'v1' AND closure_subcategory IS NULL;
   ```

3. **Flag inferred values:**
   ```sql
   ALTER TABLE archive.leads ADD COLUMN closure_subcategory_inferred BOOLEAN DEFAULT FALSE;
   
   UPDATE archive.leads
   SET 
       closure_subcategory = 'Early Repayment',
       closure_subcategory_inferred = TRUE
   WHERE schema_version = 'v1' AND closure_reason LIKE '%Repaid%';
   ```

**Best Practice:** Document which fields are inferred and the inference logic.

---

### Schema Evolution in Cold Tier

**Cold tier (Parquet) is simpler:**
- Each Parquet file is immutable with its schema version
- No migrations needed
- v1 Parquet stays v1 forever
- Query engines (DuckDB, Athena) handle schema union automatically

**When Phase 2 archival (warm → cold) happens:**
- Export data in its **current schema** (as it exists in warm DB)
- If v1 data was backfilled to v6 in warm tier, export as v6
- If v1 data was never backfilled, export as v1

**Decision:**
Should you normalize schema versions before exporting to cold?

**Option A: Export as-is (Mixed Schemas)**
- Pro: Simple, no transformation
- Con: Cold tier has v1, v2, v3... Parquets

**Option B: Normalize to latest before export**
- Pro: Consistent schema in cold tier
- Con: Requires transformation during warm→cold migration

**Recommendation:** Export as-is (Option A). Cold query engines handle mixed schemas well.

---

## 8. Query Federation

### The Problem

Users want to query **all data** regardless of tier, but data is split across:
- Primary DB (hot)
- Warm Archive DB (warm)
- Cold Storage (cold)

**Challenge:** Provide a unified query interface that hides this complexity.

---

### Solution: Datalens Query Federation

Datalens acts as a **federated query engine** that routes and combines queries across all three tiers.

```
User writes single query in Datalens
    ↓
Query Analyzer determines which tiers are needed
    ↓
Query Executor runs sub-queries against each tier
    ↓
Result Combiner unions results
    ↓
Return unified result to user
```

---

### Implementation Patterns

#### Pattern 1: Single-Tier Query (Simple)

```sql
-- User query
SELECT * FROM leads WHERE lead_id = 'LEAD-5000'

-- Datalens analysis:
-- - Check manifest: tier = 'warm'
-- - Route to warm DB only

-- Executed query:
SELECT * FROM warm_archive_db.archive.leads WHERE lead_id = 'LEAD-5000'
```

---

#### Pattern 2: Time-Range Query (Multi-Tier)

```sql
-- User query
SELECT lead_id, closed_at, amount
FROM leads
WHERE closed_at BETWEEN '2023-01-01' AND '2026-12-31'

-- Datalens analysis:
-- - 2023: cold tier
-- - 2024-2026: warm tier
-- - Split query

-- Executed queries:

-- Query 1: Warm tier
SELECT lead_id, closed_at, amount
FROM warm_archive_db.archive.leads
WHERE closed_at BETWEEN '2024-01-01' AND '2026-12-31'

-- Query 2: Cold tier (via DuckDB)
SELECT lead_id, closed_at, amount
FROM 'minio://archive/leads/year=2023/**/*.parquet'

-- Query 3: Union
-- Combine results and return to user
```

---

#### Pattern 3: Aggregation Across Tiers

```sql
-- User query
SELECT 
    DATE_TRUNC('month', closed_at) as month,
    SUM(amount) as total
FROM leads
WHERE closed_at >= '2022-01-01'
GROUP BY 1

-- Datalens rewrites as:

-- Warm tier aggregation
WITH warm_agg AS (
    SELECT 
        DATE_TRUNC('month', closed_at) as month,
        SUM(amount) as total
    FROM warm_archive_db.archive.leads
    WHERE closed_at >= '2024-01-01'
    GROUP BY 1
),
-- Cold tier aggregation (DuckDB)
cold_agg AS (
    SELECT 
        DATE_TRUNC('month', closed_at) as month,
        SUM(amount) as total
    FROM 'minio://archive/leads/**/*.parquet'
    WHERE closed_at BETWEEN '2022-01-01' AND '2023-12-31'
    GROUP BY 1
)
-- Union and re-aggregate
SELECT month, SUM(total) as total
FROM (
    SELECT * FROM warm_agg
    UNION ALL
    SELECT * FROM cold_agg
)
GROUP BY month
```

---

### Datalens Configuration

#### Connection Setup

```yaml
# Datalens connections config

connections:
  - name: primary_db
    type: postgresql
    host: primary-db.internal
    port: 5432
    database: production
    schema: public
    role: readonly
    description: "Active leads (0-3 months)"
  
  - name: warm_archive_db
    type: postgresql
    host: warm-archive-db.internal
    port: 5432
    database: archive
    schema: archive
    role: readonly
    description: "Archived leads (3 months - 2 years)"
  
  - name: cold_storage
    type: duckdb
    s3_endpoint: minio.internal
    s3_bucket: archive
    s3_path: leads/**/*.parquet
    description: "Cold archive (2+ years) - request-based only"
```

#### Virtual Table: Unified View

```sql
-- Create virtual table that abstracts tier logic
CREATE VIRTUAL TABLE datalens.all_leads AS
SELECT 
    'hot' as tier,
    lead_id,
    customer_name,
    closed_at,
    amount,
    'accessible' as access_type
FROM primary_db.public.leads

UNION ALL

SELECT 
    'warm' as tier,
    lead_id,
    customer_name,
    closed_at,
    amount,
    'accessible' as access_type
FROM warm_archive_db.archive.leads

UNION ALL

SELECT 
    'cold' as tier,
    lead_id,
    customer_name,
    closed_at,
    amount,
    'request-required' as access_type
FROM archival_manifest m
WHERE m.tier = 'cold';
-- Note: Cold tier shows in list but returns metadata, not actual data
```

**User Experience:**
```sql
-- User queries virtual table
SELECT * FROM all_leads WHERE customer_name LIKE '%Smith%'

-- Datalens automatically:
-- 1. Queries hot tier (instant)
-- 2. Queries warm tier (instant)
-- 3. Shows cold tier results as "Request retrieval" placeholders
```

---

### Handling Cold Tier in Federation

**Challenge:** Cold tier data is not instantly queryable.

**Options:**

**Option A: Exclude Cold from Federated Queries**
```sql
-- Virtual table only includes hot + warm
CREATE VIEW all_accessible_leads AS
SELECT * FROM primary_db.leads
UNION ALL
SELECT * FROM warm_archive_db.archive.leads;

-- Cold requires separate workflow
```

**Option B: Show Cold Metadata (Recommended)**
```sql
-- Include cold tier but return metadata, not data
SELECT 
    'cold' as tier,
    lead_id,
    NULL as customer_name,  -- Data not loaded
    closed_at,
    NULL as amount,
    'Request retrieval at /retrieval-requests' as message
FROM archival_manifest
WHERE tier = 'cold';
```

**Option C: Auto-Load to Staging on Query**
```sql
-- If user queries cold tier lead, automatically trigger load
-- Return: "Loading... Check back in 2 hours"
```

---

## 9. Implementation Details

### Warm Archive DB Schema

```sql
-- Dedicated archive schema
CREATE SCHEMA archive;

-- Denormalized lead table
CREATE TABLE archive.leads (
    lead_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    customer_region VARCHAR(50),
    customer_segment VARCHAR(50),
    
    agent_id VARCHAR(50),
    agent_name VARCHAR(100),
    agent_team VARCHAR(100),
    
    closure_reason VARCHAR(100),  -- Resolved from FK
    closure_subcategory VARCHAR(100),  -- Added in v2
    closure_type VARCHAR(100),  -- Renamed in v3 (shadow column)
    
    product_type VARCHAR(100),  -- Resolved from FK
    
    closed_at TIMESTAMP,
    amount DECIMAL(10, 2),
    
    -- Metadata
    schema_version VARCHAR(10) NOT NULL,
    archived_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Indexes for common queries
    INDEX idx_closed_at (closed_at),
    INDEX idx_customer_name (customer_name),
    INDEX idx_archived_at (archived_at),
    INDEX idx_schema_version (schema_version)
);

-- Child tables
CREATE TABLE archive.lead_documents (
    document_id UUID PRIMARY KEY,
    lead_id VARCHAR(50) REFERENCES archive.leads(lead_id) ON DELETE CASCADE,
    document_type VARCHAR(100),
    file_path TEXT,
    created_at TIMESTAMP,
    
    INDEX idx_lead_id (lead_id)
);

CREATE TABLE archive.lead_activities (
    activity_id UUID PRIMARY KEY,
    lead_id VARCHAR(50) REFERENCES archive.leads(lead_id) ON DELETE CASCADE,
    activity_type VARCHAR(100),
    description TEXT,
    performed_by VARCHAR(100),
    performed_at TIMESTAMP,
    
    INDEX idx_lead_id (lead_id),
    INDEX idx_performed_at (performed_at)
);

CREATE TABLE archive.payments (
    payment_id UUID PRIMARY KEY,
    lead_id VARCHAR(50) REFERENCES archive.leads(lead_id) ON DELETE CASCADE,
    amount DECIMAL(10, 2),
    status VARCHAR(50),
    payment_date TIMESTAMP,
    line_items JSONB,  -- Nested line items
    
    INDEX idx_lead_id (lead_id),
    INDEX idx_payment_date (payment_date)
);
```

---

### Archival Manifest Schema

```sql
CREATE TABLE archival_manifest (
    manifest_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Entity identification
    archived_entity_type VARCHAR(50) DEFAULT 'lead',
    archived_entity_id VARCHAR(100) NOT NULL UNIQUE,
    
    -- Current tier
    tier VARCHAR(20),  -- 'warm' or 'cold'
    export_target VARCHAR(50),  -- 'warm_db', 'cold_parquet'
    
    -- Warm tier metadata
    warm_archive_location TEXT,
    warm_archived_at TIMESTAMP,
    warm_row_counts JSONB,
    warm_access_count INT DEFAULT 0,
    
    -- Cold tier metadata (NULL if still in warm)
    cold_archive_location TEXT,
    cold_archived_at TIMESTAMP,
    cold_checksum VARCHAR(64),
    cold_row_counts JSONB,
    
    -- Schema versioning
    schema_version VARCHAR(10) NOT NULL,
    
    -- Job tracking
    archived_by_job_phase1 UUID,
    archived_by_job_phase2 UUID,
    
    -- Access tracking
    last_accessed_at TIMESTAMP,
    total_access_count INT DEFAULT 0,
    
    -- Indexes
    INDEX idx_tier (tier),
    INDEX idx_warm_archived_at (warm_archived_at),
    INDEX idx_cold_archived_at (cold_archived_at)
);
```

---

### Database Sizing and Capacity Planning

#### Warm Archive DB Growth

```
Assumptions:
- 500 leads archived per month
- Each lead: ~50 KB (denormalized with children)
- Retention: 24 months

Calculation:
- Monthly growth: 500 leads × 50 KB = 25 MB/month
- 24-month total: 25 MB × 24 = 600 MB
- With indexes: 600 MB × 2 = 1.2 GB
- With overhead: ~2 GB

Reality check: 2 GB is very small. More realistic:
- 500 leads/month × 500 KB (with all children) = 250 MB/month
- 24 months: 6 GB
- With indexes: 12 GB
- With buffer: Provision 20-50 GB
```

**Recommended Instance:**
- **Development:** db.t3.medium (2 vCPU, 4 GB RAM) - $50/month
- **Production:** db.r5.large (2 vCPU, 16 GB RAM) - $120/month
- **High-volume:** db.r5.xlarge (4 vCPU, 32 GB RAM) - $200/month

#### Cold Storage Growth

```
Assumptions:
- 500 leads/month migrate to cold after 2 years
- Parquet compression: 10:1 ratio
- Each lead: 50 KB → 5 KB compressed

Calculation:
- Monthly cold growth: 500 × 5 KB = 2.5 MB/month
- 5 years: 2.5 MB × 60 = 150 MB
- 7 years: 2.5 MB × 84 = 210 MB

Cost:
- MinIO/S3: $0.01/GB/month
- 0.21 GB × $0.01 = $0.002/month (negligible)
```

---

### Backup and Disaster Recovery

#### Warm Archive DB Backup Strategy

```yaml
backup_strategy:
  # Daily snapshots
  snapshot:
    frequency: daily
    time: 03:00 UTC
    retention: 30 days
    
  # Point-in-time recovery
  pitr:
    enabled: true
    retention: 7 days
    
  # Read replica (for HA)
  replica:
    enabled: true
    region: us-west-2  # Different from primary
    lag_threshold: 60 seconds  # Alert if replication lags
```

**Disaster Recovery Scenarios:**

**Scenario 1: Warm DB Corruption**
```
1. Identify corruption time (e.g., 10:00 AM)
2. Restore from snapshot (nearest before 10:00 AM)
3. Apply PITR logs to recover up to 09:59 AM
4. Rerun archival job for any leads archived 09:59-10:00 AM
5. Validate row counts against manifest
```

**Scenario 2: Accidental Deletion from Warm DB**
```
1. Identify deleted lead_id
2. Check manifest: confirm should be in warm tier
3. Option A: Restore from snapshot
4. Option B: Re-archive from primary if still there
5. Option C: Promote from cold tier (if already migrated)
```

#### Cold Storage Backup

Cold storage is immutable and versioned:
```yaml
minio_versioning:
  enabled: true
  # Every write creates new version
  # Accidental delete? Restore previous version
  
lifecycle_policy:
  # Move to Glacier Deep Archive after 5 years
  transition:
    days: 1825  # 5 years
    storage_class: DEEP_ARCHIVE
  
  # Delete after 7 years
  expiration:
    days: 2555  # 7 years
```

---

### Monitoring and Alerting

#### Key Metrics

**Archival Health:**
```
- Phase 1 job success rate (target: > 99%)
- Phase 2 job success rate (target: > 99%)
- Warm DB storage utilization (alert if > 80%)
- Cold storage growth rate (GB/month)
- Leads in warm tier (should plateau at ~12K for steady state)
```

**Query Performance:**
```
- Warm DB query latency P50, P95, P99
- Warm DB connection pool utilization
- Datalens query success rate
- Cross-tier query latency (federation overhead)
```

**Data Integrity:**
```
- Row count match: hot DB delete count = warm DB insert count
- Manifest consistency: warm DB row count = manifest row_counts
- Cold export verification: Parquet row count = warm DB row count
```

#### Alerts

**Critical (Page On-Call):**
```
- Phase 1 archival job failed 3 consecutive runs
- Phase 2 archival job failed 2 consecutive runs
- Warm DB storage > 90% capacity
- Warm DB replica lag > 5 minutes
- Manifest inconsistency detected
```

**Warning (Slack Notification):**
```
- Archival job duration > 4 hours (slowness)
- Warm DB query P95 > 5 seconds
- Phase 2 eligible leads > 1000 (backlog)
- Cold storage growth anomaly (> 2x expected)
```

---

## 10. Merits and Demerits

### Merits

#### 1. **Instant Query Access to Recent Archives**

**Benefit:**
- 90% of archive queries are for recent data (< 2 years)
- Warm tier provides instant SQL access
- No waiting for retrieval jobs

**User Experience:**
```
Customer service agent: "Show me this customer's closed lead from 6 months ago"
System: Returns data in < 200ms
Agent: Continues conversation without interruption
```

**vs Warm-on-Demand:** First access = 15 minutes

---

#### 2. **Regulatory Compliance**

**SEC Rule 17a-4: "Easily Accessible"**
- First 2 years must be instantly accessible
- SQL queries in seconds = compliant
- Auditors can self-serve via Datalens

**Audit Scenario:**
```
Auditor: "Show me all leads closed in Q2 2024 with reason 'Customer Request'"
Datalens: SELECT * FROM archive.leads WHERE ...
Result: Instant (< 2 seconds)
Auditor: ✅ Satisfied
```

---

#### 3. **Complex SQL Queries**

**Full PostgreSQL capabilities:**
- Joins across multiple archive tables
- Window functions
- CTEs (Common Table Expressions)
- Subqueries
- Aggregations with HAVING, FILTER, etc.

**Example:**
```sql
-- Complex query: Top 10 agents by closed lead value in 2024
SELECT 
    agent_name,
    COUNT(*) as closed_count,
    SUM(amount) as total_value,
    AVG(amount) as avg_value,
    RANK() OVER (ORDER BY SUM(amount) DESC) as rank
FROM archive.leads
WHERE YEAR(closed_at) = 2024
GROUP BY agent_name
HAVING COUNT(*) > 10
ORDER BY total_value DESC
LIMIT 10;
```

**DuckDB can do this too, but PostgreSQL is more familiar to analysts.**

---

#### 4. **No First-Access Latency**

**Always ready:**
- Data already loaded and indexed
- No "please wait 15 minutes" messages
- Predictable performance

**Business Impact:**
- Customer service SLAs met
- Real-time investigations possible
- Compliance requests answered immediately

---

#### 5. **Mature Ecosystem**

**PostgreSQL tooling:**
- Datalens, Metabase, Tableau all have native PostgreSQL connectors
- SQL is universal skill (no DuckDB learning curve)
- Performance tuning tools (EXPLAIN, pg_stat_statements)
- Monitoring (pg_stat_monitor, CloudWatch RDS Insights)

---

### Demerits

#### 1. **High Infrastructure Cost**

**Fixed cost regardless of usage:**
```
Warm Archive DB: $200/month (db.r5.xlarge)
Even if no one queries it: Still $200/month
```

**vs Warm-on-Demand:** $5/month average (scales to zero)

**Annual Cost:**
```
Two-Phase: $200 × 12 = $2,400/year
Warm-on-Demand: $5 × 12 = $60/year (low retrieval)

Difference: $2,340/year wasted if retrieval is infrequent
```

---

#### 2. **Schema Evolution Complexity**

**Multi-version database:**
- 24 months of data with multiple schema versions
- ALTER TABLE operations touch thousands of rows
- Migration downtime or complex online migrations
- Query logic needs version awareness

**Example Pain:**
```sql
-- v1-v2: closure_reason column
-- v3+: closure_type column (renamed)

-- Every query needs:
SELECT COALESCE(closure_type, closure_reason) as closure_reason
FROM archive.leads;

-- Analysts must understand schema versioning
```

**vs Warm-on-Demand:** Transform on load, staging always latest schema

---

#### 3. **Operational Overhead**

**Must maintain:**
- Warm DB backups (daily snapshots, PITR)
- Read replicas (for HA)
- Capacity planning (DB grows over time)
- Schema migrations (careful coordination)
- Two archival DAGs (hot→warm, warm→cold)
- Monitoring for warm DB health

**On-call burden:**
- Warm DB outages affect user access
- Replication lag needs investigation
- Backup failures require immediate action

**vs Warm-on-Demand:** Staging is ephemeral, can crash and reload from cold

---

#### 4. **Cannot Scale to Zero**

**Fixed baseline cost:**
- Must run warm DB 24/7
- Even during months with zero retrieval
- Overprovisioning for peak capacity

**Example:**
```
Month 1: 200 queries → $200/month (good value)
Month 2: 5 queries → $200/month (wasted $190)
Month 3: 0 queries → $200/month (wasted $200)

Annual waste if retrieval < 50/month: $1,500+
```

---

#### 5. **Two-Phase Migration Risk**

**Warm→Cold migration can fail:**
```
Scenario: 
1. Export to Parquet ✓
2. Verify Parquet ✓
3. Update manifest ✓
4. Delete from warm DB... ❌ DB CRASH

Result: Data in both warm and cold
Which is source of truth?
```

**Mitigation:** Extensive verification, idempotent jobs, manifest tracks both locations

**vs Single-Phase:** One migration (hot→cold), simpler failure modes

---

#### 6. **Warm Tier Storage Bloat**

**Risk of unbounded growth:**
- If Phase 2 job fails repeatedly, warm tier keeps growing
- Old data (> 2 years) accumulates in warm DB
- Eventually hits capacity limits

**Monitoring Required:**
```sql
-- Alert if leads older than 2 years in warm tier
SELECT COUNT(*) 
FROM archive.leads 
WHERE archived_at < NOW() - INTERVAL '2 years';

-- Should be near zero
-- If > 1000: Phase 2 job is failing
```

---

## 11. Operational Considerations

### Capacity Planning

#### Warm DB Growth Model

```python
def calculate_warm_db_size(leads_per_month, avg_lead_size_kb, months_retention):
    """
    Calculate expected warm DB storage requirements
    """
    total_leads = leads_per_month * months_retention
    data_size_gb = (total_leads * avg_lead_size_kb) / (1024 * 1024)
    
    # Indexes add ~100% overhead
    with_indexes = data_size_gb * 2
    
    # WAL, temp space, buffer
    with_overhead = with_indexes * 1.5
    
    return {
        'total_leads': total_leads,
        'data_size_gb': data_size_gb,
        'with_indexes_gb': with_indexes,
        'recommended_storage_gb': with_overhead,
        'recommended_instance': recommend_instance(with_overhead)
    }

# Example calculation
result = calculate_warm_db_size(
    leads_per_month=500,
    avg_lead_size_kb=500,  # Lead + all children
    months_retention=24
)

# Output:
# {
#   'total_leads': 12000,
#   'data_size_gb': 5.7,
#   'with_indexes_gb': 11.4,
#   'recommended_storage_gb': 17.1,
#   'recommended_instance': 'db.r5.large (50 GB storage)'
# }
```

#### Scaling Triggers

**Vertical Scaling (Upgrade Instance):**
```
Triggers:
- CPU utilization > 70% sustained for 1 week
- Memory utilization > 80% sustained
- Query P95 latency > 2 seconds
- Connection pool exhaustion (> 90% used)

Action:
- db.r5.large → db.r5.xlarge
- Or enable read replicas for query load
```

**Storage Scaling:**
```
Triggers:
- Storage utilization > 80%
- Phase 2 job backlog > 1000 leads (warm tier not draining)

Action:
- Increase storage allocation
- Investigate Phase 2 job failures
- Manually run warm→cold migration for old data
```

---

### Schema Migration Workflow

#### Planning Phase

```
1. Assess Impact
   - How many rows will be affected?
   - Is this breaking or additive?
   - What's the migration duration estimate?

2. Create Migration Script
   - Use shadow columns for renames
   - Avoid DROP COLUMN
   - Add indexes after data migration

3. Test in Staging
   - Load production snapshot to staging warm DB
   - Run migration script
   - Measure duration
   - Validate queries work across versions

4. Schedule Maintenance Window
   - If migration takes > 5 min, schedule downtime
   - Or use online migration tool (pg_repack, Braintree, etc.)
```

#### Execution Phase

```sql
-- Example: Add new column (safe, fast)
BEGIN;

-- Step 1: Add column with default
ALTER TABLE archive.leads 
ADD COLUMN closure_subcategory VARCHAR(100) DEFAULT 'Unknown';

-- Step 2: Backfill (optional, can be lazy)
-- Skip if table is large; let new data populate it

-- Step 3: Update schema_version tracking
-- (Handled by archival job, not migration)

COMMIT;

-- Duration: Seconds to minutes (DDL only, no data rewrite)
```

```sql
-- Example: Rename column (requires shadow column)
BEGIN;

-- Step 1: Add new column
ALTER TABLE archive.leads 
ADD COLUMN closure_type VARCHAR(100);

-- Step 2: Backfill from old column
UPDATE archive.leads 
SET closure_type = closure_reason
WHERE closure_type IS NULL;

-- Duration: Minutes to hours (depends on row count)

-- Step 3: Create index
CREATE INDEX idx_closure_type ON archive.leads(closure_type);

-- Step 4: DO NOT drop old column (preserve for old schema versions)

COMMIT;
```

#### Post-Migration Validation

```sql
-- Verify migration
SELECT 
    schema_version,
    COUNT(*) as row_count,
    COUNT(closure_type) as closure_type_filled,
    COUNT(closure_reason) as closure_reason_filled
FROM archive.leads
GROUP BY schema_version;

-- Expected:
-- v1-v2: closure_type filled (backfilled), closure_reason filled (original)
-- v3+: closure_type filled (native), closure_reason NULL
```

---

### Disaster Recovery Procedures

#### DR Scenario 1: Warm DB Complete Failure

```
Incident: Warm archive DB is unrecoverable

Recovery Steps:
1. Provision new warm DB instance
2. Restore from latest snapshot
3. Apply PITR logs to recover to last minute
4. Update connection strings in Datalens
5. Validate row counts against manifest
6. Resume archival jobs

RTO (Recovery Time Objective): 2-4 hours
RPO (Recovery Point Objective): 1 hour (PITR retention)
```

#### DR Scenario 2: Data Corruption in Warm DB

```
Incident: Batch of leads corrupted (e.g., NULL values where shouldn't be)

Recovery Steps:
1. Identify affected lead_ids and corruption time
2. Query manifest to get original schema version
3. Option A: Restore from snapshot before corruption
4. Option B: Re-archive from primary DB (if still there)
5. Option C: Reload from cold storage (if already migrated)
6. Validate data integrity

RTO: 1-8 hours (depends on recovery method)
```

#### DR Scenario 3: Accidental Mass Deletion

```
Incident: "Oops, I ran DELETE FROM archive.leads WHERE 1=1"

Recovery Steps:
1. Immediately stop Phase 2 job (prevent cold migration of deleted state)
2. Identify deletion time from pg_stat_activity or logs
3. Restore from snapshot immediately before deletion
4. Apply PITR logs to recover up to deletion moment
5. Verify row counts match manifest
6. Investigate root cause (how did delete permission exist?)

RTO: 1-2 hours
```

#### DR Scenario 4: Cold Storage Data Loss

```
Incident: MinIO bucket accidentally deleted

Recovery Steps:
1. Check MinIO versioning - restore deleted objects
2. If versioning not enabled, restore from:
   - MinIO replication (if configured)
   - S3 cross-region replication
   - Tape backups (if exist)
3. If total loss: Warm DB has last 2 years (recoverable)
   - Data > 2 years is lost (permanent data loss)
4. Update manifest: Mark cold tier data as LOST
5. Incident report, postmortem, implement safeguards

RTO: Days to weeks (depends on backup strategy)
RPO: Up to 5 years of data potentially lost ❌
```

**Mitigation:** Cold storage MUST have versioning and cross-region replication.

---

### Troubleshooting Common Issues

#### Issue 1: Phase 2 Job Failing

**Symptoms:**
- Phase 2 archival job fails repeatedly
- Warm DB growing beyond expected size
- Leads older than 2 years accumulating

**Diagnosis:**
```sql
-- Check how many leads are overdue for cold migration
SELECT COUNT(*) 
FROM archival_manifest 
WHERE tier = 'warm' 
  AND warm_archived_at < NOW() - INTERVAL '2 years';

-- Check job logs for errors
SELECT * FROM job_executions 
WHERE dag_id = 'archive_warm_to_cold' 
  AND status = 'failed' 
ORDER BY started_at DESC 
LIMIT 10;
```

**Common Causes:**
- MinIO unavailable
- Parquet write permission issues
- Warm DB connection timeout
- Schema mismatch during export

**Resolution:**
1. Fix root cause (permissions, connectivity)
2. Manually trigger Phase 2 job for backlog
3. Monitor until backlog clears

---

#### Issue 2: Query Performance Degradation

**Symptoms:**
- Warm DB query latency increasing over time
- P95 > 5 seconds (was < 1 second)

**Diagnosis:**
```sql
-- Check for missing indexes
SELECT schemaname, tablename, indexname, idx_scan
FROM pg_stat_user_indexes
WHERE schemaname = 'archive'
ORDER BY idx_scan ASC;

-- Check for slow queries
SELECT query, mean_exec_time, calls
FROM pg_stat_statements
WHERE query LIKE '%archive.leads%'
ORDER BY mean_exec_time DESC
LIMIT 10;

-- Check table bloat
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
FROM pg_tables
WHERE schemaname = 'archive';
```

**Common Causes:**
- Missing indexes on new query patterns
- Table bloat (needs VACUUM)
- Inefficient queries (missing WHERE clauses)
- Instance undersized

**Resolution:**
1. Add indexes for slow queries
2. Run VACUUM ANALYZE
3. Rewrite inefficient queries
4. Scale up instance if needed

---

#### Issue 3: Schema Version Confusion

**Symptoms:**
- Queries returning unexpected NULLs
- "Column not found" errors
- Users complaining about missing data

**Diagnosis:**
```sql
-- Check schema version distribution
SELECT schema_version, COUNT(*)
FROM archive.leads
GROUP BY schema_version;

-- Check which columns exist per version
SELECT 
    schema_version,
    COUNT(*) FILTER (WHERE closure_reason IS NOT NULL) as has_closure_reason,
    COUNT(*) FILTER (WHERE closure_type IS NOT NULL) as has_closure_type,
    COUNT(*) FILTER (WHERE closure_subcategory IS NOT NULL) as has_subcategory
FROM archive.leads
GROUP BY schema_version;
```

**Common Causes:**
- Query not using COALESCE for renamed columns
- Backfill migration incomplete
- User expecting column that doesn't exist in older versions

**Resolution:**
1. Update query to handle multiple versions:
   ```sql
   SELECT 
       lead_id,
       COALESCE(closure_type, closure_reason) as closure_reason,
       COALESCE(closure_subcategory, 'Unknown') as subcategory
   FROM archive.leads;
   ```
2. Document which columns exist in which versions
3. Complete backfill migrations if needed

---

## 12. Cost Analysis

### Infrastructure Costs (1M Leads, 7 Years)

#### Warm Archive DB

```
Instance: db.r5.xlarge (4 vCPU, 32 GB RAM)
Storage: 50 GB (allows for growth)
Region: us-east-1

Monthly Cost:
- Instance: $175/month (on-demand)
- Storage: 50 GB × $0.115/GB = $5.75/month
- Backup: 50 GB × $0.095/GB = $4.75/month
- Total: $185.50/month

Annual Cost: $2,226/year
7-Year Cost: $15,582

Reserved Instance (1-year):
- Instance: $115/month (34% savings)
- Total: $125.50/month
- 7-Year Cost: $10,542 (33% savings)
```

#### Cold Storage

```
Storage: 1.5 GB (Parquet compression)
MinIO/S3 Standard: $0.023/GB/month

Monthly Cost:
- Storage: 1.5 GB × $0.023 = $0.035/month
- Negligible

7-Year Cost: $2.94
```

#### Total Infrastructure Cost

```
Warm DB: $10,542 (with reserved instance)
Cold Storage: $3
Total: $10,545

Cost per lead: $10,545 / 42,000 leads / 7 years = $0.036 per lead per year
```

---

### Operational Costs

```
Engineering Time (Annual):
- Schema migrations: 8 hours × $150/hour = $1,200/year
- DAG maintenance: 12 hours × $150/hour = $1,800/year
- Troubleshooting: 20 hours × $150/hour = $3,000/year
- Total: $6,000/year

On-Call (Annual):
- Warm DB incidents: 10 incidents × 2 hours × $150/hour = $3,000/year
- Off-hours response: $2,000/year
- Total: $5,000/year

Backup Storage:
- Daily snapshots: 50 GB × $0.095 × 12 = $57/year

Total Operational: $11,057/year
7-Year Total: $77,399
```

---

### Total Cost of Ownership (7 Years)

```
Infrastructure: $10,545
Operations: $77,399
─────────────────────
Total: $87,944

Cost per lead archived: $87,944 / 42,000 = $2.09 per lead (over 7 years)
Monthly average: $1,047/month
```

---

### Cost Comparison: Two-Phase vs Warm-on-Demand

**Scenario: 1M Leads, 7 Years**

| Cost Category | Two-Phase | Warm-on-Demand | Difference |
|---------------|-----------|----------------|------------|
| **Infrastructure** | | | |
| Warm/Staging DB | $10,542 | $420 | **-$10,122** |
| Cold Storage | $3 | $900 | +$897 |
| **Operations** | | | |
| Engineering | $42,000 | $10,500 | **-$31,500** |
| On-call | $35,000 | $3,500 | **-$31,500** |
| **Retrieval** | | | |
| Query costs | $0 | $100 | +$100 |
| **Total** | **$87,545** | **$15,420** | **-$72,125 (82% savings)** |

**Two-Phase is more expensive unless:**
- Retrieval volume > 100 unique leads/month (then retrieval costs dominate)
- User latency requirements demand instant access
- Regulatory compliance mandates instant SQL access

---

### When Two-Phase is Cost-Effective

**Break-even calculation:**

```
Two-Phase monthly cost: $1,047
Warm-on-Demand base cost: $60

Difference: $987/month available for retrieval costs

If retrieval costs $5/lead loaded:
$987 / $5 = 197 leads/month

Break-even: ~200 leads retrieved per month
```

**Decision Rule:**
- **< 100 leads/month:** Warm-on-Demand is cheaper (70% savings)
- **100-200 leads/month:** Close, depends on ops team efficiency
- **> 200 leads/month:** Two-Phase is cheaper (retrieval costs dominate)

---

## 13. Testing Strategy

### Unit Tests

```python
def test_phase1_archival():
    """Test hot → warm archival"""
    lead = create_test_lead_in_primary_db()
    
    # Run Phase 1 archival
    denormalized = denormalize_lead(lead.id)
    archive_to_warm(denormalized)
    
    # Verify in warm DB
    warm_lead = warm_db.query("SELECT * FROM archive.leads WHERE lead_id = ?", lead.id)
    assert warm_lead is not None
    assert warm_lead['schema_version'] == CURRENT_VERSION
    
    # Verify manifest
    manifest = get_manifest(lead.id)
    assert manifest['tier'] == 'warm'
    
    # Verify deleted from primary
    assert not primary_db.has_lead(lead.id)

def test_phase2_archival():
    """Test warm → cold archival"""
    # Seed warm DB with 2-year-old lead
    lead = create_lead_in_warm_db(archived_at='2 years ago')
    
    # Run Phase 2 archival
    read_warm_and_export_cold(lead.id)
    
    # Verify Parquet exists
    parquet_path = get_cold_archive_location(lead.id)
    assert file_exists(parquet_path)
    
    # Verify row count
    df = read_parquet(parquet_path)
    assert len(df) == 1
    
    # Verify manifest updated
    manifest = get_manifest(lead.id)
    assert manifest['tier'] == 'cold'
    
    # Verify deleted from warm
    assert not warm_db.has_lead(lead.id)

def test_schema_transformation():
    """Test querying across schema versions"""
    # Create v1 and v5 leads in warm DB
    v1_lead = create_lead_in_warm_db(schema_version='v1')
    v5_lead = create_lead_in_warm_db(schema_version='v5')
    
    # Query with version-aware logic
    results = warm_db.query("""
        SELECT 
            lead_id,
            COALESCE(closure_type, closure_reason) as closure_reason
        FROM archive.leads
        WHERE lead_id IN (?, ?)
    """, v1_lead.id, v5_lead.id)
    
    # Both should return data
    assert len(results) == 2
    assert results[0]['closure_reason'] is not None
    assert results[1]['closure_reason'] is not None
```

---

### Integration Tests

```python
def test_end_to_end_two_phase():
    """Test complete flow: hot → warm → cold"""
    
    # Step 1: Create lead in primary
    lead = create_lead_in_primary_db()
    
    # Step 2: Fast-forward 3 months, run Phase 1
    freeze_time(NOW() + timedelta(days=90))
    run_phase1_job()
    
    # Verify in warm tier
    manifest = get_manifest(lead.id)
    assert manifest['tier'] == 'warm'
    assert warm_db.has_lead(lead.id)
    assert not primary_db.has_lead(lead.id)
    
    # Step 3: Fast-forward 2 years, run Phase 2
    freeze_time(NOW() + timedelta(days=730))
    run_phase2_job()
    
    # Verify in cold tier
    manifest = get_manifest(lead.id)
    assert manifest['tier'] == 'cold'
    assert not warm_db.has_lead(lead.id)
    assert cold_storage.has_parquet(lead.id)

def test_query_federation():
    """Test Datalens querying across hot, warm, cold"""
    
    # Create leads in each tier
    hot_lead = create_lead_in_primary_db()
    warm_lead = create_lead_in_warm_db()
    cold_lead = create_lead_in_cold_storage()
    
    # Query via Datalens unified interface
    results = datalens.query(f"""
        SELECT lead_id, tier 
        FROM all_leads 
        WHERE lead_id IN ('{hot_lead.id}', '{warm_lead.id}', '{cold_lead.id}')
    """)
    
    assert len(results) == 3
    assert results[0]['tier'] == 'hot'
    assert results[1]['tier'] == 'warm'
    assert results[2]['tier'] == 'cold'
```

---

### Load Tests

```python
def test_warm_db_concurrent_queries():
    """Can warm DB handle 100 concurrent queries?"""
    
    # Seed 10K leads in warm DB
    seed_warm_db(lead_count=10000)
    
    # Run 100 concurrent queries
    with ThreadPoolExecutor(max_workers=100) as executor:
        queries = [
            f"SELECT * FROM archive.leads WHERE lead_id = 'LEAD-{i}'"
            for i in range(100)
        ]
        futures = [executor.submit(warm_db.query, q) for q in queries]
        results = [f.result() for f in futures]
    
    # All should succeed
    assert all(r is not None for r in results)
    
    # Measure P95 latency
    latencies = [r.duration_ms for r in results]
    p95 = percentile(latencies, 95)
    assert p95 < 500  # P95 < 500ms

def test_phase1_throughput():
    """Can Phase 1 archive 500 leads in < 1 hour?"""
    
    # Create 500 eligible leads
    leads = [create_eligible_lead() for _ in range(500)]
    
    start = time.time()
    run_phase1_job(batch_size=500)
    duration = time.time() - start
    
    # Should complete in < 1 hour
    assert duration < 3600
    
    # Verify all archived
    for lead in leads:
        manifest = get_manifest(lead.id)
        assert manifest['tier'] == 'warm'
```

---

### Schema Migration Tests

```python
def test_schema_migration_v5_to_v6():
    """Test migration adding new column"""
    
    # Seed warm DB with v5 leads
    leads = seed_warm_db(schema_version='v5', count=1000)
    
    # Run migration: Add first_name, last_name (split from customer_name)
    run_migration('v6_split_customer_name.sql')
    
    # Verify schema updated
    columns = warm_db.get_columns('archive.leads')
    assert 'first_name' in columns
    assert 'last_name' in columns
    
    # Verify backfill (optional, depending on strategy)
    sample = warm_db.query("SELECT * FROM archive.leads LIMIT 10")
    # If backfilled:
    assert all(r['first_name'] is not None for r in sample)
    # If not backfilled:
    # assert all(r['first_name'] is None for r in sample)  # NULL is okay
    
    # Verify new archives use v6
    new_lead = archive_new_lead()
    manifest = get_manifest(new_lead.id)
    assert manifest['schema_version'] == 'v6'
```

---

## 14. Conclusion

### Summary

The **Two-Phase Archival Strategy** (Hot → Warm → Cold) provides **instant SQL access** to 2 years of archived data via a permanent warm tier database, while moving older data to cold storage for long-term compliance.

**Key Strengths:**
- ✅ Instant query access (< 200ms) for 90% of archive requests
- ✅ Regulatory compliant ("easily accessible" requirement)
- ✅ Full SQL capabilities (complex joins, aggregations, window functions)
- ✅ Mature ecosystem (PostgreSQL tooling, monitoring, backups)
- ✅ No first-access latency (data always ready)

**Key Trade-offs:**
- ❌ High infrastructure cost ($200/month fixed, regardless of usage)
- ❌ Schema evolution complexity (multi-version database)
- ❌ More operational overhead (two archival jobs, backups, migrations)
- ❌ Cannot scale to zero (permanent warm tier always running)

---

### When to Use Two-Phase Archival

**Use Two-Phase If:**

✅ **High retrieval volume** (> 100 requests/month)
- Justifies permanent warm tier cost
- User productivity gains outweigh infrastructure costs

✅ **Regulatory compliance** requires instant access
- SEC, FINRA, or other regulations mandate "easily accessible"
- Auditors need self-service SQL access

✅ **Customer-facing applications**
- Customer service agents query archives daily
- Cannot accept 15-minute first-access latency
- SLAs demand sub-second response times

✅ **Complex query requirements**
- Need full SQL capabilities (joins, CTEs, window functions)
- Analysts run ad-hoc queries frequently
- Reporting dashboards pull from archives

✅ **Mature engineering organization**
- Have resources to maintain two archival pipelines
- Can handle schema migration complexity
- Dedicated data platform or SRE team

---

### When NOT to Use Two-Phase Archival

❌ **Low retrieval volume** (< 50 requests/month)
- Warm tier sits idle 90% of time
- Wasting $200/month on unused capacity
- Warm-on-Demand would save 80%+

❌ **Budget constraints**
- Cannot justify $200/month for archival
- Need cost to scale with actual usage

❌ **Rapidly evolving schema** (> 2 changes/year)
- Schema migrations are painful on warm tier
- Warm-on-Demand handles this better (transform on load)

❌ **Small engineering team**
- Don't have resources for two-phase complexity
- Need operational simplicity

❌ **Internal tooling** (non-customer-facing)
- Users can tolerate 15-minute first-access latency
- Self-service not critical

---

### Hybrid Recommendation

**Start with usage monitoring:**

```
Deploy Two-Phase if:
- Known high retrieval (proven demand)
- Regulatory requirement is clear
- Budget approved

Deploy Warm-on-Demand if:
- Retrieval volume unknown
- Want to minimize initial cost
- Prefer simplicity

Monitor for 3-6 months:
- Track retrieval volume
- Measure user satisfaction
- Calculate actual costs

Migrate if needed:
- Low usage → Keep Warm-on-Demand
- High usage → Migrate to Two-Phase
```

**This gives you:**
- Real data before committing to expensive infrastructure
- Flexibility to change approach
- Cost optimization based on actual usage

---

### Final Thoughts

Two-Phase Archival is the **enterprise-grade solution** for organizations with:
- High archive query volume
- Regulatory compliance requirements
- Resources for operational complexity

It trades **higher cost and complexity** for **instant access and full SQL capabilities**.

For many use cases, **Warm-on-Demand is sufficient** and far more cost-effective. Two-Phase should be chosen when **instant access justifies the premium**.

---

## Appendix A: Configuration Examples

### Airflow DAG Configurations

```python
# Phase 1: Hot → Warm
DAG_CONFIG_PHASE1 = {
    'dag_id': 'archive_hot_to_warm',
    'schedule': '0 2 * * 0',  # Weekly Sunday 2 AM
    'max_active_runs': 1,
    'batch_size': 500,
    'eligibility_interval': '3 months',
    'retry_attempts': 3,
    'retry_delay': timedelta(minutes=10),
    'alert_on_failure': True,
    'alert_emails': ['data-ops@company.com']
}

# Phase 2: Warm → Cold
DAG_CONFIG_PHASE2 = {
    'dag_id': 'archive_warm_to_cold',
    'schedule': '0 3 1 * *',  # Monthly 1st 3 AM
    'max_active_runs': 1,
    'batch_size': 1000,
    'eligibility_interval': '2 years',
    'retry_attempts': 3,
    'retry_delay': timedelta(minutes=15),
    'alert_on_failure': True,
    'alert_emails': ['data-ops@company.com']
}
```

---

### Datalens Connection Strings

```yaml
# datalens_connections.yaml

connections:
  primary_db:
    type: postgresql
    host: primary-db.us-east-1.rds.amazonaws.com
    port: 5432
    database: production
    username: datalens_readonly
    ssl_mode: require
    
  warm_archive_db:
    type: postgresql
    host: warm-archive-db.us-east-1.rds.amazonaws.com
    port: 5432
    database: archive
    schema: archive
    username: datalens_readonly
    ssl_mode: require
    connection_pool_size: 20
    query_timeout: 30s
```

---

### Monitoring Dashboard Queries

```sql
-- Warm DB Storage Utilization
SELECT 
    pg_size_pretty(pg_database_size('archive')) as db_size,
    (pg_database_size('archive')::float / (50 * 1024^3)) * 100 as utilization_pct;

-- Lead Count by Tier
SELECT 
    tier,
    COUNT(*) as lead_count,
    MIN(warm_archived_at) as oldest,
    MAX(warm_archived_at) as newest
FROM archival_manifest
GROUP BY tier;

-- Schema Version Distribution in Warm Tier
SELECT 
    schema_version,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM archive.leads
GROUP BY schema_version
ORDER BY schema_version;

-- Phase 1 Job Performance (Last 30 Days)
SELECT 
    DATE(started_at) as date,
    COUNT(*) as runs,
    AVG(EXTRACT(EPOCH FROM (ended_at - started_at))) as avg_duration_sec,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successes,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failures
FROM job_executions
WHERE dag_id = 'archive_hot_to_warm'
  AND started_at > NOW() - INTERVAL '30 days'
GROUP BY DATE(started_at)
ORDER BY date DESC;
```

---

## Appendix B: References

- [PostgreSQL Partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html)
- [AWS RDS Best Practices](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_BestPractices.html)
- [SEC Rule 17a-4 Compliance](https://www.sec.gov/rules/interp/34-47806.htm)
- [Apache Parquet Format](https://parquet.apache.org/docs/)
- [pg_repack Online Migration Tool](https://reorg.github.io/pg_repack/)

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-24  
**Author:** Engineering Team  
**Status:** Design Proposal — Pending Review
