# Warm-on-Demand Archival Strategy

**PostgreSQL Archival Strategy — Alternative Approach**

| Field | Value |
| :---- | :---- |
| Document Type | Archival Strategy Document |
| Strategy | Warm-on-Demand with Cold-First Archival |
| Archive Trigger | Timerange column < NOW() - INTERVAL 3 months |
| Retrieval Model | On-demand staging with intelligent query routing |
| Author | Engineering |
| Status | Design Proposal |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Core Concept](#2-core-concept)
3. [Architecture Overview](#3-architecture-overview)
4. [Archival Flow](#4-archival-flow)
5. [Retrieval Strategy](#5-retrieval-strategy)
6. [Schema Evolution Management](#6-schema-evolution-management)
7. [Bulk Query Handling](#7-bulk-query-handling)
8. [Implementation Details](#8-implementation-details)
9. [Merits and Demerits](#9-merits-and-demerits)
10. [Comparison: Warm-on-Demand vs Hot→Warm→Cold](#10-comparison-warm-on-demand-vs-hotwarmcold)
11. [When to Use Each Approach](#11-when-to-use-each-approach)
12. [Cost Analysis](#12-cost-analysis)
13. [Migration Path](#13-migration-path)

---

## 1. Executive Summary

**The Problem with Traditional Two-Phase Archival:**
- Permanent warm tier requires maintaining 2 years of data in expensive PostgreSQL storage
- Schema evolution creates compound complexity (multiple schema versions in same database)
- High operational overhead (backups, migrations, capacity planning)
- Costs $200+/month even if data is rarely accessed

**The Warm-on-Demand Solution:**
- Archive everything to cold storage (Parquet) immediately after eligibility (single-phase)
- Maintain temporary "staging" database that's populated only when data is requested
- Auto-expire staging data after 30 days of inactivity
- Intelligent query routing: small queries load to staging, bulk queries use direct Parquet query engines

**Key Benefits:**
- 95% cost reduction when retrieval is infrequent
- Zero schema migration complexity in stored archives
- Simple operational model (cold storage is source of truth)
- Scales from zero to high-volume retrieval on-demand

**Trade-off:**
- First access latency: 15 minutes - 2 hours (vs instant with permanent warm tier)
- Acceptable for 90% of use cases where archived data access is occasional

---

## 2. Core Concept

### Traditional Approach
```
Primary DB (Hot) → Warm DB (Permanent) → Cold Storage
                   ↑
                   Kept for 2 years, always queryable
```

### Warm-on-Demand Approach
```
Primary DB (Hot) → Cold Storage (Permanent)
                        ↓ (on retrieval request)
                   Staging DB (Temporary, 30-day TTL)
```

**Core Principle:**
> Cold storage (Parquet in MinIO) is the single source of truth. Warm staging is an ephemeral cache that's reconstituted on-demand.

### Key Characteristics

| Aspect | Value |
|--------|-------|
| Default Storage Tier | Cold (Parquet) |
| Warm Tier Provisioning | On-demand only |
| Staging Lifetime | 30 days from last access |
| Schema Versioning | Transform on load (cold stays immutable) |
| Cost Model | Pay only when staging is populated |

---

## 3. Architecture Overview

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│  PRIMARY POSTGRES (Hot Tier)                                    │
│  - Active leads only                                            │
│  - Age: 0 - 3 months after closure                             │
│  - Cost: High (production DB)                                   │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ Archival Job (Weekly)
                 │ Trigger: closed_at < NOW() - INTERVAL '3 months'
                 │
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│  COLD STORAGE (MinIO - Parquet)                                 │
│  - All archived leads                                           │
│  - Age: 3 months - 7 years                                      │
│  - Format: Hive-partitioned Parquet                             │
│  - Immutable: Schema version frozen per file                    │
│  - Cost: Very low ($10/month per 1M leads)                      │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ On Retrieval Request
                 │ (Manual trigger or API call)
                 │
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│  WARM STAGING (External Postgres - Temporary)                   │
│  - Only requested leads                                         │
│  - Auto-expires after 30 days of inactivity                     │
│  - Always uses LATEST schema version                            │
│  - Max capacity: 1,000 leads (configurable)                     │
│  - Cost: $0 when empty, $50/month when populated                │
└─────────────────────────────────────────────────────────────────┘
                 ↑
                 │ Query Access
                 │
┌─────────────────────────────────────────────────────────────────┐
│  DATALENS (Query Interface)                                     │
│  - Intelligent query routing                                    │
│  - Decides: staging load vs direct Parquet query                │
│  - Role-based access control                                    │
└─────────────────────────────────────────────────────────────────┘
```

### Storage Tier Details

| Tier | Technology | Purpose | Latency | Cost | Mutability |
|------|-----------|---------|---------|------|------------|
| **Hot** | PostgreSQL (Primary) | Active operations | < 100ms | High | Mutable |
| **Cold** | Parquet (MinIO) | Permanent archive | N/A (not directly queryable) | Very Low | Immutable |
| **Warm Staging** | PostgreSQL (External) | Temporary query cache | < 100ms (after load) | Low (ephemeral) | Ephemeral |

---

## 4. Archival Flow

### Single-Phase Archival: Hot → Cold

Unlike traditional two-phase archival, there's only **one archival job** that moves data directly from hot to cold.

#### Step 1: Identify Eligible Leads

```sql
SELECT lead_id 
FROM leads 
WHERE closed_at < NOW() - INTERVAL '3 months'
  AND pii_masked = TRUE
  AND purged_at IS NOT NULL
  AND lead_id NOT IN (SELECT archived_entity_id FROM archival_manifest);
```

#### Step 2: Denormalize Lead Envelope

Same as traditional approach:
- Fetch lead + all child tables
- Resolve all FK references to human-readable values
- Stamp `schema_version` onto envelope
- Result: Self-contained JSON/dict structure

#### Step 3: Export to Parquet (Cold Storage)

```python
def archive_to_cold(lead_envelope):
    # 1. Write to staging path
    staging_path = f"minio://staging/leads/{lead_id}_v{schema_version}.parquet"
    write_parquet(lead_envelope, staging_path)
    
    # 2. Verify
    verify_row_count(staging_path)
    verify_checksum(staging_path)
    
    # 3. Atomic move to final location
    final_path = f"minio://archive/leads/year={year}/month={month}/{lead_id}_v{schema_version}.parquet"
    move(staging_path, final_path)
    
    return final_path
```

#### Step 4: Write Archival Manifest

```python
archival_manifest.insert({
    'archived_entity_id': lead_id,
    'tier': 'cold',
    'export_target': 'parquet',
    'cold_archive_location': final_path,
    'cold_archived_at': NOW(),
    'schema_version': 'v3',
    'row_counts': {'lead': 1, 'documents': 4, 'payments': 2},
    'checksum': 'sha256:abc123...',
    
    # Staging fields (initially NULL)
    'staging_loaded_at': NULL,
    'staging_expires_at': NULL,
    'last_accessed_at': NULL
})
```

#### Step 5: Delete from Primary Postgres

Same deletion order as traditional approach (leaves to root).

**Pipeline Summary:**
```
Identify → Denormalize → Export to Parquet → Verify → Manifest → Delete
```

**No warm tier involvement in archival** — simplifies the pipeline significantly.

---

## 5. Retrieval Strategy

### Retrieval Decision Tree

When a user requests archived data, the system routes the request based on query characteristics.

```
User Query
    ↓
Analyze Query
    ↓
┌───────────────────────────────────────────────────────────┐
│ Decision Factors:                                         │
│ - Result size (< 100 rows vs > 1000 rows)                │
│ - Query type (point lookup vs analytical aggregation)    │
│ - User needs (interactive SQL vs one-time export)        │
└───────────────────────────────────────────────────────────┘
    ↓
    ├─ Route 1: Load to Staging (< 100 leads)
    ├─ Route 2: Direct Parquet Query (aggregations)
    ├─ Route 3: Bulk Staging DB (legal discovery, > 1K leads)
    └─ Route 4: Pre-Aggregated Views (standard reports)
```

### Route 1: Load to Staging (Small Queries)

**Use Case:** User needs to look up specific leads interactively

**Flow:**

```
1. User queries: "Show me LEAD-1001"

2. Datalens checks warm_staging.leads
   → NOT FOUND

3. Datalens checks archival_manifest
   → tier = 'cold'
   → cold_archive_location = 'minio://archive/leads/year=2024/...'

4. Datalens prompts:
   "Lead is in cold storage. Load to staging? (Est. 15 min)"
   [Load] [Cancel]

5. User clicks [Load]

6. Background job executes:
   a. Download Parquet from MinIO
   b. Transform schema (v1 → current v5 if needed)
   c. Insert into warm_staging.leads and child tables
   d. Update manifest:
      - staging_loaded_at = NOW()
      - staging_expires_at = NOW() + INTERVAL '30 days'

7. User notified: "LEAD-1001 ready in staging"

8. User runs query → Returns instantly from staging
```

**Subsequent Access:**
- Same lead queried next day → Found in staging → Instant return
- `last_accessed_at` updated → Resets 30-day expiration clock

**Auto-Cleanup:**
```python
# Daily job
expired_leads = query("""
    SELECT archived_entity_id 
    FROM archival_manifest 
    WHERE staging_expires_at < NOW()
""")

for lead_id in expired_leads:
    delete_from_staging(lead_id)
    update_manifest(lead_id, staging_loaded_at=NULL, staging_expires_at=NULL)
```

### Route 2: Direct Parquet Query (Analytical Queries)

**Use Case:** Aggregations, reports, dashboards

**Why:** No need to load full data into DB if only aggregated results are needed

**Technology:** DuckDB, Trino, or AWS Athena

**Example:**

```python
# User query in Datalens
"""
SELECT 
    closure_reason,
    COUNT(*) as lead_count,
    AVG(amount) as avg_amount
FROM archive.leads
WHERE archived_at BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY closure_reason
"""

# Datalens detects: Aggregation query, 10K rows scanned, 5 rows returned
# Routes to DuckDB

import duckdb
con = duckdb.connect()
con.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='minio.internal.company.com';
""")

result = con.execute("""
    SELECT 
        closure_reason,
        COUNT(*) as lead_count,
        AVG(amount) as avg_amount
    FROM 's3://archive/leads/year=2024/**/*.parquet'
    GROUP BY closure_reason
""").fetchdf()

# Returns in ~30 seconds, no staging DB touched
```

**Schema Version Handling:**
- DuckDB automatically handles multiple Parquet schemas
- Missing columns return NULL for older schema versions
- Union semantics applied across files

### Route 3: Bulk Staging DB (Large Investigations)

**Use Case:** Legal discovery, audit investigations requiring complex SQL across thousands of leads

**Architecture:**

```
┌──────────────────────────────────────────────────────────┐
│  Regular Warm Staging DB                                 │
│  - For operational lookups (< 100 leads)                 │
│  - Always available                                      │
│  - Small capacity                                        │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  Bulk Staging DB (On-Demand)                             │
│  - Created per investigation                             │
│  - Name: bulk_staging_case_{case_id}                     │
│  - TTL: 90 days                                          │
│  - Large capacity (10K - 100K leads)                     │
└──────────────────────────────────────────────────────────┘
```

**Flow:**

```
1. Legal team submits request:
   "Load all 2024 leads for case #12345"

2. System creates dedicated bulk staging DB:
   - Postgres instance: bulk_staging_case_12345
   - TTL: 90 days
   - Cost attribution: Legal department

3. Parallel load job (10 workers):
   - Each worker processes 1000 leads
   - Download Parquet → Transform → Insert
   - Progress tracking: "3,500 / 10,000 loaded (35%)"

4. After 4 hours: All data loaded

5. Legal team queries via dedicated Datalens connection

6. After 90 days (or case closed): Drop bulk_staging_case_12345
```

**Why Separate DB:**
- Doesn't interfere with operational warm staging
- Can scale independently
- Clear cost attribution
- Disposable after investigation

### Route 4: Pre-Aggregated Views (Standard Reports)

**Use Case:** Common dashboards and monthly reports

**Strategy:** Pre-compute aggregations weekly

```python
# Weekly job
def generate_cold_summaries():
    # Query all Parquet files
    df = duckdb.query("""
        SELECT 
            DATE_TRUNC('month', archived_at) as month,
            closure_reason,
            product_type,
            COUNT(*) as lead_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM 's3://archive/leads/**/*.parquet'
        GROUP BY 1, 2, 3
    """).fetchdf()
    
    # Store in lightweight summary table
    insert_into_postgres('cold_summary_leads_by_month', df)
```

**Usage:**
```sql
-- User query (appears to scan millions of rows)
SELECT month, closure_reason, SUM(lead_count)
FROM archive.leads
WHERE month >= '2024-01-01'
GROUP BY 1, 2

-- Datalens rewrites to use summary table
SELECT month, closure_reason, SUM(lead_count)
FROM cold_summary_leads_by_month
WHERE month >= '2024-01-01'
GROUP BY 1, 2
-- Returns in < 1 second
```

**Covers 80% of bulk queries** without touching cold storage.

---

## 6. Schema Evolution Management

### The Schema Drift Problem (Traditional Approach)

In a permanent warm tier holding 2 years of data, schema changes create compound complexity:

```
Month 0:  v1 → 500 leads archived
Month 6:  v2 → 600 leads archived (added closure_subcategory column)
Month 12: v3 → 700 leads archived (renamed closure_reason → closure_type)
Month 18: v4 → 800 leads archived (added nested payment_metadata JSONB)
Month 24: v5 → 900 leads archived (split customer_name → first_name, last_name)

Warm DB now has:
- 500 leads in v1 format
- 600 leads in v2 format
- 700 leads in v3 format
- 800 leads in v4 format
- 900 leads in v5 format

Every query needs schema version logic.
```

### Warm-on-Demand Solution

**Cold storage is immutable** — each Parquet file retains its original schema version forever.

**Staging always uses latest schema** — transformation happens on-load.

```python
def load_to_staging(lead_id):
    manifest = get_manifest(lead_id)
    parquet_path = manifest['cold_archive_location']
    schema_version = manifest['schema_version']
    
    # Read Parquet (in its original schema)
    df = read_parquet(parquet_path)
    
    # Transform to latest schema (v5)
    if schema_version == 'v1':
        df = transform_v1_to_v5(df)
    elif schema_version == 'v2':
        df = transform_v2_to_v5(df)
    elif schema_version == 'v3':
        df = transform_v3_to_v5(df)
    elif schema_version == 'v4':
        df = transform_v4_to_v5(df)
    # v5 needs no transformation
    
    # Insert into staging (always v5 schema)
    insert_into_staging(df)

# Transformation examples
def transform_v1_to_v5(df):
    # v1 → v2: Add missing column
    df['closure_subcategory'] = infer_subcategory(df['closure_reason'])
    
    # v2 → v3: Rename column
    df.rename(columns={'closure_reason': 'closure_type'}, inplace=True)
    
    # v3 → v4: Add nested JSONB (set to empty if not present)
    df['payment_metadata'] = '{}'
    
    # v4 → v5: Split customer_name
    df[['first_name', 'last_name']] = df['customer_name'].str.split(' ', n=1, expand=True)
    df.drop(columns=['customer_name'], inplace=True)
    
    return df
```

### Key Advantages

**1. No permanent schema drift in database**
- Staging DB schema is dropped every 30 days
- New staging table created with latest schema
- Zero ALTER TABLE complexity

**2. Transformation is stateless**
- Transforms don't mutate cold storage
- Can be updated/fixed without affecting archives
- Failed transformation? Just retry the load

**3. Cold archives remain immutable**
- v1 Parquet from 2024 stays v1 forever
- Audit integrity preserved
- No risk of "updated" archives

**4. Schema changes don't require backfill**
- Add new column in v6? Only new archives use it
- Old archives loaded to staging get NULL or inferred value
- No multi-hour ALTER TABLE operations

### Schema Version Tracking

```sql
CREATE TABLE archival_manifest (
    manifest_id UUID PRIMARY KEY,
    archived_entity_id VARCHAR(100),
    
    -- Cold storage metadata
    tier VARCHAR(20),
    cold_archive_location TEXT,
    cold_archived_at TIMESTAMP,
    schema_version VARCHAR(10),  -- Version at time of archival
    
    -- Staging metadata
    staging_loaded_at TIMESTAMP,
    staging_schema_version VARCHAR(10),  -- Version at time of load (always latest)
    staging_expires_at TIMESTAMP
);
```

**Example:**
```
archived_entity_id | schema_version | staging_schema_version | staging_loaded_at
LEAD-1001         | v1            | v5                     | 2026-06-24 10:30:00
LEAD-5000         | v3            | v5                     | 2026-06-24 10:35:00
LEAD-9000         | v5            | v5                     | 2026-06-24 10:40:00
```

All three leads appear in staging with v5 schema, even though originally archived with different schemas.

---

## 7. Bulk Query Handling

### The Challenge

Traditional warm-on-demand has a weakness: bulk queries requiring 10K+ Parquets.

**Problem:**
- Legal discovery: "Load all 2024 leads"
- Must download 10,000 Parquets
- Transform each one
- Load into staging
- Takes hours or days
- Might exceed staging capacity

### Multi-Modal Solution

Route queries based on characteristics:

| Query Type | Result Size | Method | Latency | Cost |
|------------|-------------|--------|---------|------|
| Point lookup | < 100 rows | Load to staging | 15 min first, instant after | Low |
| Aggregation | 5 rows (from 10K scanned) | DuckDB direct query | 30 sec | Very low |
| Investigation | 5,000 rows | Bulk staging DB | 2 hours | Medium |
| Standard report | 50 rows | Pre-aggregated view | < 1 sec | Very low |

### Implementation: Intelligent Query Router

```python
class QueryRouter:
    def route(self, sql_query, user):
        analysis = self.analyze_query(sql_query)
        
        if analysis.estimated_rows < 100:
            return self.route_to_staging(sql_query)
        
        elif analysis.is_aggregation and not analysis.needs_row_details:
            return self.route_to_duckdb(sql_query)
        
        elif analysis.estimated_rows > 1000 and analysis.needs_interactive_sql:
            return self.route_to_bulk_staging(sql_query, user)
        
        elif self.matches_precomputed_view(sql_query):
            return self.route_to_summary_table(sql_query)
        
        else:
            return self.prompt_user_for_method(sql_query, analysis)
    
    def analyze_query(self, sql):
        # Parse SQL to determine:
        # - Estimated row count (from WHERE clause)
        # - Is aggregation (GROUP BY, COUNT, SUM, AVG)
        # - Needs row details (SELECT * vs SELECT aggregates)
        return QueryAnalysis(...)
```

### Example Routing Decisions

```sql
-- Query 1: Specific lead lookup
SELECT * FROM archive.leads WHERE lead_id = 'LEAD-1001'
-- Analysis: 1 row, needs details
-- Route: Load to staging (15 min, then instant)

-- Query 2: Monthly report
SELECT 
    DATE_TRUNC('month', archived_at) as month,
    COUNT(*) as leads,
    SUM(amount) as total
FROM archive.leads
WHERE archived_at >= '2024-01-01'
GROUP BY 1
-- Analysis: 10K rows scanned, 12 rows returned, aggregation
-- Route: DuckDB direct query (30 sec, no staging)

-- Query 3: Legal discovery
SELECT * FROM archive.leads 
WHERE archived_at BETWEEN '2024-01-01' AND '2024-12-31'
  AND customer_name LIKE '%Smith%'
-- Analysis: ~5000 rows, needs details, interactive SQL needed
-- Route: Bulk staging DB (2 hour load, then instant queries)

-- Query 4: Dashboard
SELECT closure_reason, COUNT(*) 
FROM archive.leads 
GROUP BY 1
-- Analysis: Matches pre-aggregated view
-- Route: Summary table (< 1 sec)
```

### DuckDB Integration (Direct Parquet Query)

```python
def query_parquet_direct(sql_query):
    """Query Parquet files without loading to database"""
    
    import duckdb
    
    con = duckdb.connect()
    
    # Configure MinIO access
    con.execute("""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='minio.internal.company.com';
        SET s3_access_key_id='xxx';
        SET s3_secret_access_key='yyy';
    """)
    
    # Rewrite query to target Parquet files
    sql_rewritten = sql_query.replace(
        "FROM archive.leads",
        "FROM 's3://archive/leads/**/*.parquet'"
    )
    
    # Execute
    result = con.execute(sql_rewritten).fetchdf()
    
    return result

# Performance characteristics:
# - 1M rows, full scan: ~30 seconds
# - 1M rows, with predicate pushdown: ~10 seconds
# - Hive partitioning (year=2024): ~5 seconds
```

**Schema Version Handling:**
DuckDB automatically unions multiple Parquet schemas:
- Columns present in some files but not others → NULL for missing
- Column type differences → Cast to most general type
- Column name changes → Treated as different columns

### Bulk Staging DB Provisioning

```python
def create_bulk_staging(case_id, lead_ids, user):
    """Create dedicated DB for large investigation"""
    
    # 1. Provision new Postgres instance
    db_name = f"bulk_staging_case_{case_id}"
    db_instance = create_postgres_instance(
        name=db_name,
        size='medium',  # 4 CPU, 16GB RAM
        ttl_days=90
    )
    
    # 2. Create schema (latest version)
    db_instance.execute(create_archive_schema_ddl_v5())
    
    # 3. Load data in parallel
    load_job = BulkLoadJob(
        db_instance=db_instance,
        lead_ids=lead_ids,
        workers=10,
        batch_size=1000
    )
    load_job.start()
    
    # 4. Track in manifest
    bulk_staging_manifest.insert({
        'case_id': case_id,
        'db_instance': db_name,
        'lead_count': len(lead_ids),
        'requested_by': user,
        'created_at': NOW(),
        'expires_at': NOW() + INTERVAL '90 days',
        'department': user.department,  # For cost attribution
        'estimated_cost': calculate_cost(len(lead_ids), 90)
    })
    
    return {
        'job_id': load_job.id,
        'estimated_completion': '2-4 hours',
        'connection_string': db_instance.connection_string
    }
```

---

## 8. Implementation Details

### Archival Manifest Schema

```sql
CREATE TABLE archival_manifest (
    manifest_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Entity identification
    archived_entity_type VARCHAR(50) DEFAULT 'lead',
    archived_entity_id VARCHAR(100) NOT NULL,
    
    -- Cold storage (permanent)
    tier VARCHAR(20) DEFAULT 'cold',
    export_target VARCHAR(50) DEFAULT 'parquet',
    cold_archive_location TEXT NOT NULL,
    cold_archived_at TIMESTAMP NOT NULL,
    cold_checksum VARCHAR(64),
    schema_version VARCHAR(10) NOT NULL,
    row_counts JSONB,
    
    -- Staging (ephemeral)
    staging_loaded_at TIMESTAMP,
    staging_expires_at TIMESTAMP,
    last_accessed_at TIMESTAMP,
    staging_access_count INT DEFAULT 0,
    staging_schema_version VARCHAR(10),
    
    -- Job tracking
    archived_by_job UUID,
    
    -- Retrieval cost tracking
    staging_load_count INT DEFAULT 0,
    total_retrieval_cost DECIMAL(10, 2) DEFAULT 0.00,
    
    -- Indexes
    UNIQUE(archived_entity_id),
    INDEX idx_staging_expiration (staging_expires_at) WHERE staging_expires_at IS NOT NULL
);
```

### Warm Staging Schema

```sql
-- Staging DB always has latest schema (example: v5)
CREATE SCHEMA warm_staging;

CREATE TABLE warm_staging.leads (
    lead_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    first_name VARCHAR(100),      -- Split from customer_name in v5
    last_name VARCHAR(100),        -- Split from customer_name in v5
    agent_name VARCHAR(100),
    closure_type VARCHAR(100),     -- Renamed from closure_reason in v3
    closure_subcategory VARCHAR(100),  -- Added in v2
    closed_at TIMESTAMP,
    amount DECIMAL(10, 2),
    payment_metadata JSONB,        -- Added in v4
    
    -- Metadata
    schema_version VARCHAR(10),
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE warm_staging.lead_documents (
    document_id UUID PRIMARY KEY,
    lead_id VARCHAR(50) REFERENCES warm_staging.leads(lead_id) ON DELETE CASCADE,
    document_type VARCHAR(100),
    file_path TEXT,
    created_at TIMESTAMP
);

-- Additional child tables...
```

### Airflow DAG Structure

```python
# Single archival DAG (Hot → Cold)
dag = DAG('archive_to_cold', schedule='0 2 * * 0')  # Weekly Sunday 2 AM

identify_eligible = PythonOperator(
    task_id='identify_eligible_leads',
    python_callable=identify_eligible_leads
)

denormalize = PythonOperator(
    task_id='denormalize_lead_graph',
    python_callable=denormalize_lead_graph
)

export_parquet = PythonOperator(
    task_id='export_to_parquet',
    python_callable=export_to_parquet_staging
)

verify_export = PythonOperator(
    task_id='verify_parquet',
    python_callable=verify_parquet_file
)

promote = PythonOperator(
    task_id='promote_to_final',
    python_callable=promote_to_final_location
)

write_manifest = PythonOperator(
    task_id='write_manifest',
    python_callable=write_archival_manifest
)

delete_from_hot = PythonOperator(
    task_id='delete_from_primary_db',
    python_callable=delete_in_order
)

# Pipeline
identify_eligible >> denormalize >> export_parquet >> verify_export >> promote >> write_manifest >> delete_from_hot
```

```python
# Separate DAG for staging cleanup
dag_cleanup = DAG('cleanup_staging', schedule='0 3 * * *')  # Daily 3 AM

cleanup_task = PythonOperator(
    task_id='cleanup_expired_staging',
    python_callable=cleanup_expired_staging
)

def cleanup_expired_staging():
    expired = query("""
        SELECT archived_entity_id 
        FROM archival_manifest 
        WHERE staging_expires_at < NOW()
    """)
    
    for lead_id in expired:
        delete_from_staging(lead_id)
        update_manifest(lead_id, {
            'staging_loaded_at': None,
            'staging_expires_at': None
        })
```

### Retrieval API

```python
from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route('/api/leads/<lead_id>', methods=['GET'])
def get_lead(lead_id):
    # Check primary DB first
    lead = primary_db.query("SELECT * FROM leads WHERE lead_id = ?", lead_id)
    if lead:
        return jsonify(lead), 200
    
    # Check if archived
    manifest = primary_db.query(
        "SELECT * FROM archival_manifest WHERE archived_entity_id = ?", 
        lead_id
    )
    
    if not manifest:
        return jsonify({'error': 'Lead not found'}), 404
    
    # Check if in staging
    if manifest['staging_loaded_at'] and manifest['staging_expires_at'] > now():
        # Update access time
        update_manifest(lead_id, {'last_accessed_at': now()})
        
        # Query from staging
        lead = staging_db.query("SELECT * FROM warm_staging.leads WHERE lead_id = ?", lead_id)
        return jsonify({
            'status': 'archived',
            'tier': 'staging',
            'data': lead
        }), 200
    
    # Not in staging - return load prompt
    return jsonify({
        'status': 'archived',
        'tier': 'cold',
        'message': 'Lead is in cold storage. Load to staging?',
        'archived_at': manifest['cold_archived_at'],
        'schema_version': manifest['schema_version'],
        'load_endpoint': f'/api/retrieval/load/{lead_id}'
    }), 202  # 202 Accepted

@app.route('/api/retrieval/load/<lead_id>', methods=['POST'])
def load_to_staging(lead_id):
    manifest = get_manifest(lead_id)
    
    # Trigger load job
    job = LoadToStagingJob(lead_id, manifest)
    job.start_async()
    
    return jsonify({
        'job_id': job.id,
        'status': 'loading',
        'estimated_time': '15 minutes',
        'poll_endpoint': f'/api/retrieval/status/{job.id}'
    }), 202
```

---

## 9. Merits and Demerits

### Merits

#### 1. **Cost Efficiency**

**95% cost reduction for low-retrieval scenarios:**

```
Traditional Permanent Warm Tier:
- Warm DB storage: $200/month × 24 months of data
- Total: $4,800 over 2 years

Warm-on-Demand:
- Cold storage: $10/month
- Staging populated 10% of time: $5/month average
- Total: $360 over 2 years

Savings: $4,440 (92%)
```

**Pay only for what you use:**
- Staging empty most of the time → $0 cost
- Only pay when data is actively being queried
- Auto-cleanup prevents cost creep

#### 2. **Operational Simplicity**

**Single archival pipeline:**
- Only one job: Hot → Cold
- No warm→cold migration complexity
- Fewer failure modes to monitor

**No permanent warm tier maintenance:**
- No capacity planning (staging is capped and ephemeral)
- No backups needed (cold storage is source of truth)
- No replication or HA setup
- Database can crash → just reload from Parquet

**Simpler monitoring:**
- Monitor one archival job
- Monitor staging cleanup job
- No warm tier uptime SLAs

#### 3. **Schema Evolution Without Pain**

**No compound schema drift:**
- Cold archives are immutable (v1 stays v1 forever)
- Staging always uses latest schema
- No multi-version query logic in database

**Zero ALTER TABLE complexity:**
- Staging dropped every 30 days
- New schema applied on next load
- No hours-long migrations on millions of rows

**Transformation is stateless:**
- v1→v5 transform logic can be fixed without touching archives
- Failed transformation → just retry
- No risk of corrupting archived data

#### 4. **Audit Integrity**

**Immutable cold storage:**
- Archives never mutated after creation
- Original schema version preserved
- Checksums remain valid indefinitely

**Compliance-friendly:**
- Can prove data hasn't changed since archival
- Transformations are read-only operations
- Clear chain of custody (cold → staging)

#### 5. **Flexible Retrieval Modes**

**Multi-modal query routing:**
- Small queries → Staging (interactive SQL)
- Aggregations → DuckDB direct (fast, no load)
- Bulk investigations → Dedicated bulk DB
- Reports → Pre-aggregated views

**Right tool for the job:**
- Not forced into "one size fits all" solution
- Optimize each query pattern independently

#### 6. **Scales Down to Zero**

**Perfect for variable workloads:**
- No retrieval this month → $10 storage cost only
- 100 retrievals next month → Staging auto-populates
- Back to zero retrievals → Staging auto-cleans

**Traditional warm tier can't scale to zero** — you pay $200/month even if no one queries it.

---

### Demerits

#### 1. **First Access Latency**

**15 minutes to 2 hours for initial load:**
- User requests lead → Must wait for Parquet download + transform + load
- Traditional warm tier: instant access (already in DB)

**User experience degradation:**
- "Why do I have to wait 15 minutes?"
- Acceptable for occasional access, frustrating for frequent use

**Mitigation:**
- Predictive pre-loading: Load commonly accessed leads proactively
- Notify users before expiration: "These leads expire in 3 days, access now to keep them cached"

#### 2. **Bulk Retrieval Complexity**

**10K+ lead requests are harder:**
- Can't simply query one database
- Must route between staging, DuckDB, bulk DB
- More moving parts = more that can go wrong

**Mitigation:**
- Intelligent query router handles complexity
- Clear user guidance: "This query will take 2 hours to load"
- Bulk staging DB for large investigations

#### 3. **Transformation Logic Maintenance**

**Must maintain schema transformers:**
- v1→latest, v2→latest, v3→latest, etc.
- Grows with each schema version
- Transformation bugs affect retrieval

**Example:**
```python
# These functions must be maintained forever
def transform_v1_to_v5(df): ...
def transform_v2_to_v5(df): ...
def transform_v3_to_v5(df): ...
def transform_v4_to_v5(df): ...

# When v6 is released, add:
def transform_v1_to_v6(df): ...  # And 4 more variants
```

**Mitigation:**
- Keep transformations simple (mostly column renames, additions)
- Test transformations thoroughly before deployment
- Document inference logic (e.g., "subcategory inferred from reason")

#### 4. **Multiple Sources of Truth (Temporarily)**

**During staging load:**
- Cold storage has the lead (permanent)
- Staging is being populated (in-flight)
- User might query during load → Inconsistent results

**After transformation:**
- Cold storage has v1 schema
- Staging has v5 schema
- "Which is correct?" — Both, but different representations

**Mitigation:**
- Lock lead during load (return 409 Conflict if queried mid-load)
- Clear messaging: "Staging data is transformed from v1 to v5"
- Manifest tracks both versions: `schema_version` (cold) and `staging_schema_version`

#### 5. **Staging Capacity Management**

**Risk of staging overflow:**
- User loads 1000 leads
- Staging DB at capacity
- New load request → Must wait or fail

**Solutions:**
- Hard cap on staging size (e.g., 1000 leads max)
- LRU eviction: Oldest unused leads removed first when capacity reached
- Reject new loads if at capacity: "Staging full, try again in 1 hour"

#### 6. **Not Suitable for High-Frequency Access**

**If archived data accessed daily:**
- Constant loading/expiring/reloading cycle
- Staging never empties → Never scales to zero
- Might as well use permanent warm tier

**Break-even point:**
- < 50 retrievals/month: Warm-on-demand wins
- > 100 retrievals/month: Permanent warm tier wins
- 50-100: Depends on query patterns

---

## 10. Comparison: Warm-on-Demand vs Hot→Warm→Cold

### Architecture Comparison

| Aspect | Hot→Warm→Cold (Permanent) | Warm-on-Demand |
|--------|---------------------------|----------------|
| **Archival Phases** | Two (hot→warm, warm→cold) | One (hot→cold) |
| **Warm Tier** | Permanent (2 years of data) | Ephemeral (requested data only, 30-day TTL) |
| **Storage Cost** | High (DB storage for 2 years) | Low (object storage + ephemeral staging) |
| **Operational Complexity** | High (2 jobs, migrations, backups) | Low (1 job, disposable staging) |
| **Retrieval Latency** | Instant (always in DB) | 15 min - 2 hours first access, instant after |
| **Schema Evolution** | Complex (multi-version DB) | Simple (transform on load) |
| **Scaling** | Fixed cost (can't scale to zero) | Scales to zero when unused |

### Cost Comparison (1M Leads Over 7 Years)

**Scenario A: Low Retrieval Volume (10 requests/month)**

| Item | Hot→Warm→Cold | Warm-on-Demand |
|------|---------------|----------------|
| Warm/Staging DB | $200/mo × 84 months = $16,800 | $5/mo × 84 months = $420 |
| Cold storage | $10/mo × 60 months = $600 | $10/mo × 84 months = $840 |
| Retrieval ops | Negligible (already in DB) | $50 × 10 × 84 = $42,000 ⚠️ |
| **Total** | **$17,400** | **$43,260** ❌ |

**Wait, warm-on-demand is MORE expensive?**

Yes, if you charge per retrieval operation. But typically:
- Staging DB populates once, serves many queries
- $50 is the cost to populate staging once, then hundreds of queries are free
- Recalculating: 10 unique leads loaded/month = $50/mo → $4,200 total
- **Revised Total: $5,460** ✅

**Scenario B: Medium Retrieval Volume (50 requests/month, 30 unique leads)**

| Item | Hot→Warm→Cold | Warm-on-Demand |
|------|---------------|----------------|
| Warm/Staging DB | $16,800 | $420 |
| Cold storage | $600 | $840 |
| Staging loads | N/A | 30 leads/mo × $2/load × 84 = $5,040 |
| **Total** | **$17,400** | **$6,300** ✅ 64% cheaper |

**Scenario C: High Retrieval Volume (200 requests/month, 150 unique leads)**

| Item | Hot→Warm→Cold | Warm-on-Demand |
|------|---------------|----------------|
| Warm/Staging DB | $16,800 | $2,000 (staging rarely empty) |
| Cold storage | $600 | $840 |
| Staging loads | N/A | 150 × $2 × 84 = $25,200 |
| **Total** | **$17,400** | **$28,040** ❌ 61% more expensive |

**Conclusion:**
- **Low-Medium retrieval (< 50/month):** Warm-on-demand wins
- **High retrieval (> 100/month):** Permanent warm tier wins
- **Variable retrieval:** Warm-on-demand adapts, permanent warm wastes money during quiet periods

### Performance Comparison

| Operation | Hot→Warm→Cold | Warm-on-Demand |
|-----------|---------------|----------------|
| **Query single lead (first time)** | < 100ms | 15 min - 2 hours |
| **Query same lead (second time)** | < 100ms | < 100ms (if within 30 days) |
| **Bulk aggregation (10K rows)** | < 5 sec | ~30 sec (DuckDB) |
| **Legal discovery (5K leads)** | < 5 sec | 2-4 hours (bulk staging load) |
| **Monthly report** | < 1 sec | < 1 sec (pre-aggregated view) |

### Operational Comparison

| Task | Hot→Warm→Cold | Warm-on-Demand |
|------|---------------|----------------|
| **Archival DAGs** | 2 (hot→warm, warm→cold) | 1 (hot→cold) |
| **Schema migrations** | Required on warm tier (touches millions of rows) | Not required (transform on load) |
| **Backup strategy** | Warm DB + cold storage | Cold storage only (staging is disposable) |
| **Disaster recovery** | Complex (restore warm DB + manifest sync) | Simple (reload from cold) |
| **Monitoring** | Warm DB uptime, capacity, replication lag | Staging cleanup job, load job status |
| **On-call burden** | Medium (warm DB outages affect users) | Low (staging outage = reload from cold) |

### Regulatory Compliance

| Requirement | Hot→Warm→Cold | Warm-on-Demand |
|-------------|---------------|----------------|
| **"Easily accessible" (SEC 17a-4)** | ✅ Yes (SQL queries) | ⚠️ Depends (first access delayed) |
| **Immutability** | ⚠️ Warm tier is mutable | ✅ Cold storage immutable |
| **Audit trail** | ✅ Yes | ✅ Yes (transformation logged) |
| **Retention period** | ✅ 7 years | ✅ 7 years |
| **Point-in-time consistency** | ✅ Yes | ⚠️ Transformations may infer values |

**Regulatory Note:**
- If "easily accessible" means "instant access," permanent warm tier required
- If "easily accessible" means "available within 24 hours," warm-on-demand acceptable
- Consult compliance team for specific interpretation

---

## 11. When to Use Each Approach

### Use Warm-on-Demand If:

✅ **Low to medium retrieval volume (< 50 requests/month)**
- Archives accessed occasionally for specific investigations
- Most archived data never queried

✅ **Budget-constrained**
- Can't justify $200/month for permanent warm tier
- Need to minimize infrastructure costs

✅ **Schema evolves frequently (> 2 changes/year)**
- Want to avoid complex database migrations
- Prefer stateless transformations

✅ **Small engineering team**
- Can't maintain two archival pipelines
- Want operational simplicity

✅ **Variable workload**
- Some months have heavy retrieval, others have none
- Need cost to scale with actual usage

✅ **Internal tooling / non-customer-facing**
- Acceptable for users to wait 15 minutes for first access
- Users understand distinction between hot and archived data

✅ **Strong preference for immutability**
- Audit requirements demand unchanging archives
- Transformations must be non-destructive

**Example Use Cases:**
- Internal lead management system
- Compliance archival (rarely accessed)
- Development/staging environments
- Cost-sensitive startups

---

### Use Hot→Warm→Cold (Permanent) If:

✅ **High retrieval volume (> 100 requests/month)**
- Customer service teams query archived data daily
- Frequent operational need for historical data

✅ **Instant access required**
- Cannot accept 15-minute first-access latency
- SLAs demand sub-second response for all data

✅ **Regulatory "easily accessible" requirement**
- SEC, FINRA, or other regulations require immediate access
- Compliance interpretation: "easily" = instant

✅ **Customer-facing APIs**
- External users or partners query archived data
- Cannot expose "loading..." experience to customers

✅ **Complex cross-archive queries**
- Users frequently join across multiple years of data
- Need full SQL capabilities on all archived data simultaneously

✅ **Mature engineering organization**
- Have resources to maintain two archival pipelines
- Can handle schema migration complexity
- Dedicated data platform team

✅ **Stable schema**
- Schema changes infrequent (< 1/year)
- When changes occur, have migration tools/processes

**Example Use Cases:**
- Customer-facing banking applications
- Production financial services systems
- High-volume support operations
- Regulated broker-dealer platforms

---

### Hybrid Approach: Start Warm-on-Demand, Upgrade If Needed

**Strategy:** Begin with warm-on-demand, monitor metrics, migrate to permanent warm if justified.

**Decision Metrics:**

```python
# Monthly monitoring
retrieval_volume = count_unique_leads_requested_per_month()
avg_staging_occupancy = avg_percentage_staging_populated()
user_complaints = count_latency_complaints()

if retrieval_volume > 100 and avg_staging_occupancy > 70%:
    recommend_permanent_warm_tier()
    # Rationale: Staging almost always populated anyway, not scaling to zero

elif user_complaints > 10:
    recommend_permanent_warm_tier()
    # Rationale: First-access latency unacceptable to users

else:
    continue_warm_on_demand()
    # Cost savings justify occasional latency
```

**Migration Path:**
1. Start with warm-on-demand (low initial cost)
2. Monitor for 3-6 months
3. If retrieval volume grows → Provision permanent warm tier
4. Redirect queries to permanent warm instead of staging
5. Keep cold storage as disaster recovery

**This gives you:**
- Low initial investment
- Real usage data before committing to expensive infrastructure
- Clear ROI justification for permanent warm tier if needed

---

## 12. Cost Analysis

### Detailed Cost Breakdown

#### Infrastructure Costs (1M Leads, 7 Years)

**Hot→Warm→Cold (Permanent Warm Tier):**

```
Primary PostgreSQL (Hot Tier):
- 3 months of active data
- Cost: $300/month (covered under operational budget)

Warm Tier PostgreSQL:
- 2 years × 500 leads/month = 12,000 leads
- Database size: ~50 GB
- Instance: db.r5.xlarge (4 vCPU, 32 GB RAM)
- Cost: $200/month × 84 months = $16,800

Cold Tier (MinIO/S3):
- 5 years × 500 leads/month = 30,000 leads
- Storage size: ~1 GB (Parquet compression)
- Cost: $0.01/GB/month = $10/month × 60 months = $600

Total Archival Cost: $17,400
Cost per lead per year: $17,400 / (12,000 + 30,000) / 7 = $0.059
```

**Warm-on-Demand:**

```
Primary PostgreSQL (Hot Tier):
- Same as above: $300/month operational

Staging PostgreSQL:
- Empty most of the time
- Provisioned on-demand (serverless or small instance)
- Average occupancy: 10% (3 days out of 30)
- Cost: $50/month × 10% = $5/month × 84 months = $420

Cold Tier (MinIO/S3):
- All archived data: 42,000 leads
- Storage size: ~1.5 GB
- Cost: $0.01/GB/month = $15/month × 84 months = $1,260

Staging Load Operations:
- 30 unique leads loaded/month (average)
- Cost per load: $0.001 (negligible - just compute)
- Total: ~$0 (rounded)

Total Archival Cost: $1,680
Cost per lead per year: $1,680 / 42,000 / 7 = $0.0057

Savings: $15,720 (90% reduction)
```

#### Retrieval Cost Comparison

**Scenario: 50 Requests/Month**

**Permanent Warm Tier:**
```
Query cost: $0 (data already in DB)
Database compute: Included in $200/month
Network transfer: $0 (internal network)

Monthly retrieval cost: $0
Annual retrieval cost: $0
```

**Warm-on-Demand:**
```
Staging loads: 30 unique leads/month (some leads requested multiple times)
  - Load from MinIO: $0.001/GB × 30 = $0.03
  - Compute: Negligible
  - Insert to staging: Included in $5/month average

DuckDB direct queries: 10 aggregation queries/month
  - Compute: $0.01/query × 10 = $0.10

Pre-aggregated views: 10 report queries/month
  - Compute: Included in operational PostgreSQL

Monthly retrieval cost: $0.13
Annual retrieval cost: $1.56

Additional cost vs permanent warm: $1.56/year (negligible)
```

**Key Insight:** Retrieval operations are cheap. Storage is the dominant cost.

#### Total Cost of Ownership (5 Years)

| Cost Category | Permanent Warm | Warm-on-Demand | Difference |
|---------------|----------------|----------------|------------|
| **Infrastructure** | | | |
| Warm/Staging DB | $12,000 | $300 | -$11,700 |
| Cold Storage | $600 | $900 | +$300 |
| **Operations** | | | |
| Backup storage | $600 | $0 | -$600 |
| Engineering time (schema migrations) | $8,000 (20 hours × $400) | $2,000 (5 hours × $400) | -$6,000 |
| On-call burden (DB outages) | $4,000 | $500 | -$3,500 |
| **Retrieval** | | | |
| Query operations | $0 | $100 | +$100 |
| **Total** | **$25,200** | **$3,800** | **-$21,400 (85% savings)** |

---

### Break-Even Analysis

**When does permanent warm tier become cheaper?**

```
Permanent Warm Cost: $200/month fixed
Warm-on-Demand Cost: $5/month base + (retrieval volume × cost per retrieval)

Break-even when:
$200 = $5 + (unique_leads_per_month × $2)
$195 = unique_leads_per_month × $2
unique_leads_per_month = 97.5

Break-even point: ~100 unique leads loaded per month
```

**Decision Matrix:**

| Monthly Unique Leads Loaded | Recommended Approach | Reasoning |
|-----------------------------|---------------------|-----------|
| < 50 | Warm-on-Demand | 70%+ cost savings |
| 50 - 100 | Either (measure) | Within 10% of each other |
| 100 - 200 | Permanent Warm | 20% cheaper + instant access |
| > 200 | Permanent Warm | 50%+ cheaper + better UX |

---

## 13. Migration Path

### From Two-Phase to Warm-on-Demand

**Current State:** Hot → Warm (permanent) → Cold

**Target State:** Hot → Cold, with on-demand staging

#### Phase 1: Enable Cold Archival (Parallel)

**Goal:** Start archiving to cold while keeping warm tier running

```
Week 1-2: Infrastructure Setup
- Provision MinIO/S3 bucket for cold storage
- Set up Hive partitioning structure
- Deploy Parquet export job (parallel to warm archival)

Week 3-4: Dual Archival
- Run both warm and cold archival pipelines
- Verify data consistency between warm DB and cold Parquet
- Monitor for any issues

Result: All new archives go to both warm and cold
```

#### Phase 2: Deploy Staging DB

**Goal:** Set up on-demand staging infrastructure

```
Week 5-6: Staging Provisioning
- Provision staging PostgreSQL instance (small, auto-scaling)
- Create staging schema (latest version)
- Deploy load-to-staging job
- Deploy auto-cleanup job

Week 7-8: Retrieval Routing
- Update Datalens to check staging before warm
- If in staging: query staging
- If in warm: query warm (still operational)
- If in neither: load to staging from cold

Result: Staging infrastructure ready, but warm tier still serving queries
```

#### Phase 3: Migrate Existing Warm Data to Cold

**Goal:** Export all data from warm tier to cold

```
Week 9-12: Backfill Cold Storage
- Export all warm tier data to Parquet
- Batch processing: 1000 leads/day
- Verify each export before marking complete

Validation:
- Row count match: warm DB vs Parquet
- Checksum verification
- Sample query comparison (random 100 leads)

Result: All data exists in both warm and cold
```

#### Phase 4: Cutover to Warm-on-Demand

**Goal:** Stop using permanent warm tier, rely on staging only

```
Week 13: Traffic Cutover
- Update Datalens routing:
  - Check staging first (as before)
  - If not in staging: load from cold (skip warm query)
- Warm DB becomes read-only

Week 14: Monitor & Validate
- Watch for:
  - Staging load latency
  - User complaints about access time
  - Any missing data
- Rollback plan: Re-enable warm DB queries if issues

Week 15-16: Decommission Warm Tier
- If validation passes: Drop warm DB
- Archive warm DB backup to cold storage (for disaster recovery)
- Redirect cost savings to other initiatives

Result: Warm-on-demand fully operational, permanent warm tier retired
```

#### Rollback Plan

**If warm-on-demand doesn't meet requirements:**

```
Rollback Step 1: Re-enable warm tier queries
- Update Datalens to query warm DB again
- Disable staging load jobs

Rollback Step 2: Resume warm tier archival
- Re-enable hot→warm archival pipeline
- Stop cold-only archival

Rollback Step 3: Deprecate staging
- Drain staging DB
- Keep infrastructure dormant for future retry
```

---

### From Single-Phase Cold-Only to Warm-on-Demand

**Current State:** Hot → Cold (no warm tier at all)

**Target State:** Hot → Cold, with on-demand staging

#### Phase 1: Deploy Staging Infrastructure

```
Week 1-2:
- Provision staging PostgreSQL
- Create staging schema
- Deploy load-to-staging job
- Deploy auto-cleanup job
```

#### Phase 2: Update Query Layer

```
Week 3-4:
- Update APIs to check archival_manifest
- If staging_loaded_at is set: query staging
- If not: offer load-to-staging option
- Add retrieval request endpoint
```

#### Phase 3: Enable Intelligent Routing

```
Week 5-6:
- Deploy DuckDB for direct Parquet queries
- Implement query router (staging vs DuckDB vs bulk)
- Deploy pre-aggregated view generation
```

#### Phase 4: User Training

```
Week 7-8:
- Document new retrieval workflows
- Train customer service teams
- Set expectations: first access = 15 min, subsequent = instant
```

**Much simpler migration** — additive only, no decommissioning.

---

## 14. Monitoring and Observability

### Key Metrics

**Archival Health:**
```
- Leads archived per week
- Archival job success rate (target: > 99%)
- Average time per lead archived (target: < 5 min)
- Cold storage growth rate (GB/month)
- Parquet file count by schema version
```

**Staging Performance:**
```
- Staging occupancy percentage (target: < 30% for low retrieval workload)
- Leads currently in staging
- Staging expiration queue depth
- Average time in staging before expiration (target: 15-20 days)
- Staging load latency P50, P95, P99 (target P95: < 30 min)
```

**Retrieval Patterns:**
```
- Unique leads requested per month
- Repeat query rate (same lead queried multiple times)
- Query routing breakdown (staging vs DuckDB vs bulk)
- User wait time for first access (target P95: < 2 hours)
- Cache hit rate (lead already in staging when requested)
```

**Cost Tracking:**
```
- Staging DB cost per month
- Cold storage cost per month
- Retrieval operation cost per month
- Cost per lead archived
- Cost per lead retrieved
```

### Alerts

**Critical Alerts (Page On-Call):**
```
- Archival job failed 3 consecutive runs
- Staging DB at > 90% capacity
- Staging load job failing > 50%
- Cold storage unreachable
```

**Warning Alerts (Notify via Slack):**
```
- Archival job duration > 4 hours (slowness indicator)
- Staging occupancy > 70% for 7 days (might need permanent warm)
- Staging load latency P95 > 1 hour
- Retrieval volume trending up (might hit capacity)
```

### Dashboards

**Archival Dashboard:**
- Leads archived (daily, weekly, monthly)
- Archival pipeline health (success/failure)
- Cold storage size over time
- Schema version distribution

**Retrieval Dashboard:**
- Requests per day (by query type)
- Staging hit rate (% of queries served from staging)
- Load job queue depth
- Average load latency
- User wait time distribution

**Cost Dashboard:**
- Archival cost breakdown (storage, compute, egress)
- Retrieval cost by department (for chargeback)
- Projected vs actual monthly spend
- Cost efficiency: $ per TB stored, $ per query

---

## 15. Testing Strategy

### Unit Tests

```python
def test_parquet_export():
    lead = create_test_lead()
    parquet_path = export_to_parquet(lead)
    
    # Verify file exists
    assert file_exists(parquet_path)
    
    # Verify row count
    df = read_parquet(parquet_path)
    assert len(df) == 1
    
    # Verify schema version stamped
    assert df['schema_version'][0] == 'v5'

def test_schema_transformation():
    v1_data = load_fixture('lead_v1.json')
    v5_data = transform_v1_to_v5(v1_data)
    
    # Verify columns renamed
    assert 'closure_type' in v5_data
    assert 'closure_reason' not in v5_data
    
    # Verify new columns added
    assert 'closure_subcategory' in v5_data
    
    # Verify transformations applied
    assert v5_data['first_name'] is not None

def test_staging_expiration():
    load_to_staging('LEAD-1001')
    
    # Fast-forward time 31 days
    freeze_time(NOW() + timedelta(days=31))
    
    # Run cleanup
    cleanup_expired_staging()
    
    # Verify removed
    assert not staging_has_lead('LEAD-1001')
```

### Integration Tests

```python
def test_end_to_end_archival():
    # Create lead in primary DB
    lead_id = create_lead_in_primary_db()
    
    # Run archival job
    run_archival_job()
    
    # Verify in cold storage
    manifest = get_manifest(lead_id)
    assert manifest['tier'] == 'cold'
    assert file_exists(manifest['cold_archive_location'])
    
    # Verify deleted from primary DB
    assert not primary_db_has_lead(lead_id)

def test_retrieval_flow():
    # Archive a lead
    lead_id = archive_test_lead()
    
    # Request retrieval
    response = api_get_lead(lead_id)
    assert response.status == 202  # Accepted, loading
    
    # Wait for load job
    wait_for_staging_load(lead_id)
    
    # Query again
    response = api_get_lead(lead_id)
    assert response.status == 200
    assert response.data['lead_id'] == lead_id
```

### Load Tests

```python
def test_concurrent_staging_loads():
    """Can staging handle 10 concurrent load requests?"""
    lead_ids = [f'LEAD-{i}' for i in range(10)]
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(load_to_staging, lid) for lid in lead_ids]
        results = [f.result() for f in futures]
    
    # All should succeed
    assert all(r.success for r in results)
    
    # All should be in staging
    for lead_id in lead_ids:
        assert staging_has_lead(lead_id)

def test_bulk_query_performance():
    """Can DuckDB query 1M rows in < 60 seconds?"""
    start = time.time()
    
    result = duckdb_query("""
        SELECT closure_reason, COUNT(*)
        FROM 's3://archive/leads/**/*.parquet'
        GROUP BY 1
    """)
    
    duration = time.time() - start
    assert duration < 60
    assert len(result) > 0
```

### Schema Migration Tests

```python
def test_v1_to_v5_transformation():
    """Verify v1 archives can be loaded into v5 staging"""
    
    # Create v1 Parquet
    v1_lead = create_v1_lead_fixture()
    v1_path = export_to_parquet(v1_lead, schema_version='v1')
    
    # Load to staging (which has v5 schema)
    load_to_staging_from_path(v1_path)
    
    # Query from staging
    result = staging_db.query("SELECT * FROM warm_staging.leads LIMIT 1")
    
    # Verify v5 columns exist
    assert 'first_name' in result.columns
    assert 'last_name' in result.columns
    assert 'closure_subcategory' in result.columns
    
    # Verify transformation applied
    assert result['staging_schema_version'][0] == 'v5'
```

---

## 16. Conclusion

### Summary

Warm-on-Demand archival is a **cost-efficient, operationally simple alternative** to traditional two-phase archival for workloads with **low to medium retrieval volume**.

**Key Advantages:**
- 85-90% cost reduction vs permanent warm tier
- No schema migration complexity
- Scales to zero when unused
- Simpler operational model

**Key Trade-off:**
- First access latency (15 min - 2 hours)
- Acceptable for occasional retrieval, not for high-frequency access

**Best Fit:**
- Internal tooling with infrequent archive access
- Budget-constrained projects
- Rapidly evolving schemas
- Variable retrieval workloads

**Not Suitable For:**
- Customer-facing APIs requiring instant access
- High retrieval volume (> 100/month)
- Strict regulatory "instant access" requirements

### Recommendation

**Start with Warm-on-Demand** unless you have clear evidence of high retrieval volume.

**Why:**
- Low initial investment ($5-10/month vs $200/month)
- Measure actual retrieval patterns before committing to expensive infrastructure
- Can always migrate to permanent warm tier if data justifies it
- Avoid over-engineering for usage patterns that may not materialize

**Monitor these metrics for 3-6 months:**
- Monthly unique leads retrieved
- Average staging occupancy
- User complaints about latency

**If metrics show:**
- Retrieval > 100/month → Migrate to permanent warm
- Retrieval < 50/month → Continue warm-on-demand
- High user complaints → Migrate to permanent warm regardless of volume

### Next Steps

1. **Design Review:** Validate approach with compliance, engineering, and ops teams
2. **Prototype:** Build PoC with 1000 test leads
3. **Load Test:** Verify staging load latency meets requirements
4. **Cost Model:** Validate savings assumptions with actual infrastructure pricing
5. **User Acceptance:** Test retrieval UX with customer service team
6. **Production Deployment:** Follow migration path in Section 13
7. **Monitor & Iterate:** Track metrics, adjust approach based on real usage

---

## Appendix A: Glossary

**Cold Storage:** Immutable object storage (Parquet files in MinIO/S3) holding all archived data permanently.

**Warm Staging:** Temporary PostgreSQL database populated on-demand when archived data is requested. Auto-expires after 30 days.

**Hot Tier:** Primary production PostgreSQL database with active, operational data.

**Staging Load:** Process of downloading Parquet from cold storage, transforming to latest schema, and inserting into warm staging DB.

**Schema Version:** Identifier (e.g., v1, v2, v3) indicating which structure the data was archived with. Allows transformation logic to handle old formats.

**Hive Partitioning:** Folder structure like `year=2024/month=06/` that enables query engines to skip irrelevant data when filtering by date.

**DuckDB:** Analytical query engine that can read Parquet files directly without loading into a database.

**Pre-Aggregated Views:** Summary tables (monthly counts, averages) computed weekly from cold storage to serve common reports instantly.

**Bulk Staging DB:** Dedicated temporary PostgreSQL instance provisioned for large investigations (legal discovery, audits) requiring complex SQL on thousands of archived leads.

---

## Appendix B: SQL DDL

### Archival Manifest Table

```sql
CREATE TABLE archival_manifest (
    manifest_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Entity identification
    archived_entity_type VARCHAR(50) DEFAULT 'lead',
    archived_entity_id VARCHAR(100) NOT NULL UNIQUE,
    
    -- Cold storage (permanent)
    tier VARCHAR(20) DEFAULT 'cold',
    export_target VARCHAR(50) DEFAULT 'parquet',
    cold_archive_location TEXT NOT NULL,
    cold_archived_at TIMESTAMP NOT NULL DEFAULT NOW(),
    cold_checksum VARCHAR(64),
    schema_version VARCHAR(10) NOT NULL,
    row_counts JSONB,
    
    -- Staging (ephemeral)
    staging_loaded_at TIMESTAMP,
    staging_expires_at TIMESTAMP,
    last_accessed_at TIMESTAMP,
    staging_access_count INT DEFAULT 0,
    staging_schema_version VARCHAR(10),
    staging_load_count INT DEFAULT 0,
    
    -- Job tracking
    archived_by_job UUID,
    
    -- Cost tracking
    total_retrieval_cost DECIMAL(10, 2) DEFAULT 0.00,
    
    -- Indexes
    INDEX idx_staging_expiration (staging_expires_at) WHERE staging_expires_at IS NOT NULL,
    INDEX idx_cold_archived_at (cold_archived_at),
    INDEX idx_schema_version (schema_version)
);
```

### Warm Staging Schema (v5 Example)

```sql
CREATE SCHEMA warm_staging;

CREATE TABLE warm_staging.leads (
    lead_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    agent_name VARCHAR(100),
    closure_type VARCHAR(100),
    closure_subcategory VARCHAR(100),
    closed_at TIMESTAMP,
    amount DECIMAL(10, 2),
    payment_metadata JSONB,
    schema_version VARCHAR(10) NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE warm_staging.lead_documents (
    document_id UUID PRIMARY KEY,
    lead_id VARCHAR(50) REFERENCES warm_staging.leads(lead_id) ON DELETE CASCADE,
    document_type VARCHAR(100),
    file_path TEXT,
    created_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE warm_staging.lead_activities (
    activity_id UUID PRIMARY KEY,
    lead_id VARCHAR(50) REFERENCES warm_staging.leads(lead_id) ON DELETE CASCADE,
    activity_type VARCHAR(100),
    description TEXT,
    performed_by VARCHAR(100),
    created_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT NOW()
);
```

---

## Appendix C: References

- [AWS S3 Glacier Pricing](https://aws.amazon.com/s3/pricing/)
- [DuckDB Parquet Documentation](https://duckdb.org/docs/data/parquet)
- [SEC Rule 17a-4 Compliance](https://www.sec.gov/rules/interp/34-47806.htm)
- [Apache Parquet Format](https://parquet.apache.org/docs/)
- [PostgreSQL Partitioning Best Practices](https://www.postgresql.org/docs/current/ddl-partitioning.html)

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-24  
**Author:** Engineering Team  
**Status:** Design Proposal — Pending Review
