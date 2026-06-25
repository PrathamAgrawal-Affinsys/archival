# Hybrid Two-Phase Archival Strategy
## Hot (Primary) → Warm (Denormalized) → Cold (Parquet)

**PostgreSQL + Parquet Archival Strategy**

| Field | Value |
| :---- | :---- |
| Document Type | Hybrid Archival Design Document |
| Strategy | Two-Phase: Primary → Warm Denormalized DB → Cold Parquet |
| Warm Tier | Denormalized read-optimized PostgreSQL |
| Cold Tier | Parquet in MinIO with intelligent retrieval |
| Archive Trigger | Phase 1: 3 months, Phase 2: 2 years |
| Author | Engineering |
| Status | Design Proposal |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Core Concept & Philosophy](#2-core-concept--philosophy)
3. [Architecture Overview](#3-architecture-overview)
4. [Phase 1: Hot → Warm (Denormalized)](#4-phase-1-hot--warm-denormalized)
5. [Phase 2: Warm → Cold (Parquet)](#5-phase-2-warm--cold-parquet)
6. [Robust Retrieval Strategy](#6-robust-retrieval-strategy)
7. [Schema Evolution Strategy](#7-schema-evolution-strategy)
8. [Implementation Details](#8-implementation-details)
9. [Merits and Demerits](#9-merits-and-demerits)
10. [Use Cases and When to Use](#10-use-cases-and-when-to-use)
11. [Operational Playbook](#11-operational-playbook)
12. [Cost Analysis](#12-cost-analysis)

---

## 1. Executive Summary

### The Hybrid Approach

This strategy combines the best of both worlds:
- **Permanent warm tier** for instant SQL access to recent archives
- **Denormalized schema** in warm tier for read performance and simpler queries
- **Cold Parquet storage** for long-term cost-efficient compliance
- **Intelligent retrieval** with multiple modes (instant, staged, bulk)

### Key Innovation: Denormalization at Archival

Unlike traditional approaches that preserve normalized schema in the archive, this strategy **denormalizes at the point of archival**:

```
Hot Tier (Normalized):
  leads ← FK → closure_reasons
  leads ← FK → agents
  leads ← FK → product_types
  leads → 1:N → documents
  leads → 1:N → activities
  
Warm Tier (Denormalized):
  archive.leads (all data flattened, no FKs)
    - closure_reason_name (resolved)
    - agent_name (resolved)
    - product_type_name (resolved)
    - documents[] (embedded JSONB array)
    - activities[] (embedded JSONB array)
```

**Benefits:**
- ✅ No JOINs needed in warm tier (faster queries)
- ✅ Simpler for analysts (flat table structure)
- ✅ No FK constraints to manage
- ✅ Self-contained records (no dependency on reference tables)

---

## 2. Core Concept & Philosophy

### Design Principles

**1. Denormalize for Read Performance**
```
Query in Normalized Schema (Primary DB):
  SELECT l.*, cr.name, a.name, pt.name
  FROM leads l
  JOIN closure_reasons cr ON l.closure_reason_id = cr.id
  JOIN agents a ON l.agent_id = a.id
  JOIN product_types pt ON l.product_type_id = pt.id
  WHERE l.lead_id = 'LEAD-1001';
  
  → 4 table joins, 4 index lookups

Query in Denormalized Schema (Warm Archive):
  SELECT * FROM archive.leads WHERE lead_id = 'LEAD-1001';
  
  → Single table, single index lookup
  → 10x faster
```

**2. Embed Children as JSONB**
```sql
-- Instead of separate child tables:
CREATE TABLE archive.lead_documents (...);  -- ❌ Multiple tables

-- Embed as JSONB array:
CREATE TABLE archive.leads (
    lead_id VARCHAR(50) PRIMARY KEY,
    documents JSONB,  -- [{doc_id: 1, type: 'proof'}, ...]
    activities JSONB,
    payments JSONB
);  -- ✅ Single table
```

**3. Optimize for 90% Use Case**
- 90% of queries: "Show me this one lead"
- 9% of queries: "Aggregate across leads"
- 1% of queries: "Complex multi-table analysis"

Denormalized schema optimizes for the 90% case.

**4. Trade Write Complexity for Read Simplicity**
- Archival (write) happens once: Can afford complex denormalization logic
- Queries (read) happen thousands of times: Must be simple and fast

---

## 3. Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 1: HOT TIER (Primary PostgreSQL)                        │
│  ─────────────────────────────────────────────────────────────  │
│  Schema: Normalized (3NF)                                       │
│  Data: Active leads (0-3 months after closure)                 │
│  Access: Application CRUD via ORM                               │
│  Cost: High (production workload)                               │
│  Mutable: Yes                                                   │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ Phase 1 Archival (Weekly)
                 │ ─────────────────────────
                 │ 1. Fetch lead + all relations
                 │ 2. Resolve all FK references
                 │ 3. Flatten into single-row denormalized structure
                 │ 4. Insert into warm archive
                 │ 5. Delete from primary
                 │
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 2: WARM TIER (External PostgreSQL)                      │
│  ─────────────────────────────────────────────────────────────  │
│  Schema: Denormalized (flat + JSONB)                           │
│  Data: Archived leads (3 months - 2 years)                     │
│  Access: Read-only SQL via Datalens                             │
│  Cost: Medium ($150-200/month)                                  │
│  Mutable: Read-only (no updates after insert)                  │
│                                                                 │
│  Structure:                                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ archive.leads (WIDE TABLE)                               │  │
│  │ ├─ Lead fields (lead_id, amount, closed_at)             │  │
│  │ ├─ Resolved references (closure_reason_name, agent_name)│  │
│  │ ├─ Customer snapshot (customer_name, region, segment)   │  │
│  │ ├─ documents JSONB[] (embedded)                         │  │
│  │ ├─ activities JSONB[] (embedded)                        │  │
│  │ └─ payments JSONB[] (embedded with line_items)          │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ Phase 2 Archival (Monthly)
                 │ ─────────────────────────
                 │ 1. Read from warm archive (already denormalized)
                 │ 2. Export to Parquet (maintains structure)
                 │ 3. Write to MinIO with Hive partitioning
                 │ 4. Delete from warm
                 │
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 3: COLD TIER (MinIO/S3 Parquet)                         │
│  ─────────────────────────────────────────────────────────────  │
│  Schema: Denormalized (same as warm, frozen)                   │
│  Data: Long-term archive (2-7 years)                           │
│  Access: Multi-modal retrieval (see Section 6)                 │
│  Cost: Very low ($10/month)                                     │
│  Mutable: Immutable                                             │
│                                                                 │
│  Storage Layout:                                                │
│  minio://archive/leads/                                         │
│    year=2024/                                                   │
│      month=01/                                                  │
│        LEAD-1001_v3.parquet                                     │
│        LEAD-1002_v3.parquet                                     │
│      month=02/...                                               │
│    year=2025/...                                                │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow Timeline

```
Day 0:     Lead created in Primary DB (normalized)
Day 90:    Lead closed
Day 180:   Lead eligible for archival (3 months after closure)
           ↓ Phase 1 Archival
           Lead denormalized and moved to Warm Archive
           - All FKs resolved
           - Children embedded as JSONB
           - Queryable via Datalens (instant SQL)

Month 27:  Lead aged 24 months in warm tier
           ↓ Phase 2 Archival
           Lead exported to Parquet (cold storage)
           - Maintains denormalized structure
           - Deleted from warm tier
           - Available via retrieval strategies

Year 7:    Lead reaches retention limit
           ↓ Deletion
           Removed from cold storage
```

---

## 4. Phase 1: Hot → Warm (Denormalized)

### Step-by-Step Pipeline

#### Step 1: Identify Eligible Leads

```sql
-- Same eligibility as before
SELECT lead_id 
FROM leads 
WHERE closed_at < NOW() - INTERVAL '3 months'
  AND pii_masked = TRUE
  AND purged_at IS NOT NULL
  AND lead_id NOT IN (
      SELECT archived_entity_id 
      FROM archival_manifest 
      WHERE tier IN ('warm', 'cold')
  )
LIMIT 500;  -- Batch size
```

---

#### Step 2: Fetch and Denormalize

```python
def denormalize_lead_for_warm(lead_id):
    """
    Fetch lead + all relations from primary DB and flatten into 
    denormalized structure for warm archive
    """
    
    # 1. Fetch root lead
    lead = primary_db.query("""
        SELECT 
            l.lead_id,
            l.customer_id,
            l.closed_at,
            l.amount,
            l.closure_reason_id,
            l.agent_id,
            l.product_type_id,
            -- Customer snapshot (de-normalized at archive time)
            c.name as customer_name,
            c.email as customer_email,
            c.phone as customer_phone,
            c.region as customer_region,
            c.segment as customer_segment
        FROM leads l
        JOIN customers c ON l.customer_id = c.customer_id
        WHERE l.lead_id = ?
    """, lead_id)
    
    # 2. Resolve FK references to human-readable values
    lead['closure_reason_name'] = lookup_closure_reason(lead['closure_reason_id'])
    lead['agent_name'] = lookup_agent(lead['agent_id'])
    lead['agent_team'] = lookup_agent_team(lead['agent_id'])
    lead['product_type_name'] = lookup_product_type(lead['product_type_id'])
    
    # Drop FK IDs (not needed in archive)
    del lead['closure_reason_id']
    del lead['agent_id']
    del lead['product_type_id']
    
    # 3. Fetch and embed children as JSONB arrays
    lead['documents'] = primary_db.query("""
        SELECT 
            document_id,
            document_type,
            file_path,
            file_size,
            created_at,
            created_by
        FROM lead_documents 
        WHERE lead_id = ?
    """, lead_id)  # Returns array of dicts
    
    lead['activities'] = primary_db.query("""
        SELECT 
            activity_id,
            activity_type,
            description,
            performed_by,
            performed_at
        FROM lead_activities 
        WHERE lead_id = ?
        ORDER BY performed_at DESC
    """, lead_id)
    
    lead['payments'] = primary_db.query("""
        SELECT 
            p.payment_id,
            p.amount,
            p.status,
            p.payment_date,
            p.payment_method,
            -- Embed line items as nested array
            (
                SELECT json_agg(json_build_object(
                    'line_item_id', li.line_item_id,
                    'description', li.description,
                    'amount', li.amount,
                    'quantity', li.quantity
                ))
                FROM payment_line_items li
                WHERE li.payment_id = p.payment_id
            ) as line_items
        FROM payments p
        WHERE p.lead_id = ?
    """, lead_id)
    
    # 4. Stamp metadata
    lead['schema_version'] = CURRENT_SCHEMA_VERSION  # e.g., 'v3'
    lead['archived_at'] = NOW()
    lead['archive_source'] = 'primary_db'
    
    return lead
```

**Result Structure:**
```json
{
  "lead_id": "LEAD-1001",
  "customer_id": "CUST-500",
  "customer_name": "John Doe",
  "customer_email": "john@example.com",
  "customer_region": "US-West",
  "customer_segment": "Premium",
  "closed_at": "2024-06-24T10:30:00Z",
  "amount": 50000.00,
  "closure_reason_name": "Customer Request - Repaid",
  "agent_name": "Ravi Kumar",
  "agent_team": "Collections",
  "product_type_name": "Personal Loan",
  "documents": [
    {"document_id": "DOC-1", "document_type": "ID Proof", "file_path": "s3://...", "created_at": "..."},
    {"document_id": "DOC-2", "document_type": "Income Proof", "file_path": "s3://...", "created_at": "..."}
  ],
  "activities": [
    {"activity_id": "ACT-1", "activity_type": "Call", "description": "Follow-up call", "performed_at": "..."},
    {"activity_id": "ACT-2", "activity_type": "Email", "description": "Sent reminder", "performed_at": "..."}
  ],
  "payments": [
    {
      "payment_id": "PAY-1",
      "amount": 25000.00,
      "status": "completed",
      "payment_date": "2024-05-01T00:00:00Z",
      "line_items": [
        {"line_item_id": "LI-1", "description": "Principal", "amount": 20000.00},
        {"line_item_id": "LI-2", "description": "Interest", "amount": 5000.00}
      ]
    }
  ],
  "schema_version": "v3",
  "archived_at": "2024-12-24T10:30:00Z"
}
```

---

#### Step 3: Insert into Warm Archive

```python
def insert_into_warm_archive(denormalized_lead):
    """Insert flattened lead into warm archive DB"""
    
    warm_db.execute("""
        INSERT INTO archive.leads (
            lead_id,
            customer_id,
            customer_name,
            customer_email,
            customer_phone,
            customer_region,
            customer_segment,
            closed_at,
            amount,
            closure_reason_name,
            agent_name,
            agent_team,
            product_type_name,
            documents,          -- JSONB
            activities,         -- JSONB
            payments,           -- JSONB
            schema_version,
            archived_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?::jsonb,  -- Cast to JSONB
            ?::jsonb,
            ?::jsonb,
            ?, ?
        )
    """, 
        denormalized_lead['lead_id'],
        denormalized_lead['customer_id'],
        denormalized_lead['customer_name'],
        # ... all fields
        json.dumps(denormalized_lead['documents']),
        json.dumps(denormalized_lead['activities']),
        json.dumps(denormalized_lead['payments']),
        denormalized_lead['schema_version'],
        denormalized_lead['archived_at']
    )
```

---

#### Step 4: Verify and Manifest

```python
# Verify insertion
lead = warm_db.query("SELECT * FROM archive.leads WHERE lead_id = ?", lead_id)
assert lead is not None

# Verify JSONB arrays
assert len(lead['documents']) == expected_doc_count
assert len(lead['activities']) == expected_activity_count

# Write to manifest
archival_manifest.insert({
    'archived_entity_id': lead_id,
    'tier': 'warm',
    'export_target': 'warm_denormalized_db',
    'warm_archive_location': 'warm-archive-db.internal:5432/archive.leads',
    'warm_archived_at': NOW(),
    'schema_version': 'v3',
    'row_counts': {'lead': 1, 'documents': 4, 'activities': 10, 'payments': 2},
    'denormalized': True  # Flag indicating denormalized structure
})
```

---

#### Step 5: Delete from Primary

```python
# Same deletion order as before (leaves to root)
with primary_db.transaction():
    primary_db.execute("DELETE FROM payment_line_items WHERE payment_id IN (SELECT payment_id FROM payments WHERE lead_id = ?)", lead_id)
    primary_db.execute("DELETE FROM payments WHERE lead_id = ?", lead_id)
    primary_db.execute("DELETE FROM lead_activities WHERE lead_id = ?", lead_id)
    primary_db.execute("DELETE FROM lead_documents WHERE lead_id = ?", lead_id)
    primary_db.execute("DELETE FROM lead_assignments WHERE lead_id = ?", lead_id)
    primary_db.execute("DELETE FROM leads WHERE lead_id = ?", lead_id)
```

---

### Warm Archive Schema

```sql
CREATE SCHEMA archive;

-- Single wide table (denormalized)
CREATE TABLE archive.leads (
    -- Primary key
    lead_id VARCHAR(50) PRIMARY KEY,
    
    -- Lead core fields
    customer_id VARCHAR(50),
    closed_at TIMESTAMP NOT NULL,
    amount DECIMAL(10, 2),
    
    -- Customer snapshot (denormalized)
    customer_name VARCHAR(100),
    customer_email VARCHAR(255),
    customer_phone VARCHAR(20),
    customer_region VARCHAR(50),
    customer_segment VARCHAR(50),
    
    -- Resolved FK references (no more FKs!)
    closure_reason_name VARCHAR(100),
    agent_name VARCHAR(100),
    agent_team VARCHAR(100),
    product_type_name VARCHAR(100),
    
    -- Embedded children as JSONB arrays
    documents JSONB,    -- [{doc_id, type, path, ...}, ...]
    activities JSONB,   -- [{activity_id, type, desc, ...}, ...]
    payments JSONB,     -- [{payment_id, amount, line_items: [...]}]
    
    -- Metadata
    schema_version VARCHAR(10) NOT NULL,
    archived_at TIMESTAMP NOT NULL DEFAULT NOW(),
    archive_source VARCHAR(50) DEFAULT 'primary_db',
    
    -- Indexes for common query patterns
    INDEX idx_closed_at (closed_at),
    INDEX idx_customer_name (customer_name),
    INDEX idx_archived_at (archived_at),
    INDEX idx_schema_version (schema_version),
    
    -- GIN index for JSONB queries
    INDEX idx_documents_gin ON (documents) USING GIN,
    INDEX idx_activities_gin ON (activities) USING GIN,
    INDEX idx_payments_gin ON (payments) USING GIN
);
```

**Benefits of This Schema:**
- ✅ Single table (no JOINs for 90% of queries)
- ✅ No FK constraints to manage
- ✅ Self-contained rows (readable without any other table)
- ✅ JSONB allows nested querying when needed
- ✅ Smaller index footprint (fewer tables = fewer indexes)

---

## 5. Phase 2: Warm → Cold (Parquet)

### Why Phase 2 is Simpler in Hybrid Approach

**Key Insight:** Warm tier is already denormalized, so Phase 2 is a **straightforward export** (no additional denormalization needed).

```
Traditional Two-Phase:
  Warm (normalized) → Must denormalize during export → Cold (denormalized)
  
Hybrid Two-Phase:
  Warm (denormalized) → Direct export → Cold (denormalized)
  ✅ Simpler pipeline
```

---

### Pipeline Steps

#### Step 1: Identify Aged Warm Records

```sql
SELECT lead_id, schema_version
FROM archive.leads
WHERE archived_at < NOW() - INTERVAL '2 years'
  AND lead_id NOT IN (
      SELECT archived_entity_id 
      FROM archival_manifest 
      WHERE tier = 'cold'
  )
LIMIT 1000;  -- Batch size
```

---

#### Step 2: Read from Warm (Already Denormalized)

```python
def read_from_warm_for_cold(lead_id):
    """Read denormalized lead from warm archive"""
    
    lead = warm_db.query("""
        SELECT * FROM archive.leads WHERE lead_id = ?
    """, lead_id)
    
    # Already denormalized, no additional processing needed
    return lead
```

---

#### Step 3: Export to Parquet

```python
def export_to_parquet(denormalized_lead):
    """Export denormalized lead to Parquet"""
    
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    # Convert to DataFrame (single row)
    df = pd.DataFrame([denormalized_lead])
    
    # Parquet handles JSONB natively (converts to nested structures)
    # documents, activities, payments remain as arrays of structs
    
    # Write to staging
    year = denormalized_lead['closed_at'].year
    month = denormalized_lead['closed_at'].month
    lead_id = denormalized_lead['lead_id']
    schema_version = denormalized_lead['schema_version']
    
    staging_path = f"minio://staging/leads/{lead_id}_{schema_version}.parquet"
    pq.write_table(pa.Table.from_pandas(df), staging_path)
    
    # Verify
    verify_parquet(staging_path, denormalized_lead)
    
    # Atomic move to final location
    final_path = f"minio://archive/leads/year={year}/month={month:02d}/{lead_id}_{schema_version}.parquet"
    minio.move(staging_path, final_path)
    
    return final_path
```

**Parquet Structure:**
```
Parquet file maintains denormalized structure:
├─ lead_id: string
├─ customer_name: string
├─ amount: decimal
├─ documents: array<struct<document_id, type, path>>
├─ activities: array<struct<activity_id, type, description>>
├─ payments: array<struct<payment_id, amount, line_items: array<struct>>>
└─ schema_version: string
```

---

#### Step 4: Update Manifest and Delete from Warm

```python
# Update manifest
archival_manifest.update(
    where={'archived_entity_id': lead_id},
    values={
        'tier': 'cold',
        'cold_archive_location': final_path,
        'cold_archived_at': NOW(),
        'cold_checksum': calculate_sha256(final_path)
    }
)

# Delete from warm
warm_db.execute("DELETE FROM archive.leads WHERE lead_id = ?", lead_id)
```

---

## 6. Robust Retrieval Strategy

### Multi-Modal Retrieval Architecture

The hybrid approach enables **four distinct retrieval modes**, each optimized for different access patterns.

```
┌──────────────────────────────────────────────────────────────┐
│  RETRIEVAL COORDINATOR (Datalens + API Layer)                │
│  ─────────────────────────────────────────────────────────   │
│  Analyzes query → Routes to optimal retrieval mode           │
└──────┬───────────────┬───────────────┬──────────────┬────────┘
       │               │               │              │
       │               │               │              │
┌──────▼──────┐ ┌──────▼──────┐ ┌─────▼──────┐ ┌────▼────────┐
│  MODE 1:    │ │  MODE 2:    │ │  MODE 3:   │ │  MODE 4:    │
│  Instant    │ │  Staged     │ │  Bulk      │ │  Analytics  │
│  SQL        │ │  Retrieval  │ │  Staging   │ │  Direct     │
└─────────────┘ └─────────────┘ └────────────┘ └─────────────┘
```

---

### Mode 1: Instant SQL (Warm Tier)

**Use Case:** Recent archives (< 2 years), interactive queries

**Flow:**
```
User Query → Check manifest (tier = warm) → Query warm DB directly → Return
```

**Example:**
```sql
-- Query in Datalens
SELECT * FROM archive.leads WHERE lead_id = 'LEAD-5000';

-- Executed against warm DB:
-- ✅ Single table lookup
-- ✅ No JOINs needed (already denormalized)
-- ✅ Returns in < 100ms

-- Can also query nested JSONB:
SELECT 
    lead_id,
    customer_name,
    jsonb_array_length(documents) as doc_count,
    jsonb_array_length(activities) as activity_count
FROM archive.leads
WHERE customer_region = 'US-West';
```

**Latency:** < 100ms  
**Cost:** Included in warm DB  
**Access:** Self-service

---

### Mode 2: Staged Retrieval (Cold → Temp Staging)

**Use Case:** Specific leads from cold tier, need interactive SQL

**Flow:**
```
1. User requests lead from cold tier
2. System downloads Parquet from MinIO
3. Loads into temporary staging schema in warm DB
4. User queries staging via Datalens
5. Staging auto-expires after 30 days
```

**Implementation:**

```python
def stage_cold_lead(lead_id):
    """Load cold lead into temporary warm staging for interactive queries"""
    
    # 1. Get cold location from manifest
    manifest = get_manifest(lead_id)
    parquet_path = manifest['cold_archive_location']
    
    # 2. Read Parquet
    df = pd.read_parquet(parquet_path)
    lead = df.to_dict('records')[0]
    
    # 3. Insert into staging schema (same structure as warm)
    warm_db.execute("""
        INSERT INTO staging.leads (
            lead_id, customer_name, closed_at, amount,
            documents, activities, payments,
            schema_version, archived_at, staged_at
        ) VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?, NOW())
    """, lead['lead_id'], lead['customer_name'], ...)
    
    # 4. Update manifest
    update_manifest(lead_id, {
        'staging_loaded_at': NOW(),
        'staging_expires_at': NOW() + timedelta(days=30)
    })
    
    return {'status': 'staged', 'expires_at': NOW() + timedelta(days=30)}
```

**Staging Schema:**
```sql
-- Temporary staging schema (same structure as warm archive)
CREATE SCHEMA staging;

CREATE TABLE staging.leads (
    -- Same columns as archive.leads
    lead_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    documents JSONB,
    activities JSONB,
    payments JSONB,
    schema_version VARCHAR(10),
    archived_at TIMESTAMP,
    staged_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '30 days',
    
    INDEX idx_expires_at (expires_at)
);

-- Auto-cleanup job deletes expired staging data daily
```

**User Experience:**
```
User: "Show me LEAD-500 (from cold tier)"
System: "Loading from cold storage... (Est. 15 min)"
[15 minutes later]
System: "LEAD-500 is now available in staging. Accessible for 30 days."
User: [Queries via Datalens] SELECT * FROM staging.leads WHERE lead_id = 'LEAD-500'
```

**Latency:** 15 min - 2 hours (first access), then < 100ms  
**Cost:** $0.01/lead staged  
**Access:** Request-based, then self-service

---

### Mode 3: Bulk Staging DB (Large Investigations)

**Use Case:** Legal discovery, audit requiring 1000+ leads from cold

**Flow:**
```
1. User submits bulk retrieval request (case ID, lead list)
2. System provisions dedicated bulk staging DB
3. Parallel load: Download + stage 1000s of Parquets
4. User gets dedicated Datalens connection
5. DB expires after 90 days or case closure
```

**Implementation:**

```python
def provision_bulk_staging_db(case_id, lead_ids, user):
    """Create dedicated DB for large-scale cold retrieval"""
    
    # 1. Provision new Postgres instance
    db_instance = create_postgres_instance(
        name=f'bulk_staging_{case_id}',
        size='large',  # 8 CPU, 64GB RAM
        storage='500GB',
        ttl_days=90
    )
    
    # 2. Create archive schema (denormalized structure)
    db_instance.execute("""
        CREATE TABLE archive.leads (
            -- Same structure as warm archive
            lead_id VARCHAR(50) PRIMARY KEY,
            customer_name VARCHAR(100),
            documents JSONB,
            ...
        );
    """)
    
    # 3. Parallel bulk load
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for batch in chunk(lead_ids, 100):
            future = executor.submit(load_batch_to_bulk_db, batch, db_instance)
            futures.append(future)
        
        # Wait for completion
        for future in tqdm(futures, desc="Loading cold archives"):
            future.result()
    
    # 4. Create Datalens connection
    datalens_connection = create_datalens_connection(
        name=f'Bulk Staging - Case {case_id}',
        host=db_instance.host,
        database='archive',
        access_users=[user.email],
        ttl_days=90
    )
    
    return {
        'status': 'ready',
        'lead_count': len(lead_ids),
        'connection': datalens_connection,
        'expires_at': NOW() + timedelta(days=90)
    }

def load_batch_to_bulk_db(lead_ids, db_instance):
    """Load batch of leads from cold to bulk DB"""
    for lead_id in lead_ids:
        manifest = get_manifest(lead_id)
        parquet_path = manifest['cold_archive_location']
        
        # Read and insert
        df = pd.read_parquet(parquet_path)
        lead = df.to_dict('records')[0]
        
        db_instance.execute("""
            INSERT INTO archive.leads VALUES (?, ?, ?, ...)
        """, lead['lead_id'], lead['customer_name'], ...)
```

**User Experience:**
```
Legal Team: "Load all 2023 leads for case #12345 (5,000 leads)"
System: "Provisioning bulk staging DB... (Est. 4 hours)"
[4 hours later]
System: "Ready. Access at: datalens.company.com/bulk-case-12345"
Legal Team: [Runs complex queries for 3 months]
[After case closes]
System: "DB expires in 7 days. Export needed data now."
```

**Latency:** 2-8 hours (initial load), then instant  
**Cost:** $500-1000 per case (DB + compute)  
**Access:** Dedicated, request-based

---

### Mode 4: Analytics Direct Query (Cold → DuckDB)

**Use Case:** Aggregations, reports, no need for row-level SQL access

**Flow:**
```
1. User runs aggregate query
2. System detects: cold tier, aggregation pattern
3. Routes to DuckDB for direct Parquet query
4. Returns aggregated results (no staging needed)
```

**Implementation:**

```python
def query_cold_analytics(sql_query):
    """Execute analytics query directly against Parquet files"""
    
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
    
    # Rewrite query to target Parquet
    sql_rewritten = sql_query.replace(
        "FROM archive.leads",
        "FROM 'minio://archive/leads/**/*.parquet'"
    )
    
    # Execute (DuckDB reads Parquet directly)
    result = con.execute(sql_rewritten).fetchdf()
    
    return result
```

**Example Queries:**

```sql
-- Monthly closure report
SELECT 
    DATE_TRUNC('month', closed_at) as month,
    closure_reason_name,
    COUNT(*) as lead_count,
    SUM(amount) as total_amount
FROM 'minio://archive/leads/**/*.parquet'
WHERE closed_at >= '2022-01-01'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- Top products by region
SELECT 
    customer_region,
    product_type_name,
    COUNT(*) as count
FROM 'minio://archive/leads/**/*.parquet'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- Nested JSONB query (DuckDB supports this!)
SELECT 
    lead_id,
    customer_name,
    list_length(documents) as doc_count
FROM 'minio://archive/leads/**/*.parquet'
WHERE list_length(documents) > 5;
```

**Latency:** 10-60 seconds (columnar scan)  
**Cost:** Negligible (compute only)  
**Access:** Self-service

---

### Retrieval Decision Matrix

```python
class RetrievalCoordinator:
    def route_query(self, query, user):
        """Intelligently route query to optimal retrieval mode"""
        
        analysis = self.analyze_query(query)
        
        # Check manifest for tier
        if analysis.entity_ids:
            tiers = [get_manifest(eid)['tier'] for eid in analysis.entity_ids]
        else:
            tiers = self.infer_tiers_from_filters(query)
        
        # Decision logic
        if all(t == 'warm' for t in tiers):
            # All in warm tier → Mode 1: Instant SQL
            return self.mode1_instant_sql(query)
        
        elif 'cold' in tiers and analysis.is_aggregation:
            # Cold tier + aggregation → Mode 4: Analytics Direct
            return self.mode4_analytics_direct(query)
        
        elif 'cold' in tiers and len(analysis.entity_ids) < 10:
            # Cold tier + few entities → Mode 2: Staged Retrieval
            return self.mode2_staged_retrieval(analysis.entity_ids, query)
        
        elif 'cold' in tiers and len(analysis.entity_ids) > 100:
            # Cold tier + many entities → Mode 3: Bulk Staging DB
            return self.mode3_bulk_staging(analysis.entity_ids, user)
        
        else:
            # Mixed or complex → Let user choose
            return self.prompt_user_for_mode(analysis)
```

**Routing Examples:**

```
Query: SELECT * FROM archive.leads WHERE lead_id = 'LEAD-5000'
Analysis: Single entity, tier = warm
Route: Mode 1 (Instant SQL) ✅

Query: SELECT * FROM archive.leads WHERE lead_id = 'LEAD-100'
Analysis: Single entity, tier = cold
Route: Mode 2 (Staged Retrieval) → Load to staging ⏳

Query: SELECT closure_reason_name, COUNT(*) FROM archive.leads GROUP BY 1
Analysis: Aggregation, tier = cold
Route: Mode 4 (Analytics Direct) → DuckDB ✅

Query: SELECT * FROM archive.leads WHERE lead_id IN ('LEAD-100', ..., 'LEAD-5000')
Analysis: 5000 entities, tier = cold
Route: Mode 3 (Bulk Staging DB) → Provision dedicated DB 🏗️
```

---

### Retrieval SLAs

| Mode | First Access | Subsequent Access | Use Case | Cost |
|------|--------------|-------------------|----------|------|
| **Mode 1: Instant SQL** | < 100ms | < 100ms | Warm tier queries | Included |
| **Mode 2: Staged Retrieval** | 15 min - 2 hours | < 100ms (30 days) | Few cold leads | $0.01/lead |
| **Mode 3: Bulk Staging** | 2-8 hours | < 100ms (90 days) | Large investigations | $500-1000 |
| **Mode 4: Analytics Direct** | 10-60 seconds | 10-60 seconds | Aggregations, reports | Negligible |

---

## 7. Schema Evolution Strategy

### Simpler Than Traditional Normalized Archives

**Key Advantage:** Denormalized structure makes schema evolution **additive-only** in most cases.

---

### Evolution Pattern 1: Add New Field (Simple)

**Scenario:** Add `closure_subcategory` in v4

**Implementation:**
```sql
-- Warm archive DB
ALTER TABLE archive.leads ADD COLUMN closure_subcategory VARCHAR(100);

-- New archives populate it
-- Old archives have NULL (acceptable - additive change)
```

**Query Handling:**
```sql
-- Works across versions
SELECT 
    lead_id,
    closure_reason_name,
    COALESCE(closure_subcategory, 'Not Categorized') as subcategory
FROM archive.leads;
```

**No migration needed** for old data unless you want to backfill.

---

### Evolution Pattern 2: Add Nested Field in JSONB

**Scenario:** Add `document_hash` to documents array in v5

**Implementation:**
```python
# Update denormalization logic
def denormalize_lead_for_warm(lead_id):
    # ...
    lead['documents'] = primary_db.query("""
        SELECT 
            document_id,
            document_type,
            file_path,
            file_hash,  -- NEW FIELD
            created_at
        FROM lead_documents 
        WHERE lead_id = ?
    """, lead_id)
```

**Result:**
```
v4 documents: [{doc_id: 1, type: 'ID', path: '...'}]
v5 documents: [{doc_id: 1, type: 'ID', path: '...', hash: 'sha256:...'}]
```

**Query Handling:**
```sql
-- JSONB gracefully handles missing keys
SELECT 
    lead_id,
    jsonb_array_elements(documents)->>'document_hash' as doc_hash
FROM archive.leads;

-- Returns NULL for v4, actual hash for v5
```

---

### Evolution Pattern 3: Change in Reference Data

**Scenario:** `agent_team` was "Collections" in 2024, renamed to "Recovery" in 2025

**Impact in Different Approaches:**

**Traditional Normalized:**
```
Problem: 
  archive.leads.agent_id = 5 → agents.team = 'Recovery' (current)
  But lead was archived when team was 'Collections'
  
Historical accuracy lost ❌
```

**Hybrid Denormalized:**
```
Solution:
  archive.leads.agent_team = 'Collections' (frozen at archive time)
  
Historical accuracy preserved ✅
```

**This is a MAJOR benefit of denormalization** — captures point-in-time state.

---

### Evolution Pattern 4: Breaking Change (Rare)

**Scenario:** Split `customer_name` into `first_name`, `last_name` in v6

**Option A: Shadow Columns (Recommended)**
```sql
ALTER TABLE archive.leads ADD COLUMN first_name VARCHAR(100);
ALTER TABLE archive.leads ADD COLUMN last_name VARCHAR(100);

-- Keep customer_name for v1-v5 data
-- New v6 archives populate first_name, last_name
```

**Option B: Backfill Migration**
```sql
-- One-time migration (expensive)
UPDATE archive.leads
SET 
    first_name = SPLIT_PART(customer_name, ' ', 1),
    last_name = SPLIT_PART(customer_name, ' ', 2)
WHERE schema_version IN ('v1', 'v2', 'v3', 'v4', 'v5')
  AND first_name IS NULL;

-- Marking all as v6 after backfill
UPDATE archive.leads SET schema_version = 'v6';
```

**Query Handling:**
```sql
-- Use COALESCE for version-agnostic queries
SELECT 
    lead_id,
    COALESCE(first_name, SPLIT_PART(customer_name, ' ', 1)) as first_name,
    COALESCE(last_name, SPLIT_PART(customer_name, ' ', 2)) as last_name
FROM archive.leads;
```

---

### Cold Tier Schema Evolution (Non-Issue)

**Cold tier Parquets are immutable:**
- v4 Parquet stays v4 forever
- v6 Parquet has new schema
- DuckDB handles mixed schemas automatically

**No migration needed in cold tier.**

---

## 8. Implementation Details

### Denormalization Logic Library

```python
class LeadDenormalizer:
    """Encapsulates denormalization logic for lead archival"""
    
    def __init__(self, primary_db, schema_version):
        self.primary_db = primary_db
        self.schema_version = schema_version
        
    def denormalize(self, lead_id):
        """Main entry point for denormalization"""
        lead = self._fetch_lead_core(lead_id)
        lead.update(self._fetch_customer_snapshot(lead['customer_id']))
        lead.update(self._resolve_references(lead))
        lead['documents'] = self._fetch_documents(lead_id)
        lead['activities'] = self._fetch_activities(lead_id)
        lead['payments'] = self._fetch_payments(lead_id)
        lead['schema_version'] = self.schema_version
        lead['archived_at'] = NOW()
        
        return lead
    
    def _fetch_lead_core(self, lead_id):
        return self.primary_db.query_one("""
            SELECT 
                lead_id,
                customer_id,
                closed_at,
                amount,
                closure_reason_id,
                agent_id,
                product_type_id
            FROM leads WHERE lead_id = ?
        """, lead_id)
    
    def _fetch_customer_snapshot(self, customer_id):
        """Snapshot customer data at archive time"""
        return self.primary_db.query_one("""
            SELECT 
                name as customer_name,
                email as customer_email,
                phone as customer_phone,
                region as customer_region,
                segment as customer_segment
            FROM customers WHERE customer_id = ?
        """, customer_id)
    
    def _resolve_references(self, lead):
        """Resolve all FK references to human-readable values"""
        return {
            'closure_reason_name': self._lookup_closure_reason(lead['closure_reason_id']),
            'agent_name': self._lookup_agent_name(lead['agent_id']),
            'agent_team': self._lookup_agent_team(lead['agent_id']),
            'product_type_name': self._lookup_product_type(lead['product_type_id'])
        }
    
    def _fetch_documents(self, lead_id):
        return self.primary_db.query("""
            SELECT 
                document_id,
                document_type,
                file_path,
                file_size,
                created_at
            FROM lead_documents 
            WHERE lead_id = ?
            ORDER BY created_at
        """, lead_id)
    
    def _fetch_activities(self, lead_id):
        return self.primary_db.query("""
            SELECT 
                activity_id,
                activity_type,
                description,
                performed_by,
                performed_at
            FROM lead_activities 
            WHERE lead_id = ?
            ORDER BY performed_at DESC
        """, lead_id)
    
    def _fetch_payments(self, lead_id):
        """Fetch payments with nested line items"""
        payments = self.primary_db.query("""
            SELECT 
                p.payment_id,
                p.amount,
                p.status,
                p.payment_date,
                p.payment_method
            FROM payments p
            WHERE p.lead_id = ?
        """, lead_id)
        
        # Fetch line items for each payment
        for payment in payments:
            payment['line_items'] = self.primary_db.query("""
                SELECT 
                    line_item_id,
                    description,
                    amount,
                    quantity
                FROM payment_line_items
                WHERE payment_id = ?
            """, payment['payment_id'])
        
        return payments
    
    # Reference lookup methods (cached)
    @lru_cache(maxsize=1000)
    def _lookup_closure_reason(self, reason_id):
        result = self.primary_db.query_one(
            "SELECT name FROM closure_reasons WHERE id = ?", reason_id
        )
        return result['name'] if result else 'Unknown'
    
    @lru_cache(maxsize=1000)
    def _lookup_agent_name(self, agent_id):
        result = self.primary_db.query_one(
            "SELECT name FROM agents WHERE id = ?", agent_id
        )
        return result['name'] if result else 'Unknown'
    
    @lru_cache(maxsize=1000)
    def _lookup_agent_team(self, agent_id):
        result = self.primary_db.query_one(
            "SELECT team FROM agents WHERE id = ?", agent_id
        )
        return result['team'] if result else 'Unknown'
    
    @lru_cache(maxsize=1000)
    def _lookup_product_type(self, product_type_id):
        result = self.primary_db.query_one(
            "SELECT name FROM product_types WHERE id = ?", product_type_id
        )
        return result['name'] if result else 'Unknown'
```

---

### Airflow DAG Definitions

```python
# Phase 1: Hot → Warm (Denormalized)
dag_phase1 = DAG(
    'archive_hot_to_warm_denormalized',
    schedule='0 2 * * 0',  # Weekly Sunday 2 AM
    max_active_runs=1
)

identify_eligible = PythonOperator(
    task_id='identify_eligible_leads',
    python_callable=identify_eligible_leads_for_phase1,
    op_kwargs={'batch_size': 500}
)

denormalize = PythonOperator(
    task_id='denormalize_leads',
    python_callable=denormalize_batch,
    op_kwargs={'schema_version': 'v3'}
)

insert_warm = PythonOperator(
    task_id='insert_into_warm_denormalized',
    python_callable=bulk_insert_warm
)

verify = PythonOperator(
    task_id='verify_warm_insertion',
    python_callable=verify_batch
)

write_manifest = PythonOperator(
    task_id='write_archival_manifest',
    python_callable=write_manifest_batch
)

delete_hot = PythonOperator(
    task_id='delete_from_primary',
    python_callable=delete_batch_from_primary
)

log_audit = PythonOperator(
    task_id='log_audit_events',
    python_callable=log_phase1_audit
)

# Pipeline
identify_eligible >> denormalize >> insert_warm >> verify >> write_manifest >> delete_hot >> log_audit


# Phase 2: Warm → Cold (Already Denormalized)
dag_phase2 = DAG(
    'archive_warm_to_cold_parquet',
    schedule='0 3 1 * *',  # Monthly 1st at 3 AM
    max_active_runs=1
)

identify_aged = PythonOperator(
    task_id='identify_aged_warm_records',
    python_callable=identify_aged_warm_leads,
    op_kwargs={'batch_size': 1000}
)

export_parquet = PythonOperator(
    task_id='export_to_parquet',
    python_callable=batch_export_parquet
)

verify_parquet = PythonOperator(
    task_id='verify_parquet_files',
    python_callable=verify_parquet_batch
)

update_manifest = PythonOperator(
    task_id='update_manifest_to_cold',
    python_callable=update_manifest_batch
)

delete_warm = PythonOperator(
    task_id='delete_from_warm',
    python_callable=delete_batch_from_warm
)

log_audit2 = PythonOperator(
    task_id='log_audit_events',
    python_callable=log_phase2_audit
)

# Pipeline
identify_aged >> export_parquet >> verify_parquet >> update_manifest >> delete_warm >> log_audit2
```

---

### Query Examples

#### Simple Queries (No JOINs Needed)

```sql
-- Get lead with all details (single table query)
SELECT * FROM archive.leads WHERE lead_id = 'LEAD-1001';

-- Filter by customer
SELECT lead_id, customer_name, closed_at, amount
FROM archive.leads
WHERE customer_name LIKE '%Smith%';

-- Filter by closure reason (no JOIN!)
SELECT lead_id, customer_name, closure_reason_name, amount
FROM archive.leads
WHERE closure_reason_name = 'Customer Request - Repaid';

-- Date range
SELECT lead_id, closed_at, amount
FROM archive.leads
WHERE closed_at BETWEEN '2024-01-01' AND '2024-12-31';
```

---

#### JSONB Queries (Nested Data)

```sql
-- Count documents per lead
SELECT 
    lead_id,
    customer_name,
    jsonb_array_length(documents) as document_count
FROM archive.leads
WHERE jsonb_array_length(documents) > 5;

-- Extract specific document types
SELECT 
    lead_id,
    customer_name,
    doc->>'document_type' as doc_type,
    doc->>'file_path' as path
FROM archive.leads,
     jsonb_array_elements(documents) as doc
WHERE doc->>'document_type' = 'ID Proof';

-- Query activities
SELECT 
    lead_id,
    activity->>'activity_type' as activity_type,
    activity->>'performed_at' as performed_at
FROM archive.leads,
     jsonb_array_elements(activities) as activity
WHERE activity->>'activity_type' = 'Call'
ORDER BY activity->>'performed_at' DESC;

-- Complex: Total payment amount including line items
SELECT 
    lead_id,
    customer_name,
    (
        SELECT SUM((payment->>'amount')::decimal)
        FROM jsonb_array_elements(payments) as payment
    ) as total_paid
FROM archive.leads
WHERE closed_at > '2024-01-01';
```

---

#### Aggregation Queries

```sql
-- Leads by closure reason
SELECT 
    closure_reason_name,
    COUNT(*) as count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM archive.leads
GROUP BY closure_reason_name
ORDER BY count DESC;

-- Monthly trend
SELECT 
    DATE_TRUNC('month', closed_at) as month,
    COUNT(*) as lead_count,
    SUM(amount) as total
FROM archive.leads
WHERE closed_at >= '2023-01-01'
GROUP BY 1
ORDER BY 1;

-- By agent and product
SELECT 
    agent_name,
    product_type_name,
    COUNT(*) as leads_closed,
    SUM(amount) as total_value
FROM archive.leads
GROUP BY agent_name, product_type_name
HAVING COUNT(*) > 10
ORDER BY total_value DESC;
```

---

## 9. Merits and Demerits

### Merits

#### 1. **Simplified Query Experience (Major Benefit)**

**Single-Table Queries:**
```sql
-- Before (Normalized): 4 table JOINs
SELECT l.*, cr.name, a.name, pt.name
FROM leads l
JOIN closure_reasons cr ON l.closure_reason_id = cr.id
JOIN agents a ON l.agent_id = a.id
JOIN product_types pt ON l.product_type_id = pt.id;

-- After (Denormalized): Single table
SELECT * FROM archive.leads;
```

**Impact:**
- 10x faster queries (no JOIN overhead)
- Simpler for analysts (no SQL expert needed)
- Fewer query errors (no missing JOINs)

---

#### 2. **Better Read Performance**

**Benchmark:**
```
Query: "Get lead with all details"

Normalized Schema:
- 5 tables to scan
- 5 index lookups
- 4 JOINs to compute
- Latency: 50-100ms

Denormalized Schema:
- 1 table to scan
- 1 index lookup
- 0 JOINs
- Latency: 5-10ms (10x faster)
```

**GIN Indexes on JSONB:**
```sql
-- Can query nested data efficiently
CREATE INDEX idx_documents_gin ON archive.leads USING GIN (documents);

-- Query uses index:
SELECT * FROM archive.leads
WHERE documents @> '[{"document_type": "ID Proof"}]';
```

---

#### 3. **Historical Accuracy (Point-in-Time Snapshot)**

**Example:**
```
Scenario: Agent "John" was in "Collections" team in 2023,
          moved to "Recovery" team in 2024

Normalized Archive:
  archive.leads.agent_id = 5
  → Current agents.team = 'Recovery' (2024)
  ❌ Historical context lost

Denormalized Archive:
  archive.leads.agent_team = 'Collections'
  ✅ Frozen at archival time, accurate historical record
```

**Compliance Benefit:** Can prove exactly what data looked like at time of archival.

---

#### 4. **Simpler Phase 2 Migration**

**Warm → Cold is straightforward:**
```python
# Read from warm (already flat)
lead = warm_db.query("SELECT * FROM archive.leads WHERE lead_id = ?")

# Write to Parquet (no transformation needed)
write_parquet(lead, path)
```

**vs Traditional:**
```python
# Must denormalize during Phase 2
lead = warm_db.query("SELECT * FROM leads WHERE ...")
documents = warm_db.query("SELECT * FROM documents WHERE ...")
# ... fetch all children
# ... resolve FKs
# ... flatten
write_parquet(denormalized_lead, path)
```

---

#### 5. **No FK Constraint Management**

**Warm archive has zero FK constraints:**
```sql
-- No foreign keys!
CREATE TABLE archive.leads (
    lead_id VARCHAR(50) PRIMARY KEY,
    closure_reason_name VARCHAR(100),  -- Not a FK
    agent_name VARCHAR(100),            -- Not a FK
    -- ...
);
```

**Benefits:**
- Can't have FK violations
- No cascade delete complications
- No reference table dependencies
- Can delete reference tables from primary without affecting archives

---

#### 6. **Multi-Modal Retrieval Flexibility**

Four retrieval modes optimized for different patterns:
- Mode 1: Instant SQL (warm)
- Mode 2: Staged retrieval (cold → temp warm)
- Mode 3: Bulk staging DB (large investigations)
- Mode 4: Analytics direct (DuckDB on Parquet)

**Adapts to usage pattern** rather than forcing one approach.

---

#### 7. **Schema Evolution Mostly Additive**

**Most changes are simple:**
```sql
-- Add column
ALTER TABLE archive.leads ADD COLUMN new_field VARCHAR(100);
-- Done. Old rows have NULL, new rows populated.

-- Add nested field in JSONB
-- Update denormalization code only
-- Old JSONB: {doc_id, type}
-- New JSONB: {doc_id, type, hash}
-- Queries handle both gracefully
```

**Rarely need complex migrations.**

---

### Demerits

#### 1. **Data Duplication (Storage Cost)**

**Denormalization = Repeated Data:**
```
Normalized:
  leads: 10,000 rows × 500 bytes = 5 MB
  closure_reasons: 20 rows × 100 bytes = 2 KB
  agents: 50 rows × 100 bytes = 5 KB
  Total: ~5 MB

Denormalized:
  archive.leads: 10,000 rows × 700 bytes = 7 MB
  (closure_reason_name repeated 10K times)
  Total: 7 MB (40% larger)
```

**Impact:**
- Warm DB storage: 40% more expensive
- Cold Parquet: Columnar compression mitigates this (repetitive data compresses well)

**Real Cost:**
```
Warm DB: 50 GB (normalized) → 70 GB (denormalized) = +$2.30/month
Cold: Compression makes denormalized smaller due to repetition
Overall: Marginal cost increase
```

---

#### 2. **Complex Archival Logic**

**Denormalization code is complex:**
```python
class LeadDenormalizer:
    # 300 lines of code
    # Must handle:
    # - Multiple child tables
    # - Nested children (payments → line_items)
    # - FK resolution (20+ reference tables)
    # - Point-in-time snapshots
    # - Error handling
```

**vs Simple Copy:**
```python
# Normalized approach
def archive_lead(lead_id):
    lead = db.query("SELECT * FROM leads WHERE lead_id = ?", lead_id)
    archive_db.insert("archive.leads", lead)
    # Done (10 lines)
```

**Mitigation:** Encapsulate in well-tested library, invest upfront.

---

#### 3. **Cannot Update Archived Data**

**Denormalized data is frozen:**
```
Scenario: Archived lead has agent_name = 'John Doe'
          John legally changes name to 'Jane Doe'

Normalized Archive:
  agent_id = 5 → agents.name = 'Jane Doe' (updated)
  Archive reflects current name ✅

Denormalized Archive:
  agent_name = 'John Doe' (frozen)
  Cannot update without breaking immutability ❌
```

**Mitigation:** This is often a FEATURE (point-in-time accuracy), not a bug.  
For legal name changes, document that archives reflect data at archive time.

---

#### 4. **JSONB Query Complexity**

**Nested queries are verbose:**
```sql
-- Extracting nested data
SELECT 
    lead_id,
    doc->>'document_type' as type,
    doc->>'file_path' as path
FROM archive.leads,
     jsonb_array_elements(documents) as doc
WHERE doc->>'document_type' = 'ID Proof';

-- vs Normalized
SELECT ld.document_type, ld.file_path
FROM lead_documents ld
WHERE ld.document_type = 'ID Proof';
```

**Mitigation:** Create views for common patterns.

```sql
-- Helper view
CREATE VIEW archive.all_documents AS
SELECT 
    lead_id,
    doc->>'document_id' as document_id,
    doc->>'document_type' as document_type,
    doc->>'file_path' as file_path
FROM archive.leads,
     jsonb_array_elements(documents) as doc;

-- Now simple:
SELECT * FROM archive.all_documents WHERE document_type = 'ID Proof';
```

---

#### 5. **Warm Tier Still Fixed Cost**

**Same as traditional two-phase:**
- Warm DB costs $150-200/month regardless of usage
- Cannot scale to zero
- Wasted capacity if retrieval is low

**vs Warm-on-Demand:** Scales to $5/month when unused.

---

#### 6. **Reference Data Staleness**

**Denormalized data can become "stale":**
```
Archive: closure_reason_name = 'Customer Request'
Today: Renamed to 'Customer Initiated Request'

User queries archive: Sees old name
User expects new name: Confusion
```

**Mitigation:** 
- Document that archives use historical terminology
- Provide mapping table: old_name → new_name
- Query layer can translate if needed

---

## 10. Use Cases and When to Use

### Ideal Use Cases for Hybrid Approach

#### Use Case 1: Customer Service Portal with Complex Data

**Context:**
- Customer service needs full lead history
- Leads have 10+ related entities (documents, activities, payments)
- Queries like: "Show me everything about this customer's closed leads"
- 100-200 archive queries/day

**Why Hybrid Wins:**
```sql
-- Traditional Normalized: Must JOIN 10 tables
SELECT l.*, cr.name, ...
FROM archive.leads l
JOIN archive.closure_reasons cr ON ...
JOIN archive.agents a ON ...
JOIN archive.lead_documents ld ON ...
-- 10 JOINs, complex, slow

-- Hybrid Denormalized: Single query
SELECT * FROM archive.leads WHERE customer_id = 'CUST-500';
-- Instant, all data in one row
```

**Benefits:**
- ✅ 10x faster queries (no JOINs)
- ✅ Simpler for CS agents (one query gets everything)
- ✅ High retrieval volume justifies warm tier cost

---

#### Use Case 2: Compliance with Point-in-Time Auditing

**Context:**
- Regulatory audits require proving historical state
- "What did closure reason X mean in 2023?"
- Reference data evolves (agents change teams, reasons renamed)
- Need immutable point-in-time snapshots

**Why Hybrid Wins:**
- ✅ Denormalization captures exact state at archive time
- ✅ Agent team in 2023 = "Collections" (frozen)
- ✅ Even if team renamed to "Recovery" in 2024
- ✅ Audit integrity preserved

**Traditional Normalized Risk:**
- ❌ agent_id → current agents.team (not historical)
- ❌ closure_reason_id → current name (may have changed)

---

#### Use Case 3: Analytics with Nested Data

**Context:**
- Data analysts run reports on archived leads
- Need to analyze documents, activities, payments
- Queries involve aggregations across nested data

**Why Hybrid Wins:**
```sql
-- Analyze document patterns
SELECT 
    product_type_name,
    AVG(jsonb_array_length(documents)) as avg_docs
FROM archive.leads
GROUP BY product_type_name;

-- Payment analysis
SELECT 
    closure_reason_name,
    SUM((payment->>'amount')::decimal) as total_paid
FROM archive.leads,
     jsonb_array_elements(payments) as payment
GROUP BY closure_reason_name;
```

**Benefits:**
- ✅ JSONB enables flexible nested queries
- ✅ No need for separate child tables
- ✅ GIN indexes accelerate JSONB queries

---

#### Use Case 4: Self-Service BI for Non-Technical Users

**Context:**
- Business analysts query archives via Datalens
- Not SQL experts (prefer simple queries)
- Want to explore data without DBA help

**Why Hybrid Wins:**
- ✅ Single flat table (easy to understand)
- ✅ No JOINs needed (common source of errors)
- ✅ All columns visible in one place
- ✅ Pre-built views for common patterns

**Example:**
```sql
-- Simple filter (no JOINs!)
SELECT * FROM archive.leads 
WHERE closure_reason_name = 'Customer Request'
  AND customer_region = 'US-West'
  AND amount > 10000;

-- vs Traditional (requires knowing table relationships)
SELECT l.* 
FROM archive.leads l
JOIN closure_reasons cr ON l.closure_reason_id = cr.id
WHERE cr.name = 'Customer Request'
  AND l.amount > 10000;
```

---

### When NOT to Use Hybrid Approach

#### Anti-Pattern 1: Very Low Retrieval Volume

**Scenario:**
- < 20 archive queries/month
- Archives accessed only during annual audits

**Problem:**
- Warm tier costs $200/month
- Only used ~2 days/year
- Wasted $2,400/year

**Better Choice:** Warm-on-Demand
- Scales to $5/month base cost
- Load to staging only during audits

---

#### Anti-Pattern 2: Highly Volatile Reference Data

**Scenario:**
- Product names change weekly
- Agent names update frequently
- Need archives to reflect current reference data

**Problem:**
- Denormalized data is frozen at archive time
- product_type_name = 'Old Name' (can't update)
- Users confused by stale names

**Better Choice:** Traditional Normalized Archive
- agent_id → agents.name (always current)

---

#### Anti-Pattern 3: Frequent Need for Child-Table-Only Queries

**Scenario:**
- 80% of queries are: "Show me all documents of type X"
- Rarely query parent lead

**Problem:**
```sql
-- Must scan all leads to extract documents
SELECT 
    doc->>'document_id',
    doc->>'document_type'
FROM archive.leads,
     jsonb_array_elements(documents) as doc
WHERE doc->>'document_type' = 'ID Proof';

-- vs Normalized (efficient)
SELECT * FROM archive.lead_documents WHERE document_type = 'ID Proof';
```

**Better Choice:** Traditional Normalized Archive
- Separate child tables with indexes

---

#### Anti-Pattern 4: Real-Time Updates to Archived Data

**Scenario:**
- Need to update archived leads occasionally
- E.g., correcting data errors post-archival

**Problem:**
- Denormalized archive is read-only by design
- Updating JSONB arrays is complex
- Breaks immutability principle

**Better Choice:** Traditional Normalized Archive
- Can UPDATE specific tables/columns

---

### Hybrid vs Other Approaches

| Aspect | Hybrid Two-Phase | Traditional Two-Phase | Warm-on-Demand |
|--------|------------------|----------------------|----------------|
| **Query Simplicity** | ✅ Simple (no JOINs) | ❌ Complex (many JOINs) | ✅ Simple (transform on load) |
| **Query Performance** | ✅ Fast (10x) | ⚠️ Moderate (JOIN overhead) | ✅ Fast (after load) |
| **Archival Complexity** | ❌ Complex (denormalization) | ✅ Simple (copy) | ❌ Complex (denormalization) |
| **Historical Accuracy** | ✅ Point-in-time frozen | ❌ Current reference data | ✅ Point-in-time frozen |
| **Storage Cost** | ⚠️ 40% more | ✅ Minimal | ✅ Minimal |
| **Schema Evolution** | ✅ Mostly additive | ❌ Complex migrations | ✅ Transform on load |
| **Cost (low retrieval)** | ❌ $200/month fixed | ❌ $200/month fixed | ✅ $5/month scales |
| **Cost (high retrieval)** | ✅ $200/month fixed | ✅ $200/month fixed | ❌ $1000+/month |
| **Best For** | High retrieval, complex data, simple queries | Stable schema, FK accuracy needed | Low retrieval, budget-constrained |

---

## 11. Operational Playbook

### Daily Operations

#### Monitoring Warm Archive DB

```sql
-- Check warm tier size
SELECT 
    pg_size_pretty(pg_database_size('archive')) as db_size,
    COUNT(*) as lead_count,
    MIN(archived_at) as oldest,
    MAX(archived_at) as newest
FROM archive.leads;

-- Schema version distribution
SELECT 
    schema_version,
    COUNT(*) as count,
    MIN(archived_at) as first,
    MAX(archived_at) as last
FROM archive.leads
GROUP BY schema_version
ORDER BY schema_version;

-- Check for Phase 2 backlog
SELECT COUNT(*) as overdue_for_cold
FROM archive.leads
WHERE archived_at < NOW() - INTERVAL '2 years';
-- Should be near 0
```

---

#### Monitoring Phase 1 Job

```python
# Airflow monitoring
def check_phase1_health():
    last_run = get_last_dag_run('archive_hot_to_warm_denormalized')
    
    checks = {
        'success': last_run.state == 'success',
        'duration_ok': last_run.duration < 3600,  # < 1 hour
        'leads_archived': last_run.conf['leads_processed'],
        'expected_leads': calculate_expected_leads()
    }
    
    if not checks['success']:
        alert('Phase 1 archival failed!')
    
    if checks['leads_archived'] < checks['expected_leads'] * 0.8:
        alert('Phase 1 archived fewer leads than expected')
    
    return checks
```

---

#### Monitoring Phase 2 Job

```python
def check_phase2_health():
    last_run = get_last_dag_run('archive_warm_to_cold_parquet')
    
    # Check warm tier for old data
    overdue_count = warm_db.query_scalar("""
        SELECT COUNT(*) FROM archive.leads
        WHERE archived_at < NOW() - INTERVAL '2 years'
    """)
    
    if overdue_count > 1000:
        alert(f'Phase 2 backlog: {overdue_count} leads overdue for cold migration')
    
    # Check cold storage growth
    cold_size = get_minio_bucket_size('archive/leads')
    expected_size = calculate_expected_cold_size()
    
    if cold_size < expected_size * 0.9:
        alert('Cold storage smaller than expected - check Phase 2 job')
```

---

### Troubleshooting Guide

#### Issue 1: Phase 1 Denormalization Failing

**Symptoms:**
- Phase 1 job fails during denormalization
- Error: "FK reference not found"

**Diagnosis:**
```python
# Check for orphaned FKs
orphaned_closure_reasons = primary_db.query("""
    SELECT lead_id, closure_reason_id
    FROM leads
    WHERE closure_reason_id NOT IN (SELECT id FROM closure_reasons)
""")

orphaned_agents = primary_db.query("""
    SELECT lead_id, agent_id
    FROM leads
    WHERE agent_id NOT IN (SELECT id FROM agents)
""")
```

**Resolution:**
```python
# Option A: Fix orphaned FKs in primary DB
for lead in orphaned_leads:
    primary_db.execute("""
        UPDATE leads SET closure_reason_id = 1  -- Default 'Unknown'
        WHERE lead_id = ?
    """, lead['lead_id'])

# Option B: Handle gracefully in denormalization
def _lookup_closure_reason(self, reason_id):
    result = self.primary_db.query_one(
        "SELECT name FROM closure_reasons WHERE id = ?", reason_id
    )
    return result['name'] if result else 'Unknown (ID: {})'.format(reason_id)
```

---

#### Issue 2: JSONB Query Performance Degradation

**Symptoms:**
- Queries on documents, activities slow down
- P95 latency > 5 seconds

**Diagnosis:**
```sql
-- Check if GIN indexes exist
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'leads' 
  AND schemaname = 'archive';

-- Check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans
FROM pg_stat_user_indexes
WHERE schemaname = 'archive'
ORDER BY idx_scan ASC;
```

**Resolution:**
```sql
-- Create GIN indexes if missing
CREATE INDEX CONCURRENTLY idx_documents_gin 
ON archive.leads USING GIN (documents);

CREATE INDEX CONCURRENTLY idx_activities_gin 
ON archive.leads USING GIN (activities);

CREATE INDEX CONCURRENTLY idx_payments_gin 
ON archive.leads USING GIN (payments);

-- Reindex if needed
REINDEX INDEX CONCURRENTLY idx_documents_gin;
```

---

#### Issue 3: Warm Tier Running Out of Space

**Symptoms:**
- Warm DB storage > 80%
- Phase 2 job not draining old data

**Diagnosis:**
```sql
-- Check Phase 2 eligibility
SELECT 
    COUNT(*) as eligible_count,
    MIN(archived_at) as oldest
FROM archive.leads
WHERE archived_at < NOW() - INTERVAL '2 years';

-- Check if Phase 2 is running
SELECT * FROM job_executions
WHERE dag_id = 'archive_warm_to_cold_parquet'
  AND started_at > NOW() - INTERVAL '7 days'
ORDER BY started_at DESC;
```

**Resolution:**
```python
# Option A: Manually trigger Phase 2 job
trigger_dag('archive_warm_to_cold_parquet', conf={'batch_size': 5000})

# Option B: Lower Phase 2 age threshold temporarily
# Change from 2 years to 1.5 years to drain faster
update_dag_config('archive_warm_to_cold_parquet', {
    'eligibility_interval': '18 months'
})

# Option C: Increase warm DB storage (temporary)
resize_db_storage('warm-archive-db', new_size_gb=100)
```

---

### Disaster Recovery

#### Scenario 1: Warm DB Corruption

**Recovery:**
```
1. Identify corruption time (e.g., 10:00 AM today)

2. Restore from snapshot:
   - RDS: Restore to point-in-time 09:55 AM
   - Self-hosted: pg_restore from last backup

3. Verify manifest consistency:
   SELECT COUNT(*) FROM archival_manifest WHERE tier = 'warm';
   SELECT COUNT(*) FROM archive.leads;
   -- Counts should match

4. Rerun Phase 1 for any leads archived 09:55-10:00 AM:
   - Query manifest for leads archived in that window
   - Re-denormalize from primary DB (if still there)
   - Or accept data loss for that 5-minute window

5. Resume normal operations
```

---

#### Scenario 2: Cold Storage Bucket Deleted

**Recovery:**
```
1. Check MinIO versioning:
   minio-client ls --versions s3://archive/leads/
   
2. Restore deleted objects:
   minio-client undo s3://archive/leads/

3. If versioning not enabled:
   - Warm DB has last 2 years (recoverable)
   - Data older than 2 years: Check backups
   - If total loss: Export warm DB to cold to rebuild
   
4. Prevent recurrence:
   - Enable MinIO versioning
   - Enable bucket lifecycle lock
   - Add IAM policy to prevent delete
```

---

## 12. Cost Analysis

### Infrastructure Costs (1M Leads, 7 Years)

#### Warm Archive DB (Denormalized)

```
Instance: db.r5.xlarge (4 vCPU, 32 GB RAM)
Storage: 70 GB (40% more due to denormalization)
Region: us-east-1

Monthly Cost:
- Instance: $175/month (on-demand)
- Storage: 70 GB × $0.115/GB = $8.05/month
- Backup: 70 GB × $0.095/GB = $6.65/month
- Total: $189.70/month

Annual: $2,276
7-Year: $15,934

With Reserved Instance (1-year):
- Instance: $115/month (34% savings)
- Total: $129.70/month
- 7-Year: $10,893
```

**vs Traditional Normalized (50 GB):**
```
Denormalized: $10,893
Normalized: $9,500
Difference: +$1,393 (15% more) for 7 years
```

---

#### Cold Storage (Parquet)

```
Storage: 1 GB (Parquet compression handles denormalization well)
MinIO/S3 Standard: $0.023/GB/month

Monthly: $0.023
7-Year: $1.93 (negligible)
```

---

#### Retrieval Costs (Multi-Modal)

```
Mode 1 (Instant SQL - Warm):
- Cost: $0 (included in warm DB)
- Volume: 90% of queries

Mode 2 (Staged Retrieval - Cold):
- Cost per lead: $0.01 (load to staging)
- Volume: 5% of queries
- Monthly: 20 leads × $0.01 = $0.20

Mode 3 (Bulk Staging DB):
- Cost per case: $800 (DB provisioning + 7 day runtime)
- Volume: 1 case/quarter
- Annual: $3,200

Mode 4 (Analytics Direct - DuckDB):
- Cost: Negligible (compute only)
- Volume: 5% of queries

Total Retrieval Cost (Annual): $3,202
7-Year: $22,414
```

---

### Total Cost of Ownership (7 Years)

```
Infrastructure:
- Warm DB: $10,893
- Cold Storage: $2
- Subtotal: $10,895

Operations:
- Engineering (schema migrations, DAG maintenance): $35,000
- On-call (DB monitoring, incident response): $25,000
- Retrieval operations: $22,414
- Subtotal: $82,414

Total: $93,309

Cost per lead: $93,309 / 42,000 = $2.22 per lead over 7 years
Monthly average: $1,110
```

---

### Comparison: Hybrid vs Alternatives (7 Years)

| Cost Category | Hybrid Two-Phase | Traditional Two-Phase | Warm-on-Demand |
|---------------|------------------|----------------------|----------------|
| **Warm/Staging DB** | $10,893 | $9,500 | $420 |
| **Cold Storage** | $2 | $3 | $900 |
| **Engineering** | $35,000 | $42,000 | $10,500 |
| **On-Call** | $25,000 | $35,000 | $3,500 |
| **Retrieval** | $22,414 | $0 | $25,000 |
| **Total** | **$93,309** | **$86,503** | **$40,320** |
| **vs Hybrid** | Baseline | -7% cheaper | -57% cheaper |

---

### Break-Even Analysis

**Hybrid vs Traditional Two-Phase:**
```
Hybrid is 8% more expensive due to:
- +15% warm DB storage (denormalization overhead)
- +Retrieval costs (multi-modal infrastructure)

But provides:
- 10x faster queries (no JOINs)
- Simpler user experience
- Better historical accuracy

ROI: If query performance saves 1 hour/week of analyst time:
  1 hour/week × 52 weeks × $150/hour = $7,800/year
  7 years: $54,600 in productivity gains
  
Net benefit: $54,600 - $6,806 = $47,794 ✅
```

**Hybrid vs Warm-on-Demand:**
```
Hybrid is 2.3x more expensive

Break-even if retrieval > 80 leads/month:
  Warm-on-Demand: $60 base + (80 × $5) = $460/month
  Hybrid: $1,110/month but includes instant access
  
Choose Hybrid if:
- Retrieval > 80/month, OR
- Instant access required, OR
- Query complexity justifies warm tier
```

---

## 13. Conclusion

### Summary

The **Hybrid Two-Phase Archival Strategy** combines:
- **Denormalized warm tier** for simple, fast SQL queries
- **Parquet cold tier** for cost-efficient long-term storage
- **Multi-modal retrieval** for flexible access patterns

---

### Key Innovations

**1. Denormalization at Archival**
- All FKs resolved, children embedded as JSONB
- Single-table queries (10x faster than JOINs)
- Point-in-time historical accuracy

**2. Multi-Modal Retrieval**
- Mode 1: Instant SQL (warm tier)
- Mode 2: Staged retrieval (cold → temp warm)
- Mode 3: Bulk staging DB (large investigations)
- Mode 4: Analytics direct (DuckDB on Parquet)

**3. Simplified Phase 2**
- Warm already denormalized → direct Parquet export
- No denormalization logic in Phase 2

---

### When to Choose Hybrid

✅ **Choose Hybrid If:**
- High query volume (> 80 requests/month)
- Complex multi-table data relationships
- Users need simple single-table queries
- Query performance is critical
- Historical point-in-time accuracy required
- Can afford 15% storage premium
- Want flexible retrieval modes

❌ **Don't Choose Hybrid If:**
- Very low retrieval (< 20/month) → Use Warm-on-Demand
- Reference data must stay current → Use Traditional Normalized
- Budget severely constrained → Use Warm-on-Demand
- Mostly child-table-only queries → Use Traditional Normalized

---

### Recommended Approach

**For Most Organizations:**
```
1. Start with Hybrid if:
   - Known high query volume
   - Complex data model
   - Query simplicity valued
   
2. Monitor for 6 months:
   - Query patterns
   - Retrieval volume
   - User satisfaction
   
3. Optimize:
   - Add JSONB views for common patterns
   - Tune GIN indexes
   - Adjust Phase 2 age threshold
```

---

### Final Thought

**The hybrid approach trades archival complexity for query simplicity.**

It's ideal when your users (analysts, CS agents, compliance teams) query archives frequently and benefit from simple, fast, single-table queries that don't require SQL expertise.

The 10x query performance improvement and user experience gains typically justify the 15% storage cost increase and denormalization complexity.

---

## Appendix: Architecture Diagrams

### Detailed Component Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         APPLICATION LAYER                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                 │
│  │   Backend    │  │   Datalens   │  │  Retrieval   │                 │
│  │   Services   │  │   (BI Tool)  │  │  Coordinator │                 │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                 │
└─────────┼──────────────────┼──────────────────┼───────────────────────┘
          │                  │                  │
          │                  │                  │
┌─────────▼──────────────────▼──────────────────▼───────────────────────┐
│                        QUERY ROUTING LAYER                             │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  Query Analyzer & Router                                        │  │
│  │  - Parse SQL                                                    │  │
│  │  - Check manifest for tier (hot/warm/cold)                     │  │
│  │  - Determine query type (point/aggregate/bulk)                 │  │
│  │  - Route to optimal execution path                             │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└──────┬──────────────┬──────────────┬──────────────┬────────────────────┘
       │              │              │              │
       │ Hot          │ Warm         │ Cold         │ Cold
       │              │              │ (Staged)     │ (Direct)
       ↓              ↓              ↓              ↓
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   PRIMARY    │ │     WARM     │ │   STAGING    │ │    DUCKDB    │
│  POSTGRES    │ │   ARCHIVE    │ │  (Temp DB)   │ │   ENGINE     │
│              │ │    (Perm)    │ │              │ │              │
│ Normalized   │ │ Denormalized │ │ Denormalized │ │ Query        │
│ 0-3 months   │ │ 3m - 2 years │ │ Cold→Warm    │ │ Parquet      │
└──────────────┘ └──────────────┘ └──────────────┘ └──────┬───────┘
                                                           │
                                                           │
                                                    ┌──────▼───────┐
                                                    │    MINIO     │
                                                    │   (Parquet)  │
                                                    │              │
                                                    │ 2-7 years    │
                                                    │ Immutable    │
                                                    └──────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                      ARCHIVAL PIPELINE LAYER                            │
│  ┌─────────────────────┐          ┌─────────────────────┐             │
│  │  Phase 1 DAG        │          │  Phase 2 DAG        │             │
│  │  (Weekly)           │          │  (Monthly)          │             │
│  │                     │          │                     │             │
│  │  Hot → Warm         │          │  Warm → Cold        │             │
│  │  + Denormalize      │          │  + Export Parquet   │             │
│  └─────────────────────┘          └─────────────────────┘             │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                       METADATA LAYER                                    │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  Archival Manifest (Primary Postgres)                           │  │
│  │  - Tracks all archived entities                                 │  │
│  │  - Maps entity_id → tier (warm/cold)                           │  │
│  │  - Records schema_version, locations, checksums                │  │
│  │  - Never archived itself                                       │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-25  
**Author:** Engineering Team  
**Status:** Design Proposal — Pending Review
