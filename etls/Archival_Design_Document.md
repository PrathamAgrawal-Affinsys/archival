**Archival Design Proposal**  
PostgreSQL Archival Strategy — bud-core-DBOps-svc

| Field | Value |
| :---- | :---- |
| Document Type | Archival Design Document |
| Service | bud-core-DBOps-svc |
| Strategy | Logical Decoupling — Denormalized Snapshot exported via configurable target (Parquet to MinIO, CSV to MinIO, or External PostgreSQL Database) |
| Archive Trigger | Timerange column  \< NOW() \- INTERVAL 3 months |
| Author | Engineering |
| Status | Draft |

This document is made taking the leads data as an example. It will be expanded to more data sources / schemas in the near future. 

# **1\. Executive Summary**

As the bud-core-DBOps-svc lead data grows, retaining all records indefinitely in the primary PostgreSQL instance creates performance, cost, and compliance risks. This document defines the Logical Decoupling Archival strategy — an approach that dissolves all relational foreign-key dependencies into a self-contained snapshot per lead before moving data out of Postgres.

The pipeline supports **three configurable export targets**:

| # | Export Target | Destination | Best For |
| :---- | :---- | :---- | :---- |
| 1 | **Parquet** (default) | MinIO object storage | Long-term analytics, columnar queries, maximum compression |
| 2 | **CSV** | MinIO object storage | Interoperability with external teams, spreadsheet tooling, simple audits |
| 3 | **External PostgreSQL Database** | Dedicated external Postgres instance | Relational queries against archived data, SQL-native consumers |

Once the snapshot is written and verified in the chosen export target, rows can be deleted from the primary Postgres instance cleanly with no FK violations, no partial states, and no dependency on any other table to make the archive readable in the future.

| Core principle:  Copy values, not keys. Every reference (closure reason, agent name, product type) is resolved to its human-readable label at archive time. The envelope needs nothing from Postgres to be meaningful two years from now. |
| :---- |

## **Key Design Decisions**

* Root entity is the Lead — all children are archived together as one envelope  
* 3-month age threshold after closure, with masking and purging as prerequisites  
* **Configurable export target** — Parquet (default), CSV, or External PostgreSQL Database  
* Parquet/CSV: Hive-partitioned folder layout in MinIO for efficient time-range queries  
* External DB: Dedicated archive schema on a separate Postgres instance for SQL-native access  
* Permanent archival\_manifest table in primary Postgres as the index into all archives regardless of export target  
* Deletion order: leaves before root — deepest children deleted first  
* Idempotent pipeline — safe to re-run at any stage without data loss

# **2\. The Archival Unit**

The archival unit is a single Lead and everything hanging off it. All child records are embedded inside the envelope — there are no remaining foreign key references after denormalization.

## **Envelope Structure**

Archive envelope for LEAD-1001

├── Lead fields            (closure\_reason as label, not FK id)

├── Customer snapshot      (region, segment — not a live FK)

├── Agent snapshot         (name, team at time of closure)

├── lead\_documents\[\]       (full array, embedded)

├── lead\_activities\[\]      (full array, embedded)

├── lead\_assignments\[\]     (full array, embedded)

└── payments\[\]

        └── line\_items\[\]   (fully nested)

## **Why Denormalize Reference Data**

Tables such as closure\_reasons, product\_types, and agents are operational — they must stay in Postgres. The archive cannot FK-depend on them. The solution is to copy the human-readable value at archive time:

| Instead of archiving | Archive this instead |
| :---- | :---- |
| closure\_reason\_id \= 7 | closure\_reason \= "Customer Request — Repaid" |
| product\_type\_id \= 3 | product\_type \= "Personal Loan" |
| agent\_id \= 456 | agent\_name \= "Ravi Kumar", agent\_team \= "Collections" |

The schema\_version field is stamped onto every envelope so that readers know which shape to expect, even after Postgres schema migrations.

# **3\. Eligibility Criteria**

A lead qualifies for archival when all of the following conditions are simultaneously true:

| Condition | Column / Check | Reason |
| :---- | :---- | :---- |
| Closed at least 3 months ago | closed\_at \< NOW() \- INTERVAL '3 months' | Business rule — data must be inactive |
| PII has been masked | pii\_masked \= TRUE | Compliance prerequisite — must mask before archive |
| Purging is complete | purged\_at IS NOT NULL | Data lifecycle prerequisite |
| No active sibling leads | Customer has no other active leads | Cannot orphan shared customer row |
| No open payments | All payments in terminal state | No dangling financial records |

| Important:  The eligibility rule must be immutable once set in production. Changing it mid-flight risks gaps (leads that fall between two rule versions) or double-archival. Rules live in the task configuration YAML, consistent with the existing DBOps-svc pattern. |
| :---- |

# **4\. Pipeline Phases**

The archival pipeline runs as a scheduled Airflow DAG (weekly, Sunday early morning). It processes leads in batches of 500\. Each phase is independently transactional and the entire pipeline is idempotent — it is safe to re-run at any point.

## **Phase 1 — Identify and Validate**

The DAG queries eligible leads in batches. For each batch, a blocking-relationship check runs before any data is read or written:

* Query leads matching all eligibility criteria  
* For each lead, verify no active siblings under the same customer  
* Verify no open payments remain  
* Leads with blocking relationships are skipped and logged — the rest continue

## **Phase 2 — Denormalize into Self-Contained Snapshot**

The DAG fetches the complete lead graph and assembles a single Python dictionary per lead. This is the most important phase:

* Fetch lead row \+ all child tables in a single query block  
* Resolve all FK references to human-readable values  
* Stamp schema\_version onto the envelope  
* No foreign keys remain in the resulting structure

## **Phase 3 — Export, Verify, Promote**

The export phase branches based on the configured export target. In every case, data is written to a staging area first, verified, and only then promoted. If verification fails, the staged data is removed and the lead remains in Postgres untouched.

### **Option A — Parquet (Default)**

1. Write envelope(s) to MinIO staging path: /staging/leads/…  
2. Verify: row count matches export · SHA256 checksum matches · sample read of 10 rows passes  
3. Atomic move to final Hive-partitioned path:

    minio://backup/leads/year=2025/month=01/LEAD-1001\_v3.parquet

### **Option B — CSV**

1. Write envelope(s) as CSV to MinIO staging path: /staging/leads/…  
   * One CSV file per child table per lead (e.g., `LEAD-1001_payments_v3.csv`, `LEAD-1001_activities_v3.csv`)  
   * A root CSV (`LEAD-1001_lead_v3.csv`) holds the denormalized lead-level fields  
   * All files share a `_header.csv` schema definition file per batch for column ordering  
2. Verify: row count matches export · SHA256 checksum matches · header consistency check across files  
3. Atomic move to final Hive-partitioned path:

    minio://backup-csv/leads/year=2025/month=01/LEAD-1001\_lead\_v3.csv
    minio://backup-csv/leads/year=2025/month=01/LEAD-1001\_payments\_v3.csv
    …

### **Option C — External PostgreSQL Database**

1. Connect to the configured external Postgres instance using connection parameters from the DAG configuration (host, port, database, schema, credentials via secrets manager)  
2. Insert envelope data into the archive schema tables within a single transaction:
   * `archive.leads` — denormalized lead-level fields  
   * `archive.lead_documents` — embedded documents  
   * `archive.lead_activities` — embedded activities  
   * `archive.lead_assignments` — embedded assignments  
   * `archive.payments` — payments with nested line items stored as JSONB  
3. Verify: row count in external DB matches expected count · checksum of inserted rows matches source  
4. On verification failure, the transaction is rolled back. The lead remains in primary Postgres untouched.

| External DB note:  The archive schema on the external Postgres instance mirrors the denormalized envelope structure — not the normalized source schema. All FK references are already resolved to human-readable values. No foreign keys exist between archive tables and the primary instance. |
| :---- |

### **Schema Provisioning via DAG**

The archive schema tables on the external Postgres instance are **created and managed by the archival DAG itself** — not through manual DDL scripts or external migration tools. This gives the pipeline full control over table structure and versioning:

* **First-run bootstrap**: On the very first execution (or when pointing to a new external DB), the DAG runs a `ensure_archive_schema` task that creates the `archive` schema and all required tables if they do not already exist. Table definitions are derived from the current `schema_version`.
* **Version-aware migration**: When `schema_version` changes (e.g., v3 → v4), the DAG detects the mismatch between the declared version and the existing table structure. It applies **additive, non-breaking migrations** (e.g., `ALTER TABLE ADD COLUMN`) as part of a `migrate_archive_schema` task that runs before any inserts.
* **Migration safety rules**:
  * Only additive changes are applied automatically (new columns, new tables). Destructive changes (drop column, rename) require manual approval and a dedicated migration DAG.
  * Every migration is recorded in an `archive.schema_migrations` table (version, applied\_at, migration\_sql) for auditability.
  * The DAG will **refuse to insert** if the target schema version is behind and cannot be auto-migrated — it logs an `ARCHIVAL_FAILED` event and halts the batch.
* **Why not external migration tools**: Keeping schema management inside the DAG avoids a split-brain problem where the pipeline expects one schema shape but an external migration tool has applied a different one. The DAG is the single source of truth for what the archive tables should look like at any given `schema_version`.

## **Phase 4 — Write Archival Manifest**

Before any deletion, a row is written to archival\_manifest — a permanent Postgres table that is never itself archived. This is the index into all archives:

| Column | Description |
| :---- | :---- |
| manifest\_id | Primary key |
| archived\_entity\_type | 'lead' |
| archived\_entity\_id | e.g. 'LEAD-1001' |
| export\_target | 'parquet', 'csv', or 'external\_db' |
| archive\_location | Full MinIO path (Parquet/CSV) or external DB connection identifier + schema reference |
| archived\_at | Timestamp of archival |
| row\_counts | JSONB: {lead:1, documents:4, payments:2, line\_items:5} |
| checksum | SHA256 of the export (file checksum for Parquet/CSV, row-hash for external DB) |
| schema\_version | e.g. 'v3' |
| archived\_by\_job | FK to JobExecution |

## **Phase 5 — Delete from Postgres (Leaves to Root)**

Deletion order is strict. Each table is deleted in its own transaction. If any step fails, the pipeline stops — the Parquet file in MinIO is intact and the manifest records what was already deleted, making re-runs safe.

4. payment\_line\_items  (deepest child)  
5. payments  
6. lead\_assignments  
7. lead\_activities  
8. lead\_documents  
9. leads  (root — deleted last)

| Customer rows:  The customer row is NOT deleted unless all of that customer's leads are archived. The manifest tracks this. The customer row stays in Postgres until the last of their leads completes archival. |
| :---- |

## **Phase 6 — Log to AuditLog and Notify**

Two events are written to the existing AuditLog:

* ARCHIVAL\_COMPLETED — file path, row counts, checksum, schema version  
* DELETION\_COMPLETED — entity id, tables affected, row counts deleted

A summary notification is sent to the operations team listing leads archived, total export size, and any skips with their reasons.

# **5\. Airflow DAG Structure**

The archival DAG sits alongside the existing transfer, masking, and purging DAGs. It follows the same patterns — XCom for state, batch processing, partial failure handling, and idempotent task execution.

## **Task Chain**

identify\_eligible\_leads

        ↓

validate\_blocking\_relationships

        ↓

denormalize\_lead\_graph          ← assemble full self-contained envelope

        ↓

**branch\_on\_export\_target**      ← reads export\_target from DAG config

        ↓                       ↓                         ↓

┌─────────────────┐  ┌─────────────────────┐  ┌──────────────────────────────┐
│ PARQUET (default)│  │ CSV                 │  │ EXTERNAL POSTGRESQL DATABASE │
├─────────────────┤  ├─────────────────────┤  ├──────────────────────────────┤
│ write\_parquet\_ │  │ write\_csv\_to\_      │  │ connect\_external\_db         │
│  to\_staging     │  │  staging            │  │         ↓                   │
│     ↓           │  │     ↓               │  │ insert\_into\_archive\_schema  │
│ verify\_staging  │  │ verify\_staging      │  │         ↓                   │
│  \_file          │  │  \_file              │  │ verify\_external\_db\_rows     │
│     ↓           │  │     ↓               │  │                              │
│ promote\_to\_    │  │ promote\_to\_        │  │                              │
│  final\_minio   │  │  final\_minio        │  │                              │
└────────┬────────┘  └──────────┬──────────┘  └──────────────┬───────────────┘

         └───────────────────── ↓ ─────────────────────────── ┘

write\_archival\_manifest         ← Postgres insert (permanent) — records export\_target used

        ↓

delete\_in\_order                 ← leaves to root, per-table transactions

        ↓

log\_archival\_events             ← AuditLog entries

        ↓

notify

## **DAG Configuration**

| Parameter | Value | Reason |
| :---- | :---- | :---- |
| Schedule | Sunday 2 AM (0 2 \* \* 0\) | Lowest traffic window |
| Batch size | 500 leads per run | Predictable duration, easy to monitor |
| Max active runs | 1 | Prevent concurrent archival conflicts |
| Retry attempts | 3 with 10-min delay | Transient MinIO / DB errors |
| **export\_target** | **parquet** (default) / csv / external\_db | **Determines which export branch the DAG follows** |
| external\_db\_conn\_id | Airflow connection ID (only when export\_target = external\_db) | Credentials managed via Airflow secrets backend |
| Trigger rule (delete task) | all\_success | Delete only if all prior tasks passed |
| Trigger rule (notify task) | all\_done | Notify even on partial failure |

# **6\. Storage Layouts by Export Target**

## **6a. Parquet — MinIO Storage Layout**

Parquet files follow Hive-style partitioning so that query tools can filter by date without scanning all files:

minio://backup/

  leads/

    year=2025/

      month=01/

        LEAD-1001\_v3.parquet

        LEAD-1002\_v3.parquet

        LEAD-1003\_v3.parquet

      month=02/

        LEAD-2001\_v3.parquet

    year=2026/

      month=01/

        ...

## **6b. CSV — MinIO Storage Layout**

CSV files follow the same Hive-style partitioning. Because CSV cannot nest arrays, each child table gets its own file per lead:

minio://backup-csv/

  leads/

    year=2025/

      month=01/

        \_header\_v3.csv                  ← column definitions for this schema version

        LEAD-1001\_lead\_v3.csv           ← root lead fields

        LEAD-1001\_documents\_v3.csv      ← child: lead\_documents

        LEAD-1001\_activities\_v3.csv     ← child: lead\_activities

        LEAD-1001\_assignments\_v3.csv    ← child: lead\_assignments

        LEAD-1001\_payments\_v3.csv       ← child: payments + line items flattened

        LEAD-1002\_lead\_v3.csv

        ...

      month=02/

        ...

| CSV convention:  Each CSV file includes a header row. A companion `_header_v3.csv` file is written once per batch to define column names, types, and ordering for the schema version. JSONB fields (e.g., payload) are serialized as escaped JSON strings in a single column. |
| :---- |

## **6c. External PostgreSQL Database — Archive Schema**

The external Postgres instance hosts a dedicated `archive` schema. Tables mirror the denormalized envelope structure — **not** the normalized source schema. All FK references are resolved to human-readable values at export time.

### **Archive Schema Tables**

| Table | Description | Key Columns |
| :---- | :---- | :---- |
| archive.leads | One row per archived lead | lead\_id (PK), customer\_name, agent\_name, closure\_reason, product\_type, schema\_version, archived\_at |
| archive.lead\_documents | One row per document | document\_id (PK), lead\_id, document\_type, file\_path, created\_at |
| archive.lead\_activities | One row per activity | activity\_id (PK), lead\_id, activity\_type, description, performed\_by, created\_at |
| archive.lead\_assignments | One row per assignment | assignment\_id (PK), lead\_id, assigned\_to, assigned\_from, assigned\_at |
| archive.payments | One row per payment, line items as JSONB array | payment\_id (PK), lead\_id, amount, status, line\_items (JSONB), created\_at |

### **External DB Connection Configuration**

Connection details are stored as an **Airflow connection** (conn\_id) and secrets are managed through the Airflow secrets backend (e.g., Vault, AWS Secrets Manager).

| Parameter | Source | Example |
| :---- | :---- | :---- |
| host | Airflow connection | archive-db.internal.example.com |
| port | Airflow connection | 5432 |
| database | Airflow connection | archival\_store |
| schema | Airflow connection (extra) | archive |
| username | Secrets backend | archival\_writer |
| password | Secrets backend | (managed secret) |
| ssl\_mode | Airflow connection (extra) | require |

| Important:  The external DB must be a **separate Postgres instance** — never the primary application database. Write access is limited to the `archive` schema using a dedicated, least-privilege database role. Connection pooling (e.g., PgBouncer) is recommended for high-batch runs. |
| :---- |

## **Export Target Comparison**

| Property | Parquet | CSV | External PostgreSQL DB |
| :---- | :---- | :---- | :---- |
| Storage size (1M rows) | \~35 MB (columnar compression) | \~800 MB | \~600 MB (row storage + indexes) |
| Queryable without loading into DB | Yes (DuckDB, Athena) | No (must import first) | Yes (standard SQL) |
| Schema embedded | Yes | No (companion header file) | Yes (table DDL) |
| Partial column reads | Yes | No | Yes (SELECT specific columns) |
| Nested data support | Native (structs, arrays) | No (must flatten or serialize) | JSONB for nested arrays |
| Interoperability | Analytics tools, data lakes | Universal — any tool reads CSV | Any SQL client |
| Retrieval latency | Seconds (DuckDB + MinIO) | Seconds (MinIO download) | Sub-second (indexed SQL) |
| Operational overhead | Low (object storage) | Low (object storage) | Medium (DB maintenance, backups, scaling) |
| Best for | Long-term analytics, compliance | External team sharing, audits | SQL-native consumers, low-latency lookups |

| Recommendation:  Parquet remains the **default and recommended** export target for most use cases. CSV should be used when downstream consumers require flat-file interoperability. External PostgreSQL Database should be used when archived data must support ad-hoc relational queries with sub-second latency or when consumers are SQL-native applications. |
| :---- |

# **7\. Retrieval Strategy**

Retrieval behavior depends on the export target used at archival time. Not all export targets are designed for active retrieval — Parquet and CSV serve as **cold storage exports** with no built-in retrieval path back into the application. The External PostgreSQL Database export is the only target that supports live data access, via **Datalens** (our internal data exploration and visualization tool).

## **Parquet & CSV — No Application-Level Retrieval**

Parquet and CSV exports are **write-once, cold storage archives**. They are not queryable through the application's APIs or query router. Their purpose is:

* **Compliance & audit retention** — data exists in durable object storage for the legally required retention period  
* **Disaster recovery** — a full offline copy of archived data is available if ever needed  
* **Ad-hoc forensics** — if a specific archived lead needs to be inspected, an engineer can manually download the file(s) from MinIO and read them locally (e.g., using DuckDB for Parquet, or any spreadsheet tool for CSV)

| There is no automated retrieval pipeline for Parquet or CSV. If an API request hits a lead that was archived to Parquet/CSV, the application returns a `410 Gone` response with the `archived_at` timestamp and `archive_format` from the manifest. The caller is directed to the operations team for manual retrieval if needed. |
| :---- |

## **External PostgreSQL Database — Retrieval via Datalens**

When the export target is `external_db`, archived data lives in a fully queryable Postgres instance and is accessed through **Datalens**, our internal tool for data exploration, dashboarding, and ad-hoc SQL queries.

### **How Datalens Connects**

* Datalens is configured with a **read-only connection** to the external archive Postgres instance  
* The connection points to the `archive` schema, giving Datalens users access to all archive tables (`archive.leads`, `archive.lead_documents`, `archive.payments`, etc.)  
* Access is controlled through Datalens's built-in role-based permissions — only authorized teams (compliance, operations, finance) can view archived data

### **Typical Datalens Use Cases**

| Use Case | How It Works in Datalens |
| :---- | :---- |
| Look up a specific archived lead | SQL query: `SELECT * FROM archive.leads WHERE lead_id = 'LEAD-1001'` |
| Audit trail for a date range | SQL query with filter on `archived_at` column |
| Compliance reporting | Pre-built Datalens dashboard aggregating archived lead counts, closure reasons, and timelines |
| Finance reconciliation | Join `archive.leads` with `archive.payments` to review payment history for archived leads |
| Bulk export for external auditors | Datalens CSV export of query results |

### **Application API Behavior**

When an API request (e.g., `GET /lead/{id}`) hits a lead that was archived to the external DB:

1. Primary Postgres returns no result  
2. The application checks `archival_manifest` — finds `export_target = 'external_db'`  
3. The API returns a `301`-style redirect response with metadata pointing the caller to Datalens:
   * `archived_at` timestamp  
   * `archive_location` reference  
   * A message indicating the data is available in Datalens for authorized users

| The application does NOT proxy queries to the external DB on behalf of API callers. All retrieval for archived data goes through Datalens. This keeps the archival database isolated from production traffic and prevents accidental load on the archive instance. |
| :---- |

# **8\. Safety Rules and Anti-Patterns**

## **The Most Important Safety Rule**

| Never delete from Postgres and write to MinIO in the same transaction.  They are different systems — you cannot atomically commit to both. Always: (1) write to MinIO and verify, (2) write the manifest to Postgres, (3) only then delete from Postgres. If the process dies between steps 2 and 3, the manifest proves the archive exists. The next DAG run skips the write phase and goes straight to deletion. |
| :---- |

## **Idempotency Guarantee**

Every task checks the manifest before acting. If a manifest row exists for a lead:

* write\_parquet — skip, file already exists  
* write\_archival\_manifest — skip, already written  
* delete\_in\_order — check which tables are already deleted, skip those

This means the DAG can be re-run at any point without risk of data loss or double-deletion.

## **Common Mistakes to Avoid**

| Mistake | Correct approach |
| :---- | :---- |
| Deleting and archiving in the same transaction | Two separate transactions — archive first, verify, then delete |
| One giant file for all leads | One file per lead (or small batch) for targeted retrieval |
| Not versioning the archive schema | Stamp schema\_version on every envelope |
| Mixing export targets within a single lead | One lead = one export target. Never split a lead across Parquet and CSV. |
| Using the primary DB as the external archive DB | Always use a **separate** Postgres instance for external DB archival |
| Sharing credentials for external DB in plaintext | Use Airflow secrets backend (Vault, AWS Secrets Manager) |
| Designing archival without a retrieval plan | Design the retrieval query on day one for each export target |
| Checking only file size for verification | Row count \+ checksum \+ sample read — all three (for all export targets) |

# **9\. New Audit Event Types**

The following event types are added to the existing AuditLog event taxonomy defined in the TDD Section 10:

| Event type | Triggered when | Key payload fields |
| :---- | :---- | :---- |
| ARCHIVAL\_STARTED | DAG begins processing a batch | batch\_size, eligible\_count |
| ARCHIVAL\_BATCH\_COMPLETED | One export batch written and verified | export\_target, file\_path or db\_reference, row\_counts, checksum, schema\_version |
| ARCHIVAL\_VERIFIED | Staging verification passes | export\_target, checksum\_match, row\_count\_match |
| ARCHIVAL\_DELETION\_COMPLETED | Postgres rows deleted | entity\_id, tables\_affected, rows\_deleted |
| ARCHIVAL\_FAILED | Any phase fails | phase, export\_target, error, entity\_id |
| ARCHIVAL\_SKIPPED | Lead skipped due to blocking relationship | entity\_id, reason |

# **10\. Open Questions and Next Steps**

## **Decisions Required**

| Question | Options | Recommended |
| :---- | :---- | :---- |
| **Default export target** | **Parquet / CSV / External DB** | **Parquet — best compression and queryability** |
| Batch size | 100 / 500 / 1000 per DAG run | 500 — predictable duration |
| File granularity (Parquet/CSV) | One file per lead vs one file per batch | One per lead — targeted retrieval |
| Customer row handling | Never delete / delete when last lead archived | Never delete — safest option |
| Archive retention | 5 years / 7 years / indefinite | Confirm with compliance team |
| Retrieval SLA | Best effort (days) / guaranteed (hours) | Confirm with business |
| External DB instance provisioning | Shared archive cluster / dedicated per-service | Dedicated — isolation and independent scaling |
| External DB backup strategy | pg\_dump daily / streaming replication / cloud-managed | Confirm with infra team |
| CSV JSONB handling | Serialize as escaped JSON string / flatten to columns | Escaped JSON string — preserves structure |

## **Immediate Next Steps**

14. Confirm 3-month eligibility rule with compliance and business teams  
15. Map full child-table hierarchy for the leads schema (all FK relationships)  
16. Define schema\_version strategy and backwards-compatible reader interface  
17. Create archival\_manifest table DDL and migration (including `export_target` and `archive_location` columns)  
18. Build and test denormalization query for a sample lead in staging  
19. Implement and test Airflow DAG with Parquet export in development environment  
20. Implement CSV export branch and test with sample leads  
21. Provision external Postgres archive instance and create `archive` schema DDL  
22. Implement external DB export branch — connection management, insert logic, verification  
23. Build unified query router that dispatches retrieval based on `export_target` in manifest  
24. Load test: measure primary Postgres performance before and after first archival run  
25. Load test: measure external DB query performance under expected archive volume

*Document prepared for internal engineering review. All decisions subject to compliance and business approval.*