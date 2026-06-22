**Archival Design Proposal**  
PostgreSQL Archival Strategy — bud-core-DBOps-svc

| Field | Value |
| :---- | :---- |
| Document Type | Archival Design Document |
| Service | bud-core-DBOps-svc |
| Strategy | Logical Decoupling — Denormalized Snapshot to CSV / Parquet / MinIO |
| Archive Trigger | Timerange column  \< NOW() \- INTERVAL 3 months |
| Author | Engineering |
| Status | Draft |

This document is made taking the leads data as an example. It will be expanded to more data sources / schemas in the near future. 

# **1\. Executive Summary**

As the bud-core-DBOps-svc lead data grows, retaining all records indefinitely in the primary PostgreSQL instance creates performance, cost, and compliance risks. This document defines the Logical Decoupling Archival strategy — an approach that dissolves all relational foreign-key dependencies into a self-contained snapshot per lead before moving data out of Postgres.

Once the snapshot is written and verified in MinIO object storage as a Parquet file, rows can be deleted from Postgres cleanly with no FK violations, no partial states, and no dependency on any other table to make the archive readable in the future.

| Core principle:  Copy values, not keys. Every reference (closure reason, agent name, product type) is resolved to its human-readable label at archive time. The envelope needs nothing from Postgres to be meaningful two years from now. |
| :---- |

## **Key Design Decisions**

* Root entity is the Lead — all children are archived together as one envelope  
* 3-month age threshold after closure, with masking and purging as prerequisites  
* Parquet format for columnar compression and direct queryability via DuckDB  
* Hive-partitioned folder layout in MinIO for efficient time-range queries  
* Permanent archival\_manifest table in Postgres as the index into archives  
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

## **Phase 3 — Write Parquet, Verify, Promote**

Files are written to a staging path first. Verification runs before promotion:

1. Write envelope(s) to MinIO staging path: /staging/leads/…  
2. Verify: row count matches export · SHA256 checksum matches · sample read of 10 rows passes  
3. Atomic move to final Hive-partitioned path:

    minio://backup/leads/year=2025/month=01/LEAD-1001\_v3.parquet

If verification fails, the staging file is deleted. The lead remains in Postgres untouched.

## **Phase 4 — Write Archival Manifest**

Before any deletion, a row is written to archival\_manifest — a permanent Postgres table that is never itself archived. This is the index into all archives:

| Column | Description |
| :---- | :---- |
| manifest\_id | Primary key |
| archived\_entity\_type | 'lead' |
| archived\_entity\_id | e.g. 'LEAD-1001' |
| archive\_file\_path | Full MinIO path to the Parquet file |
| archived\_at | Timestamp of archival |
| row\_counts | JSONB: {lead:1, documents:4, payments:2, line\_items:5} |
| checksum | SHA256 of the Parquet file |
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

write\_parquet\_to\_staging

        ↓

verify\_staging\_file             ← row count \+ checksum \+ sample read

        ↓

promote\_to\_final\_minio\_path     ← atomic move

        ↓

write\_archival\_manifest         ← Postgres insert (permanent)

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
| Trigger rule (delete task) | all\_success | Delete only if all prior tasks passed |
| Trigger rule (notify task) | all\_done | Notify even on partial failure |

# **6\. MinIO Storage Layout**

Files follow Hive-style partitioning so that query tools can filter by date without scanning all files. This is the layout inside the backup bucket:

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

## **Why Parquet**

| Property | CSV | Parquet |
| :---- | :---- | :---- |
| Storage size (1M rows) | \~800 MB | \~35 MB (columnar compression) |
| Queryable without loading into DB | No | Yes (DuckDB, Athena) |
| Schema embedded | No | Yes |
| Partial column reads | No | Yes |
| Row group statistics (skip scan) | No | Yes |

The payload JSONB column compresses especially well in Parquet because similar keys repeat across millions of rows — dictionary encoding reduces this column by 90%+ compared to raw JSON storage.

# **7\. Retrieval Strategy**

The retrieval layer is designed to be transparent to callers. Any API endpoint that queries lead data gets a two-stage fallback. The caller receives the same response shape regardless of whether data is in Postgres or archived.

## **Query Router Flow**

10. Check Postgres — data found? Return it. Done.  
11. Check archival\_manifest — manifest row exists for this entity?  
12. DuckDB reads the specific Parquet file from MinIO using the path in the manifest  
13. Result returned in the same shape as a live Postgres response

## **DuckDB Query Example**

A targeted lookup for a single lead reads only one Parquet file:

SELECT \* FROM read\_parquet('s3://backup/leads/year=2025/month=01/LEAD-1001\_v3.parquet')

A range query across all of 2025 uses the Hive partition path:

SELECT \* FROM read\_parquet('s3://backup/leads/year=2025/\*\*/\*.parquet')

WHERE closure\_reason \= 'Customer Request — Repaid'

## **Internal Access Patterns**

| Use case | Access method |
| :---- | :---- |
| Compliance query for single lead | archival\_manifest lookup → single Parquet file via DuckDB |
| Audit trail for a date range | DuckDB with Hive partition filter on year=/month= |
| Operational API (GET /lead/{id}) | Query router: Postgres first, MinIO fallback |
| Bulk compliance export | DuckDB wildcard scan across all partitions |

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
| One giant Parquet file for all leads | One file per lead (or small batch) for targeted retrieval |
| Not versioning the archive schema | Stamp schema\_version on every envelope |
| Using CSV instead of Parquet | Parquet: 20x smaller, queryable, schema-embedded |
| Designing archival without a retrieval plan | Design the retrieval query on day one |
| Checking only file size for verification | Row count \+ checksum \+ sample read — all three |

# **9\. New Audit Event Types**

The following event types are added to the existing AuditLog event taxonomy defined in the TDD Section 10:

| Event type | Triggered when | Key payload fields |
| :---- | :---- | :---- |
| ARCHIVAL\_STARTED | DAG begins processing a batch | batch\_size, eligible\_count |
| ARCHIVAL\_BATCH\_COMPLETED | One Parquet file written and verified | file\_path, row\_counts, checksum, schema\_version |
| ARCHIVAL\_VERIFIED | Staging verification passes | checksum\_match, row\_count\_match |
| ARCHIVAL\_DELETION\_COMPLETED | Postgres rows deleted | entity\_id, tables\_affected, rows\_deleted |
| ARCHIVAL\_FAILED | Any phase fails | phase, error, entity\_id |
| ARCHIVAL\_SKIPPED | Lead skipped due to blocking relationship | entity\_id, reason |

# **10\. Open Questions and Next Steps**

## **Decisions Required**

| Question | Options | Recommended |
| :---- | :---- | :---- |
| Batch size | 100 / 500 / 1000 per DAG run | 500 — predictable duration |
| Parquet file granularity | One file per lead vs one file per batch | One per lead — targeted retrieval |
| Customer row handling | Never delete / delete when last lead archived | Never delete — safest option |
| Archive retention | 5 years / 7 years / indefinite | Confirm with compliance team |
| Retrieval SLA | Best effort (days) / guaranteed (hours) | Confirm with business |

## **Immediate Next Steps**

14. Confirm 3-month eligibility rule with compliance and business teams  
15. Map full child-table hierarchy for the leads schema (all FK relationships)  
16. Define schema\_version strategy and backwards-compatible reader interface  
17. Create archival\_manifest table DDL and migration  
18. Build and test denormalization query for a sample lead in staging  
19. Implement and test Airflow DAG in development environment  
20. Load test: measure Postgres performance before and after first archival run

*Document prepared for internal engineering review. All decisions subject to compliance and business approval.*