# Role

Act as a Principal Software Architect with deep expertise in Django, PostgreSQL, distributed systems, data engineering, database migration, and enterprise archival systems.

I want you to design an enterprise-grade archival library from scratch.

Do not jump into implementation immediately.

Instead, first think through the architecture, possible approaches, tradeoffs, failure scenarios, scalability concerns, and extensibility before proposing any code.

I expect production-level design, not a toy implementation.

---

# Background

Our organization follows a **microservice architecture**.

Every service is built using **Django**.

Each service owns its own PostgreSQL schema.

Many of these schema continuously grow because transactional data is never archived.

We want to build a reusable archival library that any Django service can install and configure.

Instead of every team implementing archival differently, we want a standardized framework.

The archival library should move historical data from a primary PostgreSQL database into another PostgreSQL archival database.

The archival database should preserve relational integrity and remain queryable for analytics or audit purposes.

The archival library should be generic enough to work across different Django services without requiring custom code for every service.

---

# Primary Goals

The library should support:

1. Archiving data from one PostgreSQL database to another.

2. Incremental archival.

3. Batch archival.

4. Large datasets.

5. Resume after failure.

6. Idempotent execution.

7. High performance.

8. Low impact on production traffic.

9. Easy configuration.

10. Extensible architecture.

---

# Additional Functionalities

The archival process should not simply copy everything.

It should support configurable strategies.

Examples:

### Strategy A

Move rows from selected tables.

Delete them from source.

Retain them only in archive.

---

### Strategy B

Copy rows.

Retain source rows.

No deletion.

---

### Strategy C

Delete rows from selected tables.

Retain related rows in other tables.

Example:

Archive

Orders

Order Items

Payments

but retain

Customer

Product

Merchant

because those are master tables.

---

### Strategy D

Archive complete object graphs.

For example

Customer

↓

Orders

↓

Invoices

↓

Payments

↓

Shipment

↓

Events

Archive the entire dependency graph.

---

### Strategy E

Archive based on business rules.

Example

created_at < 3 years

status = COMPLETED

tenant_id = X

deleted = TRUE

custom SQL filter

custom Django ORM filter

etc.

---

# Schema Synchronization

The archival database schema should stay synchronized with production.

Possible cases:

New columns added.

Columns removed.

Column renamed.

Indexes added.

Constraints changed.

Foreign keys added.

Unique constraints.

Partitioning.

Triggers.

Generated columns.

Views.

Materialized views.

Enums.

Extensions.

Sequences.

Identity columns.

Default values.

Check constraints.

Composite indexes.

The archival system should replicate schema evolution.

Discuss different approaches.

For example:

Using Django migrations

Using migration history

Using schema diff

Using pg_dump

Using Alembic-like diffing

Using PostgreSQL catalogs

etc.

Compare advantages and disadvantages.

---

# Configuration

The library should require minimal code.

Something like

```python
ARCHIVAL_CONFIG = {
    ...
}
```

or

```python
class OrderArchive(ArchiveDefinition):
    ...
```

Discuss the best configuration model.

Should it be

YAML

JSON

Python class

Decorator

Django settings

Database-driven

Plugin architecture

etc.

---

# Dependency Resolution

One of the hardest parts is handling foreign key relationships.

Suppose

Customer

↓

Orders

↓

OrderItems

↓

Payments

↓

Refunds

↓

Shipment

↓

ShipmentEvents

How should dependency traversal work?

Should it

Build dependency graph?

Use DFS?

Use BFS?

Topological ordering?

Recursive SQL?

Recursive CTE?

Metadata inspection?

How to avoid cycles?

How to archive self-referencing tables?

How to archive many-to-many relationships?

How to archive polymorphic relations?

How to archive GenericForeignKey?

How to archive nullable foreign keys?

Discuss in detail.

---

# Deletion Strategy

Deleting from source is harder than copying.

Discuss

Child-first delete

Parent-first delete

Soft delete

Hard delete

Logical delete

Batch delete

DELETE

Partition drop

Detach partition

VACUUM implications

Dead tuples

Bloat

Transaction size

Long-running transactions

Rollback cost

Replication lag

Autovacuum interaction

---

# Performance

Assume

Hundreds of millions of rows.

Potentially billions.

The system must scale.

Discuss

Batch sizes

Chunking

Pagination

OFFSET vs cursor

Primary key iteration

Snapshot consistency

COPY command

COPY TO

COPY FROM

Logical replication

Parallel workers

Connection pooling

Streaming

Memory usage

Index usage

Bulk inserts

Bulk deletes

Transaction boundaries

Checkpoint strategy

Compression

Partition-aware archival

---

# Failure Recovery

Suppose the archival job crashes halfway.

How should recovery work?

Need

Checkpointing

Job state

Progress table

Resume

Retries

Duplicate prevention

Partial rollback

Exactly-once semantics

At-least-once semantics

Idempotency

Poison batches

Audit logs

Recovery workflow

---

# Concurrency

Suppose

Two archival jobs start simultaneously.

Or

Production writes continue while archival is happening.

Need discussion around

Isolation levels

Repeatable Read

Serializable

Read Committed

SELECT FOR UPDATE

Advisory locks

Distributed locks

Leader election

Race conditions

Lost updates

Duplicate archival

---

# Multi-Tenancy

Some services are tenant-aware.

Need ability to archive

Tenant-wise

Date-wise

Need configuration support.

---

# Scheduling

The library itself should not depend on Celery.

Instead it should expose APIs.

Then applications can invoke using

Celery


Design the interfaces accordingly.

---

# Monitoring

Need production observability.

Metrics

Rows archived

Rows deleted

Rows copied

Duration

Throughput

Failures

Retries

Skipped rows

Deadlocks

Lag

Estimated completion

Prometheus support

OpenTelemetry support

Structured logging

Audit trail

---

# API Design

Design a clean public API.

Example

ArchiveJob

ArchiveExecutor

SchemaReplicator

DependencyResolver

BatchProcessor

CheckpointStore

DeleteStrategy

CopyStrategy

ValidationEngine

ConfigurationLoader

PluginManager

ProgressTracker

JobRunner

Lifecycle hooks

etc.

Explain responsibilities.

---

# Extensibility

The library should support plugins.

For example

Compression plugin

Encryption plugin

S3 backup plugin

CSV export plugin

Parquet export plugin

Kafka publisher

Notification plugin

Slack alerts

Custom validators

Custom filters

Custom retention policies

---

# Security

Need discussion around

Least privilege

Separate archive credentials

Encryption in transit

Encryption at rest

PII masking

Column-level masking

GDPR

Audit logging

Access control

Secrets management

---

# Testing Strategy

How should the library be tested?

Unit tests

Integration tests

Database tests

Large dataset simulation

Failure simulation

Concurrency tests

Chaos testing

Migration testing

Performance benchmarks

---

# Packaging

Since every Django service will use this library,

Discuss

Project structure

Python package layout

Entry points

Django AppConfig

Management commands

CLI

Versioning

Backward compatibility

Semantic versioning

Migration strategy

Documentation

---

# Deliverables

I want the answer organized into sections.

1. High-level architecture.

2. Component diagram.

3. Class design.

4. Sequence diagrams.

5. Database metadata flow.

6. Dependency resolution algorithm.

7. Schema synchronization strategy.

8. Archival algorithm.

9. Deletion algorithm.

10. Failure recovery.

11. Scaling strategy.

12. Configuration examples.

13. Sample APIs.

14. Production deployment recommendations.

15. Tradeoff analysis.

16. Future enhancements.

17. Risks and mitigation.

18. Recommended architecture.

For every architectural decision, explain **why** it was chosen, discuss alternatives, and compare the trade-offs. Where useful, include pseudocode, flow diagrams (ASCII is fine), and example configurations. Focus on building a reusable, production-ready library that can be adopted across multiple Django microservices with minimal service-specific customization.
