# Enterprise Django Archival Library - Architectural Design Document

**Document Version:** 1.0  
**Date:** June 30, 2026  
**Prepared By:** Principal Software Architect

---

## Executive Summary

This document presents a comprehensive architectural design for an enterprise-grade archival library for Django-based microservices. The library enables standardized, scalable, and resilient archival of PostgreSQL data across multiple services while maintaining relational integrity, supporting incremental processing, and providing production-grade observability.

The proposed solution balances performance, reliability, and extensibility while minimizing production impact and operational complexity.

---

## Table of Contents

1. [High-Level Architecture](#1-high-level-architecture)
2. [Component Design](#2-component-design)
3. [Class Design](#3-class-design)
4. [Sequence Diagrams](#4-sequence-diagrams)
5. [Database Metadata Flow](#5-database-metadata-flow)
6. [Dependency Resolution Algorithm](#6-dependency-resolution-algorithm)
7. [Schema Synchronization Strategy](#7-schema-synchronization-strategy)
8. [Archival Algorithm](#8-archival-algorithm)
9. [Deletion Algorithm](#9-deletion-algorithm)
10. [Failure Recovery](#10-failure-recovery)
11. [Scaling Strategy](#11-scaling-strategy)
12. [Configuration Examples](#12-configuration-examples)
13. [Sample APIs](#13-sample-apis)
14. [Production Deployment Recommendations](#14-production-deployment-recommendations)
15. [Tradeoff Analysis](#15-tradeoff-analysis)
16. [Future Enhancements](#16-future-enhancements)
17. [Risks and Mitigation](#17-risks-and-mitigation)
18. [Recommended Architecture](#18-recommended-architecture)

---

# 1. High-Level Architecture

## 1.1 Architectural Principles

The archival library is built on these core principles:

1. **Separation of Concerns**: Clear separation between configuration, execution, storage, and monitoring
2. **Idempotency**: All operations can be safely retried without data duplication
3. **Incremental Processing**: Support for checkpointing and resumption
4. **Minimal Production Impact**: Rate limiting, connection pooling, and batch processing
5. **Extensibility**: Plugin architecture for custom behaviors
6. **Schema-Agnostic**: Works with any Django model structure
7. **Observability-First**: Comprehensive metrics, logging, and audit trails

## 1.2 System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Django Service Layer                        │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  Celery Task │  │ Management   │  │   HTTP API   │         │
│  │              │  │   Command    │  │              │         │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘         │
│         │                 │                  │                  │
│         └─────────────────┼──────────────────┘                  │
│                           │                                     │
└───────────────────────────┼─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Django Archival Library                         │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │              Public API Layer                               │ │
│  │  • ArchiveJobRunner                                         │ │
│  │  • ConfigurationLoader                                      │ │
│  │  • ProgressTracker                                          │ │
│  └────────────────────────────────────────────────────────────┘ │
│                            │                                     │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │           Orchestration Layer                               │ │
│  │  • JobOrchestrator                                          │ │
│  │  • DependencyResolver                                       │ │
│  │  • SchemaReplicator                                         │ │
│  │  • CheckpointManager                                        │ │
│  └────────────────────────────────────────────────────────────┘ │
│                            │                                     │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │           Execution Layer                                   │ │
│  │  • ArchiveExecutor                                          │ │
│  │  • BatchProcessor                                           │ │
│  │  • DeleteExecutor                                           │ │
│  │  • ValidationEngine                                         │ │
│  └────────────────────────────────────────────────────────────┘ │
│                            │                                     │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │           Data Access Layer                                 │ │
│  │  • SourceDatabaseAdapter                                    │ │
│  │  • ArchiveDatabaseAdapter                                   │ │
│  │  • MetadataStore                                            │ │
│  └────────────────────────────────────────────────────────────┘ │
│                            │                                     │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │           Plugin Layer                                      │ │
│  │  • FilterPlugin                                             │ │
│  │  • TransformPlugin                                          │ │
│  │  • NotificationPlugin                                       │ │
│  │  • StoragePlugin                                            │ │
│  └────────────────────────────────────────────────────────────┘ │
│                            │                                     │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │        Observability Layer                                  │ │
│  │  • MetricsCollector                                         │ │
│  │  • AuditLogger                                              │ │
│  │  • ProgressReporter                                         │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────┐  ┌────────────────┐  ┌──────────────────┐
│   Source DB   │  │   Archive DB   │  │  Metadata DB     │
│  (PostgreSQL) │  │  (PostgreSQL)  │  │  (Same as Source)│
└───────────────┘  └────────────────┘  └──────────────────┘
```

## 1.3 Key Architectural Decisions

### Decision 1: Library vs Service
**Choice:** Library (not a standalone service)

**Rationale:**
- Deployed as a Python package within each Django service
- No additional operational overhead (no new service to deploy/monitor)
- Direct access to Django ORM and models
- Simplified authentication (uses existing DB credentials)
- Lower latency (no network calls)

**Tradeoffs:**
- ✅ Simpler deployment model
- ✅ No cross-service authentication needed
- ✅ Can leverage Django's connection pooling
- ❌ No centralized control plane
- ❌ Each service maintains its own archival configuration
- ❌ Library version must be synchronized across services

**Alternative Considered:** Standalone archival service
- Would require service discovery, authentication, and API design
- Adds operational complexity
- Better for centralized governance but overkill for our use case

---

### Decision 2: Push vs Pull Model
**Choice:** Push model (library pushes data to archive DB)

**Rationale:**
- Source service has full context about data relationships
- Easier to handle Django-specific features (GenericForeignKey, etc.)
- Can use Django ORM for queries
- No need to expose source DB to external service

**Tradeoffs:**
- ✅ Full access to Django model metadata
- ✅ Can use existing connection pools
- ✅ Simpler security model
- ❌ Each service runs archival independently
- ❌ No shared resource pool

---

### Decision 3: Configuration Approach
**Choice:** Python class-based configuration with Django integration

**Rationale:**
- Type-safe and IDE-friendly
- Can leverage Python's expressiveness for complex rules
- Integrates naturally with Django settings
- Supports programmatic configuration

**Tradeoffs:**
- ✅ Type checking and autocomplete
- ✅ Can define custom methods and validators
- ✅ Supports inheritance and composition
- ❌ Requires Python knowledge (vs YAML)
- ❌ Can't be changed without code deployment

**Alternative:** YAML/JSON configuration
- More declarative but less flexible
- Harder to validate at development time
- Requires custom DSL for complex logic

---

### Decision 4: Two-Phase vs Single-Phase Archival
**Choice:** Two-phase (copy then delete)

**Rationale:**
- Safer: data exists in archive before deletion
- Allows validation between phases
- Can run copy continuously and delete separately
- Better failure recovery

**Tradeoffs:**
- ✅ No data loss risk
- ✅ Can validate archived data before deletion
- ✅ Copy and delete can run at different cadences
- ❌ More complex state management
- ❌ Requires more storage temporarily
- ❌ Longer overall process time

---

### Decision 5: Metadata Storage
**Choice:** Dedicated tables in source database

**Rationale:**
- Uses existing infrastructure
- Atomic updates with source data
- No external dependency
- Survives archive DB failures

**Tradeoffs:**
- ✅ High availability (same as source DB)
- ✅ Strong consistency guarantees
- ✅ Simple deployment
- ❌ Slight increase in source DB size
- ❌ Couples library to PostgreSQL

Tables created:
- `django_archive_jobs` - Job definitions and status
- `django_archive_checkpoints` - Progress tracking
- `django_archive_audit` - Audit trail
- `django_archive_schema_versions` - Schema sync tracking


---

# 2. Component Design

## 2.1 Core Components

### 2.1.1 ArchiveJobRunner

**Responsibility:** Entry point for executing archival jobs

**Key Methods:**
- `run(job_config)` - Execute a complete archival job
- `dry_run(job_config)` - Validate configuration without archiving
- `estimate(job_config)` - Estimate rows and time required

**Dependencies:**
- ConfigurationLoader
- JobOrchestrator
- ProgressTracker
- MetricsCollector

**Design Rationale:**
- Single entry point simplifies API surface
- Handles initialization, validation, and cleanup
- Manages transaction boundaries
- Coordinates all subsystems

---

### 2.1.2 ConfigurationLoader

**Responsibility:** Load, parse, and validate archival configuration

**Key Methods:**
- `load_from_class(config_class)` - Load from Python class
- `validate()` - Validate configuration completeness
- `resolve_models()` - Map model names to Django models
- `build_execution_plan()` - Create execution graph

**Configuration Schema:**
```python
class ArchiveConfig:
    name: str
    models: List[ModelConfig]
    strategy: ArchivalStrategy
    filters: Dict[str, FilterConfig]
    batch_size: int
    max_workers: int
    retention_policy: RetentionPolicy
    schema_sync: SchemaSyncConfig
    plugins: List[Plugin]
```

**Design Rationale:**
- Validates configuration early (fail fast)
- Converts declarative config to execution plan
- Resolves Django model references safely
- Supports multiple configuration sources

---

### 2.1.3 DependencyResolver

**Responsibility:** Build dependency graph of related models

**Key Methods:**
- `build_graph(root_models)` - Build complete dependency graph
- `get_topological_order()` - Return archive order
- `get_reverse_order()` - Return deletion order
- `detect_cycles()` - Identify circular dependencies

**Algorithm:**
```
1. Start with root models (configured entry points)
2. For each model:
   a. Inspect foreign keys (via Django _meta API)
   b. Add edges: child -> parent
   c. Recursively process referenced models
3. Build adjacency list representation
4. Run topological sort (Kahn's algorithm)
5. Detect cycles using DFS
6. Handle special cases:
   - Self-referencing FKs
   - GenericForeignKey
   - Many-to-many through tables
   - Nullable foreign keys
```

**Graph Representation:**
```
Node: {
    model: Django Model class,
    table_name: str,
    fields: List[Field],
    incoming_edges: List[ForeignKeyRelation],
    outgoing_edges: List[ForeignKeyRelation]
}

Edge: {
    from_model: Model,
    to_model: Model,
    field_name: str,
    nullable: bool,
    constraint_name: str
}
```

**Handling Special Cases:**

**Self-Referencing Tables:**
```python
# Example: Employee.manager_id -> Employee.id
# Strategy: Process in batches, archive parents first
# Then archive children in subsequent passes
```

**GenericForeignKey:**
```python
# Example: Comment.content_type + Comment.object_id
# Strategy: 
# 1. Resolve actual target models at runtime
# 2. Add dynamic edges to graph
# 3. Archive based on content_type grouping
```

**Many-to-Many:**
```python
# Example: User <-> Group (through UserGroup)
# Strategy:
# 1. Treat through table as regular model
# 2. Archive both sides first
# 3. Then archive through table
```

**Nullable Foreign Keys:**
```python
# Example: Order.coupon_id -> Coupon.id (nullable)
# Strategy:
# 1. Check configuration: archive referenced data or not?
# 2. If yes, include in graph
# 3. If no, mark as optional edge
```

**Design Rationale:**
- Prevents foreign key violations during archival
- Ensures data consistency in archive DB
- Handles complex relationship patterns
- Works with Django's model introspection

---

### 2.1.4 SchemaReplicator

**Responsibility:** Synchronize schema between source and archive DB

**Key Methods:**
- `sync_schema(models)` - Synchronize schema for given models
- `get_schema_diff()` - Compare source and archive schemas
- `apply_migrations()` - Apply schema changes to archive
- `validate_compatibility()` - Ensure schemas are compatible

**Approach Comparison:**

| Approach | Pros | Cons | Recommendation |
|----------|------|------|----------------|
| **Django Migrations** | - Native Django integration<br>- Version controlled<br>- Tested by Django | - Requires migration files<br>- Can't handle custom SQL<br>- Migrations may not exist | ✅ **Primary approach** |
| **pg_dump + restore** | - Complete schema capture<br>- Handles all PG features | - Heavy operation<br>- All-or-nothing<br>- Downtime required | Use for initial setup only |
| **Schema Diffing** | - Detects actual differences<br>- Works without migrations | - Complex to implement<br>- May miss subtle changes | ✅ **Fallback approach** |
| **Metadata Catalogs** | - Accurate<br>- Handles PG-specific features | - Requires deep PG knowledge<br>- Complex queries | Use for validation only |

**Recommended Strategy: Hybrid Approach**

```python
class SchemaReplicator:
    def sync_schema(self, models):
        """
        Phase 1: Migration-based sync (primary)
        - Check django_migrations table for applied migrations
        - Find unapplied migrations for target models
        - Apply migrations to archive DB
        
        Phase 2: Schema diff validation (fallback)
        - Query information_schema on both DBs
        - Compare:
          * Columns (name, type, nullable, default)
          * Indexes
          * Constraints (PK, FK, unique, check)
          * Sequences
        - Report any differences
        
        Phase 3: Manual intervention (if needed)
        - If differences detected, log warnings
        - Optionally generate SQL to fix
        - Require explicit confirmation
        """
```

**Schema Diff Algorithm:**
```sql
-- Compare columns
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (...)

-- Compare indexes
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'

-- Compare constraints
SELECT
    conname,
    contype,
    conrelid::regclass,
    confrelid::regclass,
    pg_get_constraintdef(oid)
FROM pg_constraint
WHERE connamespace = 'public'::regnamespace
```

**Handling Edge Cases:**

1. **Custom SQL Migrations:**
   - Parse SQL from migration files
   - Apply to archive DB verbatim
   - Log for manual review

2. **Data Migrations:**
   - Skip data migrations (we're archiving data separately)
   - Mark as applied in django_migrations

3. **Renamed Columns:**
   - Detect via migration history
   - Create mapping for data transfer

4. **Dropped Columns:**
   - Keep in archive DB (historical data)
   - Mark as deprecated in metadata

5. **Partitioned Tables:**
   - Replicate partition structure
   - Consider partition-aware archival

**Design Rationale:**
- Leverages Django's migration system where possible
- Falls back to schema inspection when needed
- Prevents schema drift between source and archive
- Handles complex PostgreSQL features

---

### 2.1.5 ArchiveExecutor

**Responsibility:** Execute the data archival process

**Key Methods:**
- `archive_batch(model, pks)` - Archive a batch of rows
- `copy_with_dependencies(model, pks)` - Copy with related data
- `validate_archived_data(model, pks)` - Verify archive integrity

**Archival Strategies:**

**Strategy A: Move (Copy + Delete)**
```python
def strategy_move(model, filter_conditions):
    # Phase 1: Copy
    copied_pks = copy_to_archive(model, filter_conditions)
    checkpoint_save(model, copied_pks, phase='COPIED')
    
    # Phase 2: Validate
    validate_archived_data(model, copied_pks)
    
    # Phase 3: Delete
    delete_from_source(model, copied_pks)
    checkpoint_save(model, copied_pks, phase='DELETED')
```

**Strategy B: Copy Only**
```python
def strategy_copy(model, filter_conditions):
    # Only phase 1
    copied_pks = copy_to_archive(model, filter_conditions)
    checkpoint_save(model, copied_pks, phase='COPIED')
```

**Strategy C: Selective Retention**
```python
def strategy_selective(model, filter_conditions, retention_config):
    # Archive transactions but keep master data
    if model in retention_config.archive_models:
        return strategy_move(model, filter_conditions)
    elif model in retention_config.copy_models:
        return strategy_copy(model, filter_conditions)
    else:
        # Skip this model
        pass
```

**Strategy D: Object Graph**
```python
def strategy_object_graph(root_model, root_pks):
    # Build complete graph
    graph = dependency_resolver.build_graph(root_model)
    
    # Traverse in topological order
    for model in graph.get_topological_order():
        # Find related PKs
        related_pks = find_related_pks(model, root_pks, graph)
        
        # Archive this level
        copy_to_archive(model, pk__in=related_pks)
```

**Design Rationale:**
- Flexible strategy pattern
- Supports all required use cases
- Clear separation between copy and delete
- Enables validation between phases

---

### 2.1.6 BatchProcessor

**Responsibility:** Process large datasets in manageable batches

**Key Methods:**
- `process_in_batches(queryset, callback)` - Batch iteration
- `get_next_batch()` - Fetch next batch efficiently
- `calculate_optimal_batch_size()` - Adaptive batch sizing

**Iteration Strategies:**

| Strategy | SQL | Pros | Cons | Recommendation |
|----------|-----|------|------|----------------|
| **OFFSET** | `OFFSET 1000 LIMIT 100` | Simple | Slow for large offsets | ❌ Avoid |
| **Keyset (Cursor)** | `WHERE id > 1000 LIMIT 100` | Fast, consistent | Requires ordered column | ✅ **Preferred** |
| **Snapshot** | `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` | Consistent view | Long transaction | Use with cursor |

**Keyset Pagination Implementation:**
```python
class KeysetPaginator:
    def __init__(self, model, filters, batch_size, order_by='pk'):
        self.model = model
        self.filters = filters
        self.batch_size = batch_size
        self.order_by = order_by
        self.last_value = None
    
    def get_next_batch(self):
        queryset = self.model.objects.filter(**self.filters)
        
        if self.last_value is not None:
            # Keyset condition
            queryset = queryset.filter(**{
                f'{self.order_by}__gt': self.last_value
            })
        
        batch = queryset.order_by(self.order_by)[:self.batch_size]
        
        if batch:
            self.last_value = getattr(batch[-1], self.order_by)
        
        return batch
```

**Adaptive Batch Sizing:**
```python
def calculate_optimal_batch_size(model, target_duration_ms=1000):
    """
    Dynamically adjust batch size based on performance
    """
    # Start with conservative size
    batch_size = 100
    
    while True:
        start = time.time()
        batch = fetch_batch(model, batch_size)
        duration = (time.time() - start) * 1000
        
        if duration < target_duration_ms * 0.5:
            # Too fast, increase batch size
            batch_size = int(batch_size * 1.5)
        elif duration > target_duration_ms * 1.5:
            # Too slow, decrease batch size
            batch_size = int(batch_size * 0.7)
        else:
            # Just right
            return batch_size
        
        # Safety bounds
        batch_size = max(10, min(10000, batch_size))
```

**Design Rationale:**
- Prevents memory exhaustion
- Maintains consistent performance
- Allows progress tracking
- Supports resumption after failure



### 2.1.7 CheckpointManager

**Responsibility:** Track progress and enable resumption

**Key Methods:**
- `save_checkpoint(job_id, model, progress)` - Save progress
- `load_checkpoint(job_id, model)` - Resume from last checkpoint
- `mark_complete(job_id, model)` - Mark model processing complete
- `get_job_progress(job_id)` - Get overall progress

**Checkpoint Schema:**
```python
class ArchiveCheckpoint:
    id: UUID
    job_id: UUID
    model_name: str
    table_name: str
    phase: str  # COPYING, COPIED, VALIDATING, DELETING, DELETED, COMPLETE
    last_processed_pk: Any
    batch_number: int
    rows_processed: int
    rows_total: int
    started_at: datetime
    updated_at: datetime
    completed_at: datetime
    metadata: JSONB
```

**Progress Tracking:**
```python
def save_checkpoint(job_id, model, last_pk, rows_processed):
    """
    Atomic checkpoint save
    """
    with transaction.atomic():
        checkpoint, created = ArchiveCheckpoint.objects.update_or_create(
            job_id=job_id,
            model_name=model.__name__,
            defaults={
                'last_processed_pk': last_pk,
                'rows_processed': rows_processed,
                'updated_at': timezone.now(),
                'metadata': {
                    'batch_size': BATCH_SIZE,
                    'worker_id': get_worker_id(),
                }
            }
        )
    return checkpoint
```

**Resumption Logic:**
```python
def resume_archival(job_id):
    """
    Resume interrupted archival job
    """
    job = ArchiveJob.objects.get(id=job_id)
    
    # Find incomplete models
    checkpoints = ArchiveCheckpoint.objects.filter(
        job_id=job_id,
        phase__in=['COPYING', 'COPIED', 'VALIDATING', 'DELETING']
    )
    
    for checkpoint in checkpoints:
        model = get_model(checkpoint.model_name)
        
        if checkpoint.phase == 'COPYING':
            # Resume copying from last_processed_pk
            continue_copy(model, from_pk=checkpoint.last_processed_pk)
        
        elif checkpoint.phase == 'COPIED':
            # Validation not started, validate then delete
            validate_then_delete(model, checkpoint)
        
        elif checkpoint.phase == 'VALIDATING':
            # Re-run validation
            validate_then_delete(model, checkpoint)
        
        elif checkpoint.phase == 'DELETING':
            # Resume deletion from last_processed_pk
            continue_delete(model, from_pk=checkpoint.last_processed_pk)
```

**Design Rationale:**
- Enables exactly-once semantics
- Minimal overhead (single row update per batch)
- Survives process crashes
- Provides progress visibility

---

### 2.1.8 DeleteExecutor

**Responsibility:** Safely delete archived data from source

**Key Methods:**
- `delete_batch(model, pks)` - Delete a batch
- `validate_safe_to_delete(model, pks)` - Pre-deletion checks
- `delete_with_cascade(model, pks)` - Handle cascades

**Deletion Order:**
```
Reverse topological order (children first, parents last)

Example:
Archive order:  Customer -> Order -> OrderItem -> Payment
Delete order:   Payment -> OrderItem -> Order -> Customer
```

**Deletion Strategies:**

**Strategy 1: Explicit Child-First Deletion**
```python
def delete_explicit(model, pks):
    """
    Manually delete in reverse dependency order
    """
    graph = dependency_resolver.build_graph(model)
    
    # Reverse topological order
    for dependent_model in graph.get_reverse_order():
        # Find related PKs
        related_pks = find_related_pks(dependent_model, pks, graph)
        
        # Delete batch by batch
        for batch_pks in batch_iterator(related_pks):
            dependent_model.objects.filter(pk__in=batch_pks).delete()
            checkpoint_save(dependent_model, batch_pks, 'DELETED')
```

**Strategy 2: Database CASCADE**
```python
def delete_with_cascade(model, pks):
    """
    Rely on database ON DELETE CASCADE
    
    Pros:
    - Database enforces referential integrity
    - Single DELETE statement
    
    Cons:
    - Less control over deletion
    - Can't checkpoint intermediate progress
    - May hit transaction size limits
    - Harder to audit
    """
    model.objects.filter(pk__in=pks).delete()
```

**Strategy 3: Soft Delete**
```python
def soft_delete(model, pks):
    """
    Mark as archived instead of physical deletion
    
    Pros:
    - Reversible
    - No referential integrity issues
    - Fast
    
    Cons:
    - Source DB size doesn't decrease
    - Requires application-level filtering
    - Index bloat
    """
    model.objects.filter(pk__in=pks).update(
        archived_at=timezone.now(),
        archived=True
    )
```

**Strategy 4: Partition Drop (Best for large deletes)**
```python
def delete_via_partition_drop(model, partition_key):
    """
    Drop entire partition
    
    Requirements:
    - Table must be partitioned
    - Partition key must align with archival criteria
    
    Pros:
    - Instant deletion (metadata operation)
    - No table bloat
    - No VACUUM needed
    
    Cons:
    - Requires partition setup
    - All-or-nothing (can't delete subset)
    """
    with connection.cursor() as cursor:
        cursor.execute(f"""
            ALTER TABLE {model._meta.db_table}
            DETACH PARTITION {partition_name}
        """)
        cursor.execute(f"DROP TABLE {partition_name}")
```

**Recommended Approach: Hybrid**

```python
def delete_intelligently(model, pks, deletion_config):
    """
    Choose strategy based on volume and table structure
    """
    row_count = len(pks)
    
    # Large deletion + partitioned table
    if row_count > 1_000_000 and is_partitioned(model):
        return delete_via_partition_drop(model, pks)
    
    # Medium deletion + soft delete enabled
    elif row_count > 10_000 and has_soft_delete_field(model):
        return soft_delete(model, pks)
    
    # Small deletion or explicit control needed
    else:
        return delete_explicit(model, pks)
```

**Handling Deletion Edge Cases:**

**1. Long-running DELETE statements:**
```python
# Problem: DELETE of millions of rows locks table
# Solution: Delete in small batches

def delete_in_small_batches(model, pks, batch_size=1000):
    for batch_pks in chunk_list(pks, batch_size):
        model.objects.filter(pk__in=batch_pks).delete()
        
        # Give other transactions a chance
        time.sleep(0.01)
        
        # Allow autovacuum to clean up
        if batch_count % 100 == 0:
            connection.cursor().execute("VACUUM ANALYZE %s" % model._meta.db_table)
```

**2. Foreign key violations:**
```python
# Problem: Deleting parent before children
# Solution: Validate archive completeness before deletion

def validate_safe_to_delete(model, pks):
    """
    Ensure all dependent rows are already archived
    """
    for fk_relation in get_incoming_foreign_keys(model):
        child_model = fk_relation.model
        fk_field = fk_relation.field
        
        # Check if any unarchived children exist
        unarchived_children = child_model.objects.filter(
            **{f'{fk_field}__in': pks}
        ).exclude(
            pk__in=get_archived_pks(child_model)
        )
        
        if unarchived_children.exists():
            raise DependencyNotArchivedException(
                f"Cannot delete {model.__name__} {pks}. "
                f"Unarchived {child_model.__name__} rows exist."
            )
```

**3. Deadlocks:**
```python
# Problem: Concurrent deletions from multiple workers
# Solution: Lock in consistent order + retry logic

def delete_with_deadlock_retry(model, pks, max_retries=3):
    # Sort PKs to ensure consistent lock order
    sorted_pks = sorted(pks)
    
    for attempt in range(max_retries):
        try:
            with transaction.atomic():
                # Lock rows explicitly
                model.objects.select_for_update().filter(
                    pk__in=sorted_pks
                ).delete()
            break
        except OperationalError as e:
            if 'deadlock detected' in str(e):
                time.sleep(random.uniform(0.1, 0.5))
                continue
            raise
```

**Design Rationale:**
- Multiple strategies for different scenarios
- Safety checks prevent data loss
- Batch processing prevents lock contention
- Supports rollback and retry

---

### 2.1.9 ValidationEngine

**Responsibility:** Ensure data integrity before and after archival

**Key Methods:**
- `validate_pre_archive(model, pks)` - Pre-archival checks
- `validate_post_archive(model, pks)` - Post-archival verification
- `validate_referential_integrity()` - Check FK consistency
- `validate_row_counts()` - Verify row count matches

**Validation Levels:**

**Level 1: Count Validation (Fast)**
```python
def validate_counts(model, pks):
    """
    Verify row counts match
    """
    source_count = model.objects.using('default').filter(pk__in=pks).count()
    archive_count = model.objects.using('archive').filter(pk__in=pks).count()
    
    if source_count != archive_count:
        raise ValidationException(
            f"Row count mismatch: source={source_count}, archive={archive_count}"
        )
```

**Level 2: Checksum Validation (Medium)**
```python
def validate_checksums(model, pks):
    """
    Compare checksums of archived rows
    """
    for pk in pks:
        source_row = model.objects.using('default').get(pk=pk)
        archive_row = model.objects.using('archive').get(pk=pk)
        
        source_hash = hash_row(source_row)
        archive_hash = hash_row(archive_row)
        
        if source_hash != archive_hash:
            raise ValidationException(
                f"Row {pk} checksum mismatch"
            )

def hash_row(obj):
    """
    Generate deterministic hash of Django model instance
    """
    field_values = []
    for field in obj._meta.fields:
        value = getattr(obj, field.name)
        # Normalize for hashing
        if isinstance(value, datetime):
            value = value.isoformat()
        field_values.append(str(value))
    
    return hashlib.sha256('|'.join(field_values).encode()).hexdigest()
```

**Level 3: Full Comparison (Slow, thorough)**
```python
def validate_full_comparison(model, pks):
    """
    Field-by-field comparison
    """
    for pk in pks:
        source_row = model.objects.using('default').get(pk=pk)
        archive_row = model.objects.using('archive').get(pk=pk)
        
        for field in model._meta.fields:
            source_val = getattr(source_row, field.name)
            archive_val = getattr(archive_row, field.name)
            
            if not values_equal(source_val, archive_val, field):
                raise ValidationException(
                    f"Row {pk} field {field.name} mismatch: "
                    f"source={source_val}, archive={archive_val}"
                )

def values_equal(val1, val2, field):
    """
    Handle type-specific comparisons
    """
    # Handle None
    if val1 is None and val2 is None:
        return True
    if val1 is None or val2 is None:
        return False
    
    # Datetime with timezone
    if isinstance(field, DateTimeField):
        return abs((val1 - val2).total_seconds()) < 0.001
    
    # Decimal precision
    elif isinstance(field, DecimalField):
        return abs(val1 - val2) < Decimal('0.00001')
    
    # JSON fields
    elif isinstance(field, JSONField):
        return json.dumps(val1, sort_keys=True) == json.dumps(val2, sort_keys=True)
    
    # Default
    else:
        return val1 == val2
```

**Referential Integrity Validation:**
```python
def validate_referential_integrity(model, pks):
    """
    Ensure all foreign key references are satisfied in archive DB
    """
    for field in model._meta.fields:
        if isinstance(field, ForeignKey):
            # Get all FK values for archived rows
            fk_values = model.objects.using('archive').filter(
                pk__in=pks
            ).values_list(field.name, flat=True).distinct()
            
            # Remove nulls
            fk_values = [v for v in fk_values if v is not None]
            
            # Check if all referenced rows exist in archive
            related_model = field.related_model
            existing_count = related_model.objects.using('archive').filter(
                pk__in=fk_values
            ).count()
            
            if existing_count != len(fk_values):
                missing = set(fk_values) - set(
                    related_model.objects.using('archive').filter(
                        pk__in=fk_values
                    ).values_list('pk', flat=True)
                )
                raise ValidationException(
                    f"Missing foreign key references in {related_model.__name__}: {missing}"
                )
```

**Configurable Validation:**
```python
class ValidationConfig:
    # Validation level
    level: ValidationLevel = ValidationLevel.CHECKSUM
    
    # Sampling for large datasets
    sample_rate: float = 1.0  # 1.0 = validate all, 0.1 = validate 10%
    
    # Fail fast or collect all errors
    fail_fast: bool = True
    
    # Skip validation (dangerous!)
    skip: bool = False
```

**Design Rationale:**
- Multiple validation levels for speed/thoroughness tradeoff
- Catches data corruption early
- Prevents foreign key violations
- Configurable for different risk tolerances



---

# 3. Class Design

## 3.1 Public API Classes

### 3.1.1 ArchiveDefinition (Base Class)

```python
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Type
from django.db import models
from enum import Enum

class ArchivalStrategy(Enum):
    MOVE = "move"  # Copy then delete
    COPY = "copy"  # Copy only
    SELECTIVE = "selective"  # Different behavior per model
    OBJECT_GRAPH = "object_graph"  # Archive complete dependency graph

class RetentionPolicy:
    """Define what data to archive"""
    def __init__(
        self,
        created_before: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        status_in: Optional[List[str]] = None,
        custom_filter: Optional[Q] = None,
    ):
        self.created_before = created_before
        self.modified_before = modified_before
        self.status_in = status_in
        self.custom_filter = custom_filter
    
    def as_q(self) -> Q:
        """Convert to Django Q object"""
        filters = Q()
        if self.created_before:
            filters &= Q(created_at__lt=self.created_before)
        if self.modified_before:
            filters &= Q(modified_at__lt=self.modified_before)
        if self.status_in:
            filters &= Q(status__in=self.status_in)
        if self.custom_filter:
            filters &= self.custom_filter
        return filters

class ArchiveDefinition(ABC):
    """
    Base class for defining archival configurations
    
    Usage:
        class OrderArchive(ArchiveDefinition):
            name = "order_archival"
            root_models = [Order]
            strategy = ArchivalStrategy.OBJECT_GRAPH
            retention = RetentionPolicy(
                created_before=datetime.now() - timedelta(days=365*3),
                status_in=['COMPLETED', 'CANCELLED']
            )
    """
    
    # Required attributes
    name: str
    root_models: List[Type[models.Model]]
    strategy: ArchivalStrategy
    retention: RetentionPolicy
    
    # Optional attributes
    batch_size: int = 1000
    max_workers: int = 4
    validation_level: ValidationLevel = ValidationLevel.CHECKSUM
    validation_sample_rate: float = 1.0
    
    # Schema sync
    auto_sync_schema: bool = True
    schema_sync_mode: str = "migrations"  # or "diff"
    
    # Deletion settings
    delete_batch_size: int = 100
    deletion_delay_seconds: int = 0
    
    # Plugin configuration
    plugins: List['Plugin'] = []
    
    # Callbacks
    def before_archive(self, model: Type[models.Model], queryset: QuerySet):
        """Hook called before archiving a model"""
        pass
    
    def after_archive(self, model: Type[models.Model], pks: List[Any]):
        """Hook called after archiving a model"""
        pass
    
    def before_delete(self, model: Type[models.Model], pks: List[Any]):
        """Hook called before deleting from source"""
        pass
    
    def after_delete(self, model: Type[models.Model], pks: List[Any]):
        """Hook called after deleting from source"""
        pass
    
    def on_error(self, error: Exception, context: Dict):
        """Hook called when error occurs"""
        pass
    
    # Model-specific overrides
    def get_retention_policy(self, model: Type[models.Model]) -> RetentionPolicy:
        """Override to provide model-specific retention policies"""
        return self.retention
    
    def should_archive_model(self, model: Type[models.Model]) -> bool:
        """Override to exclude certain models from archival"""
        return True
    
    def get_order_by(self, model: Type[models.Model]) -> List[str]:
        """Override to specify ordering for iteration"""
        return ['pk']
```

### 3.1.2 ArchiveJobRunner

```python
from dataclasses import dataclass
from uuid import UUID, uuid4

@dataclass
class ArchiveJobResult:
    """Result of archival job execution"""
    job_id: UUID
    status: str  # SUCCESS, FAILED, PARTIAL
    models_processed: int
    total_rows_archived: int
    total_rows_deleted: int
    duration_seconds: float
    errors: List[Dict]
    warnings: List[str]

class ArchiveJobRunner:
    """
    Main entry point for executing archival jobs
    """
    
    def __init__(
        self,
        archive_definition: ArchiveDefinition,
        source_db: str = 'default',
        archive_db: str = 'archive',
    ):
        self.definition = archive_definition
        self.source_db = source_db
        self.archive_db = archive_db
        self.job_id = uuid4()
        
        # Initialize subsystems
        self.config_loader = ConfigurationLoader(archive_definition)
        self.orchestrator = JobOrchestrator(self.job_id, source_db, archive_db)
        self.progress_tracker = ProgressTracker(self.job_id)
        self.metrics = MetricsCollector(self.job_id)
    
    def run(
        self,
        dry_run: bool = False,
        resume_job_id: Optional[UUID] = None,
    ) -> ArchiveJobResult:
        """
        Execute archival job
        
        Args:
            dry_run: If True, validate configuration without archiving
            resume_job_id: If provided, resume interrupted job
        
        Returns:
            ArchiveJobResult with execution details
        """
        try:
            # Load and validate configuration
            config = self.config_loader.load()
            self.config_loader.validate()
            
            if dry_run:
                return self._dry_run(config)
            
            # Resume or start new job
            if resume_job_id:
                return self.orchestrator.resume(resume_job_id)
            else:
                return self.orchestrator.execute(config)
                
        except Exception as e:
            self.definition.on_error(e, {'job_id': self.job_id})
            raise
        finally:
            self.metrics.flush()
    
    def estimate(self) -> Dict:
        """
        Estimate rows to archive and time required
        
        Returns:
            {
                'total_rows': int,
                'estimated_duration_seconds': float,
                'models': {
                    'ModelName': {
                        'rows': int,
                        'estimated_duration': float
                    }
                }
            }
        """
        config = self.config_loader.load()
        estimator = ArchivalEstimator(config, self.source_db)
        return estimator.estimate()
    
    def get_progress(self, job_id: Optional[UUID] = None) -> Dict:
        """
        Get current progress of job
        """
        job_id = job_id or self.job_id
        return self.progress_tracker.get_progress(job_id)
    
    def cancel(self, job_id: Optional[UUID] = None):
        """
        Cancel running job
        """
        job_id = job_id or self.job_id
        return self.orchestrator.cancel(job_id)
    
    def _dry_run(self, config) -> ArchiveJobResult:
        """
        Validate configuration without executing
        """
        # Build dependency graph
        graph = DependencyResolver(config.root_models).build_graph()
        
        # Check schema compatibility
        schema_replicator = SchemaReplicator(self.source_db, self.archive_db)
        schema_diffs = schema_replicator.get_schema_diff(graph.models)
        
        # Estimate rows
        estimates = self.estimate()
        
        return ArchiveJobResult(
            job_id=self.job_id,
            status='DRY_RUN',
            models_processed=len(graph.models),
            total_rows_archived=estimates['total_rows'],
            total_rows_deleted=0,
            duration_seconds=0,
            errors=[],
            warnings=schema_diffs if schema_diffs else []
        )
```

### 3.1.3 Plugin System

```python
from abc import ABC, abstractmethod

class Plugin(ABC):
    """Base class for plugins"""
    
    @abstractmethod
    def initialize(self, config: Dict):
        """Called once at startup"""
        pass
    
    @abstractmethod
    def execute(self, context: 'PluginContext'):
        """Called at plugin execution point"""
        pass

class PluginContext:
    """Context passed to plugins"""
    job_id: UUID
    model: Type[models.Model]
    phase: str  # 'before_copy', 'after_copy', 'before_delete', 'after_delete'
    pks: List[Any]
    metadata: Dict

# Example plugins

class EncryptionPlugin(Plugin):
    """Encrypt sensitive fields before archiving"""
    
    def __init__(self, fields_to_encrypt: List[str]):
        self.fields_to_encrypt = fields_to_encrypt
        self.cipher = Fernet(settings.ARCHIVE_ENCRYPTION_KEY)
    
    def initialize(self, config: Dict):
        pass
    
    def execute(self, context: PluginContext):
        if context.phase == 'before_copy':
            # Encrypt fields in archive DB
            for pk in context.pks:
                obj = context.model.objects.using('archive').get(pk=pk)
                for field_name in self.fields_to_encrypt:
                    value = getattr(obj, field_name)
                    if value:
                        encrypted = self.cipher.encrypt(value.encode())
                        setattr(obj, field_name, encrypted.decode())
                obj.save(using='archive')

class NotificationPlugin(Plugin):
    """Send notifications on completion"""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
    
    def initialize(self, config: Dict):
        pass
    
    def execute(self, context: PluginContext):
        if context.phase == 'after_delete':
            requests.post(self.webhook_url, json={
                'job_id': str(context.job_id),
                'model': context.model.__name__,
                'rows_archived': len(context.pks),
            })

class S3BackupPlugin(Plugin):
    """Export archived data to S3"""
    
    def __init__(self, bucket: str, prefix: str):
        self.bucket = bucket
        self.prefix = prefix
        self.s3_client = boto3.client('s3')
    
    def initialize(self, config: Dict):
        pass
    
    def execute(self, context: PluginContext):
        if context.phase == 'after_copy':
            # Export to CSV and upload to S3
            queryset = context.model.objects.using('archive').filter(pk__in=context.pks)
            csv_data = export_to_csv(queryset)
            
            key = f"{self.prefix}/{context.model.__name__}/{context.job_id}.csv"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=csv_data
            )
```

## 3.2 Internal Classes

### 3.2.1 DependencyGraph

```python
from dataclasses import dataclass
from typing import Set, Dict, List

@dataclass
class ForeignKeyRelation:
    """Represents a foreign key relationship"""
    from_model: Type[models.Model]
    from_field: str
    to_model: Type[models.Model]
    to_field: str
    nullable: bool
    on_delete: str  # CASCADE, SET_NULL, etc.

@dataclass
class ModelNode:
    """Node in dependency graph"""
    model: Type[models.Model]
    table_name: str
    app_label: str
    outgoing_edges: List[ForeignKeyRelation]  # FKs from this model
    incoming_edges: List[ForeignKeyRelation]  # FKs to this model
    level: int  # Topological level (0 = no dependencies)

class DependencyGraph:
    """
    Represents dependency graph of models
    """
    
    def __init__(self):
        self.nodes: Dict[Type[models.Model], ModelNode] = {}
        self.adjacency_list: Dict[Type[models.Model], Set[Type[models.Model]]] = {}
    
    def add_model(self, model: Type[models.Model]) -> ModelNode:
        """Add a model to the graph"""
        if model in self.nodes:
            return self.nodes[model]
        
        node = ModelNode(
            model=model,
            table_name=model._meta.db_table,
            app_label=model._meta.app_label,
            outgoing_edges=[],
            incoming_edges=[],
            level=-1
        )
        self.nodes[model] = node
        self.adjacency_list[model] = set()
        
        return node
    
    def add_edge(self, relation: ForeignKeyRelation):
        """Add a foreign key relationship"""
        # Add edge from child to parent
        self.adjacency_list[relation.from_model].add(relation.to_model)
        
        # Store in nodes
        self.nodes[relation.from_model].outgoing_edges.append(relation)
        self.nodes[relation.to_model].incoming_edges.append(relation)
    
    def topological_sort(self) -> List[Type[models.Model]]:
        """
        Return models in topological order (dependencies first)
        Uses Kahn's algorithm
        """
        # Calculate in-degree for each node
        in_degree = {model: 0 for model in self.nodes}
        for model, dependencies in self.adjacency_list.items():
            for dep in dependencies:
                in_degree[dep] += 1
        
        # Start with nodes that have no dependencies
        queue = [model for model, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # Sort for deterministic order
            queue.sort(key=lambda m: m.__name__)
            model = queue.pop(0)
            result.append(model)
            
            # Reduce in-degree of dependent models
            for dependent in self._get_dependents(model):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        # Check for cycles
        if len(result) != len(self.nodes):
            cycle = self._detect_cycle()
            raise CyclicDependencyException(f"Circular dependency detected: {cycle}")
        
        return result
    
    def reverse_topological_sort(self) -> List[Type[models.Model]]:
        """Return models in reverse order (for deletion)"""
        return list(reversed(self.topological_sort()))
    
    def _get_dependents(self, model: Type[models.Model]) -> List[Type[models.Model]]:
        """Get models that depend on this model"""
        dependents = []
        for other_model, dependencies in self.adjacency_list.items():
            if model in dependencies:
                dependents.append(other_model)
        return dependents
    
    def _detect_cycle(self) -> List[Type[models.Model]]:
        """Detect cycle using DFS"""
        visited = set()
        rec_stack = set()
        cycle = []
        
        def dfs(model, path):
            visited.add(model)
            rec_stack.add(model)
            path.append(model)
            
            for dependency in self.adjacency_list[model]:
                if dependency not in visited:
                    if dfs(dependency, path):
                        return True
                elif dependency in rec_stack:
                    # Found cycle
                    cycle_start = path.index(dependency)
                    cycle.extend(path[cycle_start:])
                    return True
            
            path.pop()
            rec_stack.remove(model)
            return False
        
        for model in self.nodes:
            if model not in visited:
                if dfs(model, []):
                    return cycle
        
        return []
    
    def get_subgraph(self, root_model: Type[models.Model], depth: int = -1) -> 'DependencyGraph':
        """
        Get subgraph starting from root model
        
        Args:
            root_model: Starting model
            depth: Maximum depth to traverse (-1 = unlimited)
        
        Returns:
            New DependencyGraph with subset of nodes
        """
        subgraph = DependencyGraph()
        visited = set()
        
        def traverse(model, current_depth):
            if model in visited:
                return
            if depth != -1 and current_depth > depth:
                return
            
            visited.add(model)
            subgraph.add_model(model)
            
            # Add edges and traverse dependencies
            for edge in self.nodes[model].outgoing_edges:
                subgraph.add_edge(edge)
                traverse(edge.to_model, current_depth + 1)
        
        traverse(root_model, 0)
        return subgraph
    
    def visualize(self) -> str:
        """Generate ASCII visualization of graph"""
        lines = ["Dependency Graph:", ""]
        
        for model in self.topological_sort():
            node = self.nodes[model]
            lines.append(f"{model.__name__} (level {node.level})")
            
            if node.outgoing_edges:
                for edge in node.outgoing_edges:
                    arrow = "─?→" if edge.nullable else "──→"
                    lines.append(f"  {arrow} {edge.to_model.__name__} ({edge.from_field})")
            
            lines.append("")
        
        return "\n".join(lines)
```

### 3.2.2 JobOrchestrator

```python
class JobOrchestrator:
    """
    Orchestrates the complete archival workflow
    """
    
    def __init__(self, job_id: UUID, source_db: str, archive_db: str):
        self.job_id = job_id
        self.source_db = source_db
        self.archive_db = archive_db
        
        # State
        self.cancelled = False
        
        # Subsystems
        self.dependency_resolver = None
        self.schema_replicator = None
        self.archive_executor = None
        self.delete_executor = None
        self.validation_engine = None
        self.checkpoint_manager = None
    
    def execute(self, config: ArchiveConfig) -> ArchiveJobResult:
        """Execute complete archival workflow"""
        
        start_time = time.time()
        
        try:
            # Phase 1: Build dependency graph
            logger.info(f"Job {self.job_id}: Building dependency graph")
            self.dependency_resolver = DependencyResolver(config.root_models)
            graph = self.dependency_resolver.build_graph()
            
            # Phase 2: Sync schema
            logger.info(f"Job {self.job_id}: Synchronizing schema")
            self.schema_replicator = SchemaReplicator(self.source_db, self.archive_db)
            self.schema_replicator.sync_schema(graph.topological_sort())
            
            # Phase 3: Archive data
            logger.info(f"Job {self.job_id}: Archiving data")
            archived_data = self._archive_phase(config, graph)
            
            # Phase 4: Validate
            logger.info(f"Job {self.job_id}: Validating archived data")
            self._validation_phase(config, archived_data)
            
            # Phase 5: Delete from source
            if config.strategy in [ArchivalStrategy.MOVE, ArchivalStrategy.SELECTIVE]:
                logger.info(f"Job {self.job_id}: Deleting from source")
                self._deletion_phase(config, graph, archived_data)
            
            duration = time.time() - start_time
            
            return ArchiveJobResult(
                job_id=self.job_id,
                status='SUCCESS',
                models_processed=len(archived_data),
                total_rows_archived=sum(len(pks) for pks in archived_data.values()),
                total_rows_deleted=sum(len(pks) for pks in archived_data.values()) if config.strategy == ArchivalStrategy.MOVE else 0,
                duration_seconds=duration,
                errors=[],
                warnings=[]
            )
            
        except Exception as e:
            logger.error(f"Job {self.job_id} failed: {e}", exc_info=True)
            raise
    
    def _archive_phase(
        self,
        config: ArchiveConfig,
        graph: DependencyGraph
    ) -> Dict[Type[models.Model], List[Any]]:
        """
        Archive data for all models
        
        Returns:
            Dictionary mapping model to list of archived PKs
        """
        archived_data = {}
        
        # Process in topological order (parents before children)
        for model in graph.topological_sort():
            if self.cancelled:
                raise JobCancelledException()
            
            if not config.should_archive_model(model):
                continue
            
            # Get retention policy for this model
            retention = config.get_retention_policy(model)
            
            # Build queryset
            queryset = model.objects.using(self.source_db).filter(retention.as_q())
            
            # Apply callback
            config.before_archive(model, queryset)
            
            # Archive this model
            pks = self.archive_executor.archive_model(
                model=model,
                queryset=queryset,
                batch_size=config.batch_size,
                order_by=config.get_order_by(model)
            )
            
            archived_data[model] = pks
            
            # Apply callback
            config.after_archive(model, pks)
            
            logger.info(f"Archived {len(pks)} rows from {model.__name__}")
        
        return archived_data
    
    def _validation_phase(
        self,
        config: ArchiveConfig,
        archived_data: Dict[Type[models.Model], List[Any]]
    ):
        """Validate archived data"""
        self.validation_engine = ValidationEngine(
            source_db=self.source_db,
            archive_db=self.archive_db,
            level=config.validation_level,
            sample_rate=config.validation_sample_rate
        )
        
        for model, pks in archived_data.items():
            self.validation_engine.validate(model, pks)
    
    def _deletion_phase(
        self,
        config: ArchiveConfig,
        graph: DependencyGraph,
        archived_data: Dict[Type[models.Model], List[Any]]
    ):
        """Delete archived data from source"""
        self.delete_executor = DeleteExecutor(self.source_db)
        
        # Delete in reverse topological order (children before parents)
        for model in graph.reverse_topological_sort():
            if self.cancelled:
                raise JobCancelledException()
            
            if model not in archived_data:
                continue
            
            pks = archived_data[model]
            
            # Apply callback
            config.before_delete(model, pks)
            
            # Delete
            self.delete_executor.delete_model(
                model=model,
                pks=pks,
                batch_size=config.delete_batch_size
            )
            
            # Apply callback
            config.after_delete(model, pks)
            
            logger.info(f"Deleted {len(pks)} rows from {model.__name__}")
    
    def cancel(self, job_id: UUID):
        """Cancel running job"""
        self.cancelled = True
        logger.info(f"Job {job_id} cancellation requested")
```



---

# 4. Sequence Diagrams

## 4.1 Complete Archival Flow

```
┌──────┐      ┌────────────────┐      ┌────────────────┐      ┌───────────────┐      ┌──────────┐      ┌──────────┐
│Client│      │ArchiveJobRunner│      │JobOrchestrator │      │DependencyGraph│      │SourceDB  │      │ArchiveDB │
└──┬───┘      └───────┬────────┘      └───────┬────────┘      └───────┬───────┘      └────┬─────┘      └────┬─────┘
   │                  │                        │                       │                   │                 │
   │ run(config)      │                        │                       │                   │                 │
   ├─────────────────>│                        │                       │                   │                 │
   │                  │                        │                       │                   │                 │
   │                  │ Load & Validate Config │                       │                   │                 │
   │                  ├───────────┐            │                       │                   │                 │
   │                  │           │            │                       │                   │                 │
   │                  │<──────────┘            │                       │                   │                 │
   │                  │                        │                       │                   │                 │
   │                  │ execute(config)        │                       │                   │                 │
   │                  ├───────────────────────>│                       │                   │                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │ build_graph(models)   │                   │                 │
   │                  │                        ├──────────────────────>│                   │                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │                       │ Introspect Models │                 │
   │                  │                        │                       ├──────────┐        │                 │
   │                  │                        │                       │          │        │                 │
   │                  │                        │                       │<─────────┘        │                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │   Graph               │                   │                 │
   │                  │                        │<──────────────────────┤                   │                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │ sync_schema(models)   │                   │                 │
   │                  │                        ├───────────────────────┼──────────────────>│                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │                       │                   │ Apply Migrations│
   │                  │                        │                       │                   ├────────────────>│
   │                  │                        │                       │                   │                 │
   │                  │                        │                       │                   │  Schema Synced  │
   │                  │                        │                       │                   │<────────────────┤
   │                  │                        │                       │                   │                 │
   │                  │                        │ ╔═══════════════════════════════════════════════════════════╗
   │                  │                        │ ║ LOOP: For each model in topological order                ║
   │                  │                        │ ╚═══════════════════════════════════════════════════════════╝
   │                  │                        │                       │                   │                 │
   │                  │                        │ archive_model(model)  │                   │                 │
   │                  │                        ├───────────────────────┼──────────────────>│                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │                       │ ╔═══════════════════════════════════╗
   │                  │                        │                       │ ║ LOOP: For each batch             ║
   │                  │                        │                       │ ╚═══════════════════════════════════╝
   │                  │                        │                       │                   │                 │
   │                  │                        │                       │   SELECT batch    │                 │
   │                  │                        │                       │<──────────────────┤                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │                       │   INSERT batch    │                 │
   │                  │                        │                       ├──────────────────────────────────────>│
   │                  │                        │                       │                   │                 │
   │                  │                        │                       │   Save Checkpoint │                 │
   │                  │                        │                       ├──────────────────>│                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │   archived_pks        │                   │                 │
   │                  │                        │<──────────────────────┤                   │                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │ validate(model, pks)  │                   │                 │
   │                  │                        ├───────────────────────┼──────────────────>│                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │                       │   Compare Data    │                 │
   │                  │                        │                       ├──────────────────────────────────────>│
   │                  │                        │                       │                   │                 │
   │                  │                        │   Validation OK       │                   │                 │
   │                  │                        │<──────────────────────┤                   │                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │ ╔═══════════════════════════════════════════════════════════╗
   │                  │                        │ ║ LOOP: For each model in reverse order (deletion)         ║
   │                  │                        │ ╚═══════════════════════════════════════════════════════════╝
   │                  │                        │                       │                   │                 │
   │                  │                        │ delete_model(model,pks)                   │                 │
   │                  │                        ├───────────────────────┼──────────────────>│                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │                       │  DELETE batch     │                 │
   │                  │                        │                       │<──────────────────┤                 │
   │                  │                        │                       │                   │                 │
   │                  │                        │   Deleted             │                   │                 │
   │                  │                        │<──────────────────────┤                   │                 │
   │                  │                        │                       │                   │                 │
   │                  │   ArchiveJobResult     │                       │                   │                 │
   │                  │<───────────────────────┤                       │                   │                 │
   │                  │                        │                       │                   │                 │
   │   Result         │                        │                       │                   │                 │
   │<─────────────────┤                        │                       │                   │                 │
   │                  │                        │                       │                   │                 │
```

## 4.2 Failure and Recovery Flow

```
┌──────┐      ┌────────────────┐      ┌────────────────┐      ┌──────────────┐
│Client│      │ArchiveJobRunner│      │CheckpointMgr   │      │  Database    │
└──┬───┘      └───────┬────────┘      └───────┬────────┘      └──────┬───────┘
   │                  │                        │                      │
   │ run(config)      │                        │                      │
   ├─────────────────>│                        │                      │
   │                  │                        │                      │
   │                  │ Start Job              │                      │
   │                  ├───────────────────────>│                      │
   │                  │                        │                      │
   │                  │                        │  INSERT job record   │
   │                  │                        ├─────────────────────>│
   │                  │                        │                      │
   │                  │ ╔════════════════════════════════════════════╗
   │                  │ ║ LOOP: Process batches                      ║
   │                  │ ╚════════════════════════════════════════════╝
   │                  │                        │                      │
   │                  │ Archive batch 1        │                      │
   │                  ├───────────────────────>│                      │
   │                  │                        │                      │
   │                  │                        │  Save checkpoint     │
   │                  │                        ├─────────────────────>│
   │                  │                        │                      │
   │                  │ Archive batch 2        │                      │
   │                  ├───────────────────────>│                      │
   │                  │                        │                      │
   │                  │                        │  Save checkpoint     │
   │                  │                        ├─────────────────────>│
   │                  │                        │                      │
   │                  │ Archive batch 3        │                      │
   │                  ├───────────────────────>│                      │
   │                  │                        │                      │
   │                  │          ╔══════════════════════════════════╗
   │                  │          ║ CRASH! Process dies              ║
   │                  │          ╚══════════════════════════════════╝
   │                  │                        │                      │
   │                  X                        X                      │
   │                                                                  │
   │  Time passes... System restarts                                 │
   │                                                                  │
   │ run(config,      │                        │                      │
   │  resume_job_id)  │                        │                      │
   ├─────────────────>│                        │                      │
   │                  │                        │                      │
   │                  │ load_checkpoint(job_id)│                      │
   │                  ├───────────────────────>│                      │
   │                  │                        │                      │
   │                  │                        │  SELECT checkpoints  │
   │                  │                        ├─────────────────────>│
   │                  │                        │                      │
   │                  │                        │  Checkpoint data     │
   │                  │                        │<─────────────────────┤
   │                  │                        │                      │
   │                  │   Last PK: batch 2     │                      │
   │                  │<───────────────────────┤                      │
   │                  │                        │                      │
   │                  │ Resume from batch 3    │                      │
   │                  ├───────────────────────>│                      │
   │                  │                        │                      │
   │                  │ Archive batch 3 (retry)│                      │
   │                  ├───────────────────────>│                      │
   │                  │                        │                      │
   │                  │                        │  Save checkpoint     │
   │                  │                        ├─────────────────────>│
   │                  │                        │                      │
   │                  │ Continue with batch 4  │                      │
   │                  ├───────────────────────>│                      │
   │                  │                        │                      │
   │   Result         │                        │                      │
   │<─────────────────┤                        │                      │
   │                  │                        │                      │
```

## 4.3 Schema Synchronization Flow

```
┌──────────────┐      ┌────────────────┐      ┌──────────┐      ┌──────────┐
│Orchestrator  │      │SchemaReplicator│      │SourceDB  │      │ArchiveDB │
└──────┬───────┘      └───────┬────────┘      └────┬─────┘      └────┬─────┘
       │                      │                     │                 │
       │ sync_schema(models)  │                     │                 │
       ├─────────────────────>│                     │                 │
       │                      │                     │                 │
       │                      │ ╔═══════════════════════════════════════════╗
       │                      │ ║ Phase 1: Check migration history         ║
       │                      │ ╚═══════════════════════════════════════════╝
       │                      │                     │                 │
       │                      │ SELECT FROM         │                 │
       │                      │ django_migrations   │                 │
       │                      ├────────────────────>│                 │
       │                      │                     │                 │
       │                      │ Applied migrations  │                 │
       │                      │<────────────────────┤                 │
       │                      │                     │                 │
       │                      │ SELECT FROM django_migrations         │
       │                      ├───────────────────────────────────────>│
       │                      │                     │                 │
       │                      │         Applied migrations            │
       │                      │<───────────────────────────────────────┤
       │                      │                     │                 │
       │                      │ Compare & find unapplied              │
       │                      ├────────┐            │                 │
       │                      │        │            │                 │
       │                      │<───────┘            │                 │
       │                      │                     │                 │
       │                      │ ╔═══════════════════════════════════════════╗
       │                      │ ║ Phase 2: Apply missing migrations        ║
       │                      │ ╚═══════════════════════════════════════════╝
       │                      │                     │                 │
       │                      │ ╔════════════════════════════════════════╗
       │                      │ ║ LOOP: For each unapplied migration    ║
       │                      │ ╚════════════════════════════════════════╝
       │                      │                     │                 │
       │                      │  Apply migration SQL                  │
       │                      ├───────────────────────────────────────>│
       │                      │                     │                 │
       │                      │  INSERT INTO django_migrations        │
       │                      ├───────────────────────────────────────>│
       │                      │                     │                 │
       │                      │ ╔═══════════════════════════════════════════╗
       │                      │ ║ Phase 3: Validate schema consistency      ║
       │                      │ ╚═══════════════════════════════════════════╝
       │                      │                     │                 │
       │                      │ SELECT FROM         │                 │
       │                      │ information_schema  │                 │
       │                      ├────────────────────>│                 │
       │                      │                     │                 │
       │                      │ Schema metadata     │                 │
       │                      │<────────────────────┤                 │
       │                      │                     │                 │
       │                      │ SELECT FROM information_schema        │
       │                      ├───────────────────────────────────────>│
       │                      │                     │                 │
       │                      │         Schema metadata               │
       │                      │<───────────────────────────────────────┤
       │                      │                     │                 │
       │                      │ Compare schemas     │                 │
       │                      ├─────────┐           │                 │
       │                      │         │           │                 │
       │                      │<────────┘           │                 │
       │                      │                     │                 │
       │                      │ Log any differences │                 │
       │                      ├──────────┐          │                 │
       │                      │          │          │                 │
       │                      │<─────────┘          │                 │
       │                      │                     │                 │
       │   Schema synced      │                     │                 │
       │<─────────────────────┤                     │                 │
       │                      │                     │                 │
```

## 4.4 Dependency Resolution Flow

```
┌──────────────┐      ┌──────────────────┐      ┌───────────────┐
│Orchestrator  │      │DependencyResolver│      │Django Models  │
└──────┬───────┘      └────────┬─────────┘      └───────┬───────┘
       │                       │                        │
       │ build_graph([Order])  │                        │
       ├──────────────────────>│                        │
       │                       │                        │
       │                       │ Introspect Order model │
       │                       ├───────────────────────>│
       │                       │                        │
       │                       │ Fields, FKs            │
       │                       │<───────────────────────┤
       │                       │                        │
       │                       │ Add Order to graph     │
       │                       ├────────┐               │
       │                       │        │               │
       │                       │<───────┘               │
       │                       │                        │
       │                       │ ╔═══════════════════════════════════╗
       │                       │ ║ LOOP: Process foreign keys        ║
       │                       │ ╚═══════════════════════════════════╝
       │                       │                        │
       │                       │ Found FK: customer_id  │
       │                       │ -> Customer            │
       │                       ├────────┐               │
       │                       │        │               │
       │                       │<───────┘               │
       │                       │                        │
       │                       │ Introspect Customer    │
       │                       ├───────────────────────>│
       │                       │                        │
       │                       │ Fields, FKs            │
       │                       │<───────────────────────┤
       │                       │                        │
       │                       │ Add Customer to graph  │
       │                       │ Add edge: Order->Customer
       │                       ├────────┐               │
       │                       │        │               │
       │                       │<───────┘               │
       │                       │                        │
       │                       │ Found FK: product_id   │
       │                       │ -> Product             │
       │                       │                        │
       │                       │ Recursively process    │
       │                       │ Product...             │
       │                       │                        │
       │                       │ topological_sort()     │
       │                       ├────────┐               │
       │                       │        │               │
       │                       │<───────┘               │
       │                       │                        │
       │                       │ Ordered list:          │
       │                       │ [Customer, Product,    │
       │                       │  Order, OrderItem]     │
       │                       │                        │
       │   DependencyGraph     │                        │
       │<──────────────────────┤                        │
       │                       │                        │
```



---

# 5. Database Metadata Flow

## 5.1 Metadata Tables

The library creates and maintains several metadata tables in the source database:

### Table: django_archive_jobs

```sql
CREATE TABLE django_archive_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    config_hash VARCHAR(64) NOT NULL,  -- Hash of configuration for idempotency
    status VARCHAR(50) NOT NULL,  -- PENDING, RUNNING, SUCCESS, FAILED, CANCELLED
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_by VARCHAR(255),
    metadata JSONB,  -- Flexible storage for additional context
    error_message TEXT,
    CONSTRAINT chk_status CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED'))
);

CREATE INDEX idx_archive_jobs_status ON django_archive_jobs(status);
CREATE INDEX idx_archive_jobs_started_at ON django_archive_jobs(started_at);
```

### Table: django_archive_checkpoints

```sql
CREATE TABLE django_archive_checkpoints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES django_archive_jobs(id) ON DELETE CASCADE,
    model_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    phase VARCHAR(50) NOT NULL,  -- COPYING, COPIED, VALIDATING, DELETING, DELETED, COMPLETE
    last_processed_pk VARCHAR(255),  -- Store as string to handle different PK types
    batch_number INTEGER NOT NULL DEFAULT 0,
    rows_processed BIGINT NOT NULL DEFAULT 0,
    rows_total BIGINT,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB,
    CONSTRAINT unique_job_model UNIQUE (job_id, model_name),
    CONSTRAINT chk_phase CHECK (phase IN ('COPYING', 'COPIED', 'VALIDATING', 'DELETING', 'DELETED', 'COMPLETE'))
);

CREATE INDEX idx_checkpoints_job_id ON django_archive_checkpoints(job_id);
CREATE INDEX idx_checkpoints_phase ON django_archive_checkpoints(phase);
```

### Table: django_archive_audit

```sql
CREATE TABLE django_archive_audit (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES django_archive_jobs(id) ON DELETE CASCADE,
    model_name VARCHAR(255) NOT NULL,
    operation VARCHAR(50) NOT NULL,  -- COPY, DELETE, VALIDATE, ERROR
    row_count BIGINT NOT NULL,
    duration_ms BIGINT,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    metadata JSONB,
    CONSTRAINT chk_operation CHECK (operation IN ('COPY', 'DELETE', 'VALIDATE', 'ERROR'))
);

CREATE INDEX idx_audit_job_id ON django_archive_audit(job_id);
CREATE INDEX idx_audit_timestamp ON django_archive_audit(timestamp);
CREATE INDEX idx_audit_operation ON django_archive_audit(operation);
```

### Table: django_archive_schema_versions

```sql
CREATE TABLE django_archive_schema_versions (
    id BIGSERIAL PRIMARY KEY,
    model_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    source_schema_hash VARCHAR(64) NOT NULL,
    archive_schema_hash VARCHAR(64) NOT NULL,
    last_synced_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    migration_version VARCHAR(255),  -- Last applied migration
    schema_diff JSONB,  -- Any detected differences
    CONSTRAINT unique_model UNIQUE (model_name)
);

CREATE INDEX idx_schema_versions_model ON django_archive_schema_versions(model_name);
```

## 5.2 Metadata Flow Diagram

```
                     ┌─────────────────────────────────────┐
                     │    Start Archival Job              │
                     └────────────────┬────────────────────┘
                                      │
                                      ▼
                     ┌─────────────────────────────────────┐
                     │ INSERT INTO django_archive_jobs     │
                     │ status = 'PENDING'                  │
                     └────────────────┬────────────────────┘
                                      │
                                      ▼
                     ┌─────────────────────────────────────┐
                     │ UPDATE status = 'RUNNING'           │
                     │ started_at = NOW()                  │
                     └────────────────┬────────────────────┘
                                      │
        ┌─────────────────────────────┴─────────────────────────────┐
        │                                                             │
        ▼                                                             ▼
┌────────────────────┐                                   ┌────────────────────┐
│ For each model     │                                   │ Schema Sync        │
└────────┬───────────┘                                   └────────┬───────────┘
         │                                                         │
         ▼                                                         ▼
┌────────────────────────────────────┐          ┌────────────────────────────────────┐
│ INSERT/UPDATE                      │          │ INSERT/UPDATE                      │
│ django_archive_checkpoints         │          │ django_archive_schema_versions     │
│ phase = 'COPYING'                  │          │ source_schema_hash = XXX           │
│ rows_total = COUNT(*)              │          │ archive_schema_hash = XXX          │
└────────┬───────────────────────────┘          └────────────────────────────────────┘
         │
         │  ┌─────────────────────────────────────┐
         │  │ For each batch                      │
         └─>│                                     │
            │ 1. Copy batch to archive            │
            │ 2. UPDATE checkpoint:               │
            │    - last_processed_pk = X          │
            │    - rows_processed += batch_size   │
            │    - updated_at = NOW()             │
            │                                     │
            │ 3. INSERT INTO audit:               │
            │    - operation = 'COPY'             │
            │    - row_count = batch_size         │
            └────────┬────────────────────────────┘
                     │
                     ▼
            ┌────────────────────────────────────┐
            │ UPDATE checkpoint:                 │
            │ phase = 'COPIED'                   │
            │ completed_at = NOW()               │
            └────────┬───────────────────────────┘
                     │
                     ▼
            ┌────────────────────────────────────┐
            │ Validation Phase                   │
            │ UPDATE phase = 'VALIDATING'        │
            └────────┬───────────────────────────┘
                     │
                     ▼
            ┌────────────────────────────────────┐
            │ UPDATE phase = 'DELETING'          │
            └────────┬───────────────────────────┘
                     │
                     │  ┌──────────────────────────────────┐
                     │  │ For each batch                   │
                     └─>│                                  │
                        │ 1. DELETE batch from source      │
                        │ 2. UPDATE checkpoint             │
                        │ 3. INSERT INTO audit             │
                        └────────┬─────────────────────────┘
                                 │
                                 ▼
                        ┌────────────────────────────────┐
                        │ UPDATE checkpoint:             │
                        │ phase = 'DELETED'              │
                        └────────┬───────────────────────┘
                                 │
                   ┌─────────────┴─────────────┐
                   │   All models complete?    │
                   └──────┬────────────────┬───┘
                          │ No             │ Yes
                          │                │
                          ▼                ▼
                    ┌──────────┐  ┌─────────────────────────┐
                    │ Continue │  │ UPDATE job:             │
                    │ loop     │  │ status = 'SUCCESS'      │
                    └──────────┘  │ completed_at = NOW()    │
                                  └─────────────────────────┘
```

---

# 6. Dependency Resolution Algorithm

## 6.1 Algorithm Overview

The dependency resolution algorithm builds a complete graph of model relationships and determines safe processing order.

**Input:** List of root models to archive  
**Output:** Topological ordering of all related models  
**Complexity:** O(V + E) where V = models, E = foreign keys

## 6.2 Detailed Algorithm

```python
def build_dependency_graph(root_models: List[Type[models.Model]]) -> DependencyGraph:
    """
    Build complete dependency graph using BFS traversal
    """
    graph = DependencyGraph()
    visited = set()
    queue = deque(root_models)
    
    while queue:
        model = queue.popleft()
        
        if model in visited:
            continue
        
        visited.add(model)
        node = graph.add_model(model)
        
        # Process all fields
        for field in model._meta.get_fields():
            
            # Case 1: ForeignKey
            if isinstance(field, ForeignKey):
                related_model = field.related_model
                
                # Skip self-referential for now (handle separately)
                if related_model == model:
                    node.has_self_reference = True
                    graph.add_edge(ForeignKeyRelation(
                        from_model=model,
                        from_field=field.name,
                        to_model=model,
                        to_field='pk',
                        nullable=field.null,
                        on_delete=field.remote_field.on_delete.__name__
                    ))
                    continue
                
                # Add edge: model -> related_model
                graph.add_edge(ForeignKeyRelation(
                    from_model=model,
                    from_field=field.name,
                    to_model=related_model,
                    to_field='pk',
                    nullable=field.null,
                    on_delete=field.remote_field.on_delete.__name__
                ))
                
                # Traverse to related model
                if related_model not in visited:
                    queue.append(related_model)
            
            # Case 2: ManyToManyField
            elif isinstance(field, ManyToManyField):
                # Get through model
                through_model = field.remote_field.through
                
                # Don't process auto-created through models differently
                # They'll be handled via their own FK relationships
                if through_model not in visited:
                    queue.append(through_model)
            
            # Case 3: GenericForeignKey
            elif isinstance(field, GenericForeignKey):
                # Need runtime resolution
                # Mark for special handling
                node.has_generic_fk = True
                node.generic_fk_fields.append(field.name)
            
            # Case 4: Reverse relations (FKs pointing to this model)
            elif isinstance(field, (ManyToOneRel, ManyToManyRel)):
                # These are already captured from the other side
                pass
    
    return graph


def topological_sort_kahn(graph: DependencyGraph) -> List[Type[models.Model]]:
    """
    Kahn's algorithm for topological sorting
    
    Returns models in dependency order (parents before children)
    """
    # Calculate in-degree (number of dependencies)
    in_degree = {model: 0 for model in graph.nodes}
    
    for model in graph.nodes:
        for edge in graph.nodes[model].outgoing_edges:
            # Skip self-references for topological sort
            if edge.to_model != model:
                in_degree[edge.to_model] += 1
    
    # Start with models that have no dependencies
    queue = [model for model, degree in in_degree.items() if degree == 0]
    result = []
    
    while queue:
        # Sort for deterministic output
        queue.sort(key=lambda m: (m._meta.app_label, m.__name__))
        
        model = queue.pop(0)
        result.append(model)
        
        # Get models that depend on this one
        for dependent in graph.get_dependents(model):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
    
    # Check for cycles
    if len(result) != len(graph.nodes):
        # Find cycle
        remaining = set(graph.nodes.keys()) - set(result)
        cycle = detect_cycle(graph, remaining)
        raise CyclicDependencyException(
            f"Circular dependency detected: {' -> '.join(m.__name__ for m in cycle)}"
        )
    
    return result


def detect_cycle(graph: DependencyGraph, nodes: Set[Type[models.Model]]) -> List[Type[models.Model]]:
    """
    DFS-based cycle detection
    """
    visited = set()
    rec_stack = set()
    cycle = []
    
    def dfs(model, path):
        visited.add(model)
        rec_stack.add(model)
        path.append(model)
        
        for edge in graph.nodes[model].outgoing_edges:
            dep = edge.to_model
            
            if dep == model:  # Skip self-reference
                continue
            
            if dep not in visited:
                if dfs(dep, path):
                    return True
            elif dep in rec_stack:
                # Found cycle
                cycle_start = path.index(dep)
                cycle.extend(path[cycle_start:])
                cycle.append(dep)
                return True
        
        path.pop()
        rec_stack.remove(model)
        return False
    
    for model in nodes:
        if model not in visited:
            if dfs(model, []):
                return cycle
    
    return []


def handle_self_referential_fk(model: Type[models.Model], pks: List[Any]) -> List[List[Any]]:
    """
    Handle self-referential foreign keys by grouping into levels
    
    Example: Employee.manager_id -> Employee.id
    
    Returns: List of PK groups, where each group can be archived independently
    """
    # Find the self-referential field
    self_fk_field = None
    for field in model._meta.get_fields():
        if isinstance(field, ForeignKey) and field.related_model == model:
            self_fk_field = field.name
            break
    
    if not self_fk_field:
        return [pks]
    
    # Build tree levels
    # Level 0: No parent (or parent not in archive set)
    # Level 1: Parent in level 0
    # Level 2: Parent in level 1
    # etc.
    
    pk_to_parent = {}
    rows = model.objects.filter(pk__in=pks).values('pk', self_fk_field)
    
    for row in rows:
        pk_to_parent[row['pk']] = row[self_fk_field]
    
    levels = []
    remaining = set(pks)
    
    while remaining:
        # Find PKs whose parents are not in remaining set
        current_level = []
        for pk in remaining:
            parent_pk = pk_to_parent.get(pk)
            if parent_pk is None or parent_pk not in remaining:
                current_level.append(pk)
        
        if not current_level:
            # All remaining have circular reference
            # Archive them anyway (FK is nullable or will be handled)
            current_level = list(remaining)
        
        levels.append(current_level)
        remaining -= set(current_level)
    
    return levels


def resolve_generic_foreign_keys(
    model: Type[models.Model],
    pks: List[Any],
    gfk_field_name: str
) -> Dict[Type[models.Model], Set[Any]]:
    """
    Resolve GenericForeignKey relationships at runtime
    
    Returns: Dictionary mapping target models to their PKs
    """
    from django.contrib.contenttypes.models import ContentType
    
    # GenericForeignKey has two components: content_type and object_id
    gfk_field = getattr(model, gfk_field_name)
    ct_field = gfk_field.ct_field  # Usually 'content_type'
    fk_field = gfk_field.fk_field  # Usually 'object_id'
    
    # Query for all content types and object IDs
    relations = model.objects.filter(pk__in=pks).values_list(
        ct_field, fk_field
    ).distinct()
    
    # Group by target model
    target_pks = defaultdict(set)
    for ct_id, obj_id in relations:
        if ct_id and obj_id:
            content_type = ContentType.objects.get(id=ct_id)
            target_model = content_type.model_class()
            target_pks[target_model].add(obj_id)
    
    return target_pks
```

## 6.3 Special Cases

### Self-Referential Foreign Keys

```
Employee
  id    name       manager_id
  1     Alice      NULL
  2     Bob        1
  3     Charlie    1
  4     David      2

Archive order: [1] -> [2, 3] -> [4]
```

### Circular Dependencies

```
A.b_id -> B.id
B.a_id -> A.id

Solution 1: Allow nullable FKs
  - Archive A with b_id = NULL
  - Archive B
  - Update A.b_id

Solution 2: Use deferred constraints
  - SET CONSTRAINTS ALL DEFERRED
  - Archive both
  - COMMIT (constraints checked at end)

Solution 3: Fail with clear error
  - Ask user to resolve manually
```

### Generic Foreign Keys

```python
# Comment can point to any model
class Comment(models.Model):
    content_type = ForeignKey(ContentType)
    object_id = PositiveIntegerField()
    content_object = GenericForeignKey('content_type', 'object_id')

# Runtime resolution required
comments = Comment.objects.filter(pk__in=comment_pks)
for comment in comments:
    # content_object could be Article, Video, Product, etc.
    target_model = comment.content_type.model_class()
    target_pk = comment.object_id
    # Add to graph dynamically
```

---

# 7. Schema Synchronization Strategy

## 7.1 Hybrid Approach (Recommended)

Combines migration-based sync with schema validation.

### Phase 1: Migration-Based Sync

```python
def sync_via_migrations(models: List[Type[models.Model]]):
    """
    Sync schema using Django migrations
    """
    # Get migration history from source
    source_migrations = get_applied_migrations('default')
    
    # Get migration history from archive
    archive_migrations = get_applied_migrations('archive')
    
    # Find unapplied migrations
    unapplied = source_migrations - archive_migrations
    
    # Filter to migrations affecting our models
    app_labels = {m._meta.app_label for m in models}
    relevant_migrations = [
        m for m in unapplied
        if m.app_label in app_labels
    ]
    
    # Apply migrations to archive DB
    with connections['archive'].cursor() as cursor:
        for migration in relevant_migrations:
            logger.info(f"Applying migration {migration} to archive DB")
            
            # Load migration file
            migration_obj = load_migration(migration.app_label, migration.name)
            
            # Execute SQL operations
            for operation in migration_obj.operations:
                if isinstance(operation, RunSQL):
                    cursor.execute(operation.sql)
                elif isinstance(operation, RunPython):
                    # Skip data migrations
                    logger.warning(f"Skipping RunPython in {migration}")
                else:
                    # Use Django's schema editor
                    with connections['archive'].schema_editor() as schema_editor:
                        operation.database_forwards(
                            app_label=migration.app_label,
                            schema_editor=schema_editor,
                            from_state=None,
                            to_state=None
                        )
            
            # Mark as applied
            MigrationRecorder(connections['archive']).record_applied(
                migration.app_label,
                migration.name
            )


def get_applied_migrations(db_alias: str) -> Set[Tuple[str, str]]:
    """Get set of (app_label, migration_name) tuples"""
    recorder = MigrationRecorder(connections[db_alias])
    return {
        (record.app, record.name)
        for record in recorder.applied_migrations()
    }
```

### Phase 2: Schema Diff Validation

```python
def validate_schema_consistency(models: List[Type[models.Model]]) -> List[SchemaDifference]:
    """
    Compare schemas between source and archive
    """
    differences = []
    
    for model in models:
        table_name = model._meta.db_table
        
        # Compare columns
        source_columns = get_columns('default', table_name)
        archive_columns = get_columns('archive', table_name)
        
        column_diff = compare_columns(source_columns, archive_columns)
        if column_diff:
            differences.extend(column_diff)
        
        # Compare indexes
        source_indexes = get_indexes('default', table_name)
        archive_indexes = get_indexes('archive', table_name)
        
        index_diff = compare_indexes(source_indexes, archive_indexes)
        if index_diff:
            differences.extend(index_diff)
        
        # Compare constraints
        source_constraints = get_constraints('default', table_name)
        archive_constraints = get_constraints('archive', table_name)
        
        constraint_diff = compare_constraints(source_constraints, archive_constraints)
        if constraint_diff:
            differences.extend(constraint_diff)
    
    return differences


def get_columns(db_alias: str, table_name: str) -> List[ColumnInfo]:
    """Query information_schema for column details"""
    query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        ORDER BY ordinal_position
    """
    
    with connections[db_alias].cursor() as cursor:
        cursor.execute(query, [table_name])
        columns = cursor.fetchall()
        
        return [
            ColumnInfo(
                name=row[0],
                data_type=row[1],
                max_length=row[2],
                precision=row[3],
                scale=row[4],
                nullable=row[5] == 'YES',
                default=row[6]
            )
            for row in columns
        ]


def compare_columns(
    source: List[ColumnInfo],
    archive: List[ColumnInfo]
) -> List[SchemaDifference]:
    """Compare column definitions"""
    differences = []
    
    source_map = {c.name: c for c in source}
    archive_map = {c.name: c for c in archive}
    
    # Missing columns in archive
    missing = set(source_map.keys()) - set(archive_map.keys())
    for col_name in missing:
        differences.append(SchemaDifference(
            type='MISSING_COLUMN',
            object_name=col_name,
            expected=source_map[col_name],
            actual=None
        ))
    
    # Extra columns in archive (OK, keep for historical data)
    extra = set(archive_map.keys()) - set(source_map.keys())
    for col_name in extra:
        differences.append(SchemaDifference(
            type='EXTRA_COLUMN',
            object_name=col_name,
            expected=None,
            actual=archive_map[col_name],
            severity='WARNING'
        ))
    
    # Column type mismatches
    common = set(source_map.keys()) & set(archive_map.keys())
    for col_name in common:
        src_col = source_map[col_name]
        arc_col = archive_map[col_name]
        
        if src_col.data_type != arc_col.data_type:
            differences.append(SchemaDifference(
                type='COLUMN_TYPE_MISMATCH',
                object_name=col_name,
                expected=src_col.data_type,
                actual=arc_col.data_type,
                severity='ERROR'
            ))
    
    return differences
```

### Phase 3: Automatic Remediation (Optional)

```python
def auto_fix_schema_differences(differences: List[SchemaDifference], auto_fix: bool = False):
    """
    Generate and optionally apply SQL to fix schema differences
    """
    fix_sql = []
    
    for diff in differences:
        if diff.type == 'MISSING_COLUMN':
            col = diff.expected
            sql = f"""
                ALTER TABLE {diff.table_name}
                ADD COLUMN {col.name} {col.data_type}
                {'NOT NULL' if not col.nullable else ''}
                {f'DEFAULT {col.default}' if col.default else ''}
            """
            fix_sql.append(sql)
        
        elif diff.type == 'COLUMN_TYPE_MISMATCH':
            sql = f"""
                ALTER TABLE {diff.table_name}
                ALTER COLUMN {diff.object_name}
                TYPE {diff.expected}
            """
            fix_sql.append(sql)
    
    if auto_fix:
        with connections['archive'].cursor() as cursor:
            for sql in fix_sql:
                logger.info(f"Applying fix: {sql}")
                cursor.execute(sql)
    else:
        # Just return SQL for manual review
        return fix_sql
```

## 7.2 Tradeoff Analysis

| Approach | Pros | Cons | Use When |
|----------|------|------|----------|
| **Django Migrations Only** | • Native Django<br>• Version controlled<br>• Tested | • Requires migration files<br>• May not exist for old models | Development environment |
| **Schema Diffing Only** | • Works without migrations<br>• Catches all differences | • Complex implementation<br>• May miss subtle changes | Production catch-all |
| **pg_dump/restore** | • Complete and accurate<br>• Handles all PG features | • Slow<br>• Requires downtime | Initial setup only |
| **Manual SQL** | • Full control<br>• Handles edge cases | • Error-prone<br>• Not automated | One-off fixes |
| **Hybrid (Recommended)** | • Best of both worlds<br>• Automated + validated | • More complex | Production systems |



---

# 8. Archival Algorithm

## 8.1 Core Archival Algorithm

```python
def archive_model(
    model: Type[models.Model],
    queryset: QuerySet,
    batch_size: int,
    order_by: str = 'pk'
) -> List[Any]:
    """
    Archive rows from a model
    
    Returns: List of archived PKs
    """
    archived_pks = []
    last_pk = None
    
    # Try to resume from checkpoint
    checkpoint = load_checkpoint(job_id, model)
    if checkpoint and checkpoint.phase == 'COPYING':
        last_pk = checkpoint.last_processed_pk
        archived_pks = load_archived_pks(checkpoint)
        logger.info(f"Resuming from PK {last_pk}")
    
    # Keyset pagination for efficiency
    while True:
        # Build batch query
        batch_query = queryset.order_by(order_by)
        
        if last_pk is not None:
            batch_query = batch_query.filter(**{f'{order_by}__gt': last_pk})
        
        # Fetch batch
        batch = list(batch_query[:batch_size])
        
        if not batch:
            break
        
        # Extract PKs
        batch_pks = [getattr(obj, order_by) for obj in batch]
        
        # Copy to archive DB
        copy_batch_to_archive(model, batch)
        
        # Update tracking
        archived_pks.extend(batch_pks)
        last_pk = batch_pks[-1]
        
        # Save checkpoint
        save_checkpoint(
            job_id=job_id,
            model=model,
            phase='COPYING',
            last_processed_pk=last_pk,
            rows_processed=len(archived_pks)
        )
        
        # Audit log
        log_audit_event(
            job_id=job_id,
            model=model,
            operation='COPY',
            row_count=len(batch)
        )
        
        # Rate limiting (prevent production impact)
        time.sleep(batch_delay_seconds)
    
    # Mark copying complete
    save_checkpoint(
        job_id=job_id,
        model=model,
        phase='COPIED',
        rows_processed=len(archived_pks)
    )
    
    return archived_pks


def copy_batch_to_archive(model: Type[models.Model], objects: List[models.Model]):
    """
    Copy a batch of objects to archive database
    
    Strategies:
    1. Django ORM bulk_create (simple, portable)
    2. PostgreSQL COPY (fastest, PG-specific)
    3. Raw INSERT with multi-row VALUES (middle ground)
    """
    
    # Strategy 1: Django ORM (Default, safest)
    if COPY_STRATEGY == 'orm':
        # Create new instances for archive DB
        archive_objects = [
            model(**{f.name: getattr(obj, f.name) for f in model._meta.fields})
            for obj in objects
        ]
        
        model.objects.using('archive').bulk_create(
            archive_objects,
            batch_size=batch_size,
            ignore_conflicts=True  # Idempotent
        )
    
    # Strategy 2: PostgreSQL COPY (Fastest)
    elif COPY_STRATEGY == 'copy':
        copy_via_postgresql_copy(model, objects)
    
    # Strategy 3: Raw SQL with multi-row INSERT
    elif COPY_STRATEGY == 'raw_sql':
        copy_via_raw_sql(model, objects)


def copy_via_postgresql_copy(model: Type[models.Model], objects: List[models.Model]):
    """
    Use PostgreSQL COPY for maximum performance
    
    COPY is 5-10x faster than INSERT for bulk operations
    """
    import io
    import csv
    
    # Prepare CSV data in memory
    output = io.StringIO()
    writer = csv.writer(output, delimiter='\t')
    
    fields = model._meta.fields
    
    for obj in objects:
        row = []
        for field in fields:
            value = getattr(obj, field.name)
            
            # Handle special types
            if value is None:
                row.append('\\N')  # PostgreSQL NULL
            elif isinstance(field, DateTimeField):
                row.append(value.isoformat())
            elif isinstance(field, JSONField):
                row.append(json.dumps(value))
            else:
                row.append(str(value))
        
        writer.writerow(row)
    
    # Rewind buffer
    output.seek(0)
    
    # Execute COPY
    table_name = model._meta.db_table
    columns = [f.column for f in fields]
    
    with connections['archive'].cursor() as cursor:
        cursor.copy_from(
            file=output,
            table=table_name,
            columns=columns,
            sep='\t',
            null='\\N'
        )


def copy_via_raw_sql(model: Type[models.Model], objects: List[models.Model]):
    """
    Raw SQL with multi-row INSERT
    
    Example:
    INSERT INTO table (col1, col2)
    VALUES (val1, val2), (val3, val4), ...
    ON CONFLICT DO NOTHING;  -- Idempotency
    """
    if not objects:
        return
    
    fields = [f for f in model._meta.fields]
    field_names = [f.column for f in fields]
    table_name = model._meta.db_table
    
    # Build VALUES clause
    values_rows = []
    params = []
    
    for obj in objects:
        placeholders = []
        for field in fields:
            value = getattr(obj, field.name)
            params.append(value)
            placeholders.append('%s')
        values_rows.append(f"({','.join(placeholders)})")
    
    # Build complete query
    sql = f"""
        INSERT INTO {table_name} ({','.join(field_names)})
        VALUES {','.join(values_rows)}
        ON CONFLICT DO NOTHING
    """
    
    with connections['archive'].cursor() as cursor:
        cursor.execute(sql, params)
```

## 8.2 Performance Optimizations

### Batch Size Tuning

```python
def calculate_adaptive_batch_size(model: Type[models.Model]) -> int:
    """
    Calculate optimal batch size based on:
    - Row size
    - Available memory
    - Target latency
    """
    # Estimate average row size
    sample_objects = model.objects.all()[:100]
    if not sample_objects:
        return 1000  # Default
    
    total_size = 0
    for obj in sample_objects:
        # Rough estimate of serialized size
        row_size = sum(
            len(str(getattr(obj, f.name) or ''))
            for f in model._meta.fields
        )
        total_size += row_size
    
    avg_row_size = total_size / len(sample_objects)
    
    # Target: ~10MB per batch
    target_batch_bytes = 10 * 1024 * 1024
    optimal_batch_size = int(target_batch_bytes / avg_row_size)
    
    # Bounds: 100 - 10000
    return max(100, min(10000, optimal_batch_size))
```

### Parallel Workers

```python
def archive_model_parallel(
    model: Type[models.Model],
    queryset: QuerySet,
    num_workers: int = 4
) -> List[Any]:
    """
    Archive using multiple parallel workers
    
    Strategy: Partition PK space across workers
    """
    # Get PK range
    pk_min = queryset.aggregate(min_pk=Min('pk'))['min_pk']
    pk_max = queryset.aggregate(max_pk=Max('pk'))['max_pk']
    
    if pk_min is None or pk_max is None:
        return []
    
    # Partition PK space
    pk_range = pk_max - pk_min
    partition_size = pk_range // num_workers
    
    partitions = [
        (pk_min + i * partition_size, pk_min + (i + 1) * partition_size)
        for i in range(num_workers)
    ]
    partitions[-1] = (partitions[-1][0], pk_max)  # Adjust last partition
    
    # Launch workers
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for start_pk, end_pk in partitions:
            partition_qs = queryset.filter(pk__gte=start_pk, pk__lte=end_pk)
            future = executor.submit(archive_model, model, partition_qs, batch_size)
            futures.append(future)
        
        # Wait for completion
        results = [f.result() for f in as_completed(futures)]
    
    # Combine results
    all_pks = []
    for pks in results:
        all_pks.extend(pks)
    
    return all_pks
```

### Connection Pooling

```python
# settings.py
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'source_db',
        'CONN_MAX_AGE': 600,  # Connection pooling
        'OPTIONS': {
            'connect_timeout': 10,
        }
    },
    'archive': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'archive_db',
        'CONN_MAX_AGE': 600,
        'OPTIONS': {
            'connect_timeout': 10,
        }
    }
}
```

### Index Optimization

```python
def optimize_archive_for_bulk_insert():
    """
    Temporarily disable indexes and constraints during bulk load
    
    WARNING: Use with caution, validation required after
    """
    with connections['archive'].cursor() as cursor:
        # Drop indexes
        cursor.execute("""
            SELECT 'DROP INDEX ' || indexname || ';'
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND indexname LIKE 'idx_%'
        """)
        drop_commands = [row[0] for row in cursor.fetchall()]
        
        for cmd in drop_commands:
            cursor.execute(cmd)
        
        # Disable triggers
        cursor.execute("SET session_replication_role = replica;")
        
        yield  # Execute bulk operations
        
        # Re-enable triggers
        cursor.execute("SET session_replication_role = DEFAULT;")
        
        # Recreate indexes (in parallel if possible)
        cursor.execute("""
            SELECT indexdef || ';'
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND indexname LIKE 'idx_%'
        """)
        create_commands = [row[0] for row in cursor.fetchall()]
        
        for cmd in create_commands:
            # Add CONCURRENTLY for non-blocking
            cmd = cmd.replace('CREATE INDEX', 'CREATE INDEX CONCURRENTLY')
            cursor.execute(cmd)
```

---

# 9. Deletion Algorithm

## 9.1 Core Deletion Algorithm

```python
def delete_model(
    model: Type[models.Model],
    pks: List[Any],
    batch_size: int = 100
) -> int:
    """
    Delete archived rows from source database
    
    Returns: Number of rows deleted
    """
    deleted_count = 0
    
    # Resume from checkpoint if exists
    checkpoint = load_checkpoint(job_id, model)
    if checkpoint and checkpoint.phase == 'DELETING':
        # Filter out already-deleted PKs
        completed_pks = load_deleted_pks(checkpoint)
        pks = [pk for pk in pks if pk not in completed_pks]
        logger.info(f"Resuming deletion, {len(pks)} remaining")
    
    # Pre-deletion validation
    validate_safe_to_delete(model, pks)
    
    # Mark phase
    save_checkpoint(
        job_id=job_id,
        model=model,
        phase='DELETING',
        rows_total=len(pks)
    )
    
    # Delete in batches
    for batch_pks in chunk_list(pks, batch_size):
        try:
            # Delete batch
            deleted = delete_batch_with_retry(model, batch_pks)
            deleted_count += deleted
            
            # Checkpoint progress
            save_checkpoint(
                job_id=job_id,
                model=model,
                phase='DELETING',
                last_processed_pk=batch_pks[-1],
                rows_processed=deleted_count
            )
            
            # Audit
            log_audit_event(
                job_id=job_id,
                model=model,
                operation='DELETE',
                row_count=deleted
            )
            
            # Rate limiting
            time.sleep(delete_delay_seconds)
            
        except Exception as e:
            logger.error(f"Deletion batch failed: {e}", exc_info=True)
            # Continue with next batch (mark failed batch)
            save_failed_batch(job_id, model, batch_pks, str(e))
    
    # Mark complete
    save_checkpoint(
        job_id=job_id,
        model=model,
        phase='DELETED',
        rows_processed=deleted_count
    )
    
    return deleted_count


def delete_batch_with_retry(
    model: Type[models.Model],
    pks: List[Any],
    max_retries: int = 3
) -> int:
    """
    Delete batch with exponential backoff retry
    """
    # Sort PKs for consistent lock order (prevents deadlocks)
    sorted_pks = sorted(pks)
    
    for attempt in range(max_retries):
        try:
            with transaction.atomic(using='default'):
                # Lock rows for deletion
                queryset = model.objects.select_for_update().filter(pk__in=sorted_pks)
                count = queryset.count()
                
                # Delete
                queryset.delete()
                
                return count
                
        except OperationalError as e:
            if 'deadlock detected' in str(e).lower():
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    delay = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Deadlock detected, retrying in {delay}s")
                    time.sleep(delay)
                    continue
            raise
    
    raise Exception(f"Failed to delete batch after {max_retries} attempts")


def validate_safe_to_delete(model: Type[models.Model], pks: List[Any]):
    """
    Pre-deletion safety checks
    
    Ensures:
    1. All rows exist in archive
    2. All dependent rows are already archived
    3. No new dependencies were created since archival
    """
    # Check existence in archive
    archive_count = model.objects.using('archive').filter(pk__in=pks).count()
    if archive_count != len(pks):
        raise ValidationException(
            f"Not all rows exist in archive. Expected {len(pks)}, found {archive_count}"
        )
    
    # Check dependent models
    for relation in get_incoming_foreign_keys(model):
        child_model = relation.model
        fk_field = relation.field
        
        # Find unarchived children
        unarchived = child_model.objects.using('default').filter(
            **{f'{fk_field}__in': pks}
        ).exclude(
            pk__in=Subquery(
                child_model.objects.using('archive').values('pk')
            )
        )
        
        if unarchived.exists():
            sample = list(unarchived[:5].values_list('pk', flat=True))
            raise DependencyException(
                f"Cannot delete {model.__name__}. "
                f"Unarchived {child_model.__name__} rows exist: {sample}"
            )
```

## 9.2 Deletion Strategies

### Strategy 1: Batch DELETE (Default)

```python
def delete_via_batched_delete(model, pks, batch_size=100):
    """
    Standard approach: DELETE in small batches
    
    Pros: Safe, controllable, can checkpoint
    Cons: Slower, generates WAL, requires VACUUM
    """
    for batch in chunk_list(pks, batch_size):
        model.objects.filter(pk__in=batch).delete()
        time.sleep(0.01)  # Yield to other transactions
```

### Strategy 2: Partition Drop (Fastest)

```python
def delete_via_partition_drop(model, partition_criteria):
    """
    Drop entire partition
    
    Requirements:
    - Table must be partitioned
    - All rows in partition must be archived
    
    Pros: Instant (metadata operation), no bloat
    Cons: All-or-nothing, requires partitioning setup
    """
    table_name = model._meta.db_table
    partition_name = f"{table_name}_{partition_criteria}"
    
    with connections['default'].cursor() as cursor:
        # Verify partition is fully archived
        cursor.execute(f"""
            SELECT COUNT(*) FROM {partition_name}
        """)
        source_count = cursor.fetchone()[0]
        
        cursor.execute(f"""
            SELECT COUNT(*) FROM {partition_name}
        """, using='archive')
        archive_count = cursor.fetchone()[0]
        
        if source_count != archive_count:
            raise ValidationException("Partition not fully archived")
        
        # Detach partition
        cursor.execute(f"""
            ALTER TABLE {table_name}
            DETACH PARTITION {partition_name}
        """)
        
        # Drop partition
        cursor.execute(f"DROP TABLE {partition_name}")
        
        logger.info(f"Dropped partition {partition_name} ({source_count} rows)")
```

### Strategy 3: Soft Delete

```python
def soft_delete(model, pks):
    """
    Mark as archived without physical deletion
    
    Pros: Fast, reversible, no FK issues
    Cons: Doesn't reduce storage, needs app-level filtering
    """
    # Add archived_at field to model (migration required)
    model.objects.filter(pk__in=pks).update(
        archived_at=timezone.now(),
        is_archived=True
    )
    
    # Update default manager to filter archived rows
    class DefaultManager(models.Manager):
        def get_queryset(self):
            return super().get_queryset().filter(is_archived=False)
    
    model.objects = DefaultManager()
```

### Strategy 4: TRUNCATE (Special Case)

```python
def delete_via_truncate(model):
    """
    TRUNCATE table (only if archiving ALL rows)
    
    Pros: Fastest for complete table
    Cons: All-or-nothing, resets sequences, no MVCC
    """
    table_name = model._meta.db_table
    
    with connections['default'].cursor() as cursor:
        # Disable FK checks temporarily
        cursor.execute("SET CONSTRAINTS ALL DEFERRED")
        
        # Truncate
        cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
        
        logger.info(f"Truncated table {table_name}")
```

## 9.3 Managing Database Bloat

### Automatic VACUUM

```python
def vacuum_after_deletion(model: Type[models.Model], deleted_count: int):
    """
    Run VACUUM to reclaim space after deletion
    """
    table_name = model._meta.db_table
    
    # Only vacuum if significant deletion
    if deleted_count < 10000:
        return
    
    with connections['default'].cursor() as cursor:
        # VACUUM ANALYZE reclaims space and updates statistics
        cursor.execute(f"VACUUM ANALYZE {table_name}")
        
        logger.info(f"Vacuumed {table_name} after deleting {deleted_count} rows")


def configure_autovacuum(model: Type[models.Model]):
    """
    Configure aggressive autovacuum for frequently archived tables
    """
    table_name = model._meta.db_table
    
    with connections['default'].cursor() as cursor:
        cursor.execute(f"""
            ALTER TABLE {table_name} SET (
                autovacuum_vacuum_scale_factor = 0.05,  -- More aggressive
                autovacuum_vacuum_threshold = 1000,
                autovacuum_analyze_scale_factor = 0.02,
                autovacuum_analyze_threshold = 500
            )
        """)
```

### Monitor Table Bloat

```python
def calculate_table_bloat(model: Type[models.Model]) -> float:
    """
    Calculate table bloat percentage
    """
    table_name = model._meta.db_table
    
    query = """
        SELECT
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
            pg_total_relation_size(schemaname||'.'||tablename) AS size_bytes,
            (CASE WHEN pg_total_relation_size(schemaname||'.'||tablename) > 0
                THEN (pg_total_relation_size(schemaname||'.'||tablename) - 
                      pg_relation_size(schemaname||'.'||tablename))::float / 
                     pg_total_relation_size(schemaname||'.'||tablename) * 100
                ELSE 0
            END) AS bloat_pct
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename = %s
    """
    
    with connections['default'].cursor() as cursor:
        cursor.execute(query, [table_name])
        row = cursor.fetchone()
        
        if row:
            bloat_pct = row[4]
            logger.info(f"Table {table_name} bloat: {bloat_pct:.2f}%")
            return bloat_pct
    
    return 0.0
```

## 9.4 Deletion Safety Checklist

```python
class DeletionSafetyCheck:
    """
    Comprehensive safety checks before deletion
    """
    
    def __init__(self, model: Type[models.Model], pks: List[Any]):
        self.model = model
        self.pks = pks
        self.checks_passed = []
        self.checks_failed = []
    
    def run_all_checks(self) -> bool:
        """Run all safety checks"""
        self.check_archived()
        self.check_dependencies()
        self.check_no_concurrent_modifications()
        self.check_backup_exists()
        
        return len(self.checks_failed) == 0
    
    def check_archived(self):
        """Verify all rows exist in archive"""
        archive_count = self.model.objects.using('archive').filter(
            pk__in=self.pks
        ).count()
        
        if archive_count == len(self.pks):
            self.checks_passed.append('All rows archived')
        else:
            self.checks_failed.append(
                f'Only {archive_count}/{len(self.pks)} rows in archive'
            )
    
    def check_dependencies(self):
        """Check for unarchived dependent rows"""
        for relation in get_incoming_foreign_keys(self.model):
            child_model = relation.model
            fk_field = relation.field
            
            unarchived_count = child_model.objects.using('default').filter(
                **{f'{fk_field}__in': self.pks}
            ).count()
            
            if unarchived_count > 0:
                self.checks_failed.append(
                    f'{unarchived_count} unarchived {child_model.__name__} rows'
                )
    
    def check_no_concurrent_modifications(self):
        """Ensure rows haven't been modified since archival"""
        # Compare updated_at timestamps
        if hasattr(self.model, 'updated_at'):
            source_latest = self.model.objects.using('default').filter(
                pk__in=self.pks
            ).aggregate(Max('updated_at'))['updated_at__max']
            
            archive_latest = self.model.objects.using('archive').filter(
                pk__in=self.pks
            ).aggregate(Max('updated_at'))['updated_at__max']
            
            if source_latest and archive_latest and source_latest > archive_latest:
                self.checks_failed.append(
                    f'Rows modified after archival (source: {source_latest}, '
                    f'archive: {archive_latest})'
                )
    
    def check_backup_exists(self):
        """Verify archive DB backup is recent"""
        # Implementation depends on backup strategy
        pass
```



---

# 10. Failure Recovery

## 10.1 Failure Scenarios and Recovery Strategies

### Scenario 1: Process Crash During Copy

**Symptoms:** Job status = 'RUNNING', checkpoint phase = 'COPYING'

**Recovery Strategy:**
```python
def recover_from_copy_failure(job_id: UUID):
    """
    Resume copying from last checkpoint
    """
    job = ArchiveJob.objects.get(id=job_id)
    checkpoints = ArchiveCheckpoint.objects.filter(
        job_id=job_id,
        phase='COPYING'
    )
    
    for checkpoint in checkpoints:
        model = get_model(checkpoint.model_name)
        
        # Resume from last processed PK
        logger.info(
            f"Resuming {model.__name__} from PK {checkpoint.last_processed_pk}"
        )
        
        # Get original queryset from job config
        config = load_config_from_job(job)
        retention = config.get_retention_policy(model)
        queryset = model.objects.using('default').filter(retention.as_q())
        
        # Filter to unprocessed rows
        if checkpoint.last_processed_pk:
            queryset = queryset.filter(pk__gt=checkpoint.last_processed_pk)
        
        # Continue archival
        archive_model(model, queryset, config.batch_size)
```

**Key Points:**
- Idempotent copy (use ON CONFLICT DO NOTHING)
- No data loss
- May have duplicate copy attempts (handled by idempotency)

---

### Scenario 2: Crash During Deletion

**Symptoms:** checkpoint phase = 'DELETING'

**Recovery Strategy:**
```python
def recover_from_delete_failure(job_id: UUID):
    """
    Resume deletion from checkpoint
    
    Requires careful handling to avoid deleting unarchived data
    """
    checkpoints = ArchiveCheckpoint.objects.filter(
        job_id=job_id,
        phase='DELETING'
    )
    
    for checkpoint in checkpoints:
        model = get_model(checkpoint.model_name)
        
        # Get list of PKs that were supposed to be deleted
        archived_pks = load_archived_pks_for_checkpoint(checkpoint)
        
        # Find which ones still exist in source
        remaining_pks = model.objects.using('default').filter(
            pk__in=archived_pks
        ).values_list('pk', flat=True)
        
        if remaining_pks:
            logger.info(f"Resuming deletion of {len(remaining_pks)} rows")
            
            # Re-validate before deletion
            validate_safe_to_delete(model, list(remaining_pks))
            
            # Continue deletion
            delete_model(model, list(remaining_pks), batch_size=100)
```

**Key Points:**
- Always re-validate before deleting
- Check archive integrity
- May have partially deleted batches

---

### Scenario 3: Database Connection Loss

**Symptoms:** OperationalError, connection timeout

**Recovery Strategy:**
```python
def handle_connection_loss(operation_func, *args, **kwargs):
    """
    Retry wrapper for database operations
    """
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            return operation_func(*args, **kwargs)
        except OperationalError as e:
            if 'connection' in str(e).lower() and attempt < max_retries - 1:
                logger.warning(
                    f"Connection lost, retrying in {retry_delay}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                
                # Close existing connections
                connections['default'].close()
                connections['archive'].close()
                
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            raise
    
    raise Exception("Failed after maximum retries")
```

---

### Scenario 4: Archive Database Full

**Symptoms:** DiskFullError, "no space left on device"

**Recovery Strategy:**
```python
def handle_archive_db_full(job_id: UUID):
    """
    1. Pause archival
    2. Alert operators
    3. Wait for space to be freed
    4. Resume automatically or manually
    """
    # Mark job as suspended
    ArchiveJob.objects.filter(id=job_id).update(
        status='SUSPENDED',
        error_message='Archive database storage full'
    )
    
    # Send alert
    send_alert(
        severity='CRITICAL',
        message=f'Archive job {job_id} suspended: database storage full',
        job_id=job_id
    )
    
    # Optional: Implement auto-retry with backoff
    while True:
        time.sleep(300)  # Check every 5 minutes
        
        if check_archive_db_space() > MIN_FREE_SPACE_GB:
            logger.info("Archive DB space available, resuming job")
            resume_archival(job_id)
            break
```

---

### Scenario 5: Schema Mismatch Detected

**Symptoms:** Column not found, type mismatch errors

**Recovery Strategy:**
```python
def handle_schema_mismatch(job_id: UUID, error: Exception):
    """
    1. Pause job
    2. Re-sync schema
    3. Resume job
    """
    logger.error(f"Schema mismatch detected: {error}")
    
    # Pause job
    ArchiveJob.objects.filter(id=job_id).update(status='PAUSED')
    
    # Re-sync schema
    config = load_config_from_job(job_id)
    graph = DependencyResolver(config.root_models).build_graph()
    
    schema_replicator = SchemaReplicator('default', 'archive')
    schema_replicator.sync_schema(graph.topological_sort())
    
    # Resume job
    resume_archival(job_id)
```

---

## 10.2 Idempotency Guarantees

### Copy Idempotency

```sql
-- PostgreSQL: Use ON CONFLICT
INSERT INTO archive_table (id, col1, col2, ...)
VALUES (...)
ON CONFLICT (id) DO NOTHING;

-- Or for updates:
ON CONFLICT (id) DO UPDATE
SET col1 = EXCLUDED.col1,
    col2 = EXCLUDED.col2,
    updated_at = NOW();
```

```python
# Django ORM
model.objects.using('archive').bulk_create(
    objects,
    ignore_conflicts=True  # Idempotent
)
```

### Delete Idempotency

```python
# Deletes are naturally idempotent
# Deleting non-existent rows is a no-op
model.objects.filter(pk__in=pks).delete()  # Returns 0 if already deleted
```

---

## 10.3 Audit Trail for Recovery

```python
class AuditTrail:
    """
    Comprehensive audit logging for recovery and debugging
    """
    
    @staticmethod
    def log_copy(job_id: UUID, model: Type[models.Model], pks: List[Any], duration_ms: int):
        ArchiveAudit.objects.create(
            job_id=job_id,
            model_name=model.__name__,
            operation='COPY',
            row_count=len(pks),
            duration_ms=duration_ms,
            metadata={
                'first_pk': pks[0] if pks else None,
                'last_pk': pks[-1] if pks else None,
            }
        )
    
    @staticmethod
    def log_delete(job_id: UUID, model: Type[models.Model], pks: List[Any], duration_ms: int):
        ArchiveAudit.objects.create(
            job_id=job_id,
            model_name=model.__name__,
            operation='DELETE',
            row_count=len(pks),
            duration_ms=duration_ms,
            metadata={
                'first_pk': pks[0] if pks else None,
                'last_pk': pks[-1] if pks else None,
            }
        )
    
    @staticmethod
    def log_error(job_id: UUID, model: Type[models.Model], error: Exception, context: Dict):
        ArchiveAudit.objects.create(
            job_id=job_id,
            model_name=model.__name__ if model else 'UNKNOWN',
            operation='ERROR',
            row_count=0,
            metadata={
                'error_type': type(error).__name__,
                'error_message': str(error),
                'context': context,
                'traceback': traceback.format_exc()
            }
        )
```

---

# 11. Scaling Strategy

## 11.1 Vertical Scaling

### Database Tuning

```python
# PostgreSQL configuration for archival workload

# Connection settings
max_connections = 200
shared_buffers = '4GB'  # 25% of RAM
effective_cache_size = '12GB'  # 75% of RAM

# Write performance
wal_buffers = '16MB'
checkpoint_completion_target = 0.9
max_wal_size = '4GB'
min_wal_size = '1GB'

# Maintenance
maintenance_work_mem = '1GB'
autovacuum_max_workers = 6
autovacuum_naptime = '10s'

# Parallel query
max_parallel_workers_per_gather = 4
max_parallel_workers = 8
```

### Batch Size Optimization

```python
def benchmark_batch_sizes(model: Type[models.Model]) -> Dict[int, float]:
    """
    Benchmark different batch sizes to find optimal
    """
    batch_sizes = [100, 500, 1000, 2000, 5000, 10000]
    results = {}
    
    sample_qs = model.objects.all()[:10000]
    
    for batch_size in batch_sizes:
        start = time.time()
        
        # Measure copy performance
        for batch in chunk_queryset(sample_qs, batch_size):
            copy_batch_to_archive(model, list(batch))
        
        duration = time.time() - start
        throughput = 10000 / duration  # rows/second
        
        results[batch_size] = throughput
        logger.info(f"Batch size {batch_size}: {throughput:.0f} rows/sec")
    
    # Return best batch size
    optimal = max(results, key=results.get)
    return optimal
```

---

## 11.2 Horizontal Scaling

### Multiple Workers

```python
class DistributedArchiveExecutor:
    """
    Coordinate multiple workers for parallel archival
    """
    
    def __init__(self, num_workers: int = 4):
        self.num_workers = num_workers
        self.lock_manager = PostgresAdvisoryLock()
    
    def execute_distributed(self, config: ArchiveConfig):
        """
        Launch multiple workers, each processing different models
        """
        graph = DependencyResolver(config.root_models).build_graph()
        models = graph.topological_sort()
        
        # Partition models across workers
        # Models with dependencies must be processed in order
        # But independent models can be parallel
        
        levels = self._group_by_dependency_level(graph)
        
        for level_models in levels:
            # Process this level in parallel
            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                futures = []
                
                for model in level_models:
                    # Acquire lock for this model
                    lock_acquired = self.lock_manager.try_acquire(
                        f'archive_{model.__name__}'
                    )
                    
                    if lock_acquired:
                        future = executor.submit(
                            self._archive_model_worker,
                            model,
                            config
                        )
                        futures.append(future)
                
                # Wait for level to complete
                for future in as_completed(futures):
                    future.result()
    
    def _group_by_dependency_level(self, graph: DependencyGraph) -> List[List[Type[models.Model]]]:
        """
        Group models by dependency level for parallel processing
        """
        levels = []
        processed = set()
        
        while len(processed) < len(graph.nodes):
            current_level = []
            
            for model in graph.nodes:
                if model in processed:
                    continue
                
                # Check if all dependencies are processed
                deps = graph.get_dependencies(model)
                if all(dep in processed for dep in deps):
                    current_level.append(model)
            
            if not current_level:
                break
            
            levels.append(current_level)
            processed.update(current_level)
        
        return levels


class PostgresAdvisoryLock:
    """
    Distributed locking using PostgreSQL advisory locks
    """
    
    def try_acquire(self, lock_name: str, timeout: int = 0) -> bool:
        """
        Try to acquire advisory lock
        """
        # Convert lock name to integer hash
        lock_id = hash(lock_name) % (2**31)
        
        with connections['default'].cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(%s)",
                [lock_id]
            )
            acquired = cursor.fetchone()[0]
            
            return acquired
    
    def release(self, lock_name: str):
        """Release advisory lock"""
        lock_id = hash(lock_name) % (2**31)
        
        with connections['default'].cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_unlock(%s)",
                [lock_id]
            )
```

---

## 11.3 Partition-Aware Archival

```python
def archive_partitioned_table(model: Type[models.Model], config: ArchiveConfig):
    """
    Optimize archival for partitioned tables
    
    Strategy: Process one partition at a time
    """
    table_name = model._meta.db_table
    
    # Get list of partitions
    partitions = get_table_partitions(table_name)
    
    for partition in partitions:
        # Check if partition qualifies for archival
        if should_archive_partition(partition, config):
            logger.info(f"Archiving partition {partition.name}")
            
            # Option 1: Archive then drop (fastest)
            if config.use_partition_drop:
                archive_partition_then_drop(model, partition)
            
            # Option 2: Standard archival
            else:
                archive_partition_standard(model, partition, config)


def get_table_partitions(table_name: str) -> List[PartitionInfo]:
    """
    Query PostgreSQL catalog for partition information
    """
    query = """
        SELECT
            c.relname AS partition_name,
            pg_get_expr(c.relpartbound, c.oid) AS partition_expr
        FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = %s
          AND c.relkind = 'r'
        ORDER BY c.relname
    """
    
    with connections['default'].cursor() as cursor:
        cursor.execute(query, [table_name])
        
        partitions = []
        for row in cursor.fetchall():
            partitions.append(PartitionInfo(
                name=row[0],
                expression=row[1]
            ))
        
        return partitions
```

---

# 12. Configuration Examples

## 12.1 Simple Example: Archive Old Orders

```python
from django_archival import ArchiveDefinition, ArchivalStrategy, RetentionPolicy
from myapp.models import Order
from datetime import datetime, timedelta

class OldOrdersArchive(ArchiveDefinition):
    name = "old_orders_archive"
    
    # Archive orders older than 3 years
    root_models = [Order]
    strategy = ArchivalStrategy.OBJECT_GRAPH
    
    retention = RetentionPolicy(
        created_before=datetime.now() - timedelta(days=365*3),
        status_in=['COMPLETED', 'CANCELLED']
    )
    
    # Performance settings
    batch_size = 1000
    max_workers = 4
    
    # Archive related data automatically
    # Order -> OrderItem -> Payment -> Shipment

# Run archival
from django_archival import ArchiveJobRunner

runner = ArchiveJobRunner(OldOrdersArchive())
result = runner.run()

print(f"Archived {result.total_rows_archived} rows in {result.duration_seconds}s")
```

---

## 12.2 Complex Example: Tenant-Specific Archival

```python
class TenantArchive(ArchiveDefinition):
    name = "tenant_specific_archive"
    root_models = [Customer, Order, Invoice]
    strategy = ArchivalStrategy.SELECTIVE
    
    # Override retention per model
    def get_retention_policy(self, model):
        if model == Customer:
            # Don't archive customers, only copy
            return RetentionPolicy(
                custom_filter=Q(tenant_id=self.tenant_id, is_deleted=True)
            )
        elif model == Order:
            # Archive old completed orders
            return RetentionPolicy(
                created_before=datetime.now() - timedelta(days=730),
                status_in=['COMPLETED'],
                custom_filter=Q(tenant_id=self.tenant_id)
            )
        elif model == Invoice:
            # Archive invoices linked to archived orders
            return RetentionPolicy(
                custom_filter=Q(
                    order_id__in=Subquery(
                        Order.objects.using('archive').values('id')
                    ),
                    tenant_id=self.tenant_id
                )
            )
    
    def should_archive_model(self, model):
        # Skip archiving master data
        if model in [Product, Merchant, Category]:
            return False
        return True
    
    # Callbacks
    def before_archive(self, model, queryset):
        logger.info(f"Starting archival of {model.__name__}: {queryset.count()} rows")
    
    def after_delete(self, model, pks):
        # Send notification
        send_slack_notification(
            f"Archived and deleted {len(pks)} {model.__name__} rows"
        )

# Run per tenant
for tenant_id in get_active_tenants():
    archive = TenantArchive()
    archive.tenant_id = tenant_id
    
    runner = ArchiveJobRunner(archive)
    runner.run()
```

---

## 12.3 Advanced Example: With Plugins

```python
from django_archival.plugins import EncryptionPlugin, S3BackupPlugin, NotificationPlugin

class SecureArchive(ArchiveDefinition):
    name = "secure_pii_archive"
    root_models = [UserProfile, PaymentMethod]
    strategy = ArchivalStrategy.MOVE
    
    retention = RetentionPolicy(
        modified_before=datetime.now() - timedelta(days=365*7)  # 7 years
    )
    
    # Enable plugins
    plugins = [
        # Encrypt PII before archiving
        EncryptionPlugin(
            fields_to_encrypt=['ssn', 'credit_card', 'bank_account']
        ),
        
        # Export to S3 for long-term storage
        S3BackupPlugin(
            bucket='company-archive',
            prefix='user-data'
        ),
        
        # Send notifications
        NotificationPlugin(
            webhook_url='https://hooks.slack.com/services/...'
        )
    ]
    
    # Strict validation
    validation_level = ValidationLevel.FULL
    validation_sample_rate = 1.0  # Validate 100%
    
    def on_error(self, error, context):
        # Alert security team on failure
        send_pagerduty_alert(
            severity='high',
            message=f"Secure archive failed: {error}",
            context=context
        )
```

---

## 12.4 Celery Integration

```python
from celery import shared_task

@shared_task
def run_nightly_archival():
    """
    Celery task for scheduled archival
    """
    from myapp.archives import OldOrdersArchive
    from django_archival import ArchiveJobRunner
    
    runner = ArchiveJobRunner(OldOrdersArchive())
    
    try:
        result = runner.run()
        
        return {
            'status': 'SUCCESS',
            'rows_archived': result.total_rows_archived,
            'duration': result.duration_seconds
        }
    except Exception as e:
        logger.error(f"Archival failed: {e}", exc_info=True)
        raise

# Schedule in Celery Beat
from celery.schedules import crontab

CELERY_BEAT_SCHEDULE = {
    'nightly-archival': {
        'task': 'myapp.tasks.run_nightly_archival',
        'schedule': crontab(hour=2, minute=0),  # 2 AM daily
    }
}
```

---

## 12.5 Django Management Command

```python
# management/commands/archive_data.py

from django.core.management.base import BaseCommand
from django_archival import ArchiveJobRunner
from myapp.archives import OldOrdersArchive

class Command(BaseCommand):
    help = 'Archive old data'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Validate configuration without archiving'
        )
        parser.add_argument(
            '--resume',
            type=str,
            help='Resume interrupted job by ID'
        )
    
    def handle(self, *args, **options):
        runner = ArchiveJobRunner(OldOrdersArchive())
        
        if options['dry_run']:
            self.stdout.write("Running dry run...")
            result = runner.run(dry_run=True)
            self.stdout.write(
                self.style.SUCCESS(
                    f"Would archive {result.total_rows_archived} rows"
                )
            )
        elif options['resume']:
            self.stdout.write(f"Resuming job {options['resume']}...")
            result = runner.run(resume_job_id=options['resume'])
            self.stdout.write(
                self.style.SUCCESS(
                    f"Archived {result.total_rows_archived} rows"
                )
            )
        else:
            self.stdout.write("Starting archival...")
            result = runner.run()
            self.stdout.write(
                self.style.SUCCESS(
                    f"Successfully archived {result.total_rows_archived} rows "
                    f"in {result.duration_seconds:.2f}s"
                )
            )

# Usage:
# python manage.py archive_data
# python manage.py archive_data --dry-run
# python manage.py archive_data --resume <job-id>
```



---

# 13. Sample APIs

## 13.1 Public API Reference

### ArchiveJobRunner API

```python
class ArchiveJobRunner:
    """Main entry point for archival operations"""
    
    def __init__(
        self,
        archive_definition: ArchiveDefinition,
        source_db: str = 'default',
        archive_db: str = 'archive'
    ):
        """
        Initialize archival job runner
        
        Args:
            archive_definition: Configuration for archival
            source_db: Django database alias for source
            archive_db: Django database alias for archive
        """
    
    def run(
        self,
        dry_run: bool = False,
        resume_job_id: Optional[UUID] = None
    ) -> ArchiveJobResult:
        """
        Execute archival job
        
        Args:
            dry_run: If True, validate without executing
            resume_job_id: Resume interrupted job
        
        Returns:
            ArchiveJobResult with execution details
        
        Raises:
            ValidationException: Configuration invalid
            SchemaException: Schema mismatch
            ArchivalException: Archival failed
        """
    
    def estimate(self) -> Dict[str, Any]:
        """
        Estimate archival scope
        
        Returns:
            {
                'total_rows': int,
                'estimated_duration_seconds': float,
                'models': {
                    'ModelName': {'rows': int, 'estimated_duration': float}
                }
            }
        """
    
    def get_progress(self, job_id: Optional[UUID] = None) -> Dict[str, Any]:
        """
        Get current progress
        
        Returns:
            {
                'job_id': UUID,
                'status': str,
                'progress_pct': float,
                'models': {
                    'ModelName': {
                        'phase': str,
                        'rows_processed': int,
                        'rows_total': int
                    }
                }
            }
        """
    
    def cancel(self, job_id: Optional[UUID] = None):
        """Cancel running job"""
```

### ArchiveDefinition API

```python
class ArchiveDefinition(ABC):
    """Base class for archival configuration"""
    
    # Required configuration
    name: str  # Unique identifier for this archive
    root_models: List[Type[models.Model]]  # Starting models
    strategy: ArchivalStrategy  # MOVE, COPY, SELECTIVE, OBJECT_GRAPH
    retention: RetentionPolicy  # What data to archive
    
    # Optional configuration
    batch_size: int = 1000
    max_workers: int = 4
    validation_level: ValidationLevel = ValidationLevel.CHECKSUM
    validation_sample_rate: float = 1.0
    auto_sync_schema: bool = True
    
    # Lifecycle hooks
    def before_archive(self, model: Type[models.Model], queryset: QuerySet):
        """Called before archiving each model"""
        pass
    
    def after_archive(self, model: Type[models.Model], pks: List[Any]):
        """Called after archiving each model"""
        pass
    
    def before_delete(self, model: Type[models.Model], pks: List[Any]):
        """Called before deleting each model"""
        pass
    
    def after_delete(self, model: Type[models.Model], pks: List[Any]):
        """Called after deleting each model"""
        pass
    
    def on_error(self, error: Exception, context: Dict):
        """Called when error occurs"""
        pass
    
    # Customization hooks
    def get_retention_policy(self, model: Type[models.Model]) -> RetentionPolicy:
        """Override for model-specific retention"""
        return self.retention
    
    def should_archive_model(self, model: Type[models.Model]) -> bool:
        """Override to exclude certain models"""
        return True
    
    def get_order_by(self, model: Type[models.Model]) -> List[str]:
        """Override for custom iteration order"""
        return ['pk']
```

---

## 13.2 REST API (Optional Django Views)

```python
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response

class ArchiveJobViewSet(viewsets.ViewSet):
    """
    API endpoints for archival operations
    """
    
    @action(detail=False, methods=['post'])
    def start(self, request):
        """
        Start new archival job
        
        POST /api/archive/start/
        {
            "archive_definition": "myapp.archives.OldOrdersArchive",
            "dry_run": false
        }
        """
        definition_path = request.data.get('archive_definition')
        dry_run = request.data.get('dry_run', False)
        
        # Dynamically load definition
        definition = load_class(definition_path)
        
        # Start job
        runner = ArchiveJobRunner(definition())
        result = runner.run(dry_run=dry_run)
        
        return Response({
            'job_id': str(result.job_id),
            'status': result.status,
            'rows_archived': result.total_rows_archived
        }, status=status.HTTP_201_CREATED)
    
    @action(detail=True, methods=['get'])
    def progress(self, request, pk=None):
        """
        Get job progress
        
        GET /api/archive/{job_id}/progress/
        """
        runner = ArchiveJobRunner(None)  # Definition not needed for progress
        progress = runner.get_progress(job_id=UUID(pk))
        
        return Response(progress)
    
    @action(detail=True, methods=['post'])
    def cancel(self, request, pk=None):
        """
        Cancel running job
        
        POST /api/archive/{job_id}/cancel/
        """
        runner = ArchiveJobRunner(None)
        runner.cancel(job_id=UUID(pk))
        
        return Response({'status': 'cancelled'})
    
    @action(detail=False, methods=['get'])
    def list_jobs(self, request):
        """
        List recent jobs
        
        GET /api/archive/list_jobs/?status=RUNNING&limit=10
        """
        status_filter = request.query_params.get('status')
        limit = int(request.query_params.get('limit', 10))
        
        jobs = ArchiveJob.objects.all()
        
        if status_filter:
            jobs = jobs.filter(status=status_filter)
        
        jobs = jobs.order_by('-started_at')[:limit]
        
        return Response([
            {
                'job_id': str(job.id),
                'name': job.name,
                'status': job.status,
                'started_at': job.started_at,
                'completed_at': job.completed_at
            }
            for job in jobs
        ])
```

---

# 14. Production Deployment Recommendations

## 14.1 Database Setup

### Separate Archive Database

```python
# settings.py

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'production_db',
        'USER': 'app_user',
        'PASSWORD': env('DB_PASSWORD'),
        'HOST': 'prod-db.example.com',
        'PORT': '5432',
        'CONN_MAX_AGE': 600,
        'OPTIONS': {
            'connect_timeout': 10,
            'options': '-c statement_timeout=30000'  # 30s timeout
        }
    },
    'archive': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'archive_db',
        'USER': 'archive_user',
        'PASSWORD': env('ARCHIVE_DB_PASSWORD'),
        'HOST': 'archive-db.example.com',  # Separate server
        'PORT': '5432',
        'CONN_MAX_AGE': 600,
        'OPTIONS': {
            'connect_timeout': 10,
        }
    }
}

# Archive database router
DATABASE_ROUTERS = ['django_archival.routers.ArchiveRouter']
```

### Database User Permissions

```sql
-- Production DB user (source)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_user;
GRANT DELETE ON archived_tables TO app_user;  -- Only tables being archived

-- Archive DB user
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA public TO archive_user;
GRANT CREATE ON SCHEMA public TO archive_user;  -- For schema sync

-- Metadata tables (in production DB)
GRANT ALL ON django_archive_jobs TO app_user;
GRANT ALL ON django_archive_checkpoints TO app_user;
GRANT ALL ON django_archive_audit TO app_user;
```

---

## 14.2 Deployment Architecture

### Option 1: In-Process (Simple)

```
┌─────────────────────────────────────┐
│   Django Application Servers        │
│                                     │
│   ┌────────────────────────────┐   │
│   │  Celery Beat (Scheduler)   │   │
│   └──────────┬─────────────────┘   │
│              │                      │
│   ┌──────────▼─────────────────┐   │
│   │  Celery Worker             │   │
│   │  (runs archival tasks)     │   │
│   └──────────┬─────────────────┘   │
│              │                      │
└──────────────┼──────────────────────┘
               │
      ┌────────┴────────┐
      │                 │
      ▼                 ▼
┌───────────┐     ┌───────────┐
│ Source DB │     │Archive DB │
└───────────┘     └───────────┘
```

**Pros:**
- Simple deployment
- Uses existing Celery infrastructure
- No new services

**Cons:**
- Shares resources with application
- Can impact production performance

---

### Option 2: Dedicated Service (Recommended for Scale)

```
┌─────────────────────────────────────┐
│   Django Application Servers        │
│   (Production Traffic)               │
└─────────────────────────────────────┘
               │
               │ Triggers archival via API/Queue
               ▼
┌─────────────────────────────────────┐
│   Dedicated Archival Service        │
│                                     │
│   ┌────────────────────────────┐   │
│   │  Archival Scheduler        │   │
│   └──────────┬─────────────────┘   │
│              │                      │
│   ┌──────────▼─────────────────┐   │
│   │  Archival Workers (Pool)   │   │
│   └──────────┬─────────────────┘   │
│              │                      │
└──────────────┼──────────────────────┘
               │
      ┌────────┴────────┐
      │                 │
      ▼                 ▼
┌───────────┐     ┌───────────┐
│ Source DB │     │Archive DB │
│(Read-only)│     │           │
└───────────┘     └───────────┘
```

**Pros:**
- Isolated from production
- Dedicated resources
- Can scale independently

**Cons:**
- More complex deployment
- Additional infrastructure

---

## 14.3 Monitoring and Alerting

### Prometheus Metrics

```python
from prometheus_client import Counter, Histogram, Gauge

# Metrics
archival_jobs_total = Counter(
    'archival_jobs_total',
    'Total archival jobs',
    ['status']
)

archival_rows_total = Counter(
    'archival_rows_total',
    'Total rows archived',
    ['model']
)

archival_duration_seconds = Histogram(
    'archival_duration_seconds',
    'Archival duration',
    ['model']
)

archival_jobs_running = Gauge(
    'archival_jobs_running',
    'Currently running archival jobs'
)

# Export endpoint
from django.http import HttpResponse
from prometheus_client import generate_latest

def metrics_view(request):
    return HttpResponse(
        generate_latest(),
        content_type='text/plain'
    )
```

### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "Django Archival Monitoring",
    "panels": [
      {
        "title": "Archival Rate",
        "targets": [
          {
            "expr": "rate(archival_rows_total[5m])"
          }
        ]
      },
      {
        "title": "Job Duration",
        "targets": [
          {
            "expr": "archival_duration_seconds"
          }
        ]
      },
      {
        "title": "Success Rate",
        "targets": [
          {
            "expr": "rate(archival_jobs_total{status='SUCCESS'}[1h]) / rate(archival_jobs_total[1h])"
          }
        ]
      }
    ]
  }
}
```

### Alert Rules

```yaml
groups:
  - name: archival_alerts
    rules:
      - alert: ArchivalJobFailed
        expr: archival_jobs_total{status="FAILED"} > 0
        for: 5m
        annotations:
          summary: "Archival job failed"
          description: "{{ $value }} archival jobs have failed"
      
      - alert: ArchivalJobStuck
        expr: archival_jobs_running > 0 and time() - archival_job_started_timestamp > 3600
        for: 10m
        annotations:
          summary: "Archival job stuck"
          description: "Job running for over 1 hour"
      
      - alert: ArchiveDatabaseFull
        expr: pg_database_size_bytes{database="archive_db"} / pg_database_total_bytes > 0.9
        for: 5m
        annotations:
          summary: "Archive database nearly full"
          description: "Archive DB is {{ $value | humanizePercentage }} full"
```

---

## 14.4 Backup Strategy

### Regular Backups

```bash
#!/bin/bash
# backup_archive_db.sh

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups/archive"
DB_NAME="archive_db"

# Full backup
pg_dump -Fc $DB_NAME > "$BACKUP_DIR/archive_$DATE.dump"

# Retain last 7 days
find $BACKUP_DIR -name "archive_*.dump" -mtime +7 -delete

# Upload to S3
aws s3 cp "$BACKUP_DIR/archive_$DATE.dump" \
  s3://company-backups/archive/ \
  --storage-class GLACIER
```

### Point-in-Time Recovery

```sql
-- Enable WAL archiving
wal_level = replica
archive_mode = on
archive_command = 'cp %p /archive/wal/%f'
```

---

## 14.5 Performance Testing

### Load Testing Script

```python
import time
from concurrent.futures import ThreadPoolExecutor
from django_archival import ArchiveJobRunner
from myapp.archives import TestArchive

def load_test_archival(num_concurrent_jobs=5):
    """
    Test archival performance under load
    """
    results = []
    
    def run_job(job_id):
        start = time.time()
        runner = ArchiveJobRunner(TestArchive())
        result = runner.run()
        duration = time.time() - start
        
        return {
            'job_id': job_id,
            'rows': result.total_rows_archived,
            'duration': duration,
            'throughput': result.total_rows_archived / duration
        }
    
    with ThreadPoolExecutor(max_workers=num_concurrent_jobs) as executor:
        futures = [
            executor.submit(run_job, i)
            for i in range(num_concurrent_jobs)
        ]
        
        results = [f.result() for f in futures]
    
    # Analyze results
    total_rows = sum(r['rows'] for r in results)
    total_duration = max(r['duration'] for r in results)
    avg_throughput = total_rows / total_duration
    
    print(f"Total rows: {total_rows}")
    print(f"Total duration: {total_duration:.2f}s")
    print(f"Average throughput: {avg_throughput:.0f} rows/sec")
    
    return results
```

---

# 15. Tradeoff Analysis

## 15.1 Architecture Decisions

### Library vs Service

| Aspect | Library (Chosen) | Service |
|--------|------------------|---------|
| **Deployment** | Package in each service | Standalone deployment |
| **Complexity** | Low | High |
| **Governance** | Decentralized | Centralized |
| **Scaling** | Per-service | Shared pool |
| **Auth** | Uses app credentials | Requires auth layer |
| **Version Control** | Must sync versions | Single version |
| **Network** | No network calls | HTTP/gRPC overhead |
| **Use When** | <100 services, simple | Many services, central control |

---

### Two-Phase vs Single-Phase

| Aspect | Two-Phase (Chosen) | Single-Phase |
|--------|-------------------|--------------|
| **Safety** | Very safe (copy then delete) | Risky (move in one step) |
| **Recovery** | Can validate before delete | Hard to roll back |
| **Storage** | Needs temporary 2x space | Minimal |
| **Speed** | Slower (two passes) | Faster (one pass) |
| **Complexity** | Higher (state management) | Lower |
| **Use When** | Production systems | Dev/test, small datasets |

---

### ORM vs Raw SQL vs COPY

| Aspect | Django ORM | Raw SQL INSERT | PostgreSQL COPY |
|--------|------------|----------------|-----------------|
| **Speed** | Slow (1x) | Medium (3-5x) | Fast (10x) |
| **Portability** | High (DB-agnostic) | Medium | Low (PG-only) |
| **Type Safety** | High | Low | Low |
| **Complexity** | Low | Medium | High |
| **Data Transform** | Easy | Medium | Hard |
| **Use When** | Small datasets | Medium datasets | Large datasets (millions) |

**Recommendation:** Default to ORM, use COPY for large tables.

---

### Eager vs Lazy Loading

| Aspect | Eager (Chosen) | Lazy |
|--------|---------------|------|
| **Memory** | Higher (load full batch) | Lower (stream) |
| **Performance** | Better (fewer queries) | Worse (N+1 queries) |
| **Complexity** | Lower | Higher |
| **Batch Size** | Limited by memory | Unlimited |
| **Use When** | Predictable batch sizes | Unpredictable data |

---

## 15.2 Strategy Comparisons

### Archival Strategies

| Strategy | Use Case | Complexity | Safety | Speed |
|----------|----------|------------|--------|-------|
| **MOVE** | Free up space | Medium | High | Medium |
| **COPY** | Backup/analytics | Low | Very High | Fast |
| **SELECTIVE** | Mixed requirements | High | High | Medium |
| **OBJECT_GRAPH** | Related data | Very High | High | Slow |

---

### Deletion Strategies

| Strategy | Speed | Safety | Bloat | Complexity |
|----------|-------|--------|-------|------------|
| **Batch DELETE** | Medium | High | High | Low |
| **Partition Drop** | Instant | Medium | None | Medium |
| **Soft Delete** | Fast | Very High | High | Low |
| **TRUNCATE** | Instant | Low | None | Very Low |

---

## 15.3 Performance vs Safety Tradeoffs

```
High Safety ◄────────────────────────► High Performance

│                                                        │
│ Full validation (100%)                                 │
│ Two-phase (copy + validate + delete)                   │
│ Small batch sizes                                      │
│ Checkpoints every batch                                │
│ Django ORM                                             │
│                                                        │
│                    ▼ Recommended Balance ▼            │
│                                                        │
│ Sample validation (10%)                                │
│ Two-phase with fast validation                         │
│ Adaptive batch sizes                                   │
│ Checkpoints every N batches                            │
│ Raw SQL/COPY for large tables                          │
│                                                        │
│ No validation                                          │
│ Single-phase (atomic move)                             │
│ Large batch sizes                                      │
│ No checkpoints                                         │
│ PostgreSQL COPY                                        │
│                                                        │
```

---

## 15.4 Cost Analysis

### Storage Costs

```
Scenario: 1TB production data, 80% can be archived

Option 1: Keep in production
- Production DB: 1TB at $0.23/GB/mo = $230/mo
- Total: $230/mo

Option 2: Archive to separate DB
- Production DB: 200GB at $0.23/GB/mo = $46/mo
- Archive DB: 800GB at $0.10/GB/mo = $80/mo
- Total: $126/mo
- Savings: $104/mo (45%)

Option 3: Archive to S3 (via plugin)
- Production DB: 200GB at $0.23/GB/mo = $46/mo
- Archive DB: 800GB at $0.10/GB/mo = $80/mo
- S3 Glacier: 800GB at $0.004/GB/mo = $3.20/mo
- Total: $129.20/mo
- Savings: $100.80/mo (44%)
```

### Compute Costs

```
Archival Job: 1TB data

Option 1: In-process (use existing workers)
- Additional compute: $0
- Impact on production: Moderate

Option 2: Dedicated worker
- t3.xlarge (4 vCPU, 16GB): $0.1664/hr
- Job duration: 10 hours
- Cost per run: $1.66
- Monthly (daily archival): $50/mo

ROI: Savings ($104/mo) - Additional cost ($50/mo) = $54/mo net savings
```



---

# 16. Future Enhancements

## 16.1 Phase 2 Features

### 1. Cross-Database Archival

```python
# Support archiving from MySQL to PostgreSQL, etc.
class CrossDatabaseArchive(ArchiveDefinition):
    source_db_type = 'mysql'
    archive_db_type = 'postgresql'
    
    # Automatic type mapping
    type_mappings = {
        'TINYINT': 'SMALLINT',
        'DATETIME': 'TIMESTAMP',
    }
```

### 2. Incremental Schema Evolution

```python
# Track and apply schema changes incrementally
class IncrementalSchemaSync:
    def detect_changes_since_last_sync(self):
        """
        Compare schema hashes to detect changes
        Only apply delta, not full sync
        """
        pass
```

### 3. Data Compression

```python
# Compress archived data to save space
class CompressionPlugin(Plugin):
    def execute(self, context):
        if context.phase == 'after_copy':
            # Compress JSONB fields, text fields, etc.
            compress_table_columns(
                context.model,
                columns=['metadata', 'description']
            )
```

### 4. Automatic Retention Policy

```python
# AI-driven retention policy suggestions
class SmartRetentionPolicy:
    def suggest_policy(self, model):
        """
        Analyze access patterns and suggest retention policy
        
        - Last accessed > 1 year → Archive
        - Cold data (rarely queried) → Archive
        - Large old records → Archive
        """
        pass
```

### 5. Multi-Tier Archival

```python
# Hot → Warm → Cold → Glacier
class TieredArchival(ArchiveDefinition):
    tiers = [
        {'name': 'warm', 'age_days': 90, 'storage': 'archive_db'},
        {'name': 'cold', 'age_days': 365, 'storage': 's3_standard'},
        {'name': 'glacier', 'age_days': 2555, 'storage': 's3_glacier'},
    ]
```

---

## 16.2 Performance Enhancements

### 1. Parallel Table Archival

```python
# Archive independent tables in parallel
def archive_parallel_tables(models, max_parallelism=4):
    """
    Identify independent models (no FK dependencies)
    Archive in parallel
    """
    pass
```

### 2. Smart Batch Sizing

```python
# ML-based batch size optimization
class AdaptiveBatchSizer:
    def __init__(self):
        self.performance_history = []
    
    def next_batch_size(self, model, current_performance):
        """
        Use historical data to predict optimal batch size
        """
        pass
```

### 3. Query Result Streaming

```python
# Stream results instead of loading full batch
def stream_archive(model, queryset):
    """
    Use server-side cursor for memory efficiency
    """
    with connections['default'].cursor() as cursor:
        cursor.execute(
            queryset.query,
            name='archive_cursor'  # Named cursor = server-side
        )
        
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            yield batch
```

---

## 16.3 Operational Enhancements

### 1. Web UI Dashboard

```
┌─────────────────────────────────────────────────────────┐
│  Django Archival Dashboard                              │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  Active Jobs:  [##########--------] 2 running           │
│  Completed:    142 jobs (98.5% success rate)            │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │ Job: order_archival (Running)                  │    │
│  │ Progress: 45% (450,000 / 1,000,000 rows)       │    │
│  │ ETA: 2h 15m                                    │    │
│  │ [View Details] [Cancel]                        │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│  Recent Jobs:                                            │
│  • customer_archive    SUCCESS   1.2M rows   45m        │
│  • invoice_archive     SUCCESS   800K rows   32m        │
│  • payment_archive     FAILED    See logs               │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### 2. Cost Calculator

```python
def calculate_archival_savings(model, retention_policy):
    """
    Estimate cost savings from archival
    
    Returns:
        {
            'current_monthly_cost': float,
            'projected_monthly_cost': float,
            'monthly_savings': float,
            'annual_savings': float,
            'payback_period_months': float
        }
    """
    pass
```

### 3. Rollback Capability

```python
def rollback_archival(job_id):
    """
    Restore archived data back to source
    
    Use cases:
    - Archival done by mistake
    - Business requirement changed
    - Data needed back in production
    """
    pass
```

---

## 16.4 Integration Enhancements

### 1. Data Warehouse Sync

```python
# Automatically sync archived data to data warehouse
class DataWarehousePlugin(Plugin):
    def execute(self, context):
        if context.phase == 'after_copy':
            sync_to_redshift(context.model, context.pks)
            sync_to_bigquery(context.model, context.pks)
```

### 2. Audit Trail Export

```python
# Export audit logs to compliance system
class ComplianceExportPlugin(Plugin):
    def execute(self, context):
        export_audit_trail(
            system='splunk',
            job_id=context.job_id,
            operation=context.phase
        )
```

### 3. Notification Integrations

```python
# Multi-channel notifications
class NotificationManager:
    channels = [
        SlackNotifier(webhook_url='...'),
        EmailNotifier(recipients=['team@example.com']),
        PagerDutyNotifier(service_key='...'),
        DatadogEventNotifier(api_key='...')
    ]
```

---

# 17. Risks and Mitigation

## 17.1 Technical Risks

### Risk 1: Data Loss During Archival

**Likelihood:** Low  
**Impact:** Critical  
**Mitigation:**
- Two-phase approach (copy before delete)
- Mandatory validation between phases
- Checksumming for data integrity
- Regular backup of archive DB
- Dry-run mode for testing

---

### Risk 2: Schema Drift

**Likelihood:** Medium  
**Impact:** High  
**Mitigation:**
- Automatic schema synchronization
- Schema diff validation
- Alert on schema mismatches
- Version tracking in metadata
- Migration-based sync as primary

---

### Risk 3: Performance Impact on Production

**Likelihood:** Medium  
**Impact:** Medium  
**Mitigation:**
- Rate limiting between batches
- Off-peak scheduling
- Connection pooling
- Read replicas for source queries
- Monitoring and auto-throttling

---

### Risk 4: Archival Job Failures

**Likelihood:** Medium  
**Impact:** Medium  
**Mitigation:**
- Comprehensive checkpointing
- Automatic resume capability
- Retry logic with exponential backoff
- Detailed error logging
- Alerting on failures

---

### Risk 5: Archive Database Capacity

**Likelihood:** Medium  
**Impact:** High  
**Mitigation:**
- Capacity planning and monitoring
- Auto-scaling storage (if cloud)
- Multi-tier archival (DB → S3)
- Compression of archived data
- Alerts at 80% capacity

---

## 17.2 Operational Risks

### Risk 6: Incorrect Configuration

**Likelihood:** Medium  
**Impact:** High  
**Mitigation:**
- Configuration validation (dry-run mode)
- Row count estimates before execution
- Mandatory peer review for configs
- Test environments mirroring production
- Rollback capability

---

### Risk 7: Accidental Deletion of Active Data

**Likelihood:** Low  
**Impact:** Critical  
**Mitigation:**
- Pre-deletion safety checks
- Verify data exists in archive
- Check for unarchived dependencies
- Audit trail for all deletions
- Soft-delete option for critical tables

---

### Risk 8: Concurrent Archival Jobs

**Likelihood:** Low  
**Impact:** Medium  
**Mitigation:**
- Advisory locks per model
- Job status tracking
- Prevent duplicate jobs
- Deadlock detection and retry
- Clear error messages

---

## 17.3 Compliance Risks

### Risk 9: GDPR/Privacy Violations

**Likelihood:** Low  
**Impact:** Critical  
**Mitigation:**
- Encryption plugin for PII
- Access controls on archive DB
- Audit logging of all access
- Data retention policies
- Right-to-delete support

---

### Risk 10: Audit Trail Loss

**Likelihood:** Low  
**Impact:** High  
**Mitigation:**
- Separate audit table
- Export to external systems
- Tamper-proof logging
- Long-term retention (7+ years)
- Regular audit reviews

---

# 18. Recommended Architecture

## 18.1 Executive Summary

After analyzing all requirements, tradeoffs, and constraints, here is the recommended architecture:

### Core Decisions

1. **Deployment Model:** Python library (not standalone service)
   - Simpler deployment
   - Leverages existing Django infrastructure
   - Suitable for microservice architecture with <100 services

2. **Configuration:** Python class-based with decorator support
   - Type-safe and IDE-friendly
   - Flexible for complex logic
   - Integrates naturally with Django

3. **Archival Strategy:** Two-phase (copy then delete)
   - Safety first: validate before deletion
   - Enables rollback
   - Industry standard approach

4. **Dependency Resolution:** Topological sort with special case handling
   - Automatic via Django model introspection
   - Handles circular dependencies
   - Supports GenericForeignKey

5. **Schema Synchronization:** Hybrid (migrations + validation)
   - Primary: Django migrations
   - Fallback: Schema diffing
   - Best of both worlds

6. **Performance:** Adaptive with multiple strategies
   - Default: Django ORM for portability
   - Large datasets: PostgreSQL COPY
   - Parallel workers for independent models

7. **Failure Recovery:** Checkpoint-based with idempotency
   - Resume from last successful batch
   - No data loss on crashes
   - Comprehensive audit trail

---

## 18.2 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Django Service                            │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Application Layer                                          │ │
│  │  • Celery Tasks                                             │ │
│  │  • Management Commands                                      │ │
│  │  • HTTP APIs (optional)                                     │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                │                                 │
│                                ▼                                 │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  django-archival Library                                    │ │
│  │                                                              │ │
│  │  ┌──────────────────────────────────────────────────────┐  │ │
│  │  │  Public API                                           │  │ │
│  │  │  • ArchiveJobRunner                                   │  │ │
│  │  │  • ArchiveDefinition (base class)                     │  │ │
│  │  │  • ProgressTracker                                    │  │ │
│  │  └──────────────────────────────────────────────────────┘  │ │
│  │                                                              │ │
│  │  ┌──────────────────────────────────────────────────────┐  │ │
│  │  │  Orchestration                                        │  │ │
│  │  │  • JobOrchestrator (workflow management)              │  │ │
│  │  │  • DependencyResolver (build graph)                   │  │ │
│  │  │  • SchemaReplicator (sync schemas)                    │  │ │
│  │  │  • CheckpointManager (progress tracking)              │  │ │
│  │  └──────────────────────────────────────────────────────┘  │ │
│  │                                                              │ │
│  │  ┌──────────────────────────────────────────────────────┐  │ │
│  │  │  Execution                                            │  │ │
│  │  │  • ArchiveExecutor (copy data)                        │  │ │
│  │  │  • DeleteExecutor (delete data)                       │  │ │
│  │  │  • ValidationEngine (verify integrity)                │  │ │
│  │  │  • BatchProcessor (iteration)                         │  │ │
│  │  └──────────────────────────────────────────────────────┘  │ │
│  │                                                              │ │
│  │  ┌──────────────────────────────────────────────────────┐  │ │
│  │  │  Plugin System                                        │  │ │
│  │  │  • EncryptionPlugin                                   │  │ │
│  │  │  • CompressionPlugin                                  │  │ │
│  │  │  • NotificationPlugin                                 │  │ │
│  │  │  • S3ExportPlugin                                     │  │ │
│  │  └──────────────────────────────────────────────────────┘  │ │
│  │                                                              │ │
│  │  ┌──────────────────────────────────────────────────────┐  │ │
│  │  │  Observability                                        │  │ │
│  │  │  • MetricsCollector (Prometheus)                      │  │ │
│  │  │  • AuditLogger                                        │  │ │
│  │  │  • ProgressReporter                                   │  │ │
│  │  └──────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            │                   │                   │
            ▼                   ▼                   ▼
  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────────┐
  │   Source DB     │  │   Archive DB    │  │  Metadata Tables │
  │  (PostgreSQL)   │  │  (PostgreSQL)   │  │  (in Source DB)  │
  │                 │  │                 │  │                  │
  │ • Production    │  │ • Historical    │  │ • Jobs           │
  │   tables        │  │   data          │  │ • Checkpoints    │
  │                 │  │ • Same schema   │  │ • Audit logs     │
  │                 │  │   as source     │  │ • Schema ver     │
  └─────────────────┘  └─────────────────┘  └──────────────────┘
```

---

## 18.3 Implementation Roadmap

### Phase 1: MVP (4-6 weeks)

**Week 1-2: Core Infrastructure**
- [ ] Django app skeleton
- [ ] Configuration system (ArchiveDefinition)
- [ ] Database setup (metadata tables)
- [ ] Basic models and migrations

**Week 3-4: Core Functionality**
- [ ] DependencyResolver (basic)
- [ ] ArchiveExecutor (ORM-based copy)
- [ ] BatchProcessor (keyset pagination)
- [ ] CheckpointManager

**Week 5-6: Testing & Documentation**
- [ ] Unit tests
- [ ] Integration tests
- [ ] Basic documentation
- [ ] Example configurations

**MVP Features:**
- Simple archival (single model)
- Copy and delete
- Basic checkpointing
- Django ORM only

---

### Phase 2: Production Ready (6-8 weeks)

**Week 1-2: Dependency Management**
- [ ] Complete DependencyResolver
- [ ] Handle self-referential FKs
- [ ] GenericForeignKey support
- [ ] Many-to-many support

**Week 3-4: Schema & Validation**
- [ ] SchemaReplicator (migrations-based)
- [ ] Schema diff validation
- [ ] ValidationEngine
- [ ] Data integrity checks

**Week 5-6: Advanced Features**
- [ ] Multiple archival strategies
- [ ] DeleteExecutor with safety checks
- [ ] Failure recovery
- [ ] Progress tracking API

**Week 7-8: Operations**
- [ ] Monitoring (Prometheus)
- [ ] Management commands
- [ ] Celery integration
- [ ] Production testing

---

### Phase 3: Scale & Polish (4-6 weeks)

**Week 1-2: Performance**
- [ ] PostgreSQL COPY support
- [ ] Parallel workers
- [ ] Adaptive batch sizing
- [ ] Connection pooling

**Week 3-4: Plugins**
- [ ] Plugin architecture
- [ ] Encryption plugin
- [ ] S3 export plugin
- [ ] Notification plugin

**Week 5-6: Polish**
- [ ] Web UI (optional)
- [ ] Advanced monitoring
- [ ] Comprehensive docs
- [ ] Production deployment guide

---

## 18.4 Success Metrics

### Technical Metrics

1. **Throughput:** 
   - Target: 100,000+ rows/minute for typical tables
   - Measurement: Monitor archival_rows_total metric

2. **Reliability:**
   - Target: 99.9% success rate
   - Measurement: archival_jobs_total{status="SUCCESS"} / total

3. **Recovery Time:**
   - Target: <5 minutes to resume after failure
   - Measurement: Time from crash to resumed operation

4. **Production Impact:**
   - Target: <5% increase in DB CPU during archival
   - Measurement: CloudWatch/Datadog metrics

### Business Metrics

1. **Cost Savings:**
   - Target: 30-50% reduction in database costs
   - Measurement: Monthly AWS RDS bills

2. **Storage Reduction:**
   - Target: 50-70% reduction in production DB size
   - Measurement: pg_database_size_bytes

3. **Query Performance:**
   - Target: 20-30% improvement in production queries
   - Measurement: p95 query latency

4. **Adoption:**
   - Target: 80% of eligible services adopt within 6 months
   - Measurement: Number of services using library

---

## 18.5 Final Recommendations

### Do's ✅

1. **Start simple:** Begin with MOVE strategy on non-critical tables
2. **Test extensively:** Use dry-run mode and test environments
3. **Monitor closely:** Set up alerts from day one
4. **Document everything:** Maintain runbooks for operators
5. **Iterate:** Gather feedback and improve incrementally
6. **Validate constantly:** Never skip validation phase
7. **Backup first:** Ensure backups before first production run
8. **Communicate:** Inform stakeholders of archival schedules

### Don'ts ❌

1. **Don't skip dry-runs:** Always test configuration first
2. **Don't archive without backups:** Backups are mandatory
3. **Don't delete before validation:** Two-phase is critical
4. **Don't ignore monitoring:** Archival impacts production
5. **Don't over-optimize early:** Start with simple, add complexity later
6. **Don't use TRUNCATE carelessly:** Very risky for mistakes
7. **Don't forget about time zones:** Handle datetime correctly
8. **Don't skip documentation:** Future you will thank you

---

## 18.6 Conclusion

This architecture provides:

✅ **Safety:** Two-phase approach, validation, checkpointing  
✅ **Scalability:** Handles billions of rows via batching and parallelism  
✅ **Reliability:** Comprehensive failure recovery and idempotency  
✅ **Flexibility:** Multiple strategies, plugins, and configurations  
✅ **Observability:** Metrics, logs, and audit trails  
✅ **Maintainability:** Clean API, good documentation, test coverage  
✅ **Performance:** Adaptive optimization without sacrificing safety  

The recommended approach balances production-grade requirements with implementation complexity, providing a solid foundation that can evolve as needs grow.

**Next Steps:**
1. Review and approve this design
2. Set up development environment
3. Begin Phase 1 implementation
4. Schedule design review checkpoints
5. Plan pilot deployment with one service

---

**Document End**

For questions or clarifications, please reach out to the architecture team.

