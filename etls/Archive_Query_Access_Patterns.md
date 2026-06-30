# Archive Query Access Patterns: Predefined vs Ad-Hoc

## The Critical Question

**When users access archived data, can they:**
- **Option A:** Only run predefined queries (e.g., "View Lead Details", "Customer History")
- **Option B:** Write arbitrary SQL queries (full Datalens/SQL access)

This decision fundamentally changes your architecture complexity.

---

## Industry Standard by Use Case

### Banking & Financial Services

**Typical Pattern:** **90% Predefined, 10% Ad-Hoc**

```
User Types and Access:

┌─────────────────────────────────────────────────────────────┐
│ CUSTOMER SERVICE AGENTS (70% of users)                      │
│ Access: Predefined queries only                             │
│ Why: Not SQL trained, need simple UI                        │
│                                                              │
│ Available Queries:                                           │
│ - "View Lead by ID"                                         │
│ - "Customer's Lead History"                                 │
│ - "Lead Documents"                                          │
│ - "Payment History"                                         │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ COMPLIANCE/AUDIT TEAMS (20% of users)                       │
│ Access: Predefined reports + limited ad-hoc                 │
│ Why: Need standard reports, occasional custom queries       │
│                                                              │
│ Available:                                                   │
│ - Standard compliance reports                               │
│ - Date range filters                                        │
│ - Basic WHERE clause filters (approved fields only)         │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ DATA ANALYSTS (10% of users)                                │
│ Access: Full SQL via Datalens                               │
│ Why: Need flexibility for analysis                          │
│                                                              │
│ Access:                                                      │
│ - Full SQL query capability                                 │
│ - All tables and columns                                    │
│ - Complex joins, aggregations                               │
└─────────────────────────────────────────────────────────────┘
```

**Banking Reality:**
- Most users get **UI-based predefined queries**
- Small team gets **full SQL access** (data analysts, engineers)
- **Why:** Security, performance control, cost control

---

## Option A: Predefined Queries (Recommended for Banks)

### What It Means

Users interact with **application UI**, not SQL directly:

```
┌──────────────────────────────────────────────────────┐
│  APPLICATION UI                                      │
│                                                      │
│  [View Lead]  [Customer History]  [Documents]       │
│                                                      │
│  Lead ID: [LEAD-1001____]  [Search]                 │
│                                                      │
│  Results:                                            │
│  ┌─────────────────────────────────────────────┐   │
│  │ Lead ID: LEAD-1001                          │   │
│  │ Customer: John Doe                          │   │
│  │ Amount: $50,000                             │   │
│  │ Status: Closed                              │   │
│  │ Date: 2023-06-15                            │   │
│  │                                             │   │
│  │ [View Documents] [View Activities]          │   │
│  └─────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────┘
```

**Backend executes predefined SQL:**

```python
# User clicks "View Lead" → App executes this specific query
def get_lead_details(lead_id, user):
    """Predefined query: Get lead by ID"""
    
    # Check tier
    manifest = get_manifest(lead_id)
    
    if manifest['tier'] == 'hot':
        return primary_db.query(PREDEFINED_QUERIES['get_lead'], lead_id)
    
    elif manifest['tier'] == 'warm':
        return warm_db.query(PREDEFINED_QUERIES['get_lead_warm'], lead_id)
    
    elif manifest['tier'] == 'cold':
        return execute_cold_query(PREDEFINED_QUERIES['get_lead_cold'], lead_id)

# All queries are predefined constants
PREDEFINED_QUERIES = {
    'get_lead': "SELECT * FROM leads WHERE lead_id = ?",
    'get_lead_warm': "SELECT * FROM archive.leads WHERE lead_id = ?",
    'get_lead_cold': "SELECT * FROM archive.leads WHERE lead_id = ? AND customer_id = ?",
    'get_customer_leads': "SELECT * FROM archive.leads WHERE customer_id = ? ORDER BY closed_at DESC LIMIT 100",
    'get_lead_documents': "SELECT documents FROM archive.leads WHERE lead_id = ?",
    # ... 20-50 predefined queries
}
```

### Advantages

**1. Security & Compliance**
```
✅ No SQL injection risk (no user input in SQL)
✅ Access control per query (audit who accessed what)
✅ Query validation (can't query sensitive columns)
✅ Rate limiting per query type
✅ Data masking (can hide sensitive fields)
```

**2. Performance Optimization**
```
✅ Queries are pre-optimized
✅ Can add caching per query
✅ Predictable load patterns
✅ Can optimize indexes for known queries
✅ No accidental full table scans
```

**3. Cost Control**
```
✅ Athena costs predictable (same queries = same cost)
✅ No expensive ad-hoc queries (accidental SELECT *)
✅ Can set cost limits per query
```

**4. Simpler Architecture**
```
✅ No query parsing/validation needed
✅ No complex query router
✅ Easier to test (finite query set)
✅ Clear performance SLAs per query
```

### Disadvantages

```
❌ Less flexible for users
❌ Need to add new queries for new requirements
❌ Analysts limited to predefined reports
❌ Can't do exploratory analysis easily
```

---

## Option B: Ad-Hoc SQL Queries (Datalens Full Access)

### What It Means

Users write **arbitrary SQL** in Datalens:

```
┌─────────────────────────────────────────────────────────┐
│  DATALENS SQL EDITOR                                    │
│                                                         │
│  Query Editor:                                          │
│  ┌───────────────────────────────────────────────────┐ │
│  │ SELECT                                            │ │
│  │   l.lead_id,                                      │ │
│  │   l.customer_name,                                │ │
│  │   l.amount,                                       │ │
│  │   COUNT(d.document_id) as doc_count              │ │
│  │ FROM archive.leads l                             │ │
│  │ LEFT JOIN archive.documents d ON l.lead_id = ... │ │
│  │ WHERE l.closed_at BETWEEN '2023-01-01' AND ...   │ │
│  │ GROUP BY l.lead_id, l.customer_name, l.amount    │ │
│  │ HAVING COUNT(d.document_id) > 5                  │ │
│  │ ORDER BY l.amount DESC                           │ │
│  │ LIMIT 100                                        │ │
│  └───────────────────────────────────────────────────┘ │
│  [Execute Query]                                        │
└─────────────────────────────────────────────────────────┘
```

**Backend must handle ANY SQL:**

```python
def execute_user_query(sql, user):
    """Execute arbitrary user SQL - complex!"""
    
    # 1. Parse SQL to understand intent
    analysis = parse_sql(sql)
    
    # 2. Security checks
    if not validate_query_security(sql, user):
        raise PermissionError("Query accesses forbidden columns")
    
    # 3. Check which tiers are involved
    tables = analysis.get_tables()  # ['archive.leads']
    
    # 4. Determine if hot, warm, or cold
    # Problem: Can't know until we check manifest for EACH lead
    # User might query: SELECT * FROM archive.leads WHERE amount > 50000
    # This could hit hot, warm, AND cold!
    
    # 5. Route to appropriate query engine
    if 'cold' in tiers_involved:
        # Use Athena
        return athena.execute(sql)
    elif 'warm' in tiers_involved:
        # Use warm DB
        return warm_db.execute(sql)
    else:
        # Use primary DB
        return primary_db.execute(sql)
```

### Advantages

```
✅ Maximum flexibility for analysts
✅ Can do exploratory analysis
✅ No need to predefine queries
✅ Users self-serve (no dev needed)
✅ Complex business questions answerable
```

### Disadvantages

**1. Security Risks**
```
❌ SQL injection if not careful
❌ Users might query sensitive columns
❌ Hard to audit "who accessed what lead"
❌ Potential data exfiltration
```

**2. Performance Risks**
```
❌ Users can write slow queries (accidental full scans)
❌ Unpredictable load patterns
❌ One bad query can slow system
❌ Hard to optimize indexes (unknown access patterns)
```

**3. Cost Risks (Athena)**
```
❌ User runs: SELECT * FROM archive.leads (scans ALL data)
❌ Athena cost: $5/TB × 100 GB = $500 (one query!)
❌ Need query governors/limits
❌ Unpredictable monthly bills
```

**4. Complex Architecture**
```
❌ Must parse and validate all SQL
❌ Complex query routing logic
❌ Hard to determine tier before executing
❌ Cross-tier queries are nightmare
```

---

## Hybrid Approach (Most Common in Banking)

### Split Access by User Role

```python
class ArchiveQueryService:
    def execute_query(self, query_type, params, user):
        """Route based on user role and query type"""
        
        if user.role in ['customer_service', 'operations']:
            # Only predefined queries
            if query_type not in ALLOWED_PREDEFINED_QUERIES[user.role]:
                raise PermissionError("Query not allowed for your role")
            
            return self.execute_predefined(query_type, params)
        
        elif user.role in ['analyst', 'engineer']:
            # Full SQL access via Datalens
            return self.execute_adhoc(params['sql'], user)
        
        elif user.role in ['compliance', 'audit']:
            # Predefined reports + limited ad-hoc
            if query_type == 'custom':
                # Validate query is safe
                if not self.validate_compliance_query(params['sql']):
                    raise PermissionError("Query violates compliance rules")
                return self.execute_adhoc(params['sql'], user)
            else:
                return self.execute_predefined(query_type, params)

# Predefined queries for 90% of users
PREDEFINED_QUERIES = {
    'view_lead': {
        'sql': "SELECT * FROM leads WHERE lead_id = ?",
        'params': ['lead_id'],
        'allowed_roles': ['customer_service', 'operations', 'compliance']
    },
    'customer_history': {
        'sql': "SELECT * FROM leads WHERE customer_id = ? ORDER BY closed_at DESC LIMIT 100",
        'params': ['customer_id'],
        'allowed_roles': ['customer_service', 'operations']
    },
    'monthly_closure_report': {
        'sql': "SELECT DATE_TRUNC('month', closed_at), COUNT(*), SUM(amount) FROM leads WHERE closed_at >= ? GROUP BY 1",
        'params': ['start_date'],
        'allowed_roles': ['compliance', 'management']
    }
}
```

### Access Control Matrix

| User Role | Access Type | Use Case | % of Users |
|-----------|-------------|----------|------------|
| **Customer Service** | Predefined only | View specific lead by ID | 60% |
| **Operations** | Predefined only | Process leads, view history | 20% |
| **Compliance** | Predefined + limited ad-hoc | Reports, investigations | 10% |
| **Analysts** | Full SQL | Analytics, insights | 8% |
| **Engineers** | Full SQL | Debugging, development | 2% |

---

## Recommended Approach for Your Use Case

### **Start with Predefined Queries (80-90% Coverage)**

**Phase 1: Launch with 10-20 predefined queries**

```python
# Core predefined queries
CORE_QUERIES = {
    # Customer Service (most common)
    'get_lead_by_id': "SELECT * FROM archive.leads WHERE lead_id = ?",
    'get_customer_leads': "SELECT * FROM archive.leads WHERE customer_id = ? ORDER BY closed_at DESC",
    'get_lead_documents': "SELECT lead_id, documents FROM archive.leads WHERE lead_id = ?",
    'get_lead_activities': "SELECT lead_id, activities FROM archive.leads WHERE lead_id = ?",
    'get_lead_payments': "SELECT lead_id, payments FROM archive.leads WHERE lead_id = ?",
    
    # Compliance
    'leads_by_date_range': "SELECT * FROM archive.leads WHERE closed_at BETWEEN ? AND ? ORDER BY closed_at",
    'leads_by_closure_reason': "SELECT * FROM archive.leads WHERE closure_reason_name = ?",
    
    # Reports
    'monthly_summary': "SELECT DATE_TRUNC('month', closed_at) as month, COUNT(*) as count, SUM(amount) as total FROM archive.leads WHERE closed_at >= ? GROUP BY 1",
    'agent_performance': "SELECT agent_name, COUNT(*) as leads_closed, SUM(amount) as total_amount FROM archive.leads WHERE closed_at >= ? GROUP BY 1 ORDER BY 2 DESC"
}
```

**Implementation:**

```python
# Simple API for predefined queries
@app.route('/api/archive/query/<query_name>', methods=['POST'])
def execute_predefined_query(query_name):
    """Execute predefined query only"""
    
    # Validate query exists
    if query_name not in CORE_QUERIES:
        return {'error': 'Unknown query'}, 400
    
    # Get parameters
    params = request.json.get('params', {})
    
    # Execute based on tier
    result = execute_tiered_query(CORE_QUERIES[query_name], params)
    
    return {'data': result}

# Usage by frontend
response = requests.post('/api/archive/query/get_lead_by_id', 
    json={'params': {'lead_id': 'LEAD-1001'}}
)
```

**Benefits:**
- ✅ Simple to implement
- ✅ Covers 80-90% of use cases
- ✅ Secure and performant
- ✅ Predictable costs

---

### **Phase 2: Add Ad-Hoc for Analysts (if needed)**

**Only if business requires:**

```python
# Separate endpoint for ad-hoc queries (restricted)
@app.route('/api/archive/adhoc', methods=['POST'])
@require_role(['analyst', 'engineer'])  # Only for specific roles
def execute_adhoc_query():
    """Execute ad-hoc SQL - restricted access"""
    
    sql = request.json.get('sql')
    user = get_current_user()
    
    # Security validations
    if not validate_sql_security(sql):
        return {'error': 'Query validation failed'}, 403
    
    # Cost estimation (Athena)
    estimated_cost = estimate_query_cost(sql)
    if estimated_cost > 10:  # $10 limit
        return {'error': f'Query too expensive: ${estimated_cost}'}, 400
    
    # Execute
    result = execute_tiered_query(sql, {})
    
    # Audit log
    log_adhoc_query(user, sql, result)
    
    return {'data': result}
```

---

## Real-World Example: Banking Implementation

### JPMorgan Chase Pattern (Industry Standard)

```
Tier 1: Customer Service Portal
- Predefined queries only
- UI buttons: "View Lead", "Customer History"
- 10,000 users
- 95% of all queries

Tier 2: Compliance Dashboard
- Predefined reports + date filters
- Pre-built dashboards
- 500 users
- 4% of queries

Tier 3: Data Analyst Workbench
- Full SQL access (with governance)
- Query cost limits ($100/user/month)
- Query review for expensive queries
- 50 users
- 1% of queries
```

---

## Recommendation

### **For Your Banking Use Case:**

**Use Predefined Queries for 90% of Users**

**Why:**
1. ✅ Simpler architecture (no SQL parsing/validation)
2. ✅ Better security (no SQL injection, controlled access)
3. ✅ Predictable performance (optimized queries)
4. ✅ Cost control (no accidental expensive queries)
5. ✅ Faster to implement (10-20 queries vs full SQL engine)

**Add Ad-Hoc SQL only for:**
- Data analysts (< 10 users)
- With strict governance (cost limits, query review)
- Via Datalens with role-based access

**This matches industry standard for banking archives** ✅

---

## Impact on Your Design

### With Predefined Queries (Simpler)

**Architecture Changes:**
```
✅ Remove complex query routing logic
✅ No SQL parsing/validation needed
✅ Simpler tier determination (params include entity IDs)
✅ Easier to optimize (known query patterns)
✅ Can cache query results aggressively
```

**Code Simplification:**
```python
# Before (ad-hoc SQL):
def execute_user_query(sql):
    analysis = parse_sql(sql)  # Complex
    validate_security(sql)      # Complex
    determine_tiers(analysis)   # Complex
    route_query(sql, tiers)     # Complex

# After (predefined):
def execute_predefined(query_name, params):
    query = QUERIES[query_name]     # Simple lookup
    tier = get_tier(params['lead_id'])  # Simple manifest check
    return execute_on_tier(query, tier, params)  # Direct execution
```

**Your retrieval issues document simplifies dramatically:**
- ❌ Issue #3 (cross-customer queries) → Mostly gone (queries specify customer)
- ❌ Issue #9 (routing complexity) → Gone (predefined routes)
- ❌ Issue #10 (manifest overhead) → Reduced (fewer unique queries to cache)

---

## Final Answer

**Question:** Are queries predefined or ad-hoc?

**Industry Answer:** **90% predefined, 10% ad-hoc (analyst-only)**

**Your Strategy:** **Start with 100% predefined, add ad-hoc later if needed**

This simplifies your architecture significantly! 🎯
