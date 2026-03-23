# Project Scope Readme: Automated SQL Source Ingest Discovery in Microsoft Fabric

## 1) Problem Statement
Today, ingest pipeline creation is manual:
- Analysts/engineers build complex SQL (CTEs, nested subqueries, multi-join logic).
- A person manually identifies source tables.
- Pipelines are manually built per source.

This creates delivery bottlenecks, inconsistency, and audit risk.

## 2) Objective (Lean + Functional)
Build a **minimal, production-viable automation** that:
1. Reads SQL files from a central folder (`Queries/`),
2. Parses each query with `sqlglot`,
3. Extracts unique source tables,
4. Upserts source-table metadata into a Bronze metadata catalog table,
5. Provides a stable handoff for downstream ingest orchestration.

> Non-goal for this phase: full column-level lineage and silver/gold transform generation.

---

## 3) Proposed Fabric Architecture

### Components
1. **Lakehouse Files**
   - `/Files/Queries/*.sql` is source-of-truth for production SQL text.

2. **Notebook: `ingest_query_metadata`**
   - Reads SQL files,
   - Uses `sqlglot` to parse SQL,
   - Extracts table dependencies,
   - Upserts to Bronze Delta table.

3. **Bronze Metadata Table**
   - `bronze.ingest_source_table_catalog`
   - Stores one row per `(query_path, source_table)` pair.

4. **Fabric Pipeline Orchestration**
   - Scheduled pipeline executes notebook on cadence (e.g., hourly or daily).
   - Optional retry policy + alerts.

### Why Bronze for this table?
This dataset captures raw discovered metadata directly from source SQL with minimal transformation. That aligns best with Bronze semantics.

---

## 4) Data Model (Bronze)
`bronze.ingest_source_table_catalog`

Recommended columns (phase 1):
- `query_name` (string)
- `query_path` (string)
- `source_table` (string)
- `first_seen_utc` (string or timestamp)
- `last_seen_utc` (string or timestamp)
- `is_active` (boolean)

Primary key (logical):
- `(query_path, source_table)`

Behavior:
- New pair -> insert,
- Existing pair -> update `last_seen_utc`, keep active,
- Missing from latest scan -> optionally set `is_active = false`.

---

## 5) Lean Implementation Flow

1. **Author SQL in Files/Queries**
   - Example: `/Files/Queries/CCB.sql`.

2. **Notebook run**
   - Enumerate `.sql` files,
   - Parse query text via `sqlglot.parse_one(..., read='tsql')`,
   - Collect all `Table` nodes,
   - Normalize table names,
   - Upsert Delta table.

3. **Downstream trigger (next step)**
   - Use discovered source list to parameterize copy ingests automatically.

---

## 6) Fabric Pipeline Design (Phase 1)
Single pipeline `pl_discover_ingest_sources`:

1. Activity A: Notebook `nb_ingest_query_metadata`
2. Activity B (optional): Data quality check notebook (non-empty results)
3. Activity C: Notification (Teams/email) on failure

Operational settings:
- Retry: 2 attempts
- Timeout: 30 min
- Trigger: scheduled (daily) + manual on-demand

---

## 7) Roadmap for Silver/Gold Automation (Future)

### Silver Automation Ideas
- Auto-generate standardized cleansing notebooks by source domain.
- Drive transformations from metadata rules table:
  - null handling,
  - dedupe keys,
  - type conformance,
  - CDC merge rules.

### Gold Automation Ideas
- Semantic model templates based on curated entities.
- Rule-based generation of business marts (facts/dimensions) from silver contracts.
- Auto-generated data tests (freshness, uniqueness, referential integrity).

### Key Design Principle
Use a **metadata-driven framework**: once source metadata is dependable, transformations can become declarative configs rather than handwritten pipelines.

---

## 8) Agile Delivery Approach (Enterprise-Friendly)

Recommended operating model: **Scrum-lite with Kanban execution**
- 2-week planning cadence,
- Continuous flow for engineering tasks,
- WIP limits to reduce context switching.

### Work Cycle
1. **Backlog Refinement (weekly)**
   - Break work into vertical slices (usable increments).
   - Example story: “Extract source tables from SQL files and persist to Bronze catalog.”

2. **Sprint/Iteration Planning**
   - Define sprint goal (business outcome, not just tasks).
   - Add acceptance criteria + Definition of Done.

3. **Build + Demo Loop**
   - Deliver thin slice to working state,
   - Demo with stakeholders,
   - Capture feedback quickly.

4. **Retrospective**
   - Keep/start/stop actions,
   - Convert to concrete process improvements.

### Engineering Practices to Use
- **Trunk-based development** with short-lived branches,
- **CI checks** (lint + unit tests where possible),
- **DataOps principles** (versioned pipelines, reproducible runs, observable jobs),
- **Definition of Ready/Done** for stronger handoffs.

### Suggested Ticket Structure
- Epic: Fabric Metadata-Driven Ingestion Automation
- Features:
  - F1: SQL source discovery + Bronze catalog
  - F2: Automated ingest generation from catalog
  - F3: Silver transformation templates
  - F4: Gold semantic automation

---

## 9) Immediate Next Actions (This Week)
1. Create notebook from `notebooks/ingest_query_metadata.py` in Fabric.
2. Point `SOURCE_SQL_FOLDER` to `/Files/Queries` in the target Lakehouse.
3. Validate with `CCB.sql`.
4. Schedule daily pipeline execution.
5. Add simple monitoring: run status + row count trend.

This keeps scope lean, shows fast value, and creates a clean foundation for future silver/gold automation.
