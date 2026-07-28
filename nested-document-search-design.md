# Search Flow for Nested Documents: SQL/PPL to RelNode, Query Rewriting

**Author:** Anubhav Shrestha
**Status:** Design Review
**Parent HLD:** Nested Document Support High-Level Design (quip-amazon.com/EYVuAS2rZR5K), Section 4.1.2.4
**Branch:** `shreanu/nested-poc-search-rewrite` (github.com/ANUSVTT/OpenSearch)
**Test status:** 267/275 (97.1%), 0 errors

---

## Table of Contents

1. [Overview & Scope](#1-overview--scope)
2. [Background](#2-background)
3. [Mainline vs Our Branch](#3-mainline-vs-our-branch)
4. [Architecture](#4-architecture)
5. [Detailed Flow: A Worked Example](#5-detailed-flow-a-worked-example)
6. [Changes by Layer](#6-changes-by-layer)
7. [Key Design Decisions](#7-key-design-decisions)
8. [The Three Critical Fixes](#8-the-three-critical-fixes)
9. [Test Results](#9-test-results)
10. [Known Limitations & Open Questions](#10-known-limitations--open-questions)
11. [Discussion Points for the Team](#11-discussion-points-for-the-team)
12. [Appendix](#12-appendix)

---

## 1. Overview & Scope

This document is the detailed design for the **search-side query pipeline for nested
documents** in the Mustang (Parquet + DataFusion) engine. It implements Section
4.1.2.4 of the parent HLD, "Query rewrite (UNNEST for predicates/aggregations on
nested fields)".

The parent HLD selected the **N1 storage approach**: one Parquet row per logical
document, with nested fields stored as `LIST<STRUCT>` columns. That decision makes
storage simple and reads fast, but it moves all of the complexity to the query
layer: a predicate like `comments.score > 4` is no longer a filter over a flat
column -- it is a filter over the *elements* of a list column, and the answer the
user expects is expressed in terms of *parent* documents, not child elements.

This design answers the question: **how does a SQL/PPL query that references a
nested field get planned, rewritten, serialized, and executed** so that:

- Predicates on nested fields evaluate per child element (per-array-element
  semantics, matching OpenSearch `nested` query behavior).
- Aggregations on nested fields aggregate over child elements (matching
  OpenSearch `nested` aggregation behavior).
- Filter-style queries return **distinct parent documents**, not one row per
  matching child.
- All of this happens through the *generic* Calcite planner path -- no
  hand-crafted per-query-shape plans.

### In scope

- PPL/SQL frontend handling of dotted nested-field references.
- Calcite-level rewrite: injecting `Correlate + Uncollect` (the relational UNNEST
  shape) when nested fields are referenced.
- Marking/CBO integration: new physical nodes and marking rules so the unnest
  subtree is routed to the DataFusion backend.
- Substrait serialization of the unnest via `ExtensionSingleRel` and a proto
  post-pass that restores parent-document semantics (dedup).
- Rust-side consumption: building a DataFusion `LogicalPlan::Unnest` from the
  extension relation, and the exchange/filter-extraction plumbing needed to make
  the two-stage distributed plan work.

### Out of scope (covered elsewhere in the HLD)

- Ingestion and Parquet layout for nested fields (HLD Section 4.1.1).
- Block-expansion merge and child-deletion overlay for the Lucene-compatible
  path (HLD Section 4.1.3).
- Inner hits / nested highlighting.
- DSL (`nested` query JSON) support -- this design covers the SQL/PPL entry
  point; DSL translation is a follow-up.

---

## 2. Background

### 2.1 What the HLD decided

The HLD's Section 4.1.2.4 sketches the query-rewrite strategy for N1 storage:

1. **Detect** nested-field references during the marking phase of planning.
2. **Add a nested rewriter rule that injects UNNEST**, so that:
   - a `nested -> terms` aggregation becomes
     `Scan -> UNNEST(comments) -> GroupBy(comments.author)`;
   - `avg(comments.score)` becomes `Scan -> UNNEST -> Aggregate(avg(score))`;
   - a nested *filter* becomes `UNNEST -> Filter` followed by a step that
     returns **distinct parents**.
3. **Emit UNNEST through the convertor** using Substrait, so the DataFusion data
   nodes can execute it natively.
4. **Keep child identity** -- the pair `(parent_row_id, child_index)` -- alive
   through the expansion, so that parent dedup, inner hits, and future
   correlated predicates have something stable to anchor on.
5. Recognize that **correlated multi-field child predicates** (e.g.
   `comments.author = 'alice' AND comments.score > 4` matching the *same*
   comment) require a **single UNNEST per nested path**, not one UNNEST per
   field reference.

This document is the concrete realization of those five bullets.

### 2.2 What mainline provides (and does not)

The good news is that mainline already has most of the *skeleton*:

- **`OpenSearchSchemaBuilder`** already exposes nested mappings to Calcite as
  `ARRAY(ROW(subfield...))`. So the type system knows `comments` is an array of
  structs; a reference to `comments.score` type-checks as "field `score` of the
  element row type". Nothing needed to change here.
- **The PPL frontend (`UnifiedQueryPlanner`)** already has an `expand` command
  that produces exactly the relational shape we want: a
  `Correlate + Uncollect` pair. `expand comments` logically means "for each
  parent row, emit one row per element of `comments`, joined back to the
  parent columns" -- i.e., UNNEST with parent context preserved. This existing
  machinery is a large part of why the one-path design (Section 7.1) is viable.
- **`PlannerImpl`** provides cost-based optimization, the marking rules that
  decide which subtrees run on which backend, and distribution derivation --
  but it has **no nested-specific handling**, and (as we discovered) its
  decorrelation step actively *corrupts* the `Correlate` produced by `expand`.
- **`DataFusionFragmentConvertor`** converts marked Calcite plans to Substrait
  via Isthmus -- but Substrait has no first-class UNNEST relation, and the
  convertor has no way to emit one.
- **On the Rust side**, there is no `unnest_consumer.rs`; the Substrait
  consumer would simply fail on any plan that tried to express an unnest.
- There is no `ExtensionSingleRel` usage for unnest, no
  `NestedParentDedupRewriter`, and no `OpenSearchCorrelateRule` /
  `OpenSearchUncollectRule`.

In short: mainline can *represent* nested data and can *manually* expand it via
the PPL `expand` command end-to-end only as far as the logical plan -- but there
is no path from "user writes `where comments.score > 4`" to "DataFusion executes
an Unnest on the data node and the coordinator returns distinct parents".
Closing that gap is what this design does.

### 2.3 Why UNNEST at all? A semantics refresher

Consider the document:

```json
{ "title": "First post",
  "comments": [ {"author": "alice", "score": 5},
                {"author": "bob",   "score": 2} ] }
```

In N1 storage this is **one Parquet row**. A naive evaluation of
`comments.score > 4` has no meaning -- `comments.score` is a *list* of scores,
and `LIST<INT> > INT` is not a valid comparison (indeed, the PPL type checker
rejects it; see Section 5, Step 0). OpenSearch semantics say: the document
matches if **any** comment has score > 4.

The standard relational encoding of "any element matches" is:

```
SELECT DISTINCT parent.*
FROM parent, UNNEST(parent.comments) AS c
WHERE c.score > 4
```

UNNEST turns the one parent row into N child rows (each carrying the parent
columns plus one exploded struct), the filter runs per-child, and the DISTINCT
(in our implementation, a GROUP BY on parent identity) collapses the survivors
back to parents. Aggregations are the same pipeline minus the dedup: after
UNNEST, `avg(comments.score)` is just `avg(score)` over child rows, which is
exactly what OpenSearch `nested` aggregations compute.

Everything in this design is in service of producing, shipping, and executing
that plan shape automatically.

---

## 3. Mainline vs Our Branch

The branch adds/modifies **36 files, +2,435 lines** relative to mainline. The
table below is organized by pipeline layer, front to back.

| Layer | Component | Mainline | Our Branch |
|---|---|---|---|
| Schema | `OpenSearchSchemaBuilder` | Exposes nested mappings as `ARRAY(ROW(...))` | Unchanged (already sufficient) |
| Frontend | PPL `expand` command | Exists; produces `Correlate + Uncollect` | Reused as-is for the expand path |
| Frontend | Dotted-path translation | None -- `where comments.score > 4` is a type error (`ROW > INTEGER`) | `UnifiedQueryService`: dotted nested references auto-translated to `expand` syntax before planning |
| Service | `UnifiedQueryService` | Early POC had an N1 bypass (hand-built plans for recognized shapes) | **MODIFIED**: bypass removed; every query goes through the generic planner |
| Logical rewrite | Nested-field rewriter | None | **NEW** `OpenSearchNestedFieldRewriter.java` (313 lines): detects `ITEM($arr,'field')` in Project/Filter, injects `Correlate + Uncollect` |
| Planner | `PlannerImpl` | CBO, marking, distribution derivation; decorrelator runs unconditionally | **MODIFIED**: `containsSubQuery()` guard -- decorrelator only runs when a real subquery exists, so it cannot corrupt the expand `Correlate` |
| Marking | Physical Correlate | None | **NEW** `OpenSearchCorrelate.java` + `OpenSearchCorrelateRule.java`: physical node + marking rule; forces the DataFusion backend |
| Marking | Physical Uncollect | None | **NEW** `OpenSearchUncollect.java` + `OpenSearchUncollectRule.java` |
| Serialization | `DataFusionFragmentConvertor` | Isthmus Calcite-to-Substrait; no UNNEST emission | **MODIFIED**: `visit(Correlate)` override emits `ExtensionSingleRel("unnest_reshape:<col>|w=N")` |
| Serialization | Unnest metadata | None | **NEW** `UnnestExtensionDetail.java`: carries unnest metadata inside the `ExtensionSingleRel` |
| Serialization | Parent dedup | None | **NEW** `NestedParentDedupRewriter.java` (519 lines): Substrait proto post-pass inserting `GROUP BY [parent_cols, __row_id__]` |
| Ops control | Kill switch | None | **NEW** `NestedRewriteFlag.java`: system-property kill switch for the whole rewrite path |
| Rust consumer | Unnest consumption | Does not exist | **NEW** `unnest_consumer.rs` (296 lines): builds `LogicalPlan::Unnest` from the extension rel |
| Rust exchange | `api.rs` | `collect_reads()` does not recurse into `ExtensionSingleRel` | **MODIFIED**: recurses into `ExtensionSingleRel` for exchange-schema derivation |
| Rust filters | `substrait_to_tree.rs` | `extract_filter_expr` stops at first non-pushable node | **MODIFIED**: skips post-unnest filters but *continues* searching for pushable parent filters |
| Deleted | POC scaffolding | -- | **DELETED**: `N1SubstraitBuilder`, `N1QueryAnalyzer`, `CorrelateUncollectRewriter`, `N1Descriptor`, `N1Aggregate`, `N1Predicate`, `NestedPocOverride` (replaced by the generic path) |

The deletion row deserves emphasis: an earlier iteration of this POC worked by
*recognizing* specific query shapes (`N1QueryAnalyzer`) and hand-building
Substrait for them (`N1SubstraitBuilder`), bypassing the planner. That approach
passed its tests but could never generalize -- every new query shape needed new
recognition code. The current branch **deletes all of it** and routes every
query through the single generic planner path. Section 7.1 discusses this
trade-off in detail.

---

## 4. Architecture

### 4.1 One-path design

The guiding principle: **nested queries are not special queries**. They enter
the same PPL/SQL frontend, the same Calcite planner, the same marking + CBO
pass, the same Substrait convertor, and the same DataFusion execution engine as
every other query. Nested support is implemented as:

1. one **rewrite** (inject `Correlate + Uncollect` where a nested field is
   referenced),
2. two **marking rules** (so the new nodes get a physical convention and land
   on the DataFusion backend),
3. one **serialization extension** (the `ExtensionSingleRel` unnest carrier plus
   the dedup post-pass), and
4. one **consumer** (Rust side, extension rel to `LogicalPlan::Unnest`).

Nothing downstream of the consumer knows nested documents exist -- DataFusion's
native `Unnest` operator, filter, aggregate, and projection do all the work.

### 4.2 Pipeline diagram (overview with call chain)

```
  User PPL query: "source=blogs | where comments.score > 4 | fields title"
        |
        | UnifiedQueryService.execute(pplText)
        v
+---------------------------+
| STEP 1:                    |  UnifiedQueryService.execute()
| Translation Layer          |    -> planner.plan(pplText) throws "ROW type" error
|                            |    -> tryInjectExpand(pplText, schema)
| UnifiedQueryService.java   |    -> planner.plan(expandedQuery) succeeds
|                            |
|                            |  NOTE: only fires when PPL type-checker rejects.
|                            |  For projection-only queries (fields comments.author),
|                            |  planner.plan() succeeds on first try -- no expand injected.
+---------------------------+
        |
        | planner.plan() returns the logical RelNode tree
        v
+---------------------------+
| STEP 2:                    |  UnifiedQueryPlanner.plan(expandedPPL)
| PPL Frontend               |    -> parses "expand comments" -> Correlate+Uncollect
|                            |    -> parses "where score > 4" -> Filter
| UnifiedQueryPlanner        |    -> parses "fields title" -> Project
| (sql-plugin jar)           |
|                            |  OUTPUT (logical RelNode):
|                            |    LogicalProject(title=$1)
|                            |      LogicalFilter($4 > 4)
|                            |        LogicalProject(rating=$1,title=$2,...,score=$5)
|                            |          LogicalCorrelate(INNER, $cor0, req={0})
|                            |            LogicalTableScan(blogs)
|                            |            Uncollect(Project($cor0.comments))
+---------------------------+
        |
        | DefaultPlanExecutor.executeInternal(logicalPlan)
        |   -> PlannerImpl.createPlan(logicalPlan, plannerContext)
        |     -> runAllOptimizations(logicalPlan)
        v
+---------------------------+
| STEP 3:                    |  PlannerImpl.runAllOptimizations():
| PlannerImpl                |    3a. removeSubQueries()    -- skipped (no subquery)
|                            |    3b. pushdownRules()       -- merges projects
| PlannerImpl.java           |    3c. decomposeAggregates() -- no-op
|                            |    3d. NestedFieldRewriter.rewrite() -- no-op here
|                            |         (Correlate already present from expand.
|                            |          Only fires for projection-only queries where
|                            |          PPL left raw ITEM($0,'field') -- meaning expand
|                            |          was NOT called because type-checker didn't reject.)
|                            |    3e. mark()  -- Logical -> OpenSearch physical nodes
|                            |         OpenSearchCorrelateRule, OpenSearchUncollectRule
|                            |    3f. cbo()   -- VolcanoPlanner inserts ExchangeReducer
|                            |
|                            |  OUTPUT of createPlan() (optimized physical RelNode):
|                            |    OpenSearchExchangeReducer         [SINGLETON]
|                            |      OpenSearchProject(title=$2)     [datafusion]
|                            |        OpenSearchFilter(>($5,4))     [datafusion]
|                            |          OpenSearchCorrelate(req={0})[datafusion]
|                            |            OpenSearchTableScan(blogs)[lucene,datafusion]
|                            |            OpenSearchUncollect(...)   [datafusion]
+---------------------------+
        |
        | createPlan() returns to DefaultPlanExecutor
        v
+---------------------------+
| STEP 3.5:                  |  DefaultPlanExecutor continues:
| DAG Build + Fork + Select  |
|                            |    DAGBuilder.build(plan, capabilityRegistry, ...)
| DefaultPlanExecutor.java   |      -> cuts at ExchangeReducer into stages
| DAGBuilder.java            |      OUTPUT: QueryDAG with 2 stages:
| PlanForker.java            |        Stage 1: ExchangeReducer -> StageInputScan(0)
| PlanAlternativeSelector    |        Stage 0: Project -> Filter -> Correlate(Scan, Uncollect)
|                            |
|                            |    PlanForker.forkAll(dag)
|                            |      -> creates per-backend plan alternatives
|                            |
|                            |    BackendPlanAdapter.adaptAll(dag)
|                            |      -> adapts for chosen backend capabilities
|                            |
|                            |    PlanAlternativeSelector.selectAll(dag)
|                            |      -> picks datafusion (only viable for Correlate)
|                            |
|                            |    FragmentConversionDriver.convertAll(dag)
|                            |      -> calls convertor.convertFragment() per stage
|                            |      (this is Step 4 below)
+---------------------------+
        |
        v
+---------------------------+
| STEP 4:                    |  FragmentConversionDriver.convertAll(dag):
| Substrait Emission         |
|                            |  Stage 0 (data node fragment):
| DataFusionFragment-        |    DataFusionFragmentConvertor.convertFragment():
| Convertor.java             |      -> convertToSubstrait(fragment)
|  + NestedParentDedup-      |        -> visitor.apply(): walks tree top-down
|    Rewriter.java           |          visit(Correlate) -> tryEmitUnnest()
|                            |            -> emits ExtensionSingleRel("unnest_reshape")
|                            |          visit(Filter), visit(Project) -> Isthmus normal
|                            |      -> NestedParentDedupRewriter.rewrite(proto)
|                            |          -> injects GROUP BY for parent dedup
|                            |      returns byte[] (388 bytes)
|                            |
|                            |  Stage 1 (coordinator fragment):
|                            |    DataFusionFragmentConvertor.convertFragment():
|                            |      -> rewriteStageInputScans(): table="input-0"
|                            |      -> Isthmus emits ReadRel("input-0")
|                            |      returns byte[] (69 bytes passthrough)
+---------------------------+
        |
        | Both stage byte[] stored in QueryDAG
        | DefaultPlanExecutor -> QueryScheduler.execute(context)
        |   -> ExecutionGraph.build(dag)
        |     -> StageExecutionBuilder per stage
        |       -> ReduceStageExecutionFactory.createExecution(stage1)
        v
+---------------------------+
| STEP 5:                    |  DatafusionReduceSink constructor:
| Exchange Setup             |    1. NativeBridge.registerPartitionStream("input-0", stage0Bytes)
|                            |       [Rust] api::register_partition_stream():
| Rust: api.rs               |         -> derive_schema_from_partial_plan(stage0 bytes)
|                            |           -> collect_reads() finds ReadRel under ExtSingleRel
|                            |           -> registers stub table, lowers plan, gets schema
|                            |         -> registers StreamingTable "input-0"
|                            |    2. NativeBridge.executeLocalPlan(stage1Bytes)
|                            |       -> resolves ReadRel("input-0") -> StreamingTable
+---------------------------+
        |
        | Stage 0 bytes dispatched to data node shard
        | ShardFragmentStageExecution sends via transport
        v
+---------------------------+
| STEP 6:                    |  [Rust] indexed_executor::execute_indexed():
| DataFusion Execution       |    -> from_substrait_plan_unnest_aware(ctx, stage0Bytes)
| (data node)                |      -> UnnestConsumer: ExtSingleRel -> LogicalPlan::Unnest
|                            |      -> Standard consumer: Filter, Aggregate, Project
| Rust: unnest_consumer.rs   |    -> extract_filter_expr(): skips post-unnest filters,
|  + substrait_to_tree.rs    |       continues searching for pushable parent filters
|  + indexed_executor.rs     |    -> create_physical_plan()
|                            |    -> execute: Parquet -> Unnest -> Filter -> Dedup -> Project
|                            |    -> Arrow RecordBatch (4 rows) streamed to coordinator
+---------------------------+
        |
        | Arrow batches over exchange (Flight transport)
        | DatafusionPartitionSender -> StreamingTable channel
        v
+---------------------------+
| STEP 7:                    |  Coordinator drains StreamingTable "input-0"
| Coordinator Passthrough    |    -> 4 rows pass through to DefaultPlanExecutor
|                            |    -> UnifiedQueryService wraps as PPLResponse
| (framework, no changes)    |    -> HTTP JSON response returned
+---------------------------+
        |
        v
  {"columns":["title"],"rows":[["First post"],["Third post"],["Fourth post"],["Fifth post"]]}
```
### 4.3 Detailed stage breakdown (with input/output at each stage)

Running example throughout: `source=blogs | where comments.score > 4 | fields title`

Data:
```
blogs index (5 documents, 1 shard, Parquet composite format):
  Row 0: title="First post",  views=100, comments=[{author:"alice",score:5},{author:"bob",score:3}]
  Row 1: title="Second post", views=50,  comments=[{author:"carol",score:4}]
  Row 2: title="Third post",  views=200, comments=[{author:"dave",score:9},{author:"eve",score:7}]
  Row 3: title="Fourth post", views=300, comments=[{author:"frank",score:8},{author:"grace",score:6},{author:"helen",score:4}]
  Row 4: title="Fifth post",  views=150, comments=[{author:"ivan",score:9}]
```

Expected result: 4 parent titles (those with at least one comment.score > 4).

---

#### STEP 1: Translation Layer (UnifiedQueryService.tryInjectExpand)

```
+------------------------------------------------------------------------------+
| STAGE 0: TRANSLATION (try-catch-retry pattern)                                |
|                                                                               |
| FILE: UnifiedQueryService.java (tryInjectExpand method)                       |
| CHANGE: Implemented. ~60 lines. Catch-and-retry on type-checker rejection.    |
|                                                                               |
| PROBLEM:                                                                      |
|   PPL resolves "comments.score" to ITEM($0,'score').                          |
|   ITEM(ARRAY<ROW(author,score,text)>, 'score') -> return type = ROW           |
|   ROW > 4 -> type error: "Unsupported conversion for Relational Data type"   |
|   The plan is NEVER created. Crash before any RelNode exists.                 |
|   This happens because Calcite's ITEM operator on ARRAY<ROW> returns the      |
|   element type (the whole ROW), not the specific field type (INTEGER).         |
|   The type-checker then rejects ROW in a comparison/aggregation context.      |
|                                                                               |
| HOW IT WORKS:                                                                 |
|   1. FIRST: try planner.plan(originalQuery) as-is                             |
|   2. IF PPL type-checker throws "Unsupported conversion for Relational Data   |
|      type" (meaning a nested sub-field was used in WHERE/STATS context):      |
|      a. Extract index name from "source=<index>"                              |
|      b. Look up schema from schemaPlus -> find ARRAY-typed (nested) fields    |
|      c. Find which nested field is dotted-referenced in the query text        |
|      d. Inject "| expand <nestedField>" after source=, strip dotted prefix    |
|      e. Retry: planner.plan(expandedQuery)                                    |
|   3. IF plan succeeds on first try -> no translation, proceed as normal       |
|                                                                               |
| WHICH QUERIES TRIGGER TRANSLATION (the catch path):                           |
|   Nested sub-field in WHERE:                                                  |
|     source=blogs | where comments.score > 4 | fields title                   |
|       -> source=blogs | expand comments | where score > 4 | fields title      |
|   Nested sub-field in STATS:                                                  |
|     source=blogs | stats avg(comments.score)                                  |
|       -> source=blogs | expand comments | stats avg(score)                    |
|   Nested sub-field in equality:                                               |
|     source=blogs | where comments.author = 'alice' | fields title             |
|       -> source=blogs | expand comments | where author = 'alice' | fields title|
|                                                                               |
| WHICH QUERIES DO NOT TRIGGER TRANSLATION (pass on first try):                 |
|   Nested sub-field ONLY in FIELDS (projection):                               |
|     source=blogs | fields title, comments.author                  -> PASSES   |
|     source=blogs | where views > 100 | fields title, comments.author -> PASSES|
|   No nested reference at all:                                                 |
|     source=blogs | where views > 50 | fields title               -> PASSES   |
|                                                                               |
| WHY THE SPLIT:                                                                |
|   - In a PROJECT, ITEM($0,'author') returns type ROW but Projects accept      |
|     any type -> no error. The OpenSearchNestedFieldRewriter (Stage 2)         |
|     later injects the Correlate for these.                                    |
|   - In a WHERE/STATS, ITEM($0,'score') is used in a COMPARISON (ROW > 4)     |
|     or AGGREGATION (AVG(ROW)) -> type-checker throws BEFORE the plan exists.  |
|     The Rewriter never gets a chance to act. Translation is the only option.  |
|                                                                               |
| EXAMPLE (this query):                                                         |
|   INPUT:  "source=blogs | where comments.score > 4 | fields title"            |
|   Step 1: planner.plan(input) -> THROWS "Unsupported conversion for ROW"      |
|   Step 2: tryInjectExpand detects 'comments' is ARRAY in schema               |
|   Step 3: rewrites to "source=blogs | expand comments | where score > 4       |
|            | fields title"                                                    |
|   Step 4: planner.plan(expanded) -> SUCCEEDS                                  |
|                                                                               |
| OUTPUT (to next stage):                                                       |
|   "source=blogs | expand comments | where score > 4 | fields title"           |
|                                                                               |
| FUTURE: Once the upstream sql-plugin fixes CalciteRexNodeVisitor to resolve   |
| ITEM(ARRAY<ROW>,'score') as INTEGER (not ROW), the first plan() call will     |
| succeed for all queries and this translation becomes dead code.               |
+------------------------------------------------------------------------------+
        |
        v
```

---

#### STEP 2: PPL Frontend (UnifiedQueryPlanner)

```
+------------------------------------------------------------------------------+
| STAGE 1: PPL FRONTEND                                                         |
|                                                                               |
| FILE: UnifiedQueryPlanner (sql-plugin jar, not our code)                      |
| CHANGE NEEDED: None (PPL 'expand' already exists on mainline)                 |
|                                                                               |
| INPUT (translated query):                                                     |
|   "source=blogs | expand comments | where score > 4 | fields title"          |
|                                                                               |
| WHAT HAPPENS:                                                                 |
|                                                                               |
|   - "source=blogs" -> LogicalTableScan(blogs)                                 |
|     Schema: [comments: ARRAY(ROW(author,score,text)), rating, title, views]   |
|                                                                               |
|   - "expand comments" -> PPL natively creates Correlate + Uncollect           |
|     The array column is exploded: each parent row produces N child rows,      |
|     one per array element. The struct fields (author, score, text) become     |
|     flat columns appended after the original parent columns.                  |
|                                                                               |
|   - "where score > 4" -> LogicalFilter(condition=[score > 4])                 |
|     After expand, 'score' is a flat INTEGER column. The comparison            |
|     INTEGER > 4 type-checks fine (no ROW type issue).                         |
|                                                                               |
|   - "fields title" -> LogicalProject(title)                                   |
|                                                                               |
| OUTPUT (Calcite RelNode tree -- actual PPL output from the log):              |
|   LogicalProject(title=$1)                                                    |
|     LogicalFilter(condition=[$4 > 4])                                         |
|       LogicalProject(rating=$1, title=$2, views=$3, author=$4, score=$5,      |
|                      text=$6)        <- renaming layer (PPL inserts this      |
|         LogicalCorrelate(INNER, $cor0, requiredColumns={0})   to give clean   |
|           LogicalTableScan(blogs)            <- LEFT: parent rows  names to   |
|           Uncollect                          <- RIGHT: explodes    post-expand |
|             LogicalProject($cor0.comments)      the array          columns)   |
|               LogicalValues(oneRow)                                            |
|                                                                               |
| NOTE: title=$1 and score=$4 here because they reference the intermediate      |
| renaming Project's output (where title is position 1, score is position 4).   |
| pushdownRules (Step 3b) will later merge this renaming Project away,          |
| shifting to title=$2 and score=$5 (raw Correlate output positions).           |
+------------------------------------------------------------------------------+
        |
        v
```


#### STEP 3: PlannerImpl (all sub-steps: rewrite + mark + CBO)

```
+------------------------------------------------------------------------------+
| STAGE 3: PLANNER (RULES + MARKING + CBO)                                     |
|                                                                               |
| FILE: PlannerImpl.java (MODIFIED - containsSubQuery guard)                    |
| FILES: OpenSearchCorrelateRule.java, OpenSearchUncollectRule.java (NEW)        |
| CHANGE NEEDED: Guard + 2 marking rules + 2 physical RelNode classes           |
|                                                                               |
| PROBLEMS (without our changes):                                               |
|                                                                               |
|   PROBLEM 1 -- Decorrelator corruption:                                       |
|   The existing removeSubQueries() phase runs RelDecorrelator unconditionally. |
|   RelDecorrelator sees our Correlate+Uncollect (which represents UNNEST) and  |
|   tries to "decorrelate" it -- pushing the Filter INTO the Uncollect leg,      |
|   destroying the shape that tryEmitUnnest() in Step 4 recognizes.             |
|   Result: the convertor sees an unrecognizable tree and isthmus emits it as   |
|   a JOIN (semantically wrong), or the plan fails entirely.                    |
|   FIX: containsSubQuery() guard -- skip decorrelator when no real subquery.   |
|                                                                               |
|   PROBLEM 2 -- Unmarked orphans (Correlate and Uncollect):                    |
|   The marking phase requires EVERY node in the tree to be converted from      |
|   a generic Logical node to an OpenSearch physical node (OpenSearchRelNode).   |
|   Without our marking rules, LogicalCorrelate and Uncollect survive the       |
|   marking pass as "orphans" -- no rule claims them, no backend is assigned.   |
|   Result: plan fails with "unmarked child [LogicalCorrelate]" during the      |
|   forking/selection phase, because no backend alternative can be created.     |
|   FIX: OpenSearchCorrelateRule and OpenSearchUncollectRule mark them           |
|   as [datafusion]-only. The backend is FORCED (not derived from children)     |
|   because UNNEST reads Parquet LIST<STRUCT> directly, which only DataFusion   |
|   can do -- normal intersection-of-children logic would produce an empty      |
|   backend set when the scan is lucene-only. The rule also widens the child    |
|   TableScan's viability to include [datafusion] so PlanForker can match.      |
|                                                                               |
| INPUT (from PPL Frontend):                                                    |
|   LogicalProject(title=$1)                                                    |
|     LogicalFilter($4 > 4)                                                     |
|       LogicalProject(rating=$1,title=$2,views=$3,author=$4,score=$5,text=$6) |
|         LogicalCorrelate(INNER, $cor0, req={0})                               |
|           LogicalTableScan(blogs)                                             |
|           Uncollect(Project($cor0.comments, oneRow))                          |
|                                                                               |
| SUB-STEP 3a: removeSubQueries()                                              |
|   containsSubQuery() = false (no RexSubQuery in this tree)                    |
|   -> decorrelator SKIPPED entirely                                            |
|   WHY THIS MATTERS: Without this guard, RelDecorrelator would push the        |
|   Filter($5>4) DOWN into the Uncollect leg, destroying the shape that         |
|   the Substrait emitter recognizes. This was a hard-to-diagnose bug.          |
|                                                                               |
| SUB-STEP 3b: pushdownRules()                                                  |
|   Standard Calcite rules run. FilterProjectTranspose merges the intermediate  |
|   projection. Plan becomes:                                                    |
|   LogicalProject(title=$2)                                                    |
|     LogicalFilter($5 > 4)                                                     |
|       LogicalCorrelate(INNER, $cor0, req={0})                                 |
|         LogicalTableScan(blogs)                                               |
|         Uncollect(...)                                                         |
|                                                                               |
| SUB-STEP 3c: Marking (HepPlanner with marking rules)                          |
|   Every Logical node -> OpenSearch physical equivalent:                        |
|     OpenSearchProject(title=$2)                        [datafusion]            |
|       OpenSearchFilter(>($5, 4))                       [datafusion]            |
|         OpenSearchCorrelate(INNER, req={0})            [datafusion]            |
|           OpenSearchTableScan(blogs)                   [lucene, datafusion]    |
|           OpenSearchUncollect(...)                     [datafusion]            |
|                                                                               |
|   OpenSearchCorrelateRule forces viableBackends=[datafusion] because the       |
|   whole UNNEST operation runs in DataFusion (reads Parquet LIST<STRUCT>).      |
|                                                                               |
| SUB-STEP 3d: CBO (VolcanoPlanner)                                             |
|   Root demands SINGLETON(COORDINATOR).                                        |
|   Correlate is a two-input operator not in the DistributionDeriveRule ->       |
|   CBO cannot derive SINGLETON through it -> inserts ExchangeReducer.          |
|                                                                               |
| OUTPUT (single RelNode tree -- NOT yet split into stages):                    |
|   OpenSearchExchangeReducer(exchange=[distributionType=SINGLETON])            |
|     OpenSearchProject(title=[$2], viableBackends=[[datafusion]])              |
|       OpenSearchFilter(>($5, 4), viableBackends=[[datafusion]])               |
|         OpenSearchCorrelate(INNER, req={0}, viableBackends=[[datafusion]])    |
|           OpenSearchTableScan(blogs, viableBackends=[[lucene, datafusion]])   |
|           OpenSearchUncollect(viableBackends=[[datafusion]])                  |
|                                                                               |
| NOTE: This is still ONE tree. The ExchangeReducer at the top is a MARKER     |
| that tells DAGBuilder (Step 3.5) WHERE to cut. The actual split into          |
| Stage 0 + Stage 1 happens in the NEXT step, not here.                        |
+------------------------------------------------------------------------------+
        |
        | (two separate fragments, one per stage)
        v
```

---
---

#### STEP 3 sub-step 3d: OpenSearchNestedFieldRewriter (detail)

```
+------------------------------------------------------------------------------+
| STAGE 2: LOGICAL REWRITE                                                      |
|                                                                               |
| FILE: OpenSearchNestedFieldRewriter.java (NEW - 313 lines)                    |
| CHANGE NEEDED: The full rewriter implementation (new file)                    |
|                                                                               |
| INPUT: The RelNode tree from Stage 1                                          |
|                                                                               |
| WHAT HAPPENS (for THIS query): nothing.                                       |
|   The tree already has a Correlate (from PPL 'expand'). No ITEM($arr,'f')     |
|   expressions remain because expand resolved them into positional $-refs.     |
|   The rewriter fires as a NO-OP.                                              |
|                                                                               |
| WHEN IT DOES FIRE -- queries where nested dotted refs are ONLY in FIELDS:      |
|                                                                               |
|   These pass PPL type-checker (ITEM in PROJECT is type-safe) so they arrive   |
|   here WITHOUT a Correlate -- just raw ITEM expressions in the Project:        |
|                                                                               |
|   Example: "source=blogs | fields title, comments.author"                     |
|   Example: "source=blogs | where views > 100 | fields title, comments.author" |
|   Example: "source=employees | fields name, skills.level"                     |
|                                                                               |
|   Input:  LogicalProject(title=$2, comments.author=ITEM($0,'author'))         |
|             LogicalTableScan(blogs)                                            |
|                                                                               |
|   Detects: ITEM($0, 'author') where $0 is ARRAY<ROW> -> nested reference     |
|                                                                               |
|   Action:                                                                     |
|   1. Injects Correlate+Uncollect below (same shape as PPL 'expand')           |
|   2. Replaces ITEM($0,'author') with plain $4 (the appended unnested column)  |
|                                                                               |
|   Output: LogicalProject(title=$2, comments.author=$4)                        |
|             LogicalCorrelate(INNER, $cor0, requiredColumns={0})                |
|               LogicalTableScan(blogs)                                         |
|               Uncollect(Project($cor0.comments, oneRow))                      |
|                                                                               |
|   FIELD-NAME COLLISION FIX: When parent has field 'name' and nested has       |
|   sub-field 'name', Calcite deduplicates to 'name0'. The rewriter's lookup   |
|   map indexes both 'name0' (deduped) and 'name' (original) -> same index.    |
|                                                                               |
| WHEN IT DOES NOT FIRE:                                                        |
|   - Queries that went through 'expand' translation (Stage 0) already have     |
|     a Correlate and no ITEM calls remain -> NO-OP                             |
|   - Normal queries with no nested refs -> NO-OP                               |
|   - This example (comments.score > 4) went through expand -> NO-OP           |
|                                                                               |
| SUMMARY OF THE TWO ENTRY POINTS INTO CORRELATE:                               |
|   +---------------------+------------------+------------------+               |
|   | Query type          | Who creates the  | Rewriter role    |               |
|   |                     | Correlate?       |                  |               |
|   +---------------------+------------------+------------------+               |
|   | Nested in WHERE/    | Stage 0 transla- | NO-OP (already   |               |
|   | STATS (filter/agg)  | tion + PPL       | has Correlate)   |               |
|   |                     | 'expand'         |                  |               |
|   +---------------------+------------------+------------------+               |
|   | Nested ONLY in      | ** REWRITER **   | FIRES: injects   |               |
|   | FIELDS (projection) | (this stage)     | Correlate from   |               |
|   |                     |                  | ITEM detection   |               |
|   +---------------------+------------------+------------------+               |
|   | No nested refs      | nobody           | NO-OP            |               |
|   +---------------------+------------------+------------------+               |
|                                                                               |
| OUTPUT (for our example): UNCHANGED (Correlate already present from expand)   |
+------------------------------------------------------------------------------+
        |
        v
```

---

#### STEP 3.5: DAG Build, Fork, and Select (DefaultPlanExecutor)

```
+------------------------------------------------------------------------------+
| STEP 3.5: DAG BUILD + FORK + SELECT                                          |
|                                                                               |
| FILES: DefaultPlanExecutor.java, DAGBuilder.java, PlanForker.java,            |
|        BackendPlanAdapter.java, PlanAlternativeSelector.java                  |
| CHANGE NEEDED: None (framework code, unmodified)                              |
|                                                                               |
| INPUT: The optimized RelNode from PlannerImpl.createPlan():                   |
|   OpenSearchExchangeReducer                                                   |
|     OpenSearchProject(title=$2)                                               |
|       OpenSearchFilter(>($5, 4))                                              |
|         OpenSearchCorrelate(INNER, req={0})                                   |
|           OpenSearchTableScan(blogs)                                          |
|           OpenSearchUncollect(...)                                             |
|                                                                               |
| WHAT HAPPENS (sequential calls in DefaultPlanExecutor):                       |
|                                                                               |
|   1. DAGBuilder.build(plan, capabilityRegistry, clusterService, ...)          |
|      Splits the plan at the ExchangeReducer boundary into stages:             |
|      OUTPUT: QueryDAG with 2 stages:                                          |
|        Stage 1 (coordinator): ExchangeReducer -> StageInputScan(childId=0)    |
|        Stage 0 (data node):   Project -> Filter -> Correlate(Scan, Uncollect) |
|                                                                               |
|   2. PlanForker.forkAll(dag)                                                  |
|      For each stage, creates plan alternatives per backend. Stage 0 has       |
|      Correlate marked [datafusion] only, so only one alternative exists.      |
|                                                                               |
|   3. BackendPlanAdapter.adaptAll(dag)                                         |
|      Adapts the plan for the chosen backend's capabilities (e.g. predicate    |
|      annotation for DataFusion delegation).                                   |
|                                                                               |
|   4. PlanAlternativeSelector.selectAll(dag)                                   |
|      Picks the DataFusion alternative (the only viable one for nested).       |
|                                                                               |
|   5. FragmentConversionDriver.convertAll(dag)                                 |
|      Calls DataFusionFragmentConvertor.convertFragment() per stage.           |
|      (This is STEP 4 below.)                                                  |
|                                                                               |
| OUTPUT: QueryDAG with stages ready for Substrait conversion.                  |
+------------------------------------------------------------------------------+
        |
        v
```

---

#### STEP 4: Substrait Emission (Stage 0 - Data Node Fragment)

```
+------------------------------------------------------------------------------+
| STAGE 4a: SUBSTRAIT EMISSION (data node fragment)                             |
|                                                                               |
| FILE: DataFusionFragmentConvertor.java (MODIFIED - visit(Correlate) override) |
| FILE: NestedParentDedupRewriter.java (NEW - 519 lines, proto post-pass)       |
| FILE: UnnestExtensionDetail.java (NEW - metadata carrier)                     |
| CHANGE NEEDED: Correlate interception + unnest extension + dedup rewriter     |
|                                                                               |
| PROBLEM (without our change):                                                 |
|   Isthmus (the Calcite->Substrait serializer) has no handler for Correlate    |
|   over Uncollect. Its default behavior emits it as a JOIN -- semantically       |
|   wrong (a JOIN does cross-product; we need per-row array explosion).          |
|   Additionally, Substrait has no first-class UNNEST operator, so even if      |
|   Isthmus recognized it, there's no standard rel to emit.                     |
|   Without the dedup post-pass: a parent with 2 matching children appears      |
|   twice in the output (dave+eve both match -> Third post returned twice).     |
|                                                                               |
| INPUT (Stage 0 fragment):                                                     |
|   OpenSearchProject(title=$2)                                                 |
|     OpenSearchFilter(>($5, 4))                                                |
|       OpenSearchCorrelate(INNER, req={0})                                     |
|         OpenSearchTableScan(blogs)                                            |
|         OpenSearchUncollect(...)                                              |
|                                                                               |
| SUB-STEP 4a-i: visit(Correlate) -> tryEmitUnnest()                            |
|   Isthmus's default would emit Correlate as a JOIN (semantically wrong).      |
|   Our override detects the Correlate(left, Uncollect) shape and emits:        |
|     ExtensionSingleRel(                                                       |
|       type_url = "unnest_reshape:comments|w=7"                                |
|       input = visitor.apply(left)   <- the converted TableScan               |
|     )                                                                         |
|   w=7 is the post-unnest column count (used to locate __row_id__).            |
|                                                                               |
| SUB-STEP 4a-ii: Isthmus serializes Filter + Project normally ABOVE            |
|   Result so far:                                                              |
|     ProjectRel(title)                                                         |
|       FilterRel(score > 4)                                                    |
|         ExtensionSingleRel("unnest_reshape:comments|w=7")                     |
|           ReadRel(table="blogs", schema=[comments,rating,title,views,rowid])  |
|                                                                               |
| SUB-STEP 4a-iii: NestedParentDedupRewriter (proto post-pass)                  |
|   Detects pattern: "parent-only projection over a filtered unnest"            |
|   (title is a parent column; the filter is on a child column above unnest)    |
|   This means: multiple matching children per parent -> parent appears twice.  |
|   FIX: Insert GROUP BY [title, __row_id__] then re-project title.             |
|                                                                               |
| OUTPUT (final Stage 0 Substrait, 388 bytes):                                  |
|   ProjectRel(emit=[0])           <- title only                                |
|     AggregateRel(                                                             |
|       groupBy=[title, __row_id__])  <- dedup: distinct parents                |
|       FilterRel(score > 4)       <- keeps children with score > 4             |
|         ExtensionSingleRel(      <- unnest marker                             |
|           "unnest_reshape:comments|w=7")                                      |
|           ReadRel(table="blogs", <- the Parquet scan                          |
|             schema=[comments, rating, title, views, __row_id__])              |
+------------------------------------------------------------------------------+
        |
        v
```

---

#### STEP 4 (coordinator): Substrait Emission (Stage 1 - Coordinator Fragment)

```
+------------------------------------------------------------------------------+
| STAGE 4b: SUBSTRAIT EMISSION (coordinator fragment)                           |
|                                                                               |
| FILE: DataFusionFragmentConvertor.java (rewriteStageInputScans method)        |
| CHANGE NEEDED: None (this method already exists on mainline)                  |
|                                                                               |
| INPUT (Stage 1 fragment):                                                     |
|   OpenSearchExchangeReducer                                                   |
|     OpenSearchStageInputScan(childStageId=0)                                  |
|                                                                               |
| WHAT HAPPENS:                                                                 |
|   rewriteStageInputScans() replaces StageInputScan with a                     |
|   StageInputTableScan whose qualified name is "input-0" (NOT "blogs").        |
|   Isthmus serializes this as a standard ReadRel.                              |
|                                                                               |
| OUTPUT (Stage 1 Substrait, 69 bytes):                                         |
|   ReadRel(named_table="input-0", base_schema=[title: VARCHAR])                |
|                                                                               |
| This is a pure passthrough -- the coordinator just reads the Arrow stream     |
| that stage 0 produces and returns it to the user.                             |
+------------------------------------------------------------------------------+
        |
        v
```

---

#### STEP 5: Exchange Setup (Rust, api.rs)

```
+------------------------------------------------------------------------------+
| STAGE 5: EXCHANGE SETUP                                                       |
|                                                                               |
| FILE: api.rs (MODIFIED - ExtensionSingle arm in collect_reads)                |
| CHANGE NEEDED: 5-line addition to collect_reads() match arms                  |
|                                                                               |
| PROBLEM (without our change):                                                 |
|   The coordinator derives stage 0's output schema by lowering stage 0's plan  |
|   on a throwaway session. To lower, it must register a stub table for every   |
|   ReadRel found in the plan. But collect_reads() only walks known rel types   |
|   (Read, Filter, Project, Aggregate...) -- NOT ExtensionSingleRel.             |
|   Our unnest wraps ReadRel("blogs") INSIDE an ExtensionSingleRel. Without     |
|   the arm: ReadRel never found -> stub never registered -> lowering fails:    |
|   "Error during planning: No table named 'blogs'"                             |
|   This was the root cause of ALL 166 errors in the initial merge attempt.     |
|                                                                               |
| INPUT: Stage 0 plan bytes (388 bytes) + Stage 1 plan bytes (69 bytes)         |
|                                                                               |
| WHAT HAPPENS (ReduceStageExecutionFactory -> DatafusionReduceSink):           |
|                                                                               |
|   1. registerPartitionStream("input-0", stage0_plan_bytes):                   |
|      Rust derive_schema_from_partial_plan():                                  |
|                                                                               |
|      a. collect_reads() walks the Stage 0 Substrait to find every ReadRel:    |
|         ProjectRel -> AggregateRel -> FilterRel -> ExtensionSingleRel         |
|           -> [OUR FIX] recurses INTO ExtensionSingleRel.input                 |
|           -> finds ReadRel(named_table="blogs")                               |
|         Without this fix: ReadRel never found -> "No table named 'blogs'" !   |
|                                                                               |
|      b. Registers empty stub MemTable "blogs" on throwaway SessionContext     |
|                                                                               |
|      c. from_substrait_plan_unnest_aware(throwaway, stage0_plan):             |
|         Resolves "blogs" -> stub table (OK!)                                  |
|         Lowers full plan including unnest -> derives output schema: [title]    |
|                                                                               |
|      d. Registers StreamingTable "input-0" with schema [title: VARCHAR]       |
|         on the REAL session context                                           |
|                                                                               |
|   2. executeLocalPlan(stage1_plan_bytes):                                     |
|      Deserializes ReadRel("input-0") -> resolves to the StreamingTable        |
|      Physical plan: reads from the mpsc channel that stage 0 feeds            |
|                                                                               |
| OUTPUT: Exchange is set up. Coordinator can receive Arrow batches.            |
|                                                                               |
| ORDERING GUARANTEE: register THEN execute (table exists before plan lowers)   |
+------------------------------------------------------------------------------+
        |
        v
```

---

#### STEP 6: DataFusion Execution (Data Node, Stage 0)

```
+------------------------------------------------------------------------------+
| STAGE 6: DATAFUSION EXECUTION (data node)                                     |
|                                                                               |
| FILE: unnest_consumer.rs (NEW - 296 lines)                                    |
| FILE: substrait_to_tree.rs (MODIFIED - filter extraction continuation)        |
| CHANGE NEEDED: Full unnest consumer + filter extraction fix                   |
|                                                                               |
| PROBLEM (without our changes):                                                |
|   1. No consumer: DataFusion's stock Substrait consumer has no handler for    |
|      ExtensionSingleRel("unnest_reshape:..."). It would error with            |
|      "Missing handler for extension single rel". The unnest_consumer.rs       |
|      provides this handler, building a native LogicalPlan::Unnest.            |
|   2. Filter loss: the shard executor tries to push filters to the Parquet     |
|      scan for row-group pruning. When it encounters an Unnest node above a    |
|      Filter, the old code ABORTED filter extraction entirely (returned None). |
|      This meant a valid parent filter (e.g. views > 100) sitting BELOW the    |
|      unnest was silently dropped -- losing 22 scalar-filter results.           |
|   3. Null semantics: DataFusion's default unnest preserves nulls (a parent    |
|      with skills=null produces one all-NULL row). OpenSearch nested semantics  |
|      require zero rows for empty/absent arrays. preserve_nulls=false fixes it.|
|                                                                               |
| INPUT: Stage 0 Substrait (388 bytes)                                          |
|                                                                               |
| SUB-STEP 6a: Substrait -> LogicalPlan (from_substrait_plan_unnest_aware)      |
|   UnnestConsumer sees ExtensionSingleRel("unnest_reshape:comments|w=7"):      |
|   - Consumes inner input (ReadRel -> registers real Parquet table)            |
|   - Duplicates array column to end, unnests duplicate in-place                |
|   - Appends struct fields, renames to match Calcite's layout                  |
|   - preserve_nulls=false: null/empty arrays -> 0 child rows                  |
|   Filter and Aggregate consumed by standard Substrait consumer.               |
|                                                                               |
| SUB-STEP 6b: Filter extraction for Parquet pushdown                           |
|   extract_filter_expr() walks the LogicalPlan:                                |
|   - Encounters Filter(score > 4) above Unnest -> post-unnest filter!          |
|   - [OUR FIX] Skips this filter (can't push 'score' to Parquet scan)          |
|     BUT CONTINUES SEARCHING DEEPER for parent filters                         |
|   - If there were a "where views > 100" below the unnest, it would be found  |
|     and pushed to the ParquetExec as a row-group predicate.                   |
|                                                                               |
| SUB-STEP 6c: Physical execution                                               |
|                                                                               |
|   ParquetExec reads 5 parent rows from Parquet:                               |
|   +-------+------+--------------+------+----------+                           |
|   |comment|rating| title        |views |__row_id__|                           |
|   +-------+------+--------------+------+----------+                           |
|   |[{a,5}.|  4.5 | First post   | 100  |    0     |                           |
|   | {b,3}]|      |              |      |          |                           |
|   |[{c,4}]|  3.8 | Second post  |  50  |    1     |                           |
|   |[{d,9}.|  4.9 | Third post   | 200  |    2     |                           |
|   | {e,7}]|      |              |      |          |                           |
|   |[{f,8}.|  4.2 | Fourth post  | 300  |    3     |                           |
|   | {g,6},|      |              |      |          |                           |
|   | {h,4}]|      |              |      |          |                           |
|   |[{i,9}]|  4.7 | Fifth post   | 150  |    4     |                           |
|   +-------+------+--------------+------+----------+                           |
|                                                                               |
|   UnnestExec (reshaping, preserve_nulls=false) -> 9 child rows:               |
|   +--------------+----------+--------+-------+                                |
|   | title        |__row_id__| author | score |                                |
|   +--------------+----------+--------+-------+                                |
|   | First post   |    0     | alice  |   5   |                                |
|   | First post   |    0     | bob    |   3   |                                |
|   | Second post  |    1     | carol  |   4   |                                |
|   | Third post   |    2     | dave   |   9   |                                |
|   | Third post   |    2     | eve    |   7   |                                |
|   | Fourth post  |    3     | frank  |   8   |                                |
|   | Fourth post  |    3     | grace  |   6   |                                |
|   | Fourth post  |    3     | helen  |   4   |                                |
|   | Fifth post   |    4     | ivan   |   9   |                                |
|   +--------------+----------+--------+-------+                                |
|                                                                               |
|   FilterExec(score > 4) -> 6 rows survive:                                    |
|   +--------------+----------+--------+-------+                                |
|   | title        |__row_id__| author | score |                                |
|   +--------------+----------+--------+-------+                                |
|   | First post   |    0     | alice  |   5   | <-- bob(3) dropped             |
|   | Third post   |    2     | dave   |   9   |                                |
|   | Third post   |    2     | eve    |   7   | <-- Third post appears TWICE   |
|   | Fourth post  |    3     | frank  |   8   |                                |
|   | Fourth post  |    3     | grace  |   6   | <-- helen(4) dropped           |
|   | Fifth post   |    4     | ivan   |   9   |                                |
|   +--------------+----------+--------+-------+                                |
|                                                                               |
|   AggregateExec(GROUP BY [title, __row_id__]) -> 4 groups (PARENT DEDUP):     |
|   +--------------+----------+                                                 |
|   | title        |__row_id__|   Third post (dave + eve) collapsed to 1 group  |
|   +--------------+----------+                                                 |
|   | First post   |    0     |                                                 |
|   | Third post   |    2     |                                                 |
|   | Fourth post  |    3     |                                                 |
|   | Fifth post   |    4     |                                                 |
|   +--------------+----------+                                                 |
|                                                                               |
|   ProjectExec(title) -> final output:                                         |
|   +--------------+                                                            |
|   | title        |                                                            |
|   +--------------+                                                            |
|   | First post   |                                                            |
|   | Third post   |                                                            |
|   | Fourth post  |                                                            |
|   | Fifth post   |                                                            |
|   +--------------+                                                            |
|                                                                               |
| OUTPUT: Arrow RecordBatch (4 rows, 1 column) streamed to coordinator          |
+------------------------------------------------------------------------------+
        |
        | Arrow batches over exchange (Flight transport)
        v
```

---

#### STEP 7: Coordinator Passthrough + Response

```
+------------------------------------------------------------------------------+
| STAGE 7: COORDINATOR + RESPONSE                                               |
|                                                                               |
| FILE: (framework - no nested-specific changes)                                |
| CHANGE NEEDED: None                                                           |
|                                                                               |
| INPUT: Arrow stream from Stage 0 (4 rows of [title])                         |
|                                                                               |
| WHAT HAPPENS:                                                                 |
|   Stage 1 plan is ReadRel("input-0") -> StreamingTable                        |
|   Drains the channel, passes rows to DefaultPlanExecutor                      |
|   Executor serializes to JSON response                                        |
|                                                                               |
| OUTPUT (HTTP response):                                                       |
|   {                                                                           |
|     "columns": ["title"],                                                     |
|     "rows": [                                                                 |
|       ["First post"],                                                         |
|       ["Third post"],                                                         |
|       ["Fourth post"],                                                        |
|       ["Fifth post"]                                                          |
|     ]                                                                         |
|   }                                                                           |
|                                                                               |
| VERIFIED: 4 parent documents that have at least one comment with score > 4.   |
+------------------------------------------------------------------------------+
```

---

#### Summary: Changes Needed at Each Stage

```
+----------+---------------------------+--------------------------------------+----------+
| Stage    | Component                 | Change Required                      | Effort   |
+----------+---------------------------+--------------------------------------+----------+
| 0        | UnifiedQueryService       | Dotted->expand translation           | Small    |
|          |                           | (schema-driven string rewrite)       | (~50 LOC)|
+----------+---------------------------+--------------------------------------+----------+
| 1        | PPL Frontend              | NONE (expand already exists)         | Zero     |
+----------+---------------------------+--------------------------------------+----------+
| 2        | NestedFieldRewriter       | NEW file: detect ITEM on ARRAY,      | Medium   |
|          |                           | inject Correlate+Uncollect,          | (313 LOC)|
|          |                           | handle field-name collision dedup    |          |
+----------+---------------------------+--------------------------------------+----------+
| 3        | PlannerImpl               | containsSubQuery() guard             | Small    |
|          |                           | (1 conditional, ~10 lines)           |          |
|          +---------------------------+--------------------------------------+----------+
|          | OpenSearchCorrelateRule   | NEW: marking rule + physical node    | Small    |
|          | OpenSearchUncollectRule   | NEW: marking rule + physical node    | (2x ~80) |
+----------+---------------------------+--------------------------------------+----------+
| 4a       | DataFusionFragmentConv.   | visit(Correlate) override +          | Medium   |
|          |                           | tryEmitUnnest() (~40 lines)          |          |
|          +---------------------------+--------------------------------------+----------+
|          | NestedParentDedupRewriter | NEW: proto post-pass for parent      | Large    |
|          |                           | dedup (GROUP BY injection)           | (519 LOC)|
|          +---------------------------+--------------------------------------+----------+
|          | UnnestExtensionDetail     | NEW: metadata carrier                | Small    |
|          |                           |                                      | (69 LOC) |
+----------+---------------------------+--------------------------------------+----------+
| 4b       | FragmentConvertor         | NONE (rewriteStageInputScans         | Zero     |
|          |                           | already exists on mainline)          |          |
+----------+---------------------------+--------------------------------------+----------+
| 5        | api.rs (collect_reads)    | Add ExtensionSingle match arm        | Tiny     |
|          |                           | (5 lines)                            |          |
+----------+---------------------------+--------------------------------------+----------+
| 6        | unnest_consumer.rs        | NEW: Substrait consumer for the      | Medium   |
|          |                           | unnest_reshape extension rel         | (296 LOC)|
|          +---------------------------+--------------------------------------+----------+
|          | substrait_to_tree.rs      | Fix filter extraction to continue    | Small    |
|          |                           | past unnest for parent filters       | (~8 LOC) |
+----------+---------------------------+--------------------------------------+----------+
| 7        | Coordinator               | NONE                                 | Zero     |
+----------+---------------------------+--------------------------------------+----------+
```

### 4.4 Why `Correlate + Uncollect` as the canonical shape

Calcite has no single "Unnest" RelNode. The idiomatic representation -- and the
one the PPL `expand` command already produces -- is a `Correlate` (a
row-at-a-time nested-loop join) whose right side is an `Uncollect` over a
correlated field access. Reading it operationally:

```
Correlate (cor0)
|-- Scan(blogs)                      <- left: parent rows
+-- Uncollect                        <- right: per-parent, explode
    +-- Project($cor0.comments)         the correlated array field
```

For each left row, the right side is evaluated with `$cor0` bound to that row,
producing one output row per array element; `Correlate` glues the parent
columns to each. This is precisely UNNEST-with-parent-context.

Choosing this shape (rather than inventing a custom `LogicalUnnest` RelNode)
means:

- The PPL `expand` path and the rewriter path converge on **identical plans**,
  so everything downstream (marking, CBO, serialization) is written once.
- We inherit Calcite's existing type derivation for `Uncollect` (array element
  row type flattening) for free.
- The cost is that we must **protect** the `Correlate` from Calcite's
  decorrelator, which assumes every `Correlate` came from a subquery and tries
  to rewrite it into joins -- corrupting our shape. Hence the
  `containsSubQuery()` guard in `PlannerImpl` (Section 6.3).

### 4.5 Child identity: `(parent_row_id, child_index)`

Per the HLD, child identity must survive expansion. In this design the parent
identity is the synthetic `__row_id__` column (already present for every
Parquet row); the child index is positional within the unnested batch. The
`NestedParentDedupRewriter` relies on `__row_id__` to group children back to
parents: `GROUP BY [parent_cols, __row_id__]`. Grouping on `__row_id__` alone
would suffice for identity, but including the projected parent columns in the
key lets the aggregate directly produce the output row without a re-join.

---

## 5. Detailed Flow: A Worked Example

We trace one query end to end, showing the plan and the data at each stage.

**Query:**

```
source=blogs | where comments.score > 4 | fields title
```

**Data:** index `blogs`, 5 documents, each with a `comments` array of
`{author, score}` structs:

```
row  title         comments
---  -----------   -----------------------------------------------
0    First post    [{alice,5},{bob,2}]
1    Second post   [{carol,3}]
2    Third post    [{dave,5},{erin,4},{frank,5}]
3    Fourth post   [{grace,5}]
4    Fifth post    [{heidi,1},{ivan,5}]
```

Expected answer (documents where ANY comment has score > 4):
rows 0, 2, 3, 4 -> `First post, Third post, Fourth post, Fifth post`.
Note row 2 has TWO qualifying comments (dave:5, frank:5) -- the dedup step
must ensure "Third post" appears once, not twice.

### Step 0 -- Translation: dotted path -> expand syntax

The raw query does not survive the PPL frontend as written: the type checker
sees `comments.score` resolve (via the ARRAY(ROW) schema) such that the
comparison becomes `ROW > INTEGER` and rejects it. Rather than fork the PPL
grammar/type-checker (an external component with its own release cadence), the
translation layer in `UnifiedQueryService` rewrites dotted nested references
into the semantically equivalent `expand` form:

```
source=blogs | where comments.score > 4 | fields title
    -- becomes -->
source=blogs | expand comments | where score > 4 | fields title
```

This is a *syntactic* transformation driven by the mapping (we know `comments`
is a nested path). After `expand`, `score` is a plain flat column, so the rest
of the query type-checks with zero changes. Section 11.1 discusses where this
translation should permanently live.

### Step 1 -- PPL Frontend builds the logical plan

`UnifiedQueryPlanner` processes the translated query. `expand comments`
produces the `Correlate + Uncollect` pair; the subsequent `where` and `fields`
become an ordinary Filter and Project over the *flat* post-expand schema:

```
LogicalProject(title)
+-- LogicalFilter(score > 4)
    +-- LogicalCorrelate(cor0)
        |-- LogicalTableScan(blogs)
        +-- Uncollect
            +-- LogicalProject($cor0.comments)
```

Logical row counts: Scan emits 5 parent rows; Correlate/Uncollect emit 9 child
rows (2+1+3+1+2); Filter keeps 5 child rows (alice:5, dave:5, frank:5, grace:5,
ivan:5). Note the duplicate parent problem is now visible: two of those 5
surviving child rows belong to "Third post".

### Step 2 -- PlannerImpl

Three things matter here:

1. **`containsSubQuery()` guard.** Mainline unconditionally runs the
   decorrelator, which sees our `Correlate` and mangles it into a join shape
   that no longer means UNNEST. The guard checks whether the plan actually
   contains a `RexSubQuery`; if not (our case), decorrelation is skipped and
   the `Correlate` survives intact. If a real subquery *is* present,
   decorrelation runs exactly as before -- no behavior change for existing
   queries.
2. **Pushdown rules** run normally. (The `score > 4` filter is *above* the
   unnest so it cannot push into the scan, but parent-level filters can and
   do -- see Fix 2 in Section 8.)
3. **The rewriter is a no-op here** -- `expand` already produced the
   Correlate + Uncollect, and `OpenSearchNestedFieldRewriter` only fires when
   it finds raw `ITEM($arr,'field')` references that have NOT been expanded.
   The rewriter and the expand path are alternate producers of the same shape.

### Step 3 -- Marking + CBO

`OpenSearchCorrelateRule` and `OpenSearchUncollectRule` convert the logical
nodes to `OpenSearchCorrelate` / `OpenSearchUncollect` in the physical
convention and force the **DataFusion backend** for the subtree (the Lucene
backend has no concept of unnest; sending this subtree there would be
unexecutable). CBO then inserts an `ExchangeReducer`, splitting the plan into
the standard two-stage distributed form:

```
Stage 1 (coordinator):  Project(title) <- passthrough over exchange input
        ^
        | exchange (Arrow)
Stage 0 (data nodes):   Correlate/Uncollect -> Filter -> [dedup agg] -> Project
```

### Step 4a -- Stage 0 fragment: Substrait serialization

`DataFusionFragmentConvertor.visit(Correlate)` intercepts the Correlate before
Isthmus can see it (Isthmus cannot serialize a Correlate at all) and calls
`tryEmitUnnest`, which:

- verifies the right side is the recognized `Uncollect(Project($cor.field))`
  shape;
- emits `ExtensionSingleRel` with detail
  **`"unnest_reshape:comments|w=7"`** -- the column to unnest and the
  post-unnest output width (7 = parent columns + flattened child struct
  columns), wrapped via `UnnestExtensionDetail`;
- serializes the *input* (the parent scan) as the extension rel's single child,
  so from Substrait's perspective this is just an opaque unary operator over a
  ReadRel.

The Filter (`score > 4`) and Project above it serialize through Isthmus
normally -- they reference flat columns of the post-unnest schema and need no
special handling.

Then `NestedParentDedupRewriter` runs as a **post-pass over the Substrait
proto**: it detects the pattern "unnest extension + filter + parent-only
projection" and injects an `AggregateRel` with
`GROUP BY [title, __row_id__]` between the filter and the final projection.
This is what turns "5 matching child rows" into "4 distinct parent rows".
(Why a proto post-pass instead of a Calcite rule -- see Section 7.4.)

### Step 4b -- Stage 1 fragment

The coordinator fragment is trivial: `StageInputScan -> ReadRel("input-0")`,
i.e., read the exchange and pass rows through. No nested awareness needed.

### Step 5 -- Exchange setup (Rust, `api.rs`)

Before execution, the Rust side walks each fragment with `collect_reads()` to
find every `ReadRel`, register exchange stubs, and derive the schema flowing
between stages. Mainline's walker did not recurse into `ExtensionSingleRel`,
so the stage-0 ReadRel was invisible and exchange setup failed (Fix 1,
Section 8). With the new `ExtensionSingle` arm, the ReadRel under the unnest
wrapper is found, the stub is registered, and the inter-stage schema
(`title: utf8`) is derived correctly.

### Step 6 -- Data node execution

`unnest_consumer.rs` sees the `ExtensionSingleRel`, parses
`"unnest_reshape:comments|w=7"`, and builds `LogicalPlan::Unnest` over the
child plan with `preserve_nulls=false` (a parent with no comments produces no
child rows -- matching OpenSearch nested-filter semantics, where a document
with an empty nested array can never match a nested predicate). The physical
plan on each data node:

```
ParquetExec(blogs)                          -> 5 rows (LIST<STRUCT> intact)
  -> ReshapingUnnest(comments,              -> 9 rows (flat: title, author,
       preserve_nulls=false)                    score, __row_id__, ...)
  -> FilterExec(score > 4)                  -> 5 rows (2 of them row_id=2)
  -> AggregateExec(GROUP BY                 -> 4 rows (parent dedup:
       [title, __row_id__])                     row_id 2 collapses)
  -> ProjectExec(title)                     -> 4 rows
```

Data at each stage, concretely:

```
after Unnest (9 rows):                  after Filter (5 rows):
row_id title        author score       row_id title        author score
0      First post   alice  5           0      First post   alice  5
0      First post   bob    2           2      Third post   dave   5
1      Second post  carol  3           2      Third post   frank  5
2      Third post   dave   5           3      Fourth post  grace  5
2      Third post   erin   4           4      Fifth post   ivan   5
2      Third post   frank  5
3      Fourth post  grace  5           after GROUP BY [title,row_id] (4 rows):
4      Fifth post   heidi  1           First post / Third post /
4      Fifth post   ivan   5           Fourth post / Fifth post
```

### Step 7 -- Coordinator

Stage 1 is a passthrough. Final response:

```
[First post, Third post, Fourth post, Fifth post]
```

Correct set, correct cardinality (Third post exactly once), and every operator
in the pipeline was a stock planner/DataFusion operator except the one
extension rel and its consumer.

---

## 6. Changes by Layer

All Java paths are under
`sandbox/plugins/analytics-backend-datafusion/src/main/java/org/opensearch/be/datafusion/`
unless noted; Rust paths are in the DataFusion backend crate.

### 6.1 `OpenSearchNestedFieldRewriter.java` (NEW, 313 lines)

The generic logical rewrite. Walks the plan looking for `ITEM($arr, 'field')`
RexCalls inside `Project` and `Filter` nodes where `$arr` is an
`ARRAY(ROW(...))` column from the scan. When found, it injects the
`Correlate + Uncollect` pair under the referencing operator and rewrites the
`ITEM` calls into direct references to the flattened child columns. Key
properties:

- **One UNNEST per nested path** (HLD requirement 5): all `ITEM` references to
  the same array column share a single injected Correlate, so
  `comments.author = 'alice' AND comments.score > 4` evaluates both predicates
  against the *same* child row -- correlated child semantics, matching
  OpenSearch `nested` query behavior.
- **Collision-safe naming**: flattened child column names are deduplicated
  against parent column names *before* Calcite's own dedup can rename them
  (Fix 3, Section 8).
- Acts as the safety net for entry points that do not go through PPL `expand`
  (direct SQL, future DSL translation).

### 6.2 `OpenSearchCorrelate.java`, `OpenSearchCorrelateRule.java`, `OpenSearchUncollect.java`, `OpenSearchUncollectRule.java` (NEW)

Physical counterparts and marking rules for the two logical nodes. The rules
participate in the standard marking pass; `OpenSearchCorrelateRule`
additionally **forces the DataFusion backend** for the subtree, because unnest
is only executable there. Without these, the marking pass would either fail to
assign a convention (plan error) or route to Lucene (unexecutable).

### 6.3 `PlannerImpl.java` (MODIFIED)

Adds `containsSubQuery()`: a cheap RexShuttle scan for `RexSubQuery` before the
decorrelation step. Decorrelation now runs **only** when a genuine subquery is
present. This is the minimal intervention that protects the expand/rewriter
`Correlate`; queries with real subqueries are planned exactly as on mainline.

### 6.4 `DataFusionFragmentConvertor.java` (MODIFIED)

Adds a `visit(Correlate)` override. When the Correlate matches the unnest shape
(`tryEmitUnnest`), it emits
`ExtensionSingleRel("unnest_reshape:<col>|w=N")` with the parent input as the
child; otherwise it falls through to the mainline error path (so unsupported
correlates fail loudly rather than silently misserializing).

### 6.5 `UnnestExtensionDetail.java` (NEW)

Typed carrier for the extension detail string -- the unnest column name and
post-unnest width -- packed into the `ExtensionSingleRel`'s `detail` Any. Kept
as a simple parseable string (`unnest_reshape:comments|w=7`) rather than a new
proto message to avoid a cross-repo proto change during the POC; Section 11.5
covers the upstream path that removes this entirely.

### 6.6 `NestedParentDedupRewriter.java` (NEW, 519 lines)

Substrait **proto post-pass** (runs after Isthmus, before the plan ships).
Detects the filter-query pattern -- unnest extension present, filter over
child columns, final projection of parent-only columns -- and injects an
`AggregateRel` with `GROUP BY [parent_cols, __row_id__]` to restore distinct-
parent semantics. It carefully remaps field indices for every operator above
the injection point. It deliberately does NOT fire for aggregation queries
(where child rows are the correct aggregation input) or for queries that
project child columns (where child rows are arguably the correct output --
see Discussion Point 3).

### 6.7 `NestedRewriteFlag.java` (NEW)

System-property kill switch. When disabled, the rewriter, the convertor
override, and the dedup pass all become no-ops and nested queries fail the
same way they do on mainline. Standard operational hygiene for a new query
path: one flag flip reverts to known behavior without a redeploy.

### 6.8 `UnifiedQueryService.java` (MODIFIED)

Two changes: (a) the dotted-path -> `expand` translation (Step 0), and (b)
**removal of the N1 bypass** -- the early-POC branch that recognized specific
nested query shapes and hand-built Substrait for them. Its removal is what
makes this a one-path design.

### 6.9 `unnest_consumer.rs` (NEW, 296 lines)

Rust Substrait consumer for the extension rel. Parses the detail string,
validates the declared width against the actual post-unnest schema (defense
against Java/Rust schema drift), and constructs `LogicalPlan::Unnest` with
`preserve_nulls=false`. Registered in the extension-rel dispatch table keyed
on the `unnest_reshape:` prefix.

### 6.10 `api.rs` (MODIFIED)

`collect_reads()` gains an `ExtensionSingle` match arm that recurses into the
child. Required for exchange setup to find the ReadRel under the unnest
wrapper (Fix 1).

### 6.11 `substrait_to_tree.rs` (MODIFIED)

`extract_filter_expr` -- the routine that harvests filters for Parquet
pushdown -- previously stopped when it hit a node it could not push. Post-
unnest filters (child-column predicates) are correctly non-pushable, but the
old behavior also discarded *pushable parent filters* below them. The fix:
skip the post-unnest filter but **continue traversing** toward the scan,
collecting any parent-level filters on the way (Fix 2).

### 6.12 Deleted files

`N1SubstraitBuilder.java`, `N1QueryAnalyzer.java`,
`CorrelateUncollectRewriter.java`, `N1Descriptor.java`, `N1Aggregate.java`,
`N1Predicate.java`, `NestedPocOverride.java`. All were shape-recognition /
hand-built-plan scaffolding from the earlier POC, fully superseded by the
generic path.

---

## 7. Key Design Decisions

### 7.1 One generic path, not shape recognition

**Decision:** delete the query-shape analyzer and route everything through the
standard planner.

**Alternative considered:** keep `N1QueryAnalyzer` + `N1SubstraitBuilder` --
recognize "nested filter", "nested terms agg", etc., and emit hand-built
Substrait per shape.

**Why rejected:** the analyzer approach is a whack-a-mole generator. Every
composition (nested filter + parent filter, nested agg + sort, multi-level
nesting, ...) needs explicit recognition, and unrecognized shapes either error
or silently fall through to wrong results. The generic path gets composition
*for free* -- the 275-case test matrix (Section 9) covers combinations no one
enumerated by hand, and 97.1% pass through pure operator composition.
**Trade-off:** the generic path required touching more layers (planner guard,
marking rules, convertor, Rust consumer) and debugging integration issues
(Section 8) that the bypass never hit because it bypassed them. That is a
one-time cost; the analyzer's cost is permanent and per-feature.

### 7.2 Reuse `expand`'s Correlate+Uncollect rather than a new UNNEST RelNode

**Decision:** canonicalize on the shape the PPL frontend already emits.

**Alternative:** define `LogicalUnnest` / `PhysicalUnnest` RelNodes.

**Why:** two producers (expand, rewriter) converging on one shape means one
marking rule set, one convertor path, one consumer. A custom RelNode would be
marginally cleaner in the convertor (no shape-matching on Correlate) but would
require the expand path to be rewritten to emit it too, and would forfeit
Calcite's existing Uncollect type derivation. **Trade-off:** we must guard the
decorrelator (7.3) and shape-match in the convertor; both are small and
localized.

### 7.3 Guard the decorrelator instead of teaching it about unnest

**Decision:** `containsSubQuery()` -- skip decorrelation when no `RexSubQuery`
exists in the plan.

**Alternatives:** (a) mark our Correlate with a custom trait the decorrelator
ignores; (b) patch the decorrelator to recognize the Uncollect right-side and
leave it alone.

**Why:** the guard is ~20 lines, semantically precise (decorrelation exists
*only* to eliminate subqueries; a plan without subqueries cannot need it), and
zero-risk for existing queries. Options (a)/(b) modify shared Calcite-adjacent
machinery with a much larger blast radius. **What happens when** a query has
*both* a real subquery and an expand Correlate? Decorrelation runs and could
still touch our Correlate -- this combination is currently untested and is
flagged in Section 10 as a known edge.

### 7.4 Parent dedup as a Substrait proto post-pass, not a Calcite rule

**Decision:** `NestedParentDedupRewriter` operates on the serialized proto.

**Alternative:** inject the dedup Aggregate as a Calcite rewrite before
marking.

**Why:** whether dedup is needed depends on the *final* plan shape (is the
output parent-only? is there already an aggregate consuming child rows?),
which is only stable after CBO and fragment splitting. A Calcite-time rule
would have to predict what CBO does to the plan, and the injected Aggregate
would itself perturb CBO costing and marking. Operating on the frozen proto
sees exactly what will execute. **Trade-off:** proto-level index remapping is
fiddly (it is the largest new file, 519 lines) and the pass is another thing
that must stay in sync with the convertor's output conventions. If/when
Substrait grows native UNNEST support (11.5), this pass can move into the
convertor proper.

### 7.5 `ExtensionSingleRel` string protocol for the unnest

**Decision:** carry unnest via `ExtensionSingleRel` with a versioned string
detail (`unnest_reshape:<col>|w=N`).

**Alternative:** define a proper Substrait extension proto message, or wait
for upstream Substrait UNNEST.

**Why:** the string protocol required zero proto changes across the Java/Rust
boundary and is explicitly a stopgap -- the `w=N` width doubles as a checksum
the Rust consumer validates against its computed schema, catching drift early.
Section 11.5 is the exit plan.

### 7.6 `preserve_nulls=false`

**Decision:** parents with empty/null nested arrays vanish at the unnest.

**Why:** this matches OpenSearch nested-query semantics -- a document with no
`comments` can never match a `nested` filter on `comments`, and contributes
nothing to a `nested` aggregation. If a future query shape needs LEFT-JOIN-
style unnest (e.g., "all posts, with avg comment score or null"), the flag is
already plumbed and can be set per-plan.

### 7.7 Translation layer for dotted paths

**Decision:** translate dotted nested references to `expand` syntax in
`UnifiedQueryService`, before the PPL frontend.

**Why:** the PPL type checker rejects `ROW > INTEGER` and lives in an external
repo (sql-plugin); forking it blocks on an external release. The translation
is mapping-driven, mechanical, and confined to one method. It is explicitly a
POC placement -- Discussion Point 1 asks the team where it should live
permanently.

---

## 8. The Three Critical Fixes

These three bugs were found during hardening (running the full 275-case
matrix) and each was invisible in single-operator testing. They are documented
in detail because each represents a *class* of integration hazard for anyone
extending this path.

### Fix 1: `collect_reads` did not recurse into `ExtensionSingleRel` (`api.rs`)

**Symptom:** every distributed nested query failed at exchange setup -- the
coordinator could not find the stage-0 `ReadRel`, so no exchange stub was
registered and no inter-stage schema could be derived.

**Root cause:** `collect_reads()` pattern-matches Substrait rel variants and
recurses into known containers. `ExtensionSingleRel` was not a known
container, so traversal silently stopped at the unnest wrapper and the
`ReadRel` beneath it was never visited.

**Fix:** add an `ExtensionSingle` arm that recurses into the child.

**Lesson:** any generic proto walker on the Rust side must be audited when a
new extension rel is introduced; "unknown node -> stop" defaults fail silently.

### Fix 2: `extract_filter_expr` dropped pushable parent filters (`substrait_to_tree.rs`)

**Symptom:** queries combining a parent filter with a nested predicate (e.g.
`where category = 'tech' and comments.score > 4`) returned correct results but
scanned far more data than necessary -- and in some shapes, the parent filter
was lost entirely from the pushdown set.

**Root cause:** the filter extractor walked down from the root collecting
pushable filters and **stopped at the first non-pushable node**. The post-
unnest child filter is correctly non-pushable (it references columns that do
not exist until after the unnest), but stopping there meant a pushable parent
filter sitting *below* the unnest was never reached.

**Fix:** on encountering a post-unnest filter, skip it (leave it for in-memory
execution) but **continue traversing** toward the scan, collecting parent-
level filters for Parquet pushdown.

**Lesson:** "stop at first failure" traversals conflate "this node is not
pushable" with "nothing below this node is pushable"; unnest is exactly the
operator that breaks that assumption.

### Fix 3: Calcite field-name dedup broke `ITEM` lookup (`OpenSearchNestedFieldRewriter.java`)

**Symptom:** queries where a child field shared a name with a parent field
(e.g. parent `name` and `comments.name`) failed with a field-not-found during
rewrite.

**Root cause:** when the flattened child columns joined the parent row type,
Calcite's automatic field-name deduplication renamed the collision (`name` ->
`name0`). The rewriter's subsequent `ITEM($arr,'name')` rewrite looked up the
child field by its *original* name and missed.

**Fix:** the rewriter performs its own deterministic collision dedup when
constructing the post-unnest row type and records the final names, so `ITEM`
rewrites target the actual post-dedup field name.

**Lesson:** never assume field names survive a row-type merge in Calcite;
always resolve by the names of the *constructed* type, not the source type.

---

## 9. Test Results

**267 / 275 passing (97.1%), 0 errors** (an error meaning an exception or
plan failure; all 8 remaining failures are wrong-result or semantic-choice
mismatches, not crashes).

The matrix covers: nested filters (all comparison operators, AND/OR
combinations, correlated multi-field child predicates), nested aggregations
(count/sum/avg/min/max, group-by on nested fields, group-by parent field with
nested agg), multi-level nesting, null/empty-array edge cases, parent+nested
filter combinations, and projection variations -- against both single-shard
and multi-shard (exchange) topologies.

### The 8 remaining failures

| # | Count | Case | Cause | Layer |
|---|---|---|---|---|
| 1 | 5 | `stats count()` over nested-bearing index | Lucene backend counts **physical rows**, which after block-expansion includes child rows; expected count is logical (parent) documents | Backend contract mismatch, not the rewrite path |
| 2 | 2 | Nested filter + nested-field projection | Semantic choice: we return distinct parents; the test expects one row per matching child (SQL UNNEST convention) | Deliberate; see Discussion Point 3 |
| 3 | 1 | Mixed parent + nested aggregation in one query | Requires a two-branch plan (aggregate parents at parent grain AND children at child grain, then join); single-pipeline plan cannot express both grains | Known limitation; Section 10 |

Note that failure class 1 is not a defect in this design at all -- those
queries never enter the unnest path; they expose a pre-existing question about
what `count()` means over block-expanded storage (Discussion Point 2).

---

## 10. Known Limitations & Open Questions

1. **Mixed-grain aggregation** (failure class 3): `stats count(), avg(comments.score)`
   needs parent-grain and child-grain aggregates in one query. Requires a
   two-branch plan (shared scan, one branch unnested, join on nothing/scalar
   combine). Planner support for this is a designed follow-up, not attempted
   in this branch.
2. **Subquery + expand coexistence**: the `containsSubQuery()` guard disables
   decorrelation skipping when a real subquery is present; a query containing
   BOTH a subquery and an expand Correlate could still have its Correlate
   decorrelated. Untested combination; needs either trait-based protection or
   a targeted decorrelator patch before GA.
3. **Multiple distinct nested paths in one query** (`comments.score > 4 AND
   tags.name = 'x'`): requires two independent unnests. The rewriter's
   one-unnest-per-path logic supports it structurally, but the dedup rewriter's
   pattern matching currently assumes a single unnest; multi-array is untested
   (Discussion Point 6).
4. **Single-shard exchange overhead**: CBO always inserts the ExchangeReducer,
   so even a one-shard index pays a serialize/deserialize round trip
   (Discussion Point 4).
5. **`ExtensionSingleRel` string protocol** is a stopgap; schema drift between
   Java emission and Rust consumption is caught only by the `w=N` width check
   (Discussion Point 5).
6. **DSL entry point**: `nested` query JSON is not yet translated into this
   path; only SQL/PPL is wired.
7. **Inner hits / child identity exposure**: `(parent_row_id, child_index)` is
   preserved internally but not yet surfaced in responses.

---

## 11. Discussion Points for the Team

These are the decisions where reviewer input will change the code.

### 11.1 Where should dotted -> expand translation live?

Current: string/AST translation in `UnifiedQueryService`.
Options:
- **(a) Keep in `UnifiedQueryService`** -- ships now, engine-local, but it is a
  pre-parser textual layer and fragile to PPL syntax evolution.
- **(b) Fix in sql-plugin** -- teach the PPL type checker that
  `ARRAY(ROW).field` in a predicate context implies element-wise semantics
  (or auto-insert expand in the AST). Correct long-term home, but external
  repo, external release train.
- **(c) Require explicit `expand`** -- no translation; users write
  `expand comments | where score > 4`. Zero magic, but breaks the promise that
  nested fields "just work" like OpenSearch DSL users expect.

Recommendation: (a) now with (b) as the tracked follow-up.

### 11.2 `count()` semantics over N1/block-expanded storage

Should `count()` mean logical documents everywhere? Options: rewrite `count()`
at the query layer to count parents (e.g., count distinct `__row_id__` /
count where `child_index == 0`), or define a storage-layer contract that scans
expose logical row counts. This decision also resolves the 5 Lucene-backend
failures, which are outside this branch's code.

### 11.3 Child rows vs parent rows for filter + nested projection

`where comments.score > 4 | fields title, comments.author`: SQL UNNEST
convention returns one row per matching child (`Third post` twice, with dave
and frank); OpenSearch convention returns matching *documents*. We currently
dedup to parents when the projection is parent-only and are inconsistent
pressure-tested when child fields are projected (the 2 semantic failures).
Need a ruling: follow SQL when child columns are projected, OpenSearch
otherwise? Always OpenSearch? Make it a query option?

### 11.4 Single-shard exchange elimination

Performance follow-up: teach CBO/distribution derivation to elide the
ExchangeReducer when the source is single-shard (or shard-local final
aggregation is provably correct). Not nested-specific but nested queries
(agg-heavy) feel it most.

### 11.5 Substrait UNNEST upstream

The `ExtensionSingleRel("unnest_reshape:...")` string protocol should be
replaced by (in order of preference): a first-class Substrait unnest relation
(engage upstream), or a proper extension proto message shared by the Java
emitter and Rust consumer. Either eliminates the width-checksum hack and the
bespoke consumer parsing.

### 11.6 Multi-array queries and same-child correlation

Two related semantics questions: (a) queries touching two different nested
paths (two unnests -- cross-product semantics? sequential?); (b) confirming
that our one-unnest-per-path rule fully matches OpenSearch `nested` query
correlation for every operator combination (NOT/OR over correlated child
predicates is the classic trap). Proposal: extend the test matrix with an
OpenSearch-DSL differential harness before GA.

---

## 12. Appendix

### 12.1 Reproduction steps

```
# 1. Check out the branch
git clone git@github.com:ANUSVTT/OpenSearch.git && cd OpenSearch
git checkout shreanu/nested-poc-search-rewrite

# 2. Run OpenSearch from source with the DataFusion backend sandbox plugin
./gradlew run

# 3. Create the test index and load the worked-example data
#    (mapping: title keyword, comments nested {author keyword, score integer})
#    Index the 5 blog documents from Section 5.

# 4. Issue the worked-example query via the PPL endpoint
POST /_plugins/_ppl
{ "query": "source=blogs | where comments.score > 4 | fields title" }
# Expected: First post, Third post, Fourth post, Fifth post

# 5. Full matrix
python3 run-nested-275-tests.py     # in the repo root
# Reports: nested-250-generic-path-report.html

# 6. Kill switch (reverts nested rewrite path entirely)
#    Start with -DNestedRewriteFlag=false (see NestedRewriteFlag.java)
```

### 12.2 Key file index

Java (under `sandbox/plugins/analytics-backend-datafusion/.../datafusion/`):

```
OpenSearchNestedFieldRewriter.java   NEW   313  ITEM detection -> Correlate+Uncollect
OpenSearchCorrelate.java             NEW        physical Correlate node
OpenSearchCorrelateRule.java         NEW        marking rule, forces DataFusion
OpenSearchUncollect.java             NEW        physical Uncollect node
OpenSearchUncollectRule.java         NEW        marking rule
UnnestExtensionDetail.java           NEW        extension-rel metadata carrier
NestedParentDedupRewriter.java       NEW   519  proto post-pass, parent dedup
NestedRewriteFlag.java               NEW        kill switch
DataFusionFragmentConvertor.java     MOD        visit(Correlate) -> ExtensionSingleRel
PlannerImpl.java                     MOD        containsSubQuery() decorrelator guard
UnifiedQueryService.java             MOD        dotted->expand translation; bypass removed
```

Rust:

```
unnest_consumer.rs                   NEW   296  ExtensionSingleRel -> LogicalPlan::Unnest
api.rs                               MOD        collect_reads recurses into ExtensionSingle
substrait_to_tree.rs                 MOD        parent-filter extraction past unnest
```

### 12.3 Glossary

- **N1**: the HLD storage approach -- one Parquet row per logical document,
  nested fields as `LIST<STRUCT>` columns.
- **UNNEST**: relational operator exploding an array column into one row per
  element, replicating the other columns.
- **Correlate**: Calcite RelNode for a row-at-a-time dependent join; with an
  `Uncollect` right side it encodes UNNEST-with-parent-context.
- **Uncollect**: Calcite RelNode converting a collection-typed input into rows.
- **ITEM**: Calcite's element/field access operator; `ITEM($arr,'field')` is
  how a dotted reference into an array-of-struct column appears in RexCalls.
- **Marking**: Mustang's planner phase assigning subtrees to execution backends
  (Lucene vs DataFusion) via physical conversion rules.
- **ExtensionSingleRel**: Substrait's escape hatch -- a unary relation with an
  opaque payload, used here to carry the unnest until Substrait has a native
  representation.
- **Isthmus**: the Calcite-to-Substrait serialization library used by
  `DataFusionFragmentConvertor`.
- **ExchangeReducer**: the operator CBO inserts to split a plan into a
  data-node stage and a coordinator stage connected by an Arrow exchange.
- **Block expansion**: the Lucene-compatible read path that materializes child
  rows physically (relevant to the `count()` failures; not part of this path).
- **`__row_id__`**: synthetic per-Parquet-row identifier; serves as the parent
  identity for dedup and as the `parent_row_id` half of child identity.
- **Parent dedup**: collapsing post-filter child rows back to distinct parent
  documents via `GROUP BY [parent_cols, __row_id__]`.
- **Kill switch**: `NestedRewriteFlag` system property disabling the entire
  nested rewrite path at runtime.

---

*End of document.*
