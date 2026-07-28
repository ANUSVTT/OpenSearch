# Parent-Semantics Fix Plan

## Problem
`where subs.views > 40 | stats sum(count)` returns 5 on Mustang (child semantics: the
auto-expand flattens and the parent field is summed once per matching child) but 3 on
vanilla (parent semantics: the nested filter is an existence check). Verified live:
vanilla=3/expand=5 on `/_plugins/_ppl`; the parent-restoring plan produces 3 on Mustang's
own Parquet via datafusion-cli.

## Design (chosen over the BOOL_OR alternative)
The dedup machinery in NestedParentDedupRewriter already implements parent-restore for two
shapes (count(), parent-only projection) by inserting `Aggregate(GROUP BY __row_id__ ...)`
over the filter/reshape chain. The Filter below the group-by removes non-matching child
rows; the group-by collapses the survivors to one row per parent — which IS the existence
semantics. No new aggregate function needed.

Two gaps to close:
1. **The general aggregate shape** (`sum/avg/min/max(parent_col)`): tryCountDedup bails when
   the measure has arguments. Generalize: when an ungrouped aggregate's input is a
   PARENT-only projection (every emitted column < scanWidth), apply the docs-dedup transform
   to that projection (group by [parent cols + __row_id__], re-project) and keep the user's
   aggregate above it, now running over true parent rows.
2. **Dotted vs explicit expand**: both arrive as identical plans (the front-end translates
   dotted→expand text). Semantics differ: dotted = parent, explicit expand = child (per
   reviewer feedback: expand is the user opting INTO the flatten). Carry the distinction via
   NestedQueryOrigin (ThreadLocal set from QueryRequestContext.querySource — the ORIGINAL
   pre-translation text — around FragmentConversionDriver.convertAll). Gate ALL dedup shapes
   on !isExplicitExpand().

## Behavior changes
| Query | Before | After |
|---|---|---|
| dotted `where subs.views>40 \| stats sum(count)` | 5 (wrong) | 3 (= vanilla) |
| dotted `... \| stats count()` | 2/3 (dedup) | unchanged |
| dotted `... \| fields name` | parents once | unchanged |
| explicit `expand subs \| where views>40 \| stats count()` | parent count (deduped) | CHILD count (5) — expand now honors user's flatten request |
| explicit `expand ... \| fields name` | deduped | child rows — same reason |
| `stats avg(subs.views)` (child metric) | per-child | unchanged (measure references child col → not parent-only → left alone) |

## Files
1. NEW  sandbox/libs/analytics-api/.../analytics/NestedQueryOrigin.java  (ThreadLocal marker)
2. EDIT sandbox/plugins/analytics-engine/.../exec/DefaultPlanExecutor.java  (set/clear around convertAll)
3. EDIT sandbox/plugins/analytics-backend-datafusion/.../NestedParentDedupRewriter.java
   - gate rewrite() on !NestedQueryOrigin.isExplicitExpand()
   - extract docs-dedup core into dedupParentProjection(ProjectRel) helper
   - new tryAggDedup: ungrouped aggregate w/ parent-only-projection input → dedup the projection
4. Tests: semantics_test IT cases (dotted sum=3, expand sum=5, >55 variants 2/3)

## Not in scope (later)
- Upstream ITEM fix (sql repo) — removes the text translation entirely
- Dotted projection arity (fields name, subs.views) — product decision pending
- Calcite-level restructure (move dedup from proto pass into OpenSearchNestedFieldRewriter)
