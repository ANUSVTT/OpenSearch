# Multi-Level Nested Lucene Delegation (Child-Grain Split Across N≥2 Boundaries)

**Status:** Draft for review — no code written yet.
**Repo in scope:** `mustang` (`/Users/shreanu/repos/mustang`, branch `shreanu/nested-poc-search-rewrite`).
**Depends on:** `MULTI_LEVEL_NESTED_PLAN.md` (multi-level correctness — landed, commit `1bbf47b25dd`). This document extends that work; it does not touch correctness, only performance/routing.
**Methodology:** Requirements → Design → Implementation, same as the prior plan. No implementation begins until Requirements + Design are signed off, and until the empirical risk in §2.6 is validated.

---

## 1. Requirements

### 1.1 Background

The multi-level correctness work (prior plan) made a dotted path crossing N≥2 nested-array boundaries (e.g. `products.variants.color`, or 7 levels deep in `poc_deep7`) return correct, element-correlated results — but **only via DataFusion**. This was a deliberate, signed-off scope boundary (prior plan §1.5: "Extending Lucene delegation to work *across* nested boundaries... is a future performance project, not a correctness requirement"), enforced by Component D (`tryDirectEqualityRewrite`/`tryDirectEqualityChildRewrite` declining to delegate a multi-hop chain).

Separately, single-level nested predicates already have a working Lucene acceleration path, the **child-grain split** (flag: `opensearch.analytics.nested.child_grain_split`, default off): a keyword-equality conjunct inside a compound nested filter (e.g. `comments.author='alice' AND comments.score<50`) is evaluated by Lucene at CHILD-element grain (not just parent-grain block-join), and its per-element verdict is intersected with DataFusion's range/other clauses at the SAME element, before the ∃ roll-up. This is strictly tighter pruning than the parent-grain "superset peer" fallback.

**This document's scope:** extend the child-grain split to fire for a keyword-equality leaf that sits at the END of a multi-hop chain (e.g. the `color` leaf in `products.variants.color`, or the `city` leaf in `regions.offices.city`), so multi-level predicates get the same Lucene acceleration single-level ones already do — without weakening the correctness guarantee the prior plan established.

### 1.2 Problem Statement

**Reproduction** (confirmed live, this session, against `poc_deep7` with `child_grain_split=true`):

```
PPL:     source=poc_deep7 | where regions.offices.city="Seattle" | fields company
Routing: NESTED_ANY_MATCH_EXPR($3, '{"nested":"offices","inner":{"op":"=","args":[{"field":"city"},{"lit":"Seattle"}]}}')
         backends=[datafusion]                     ← no Lucene peer, even for a plain keyword equality
```

By contrast, the exact same *shape* of predicate at single level gets Lucene delegation:

```
PPL:     source=poc_deep7 | where auditors.who="ann" | fields company
Routing: (expected) NESTED_ANY_MATCH_EXPR(...) — dual-viable [lucene, datafusion]
```

This is not a bug — it is Component D's boundary check (`isItemOnArray`, `OpenSearchNestedFieldRewriter.java:803-808`) correctly declining to attempt something it was never extended to do safely. The result is correct but unaccelerated.

### 1.3 Research Findings This Plan Is Built On

A research pass (Explore agent, this session) established the following, with file:line citations retained here for implementation reference:

1. **`NestedChildOrdinalMap`'s per-child `offset`** (`.../NestedChildOrdinalMap.java:124-157`) is a flat, per-ROOT, per-`_nested_path`-string counter — it has no intermediate-parent dimension today, and Lucene's ingest side (`LuceneDocumentInput.startNestedChild`, `.../index/LuceneDocumentInput.java:129-134`) writes only the leaf path string per child doc, nothing identifying which intermediate parent element (e.g. which *variant*) a deep child (e.g. a *spec*) belongs to.
2. **This is not the blocker it looks like.** Both Lucene and Arrow/Parquet are driven by the same `DocumentParser` parse-order callbacks (`startNestedChild`/`endNestedChild`, `DocumentParser.java:612,629`). Lucene writes children in ingest post-order; Arrow's `VSRManager.writeChildList` (`:285-325`) appends to each level's own `ListVector` in the same parse order. **Position implies parentage** — the two sides never need to exchange intermediate-parent identity, only agree on ordering within a root, which the shared parse sequence already guarantees. Therefore: **the existing flat per-root offset is already the numerically correct value at any depth** — what needs to generalize is only the *base* (chaining through each level's own `value_offsets`, computed entirely on the Rust/DataFusion side), not the ingest format or the Lucene-side map.
3. **The Rust UDF already threads a `lucene: Option<&LuceneClauseBits>` parameter through its recursive `{"nested":...}` descent**, but deliberately passes `None` at each recursion (`nested_any_match_expr.rs:293`, comment at `:249-250`: *"Lucene split bits do NOT cross a nested boundary... so we descend with `None`"*) — this is the mechanism's current safe-by-construction stop, and is exactly the one line that needs to change (plus supporting plumbing, below).
4. **Two additional silent-wrong-result risks were identified**, both fixable but requiring explicit handling, not just "flip the gate":
   - **(a)** `NestedAnyMatchChildSerializer`'s `_nested_path` filter term must target the deepest level's OWN path (e.g. `"products.variants"`), not the outer array column's path (e.g. `"products"`) — using the wrong level doesn't error, it silently returns zero matches for every element while `has_clause` still reports true, producing false negatives (never falling back).
   - **(b)** The `{"lucene": idx}` marker must be placed at the DEEPEST position in the JSON tree (inside the `{"nested":...}` wrapper(s)), both because that's where the correct `elem_idx` frame is, AND because placing it at the top level (replacing the whole conjunct, as the single-level code does today) would hide the `"nested"` key from `mergeSharedNestedPrefixes`' grouping check — silently defeating the existing "Delta"-bug-prevention merge for two conjuncts sharing a deep prefix. Ordering of these two transforms is load-bearing.
5. **One critical assumption is unverified and untested at any depth ≥2**: that Lucene's segment-merge block reordering preserves intra-block child docId order for the ordering-implies-parentage argument in finding #2 to hold. `NestedChildOrdinalMap` has zero existing test coverage (`grep` across the repo found only its own file and its one caller). If this invariant is ever violated by a merge, the failure mode is **silently wrong results, not a fallback or a crash** — this is the single highest-priority thing to validate empirically before writing any production code (see §2.6).

### 1.4 Functional Requirements

| ID | Requirement | Rationale |
|---|---|---|
| **FR-1** | A multi-level keyword-equality leaf at the end of a dotted chain (e.g. `products.variants.color = "red"`) that is part of a compound nested predicate SHALL be eligible for child-grain Lucene delegation, exactly as a single-level leaf already is, when `child_grain_split=true`. | This is the feature. |
| **FR-2** | The DataFusion-authoritative result SHALL be unchanged for every case already passing under the prior (correctness-only) plan — Lucene remains a pure accelerant, never a correctness dependency, matching the single-level design's existing contract (`nested_any_match_expr.rs`'s `"fallback"` mechanism). | Same non-negotiable bar as the correctness work: a query that was correct before this change must remain correct if Lucene delegation is declined, disabled, or wrong for any reason (segment merge, a future Lucene version, etc.) — the `"fallback"` path must always produce the right answer independent of whether bits were supplied. |
| **FR-3** | The `_nested_path` Lucene filter for a multi-level child peer SHALL target the deepest crossed level's own path string, never the outer array column's path. | Directly addresses research finding 4(a) — using the wrong level produces a silent all-false clause, not an error. |
| **FR-4** | The `{"lucene": idx}` marker for a multi-level leaf SHALL be placed at the innermost position in the JSON tree (inside all `{"nested":...}` wrappers the leaf crosses), and `mergeSharedNestedPrefixes` SHALL still correctly merge multiple deep conjuncts sharing a nested prefix even when one or more of them has been replaced by a `{"lucene"}` marker. | Directly addresses research finding 4(b) — wrong placement either breaks `elem_idx` correctness or silently defeats the existing Delta-bug-prevention merge. |
| **FR-5** | Before any production code lands, the Lucene-block-order-vs-Arrow-flattened-order equivalence (research finding #5) SHALL be empirically validated at depth ≥2, under at least one forced segment merge, with an automated test — not just a live spot-check. | This is the one unverified assumption whose violation produces silently wrong results. It gates the whole feature; see §2.6/§3.1. |
| **FR-6** | The existing single-level child-grain-split behavior and all corpus/engine tests SHALL continue to pass with zero regression. | Same non-regression bar as every prior change to this file. |
| **FR-7** | Multiple clauses at *different* depths within the same compound predicate (e.g. one single-level and one 3-level conjunct in the same `AND`) SHALL either be correctly handled or explicitly declined (falling back to the existing superset/DataFusion-only path) — never silently mishandled. | Per research finding: no existing test covers even one deep clause, let alone mixed-depth; this must not be assumed to "just work" from the single-clause design. |

### 1.5 Non-Functional Requirements

| ID | Requirement |
|---|---|
| **NFR-1** | No change to the Lucene ingest format (`LuceneDocumentInput`, `_nested_path`/`__row_id__` fields) — research finding #2 establishes this isn't needed. If implementation reveals it IS needed after all, that is a signal to stop and re-plan (same gating discipline as the prior plan's Phase 1→2 checkpoint), not to proceed with an ingest-format change unreviewed. |
| **NFR-2** | No change to the FFM ABI shape (`CollectChildDocsFn`'s `*const i32` + scalar `total_children`) — only the *meaning*/*computation* of those values deepens per clause. |
| **NFR-3** | Every code change must be exercised by an automated test — this feature is explicitly high-risk for silent-wrong-results (§1.3 findings 4a/4b, §2.6), so manual curl/profile spot-checks are not sufficient sign-off for any component here, even though they were acceptable spot-checks for the read-only reporting work earlier in this session. |
| **NFR-4** | Changes should be minimal and localized to the specific methods identified in root-cause analysis (§2.2) — same discipline as the prior plan. |

### 1.6 Out of Scope

- Any change to how single-level (depth-1) child-grain split works today — this plan only adds a new case, it does not modify the existing one.
- Extending the *superset-pruning-peer* (non-child-grain, `child_grain_split=false`) path to cross boundaries — that path's peer is a parent-grain existence check (`NESTED_ANY_MATCH_EXPR` with a single-equality-leaf tree), and a parent-grain "some descendant somewhere matches" superset is a different, separately-arguable correctness question at multi-level (does a superset argument even hold across two independent ∃ loops?) — deliberately deferred to a future plan if wanted; this plan targets child-grain only, where the fallback contract makes safety independent of that question.
- Fixing item #4 from the research report's "not yet verified" list (mixed `object`-inside-`nested` path leaf-naming) unless it's discovered to block this specific feature during implementation.
- Any change to `poc_deep7`'s corpus status (it remains outside `nqx_corpus.jsonl` per the prior plan's own out-of-scope note) — though it remains the natural manual/ad hoc test bed for depth ≥3 given `c3` only reaches depth 3 with limited leaf variety.

### 1.7 Acceptance Criteria (Definition of Done)

1. FR-5's empirical validation (§2.6) passes, with an automated test committed, BEFORE any of the remaining components are implemented.
2. `./gradlew :sandbox:plugins:analytics-engine:test` passes with zero new failures relative to the established baseline (the same 4 pre-existing `RuleProfilingListenerTests` failures excluded).
3. `cargo test --lib` (analytics-backend-datafusion) passes with zero new failures, including new tests for: a multi-level child-grain clause, a mixed single-level+multi-level compound predicate, and the two silent-wrong-result regressions from §1.3 finding 4(a)/4(b) (explicit "would have been wrong if not for this fix" tests, not just happy-path).
4. `python3 run_corpus_routing.py` (no filters) reports the same 12 pre-existing, out-of-scope mismatches and zero new ones; `--show-routing` on the `F6_deep`/`c3` cases shows Lucene delegation now firing for the single-equality-leaf cases (`f6_000`, `f6_002`, `f6_006`'s equality half, `f6_008`, `f6_010`) that previously showed `[datafusion]` only.
5. A live profile-enabled query against `poc_deep7` for `regions.offices.city="Seattle" AND regions.offices.floor>5`-style compound predicates at depth 2 AND depth 7 shows `backends=[lucene, datafusion]` for the keyword conjunct, with results matching vanilla golden (captured the same live-comparison way as the existing depth-7 report).
6. The HTML report (or a new one) is updated to show the routing change for at least the depth-2 and depth-7 cases, so the improvement is visible the same way correctness was.

---

## 2. Design

### 2.1 Current Architecture — Single-Level Child-Grain Split (Baseline, Working)

```
PPL: "where comments.author='alice' AND comments.score<50"
    │
    ▼
[mustang] tryLambdaRewrite → arrayConjuncts.size()>1 && childGrainSplitEnabled()
    for each conjunct: tryDirectEqualityChildRewrite(conjunct, arrayCol, ..., clauseIdx)
      isItemOnArray(left, arrayCol) — REQUIRES single ITEM directly on arrayCol
    → author='alice' matches → NESTED_ANY_MATCH_CHILD($comments,'author','EQUALS','alice','0')
    → arrayTrees[0] replaced with {"lucene":0,"fallback":{"op":"=","args":[{"field":"author"},{"lit":"alice"}]}}
    │
    ▼
[Lucene backend] NestedAnyMatchChildSerializer
    bool(must: term("comments.author","alice"), filter: term(_nested_path,"comments"))
    → scorer yields raw CHILD docIds (no block-join wrap)
    │
    ▼
[Java executor] LuceneFilterDelegationHandle.collectChildDocs
    NestedChildOrdinalMap.build(reader, {"comments"}) → per-root flat offset per child docId
    sets bit child_base[row-minDoc] + offset  (child_base = value_offsets[row], from DataFusion)
    │
    ▼
[Rust UDF] nested_any_match_expr.rs eval_bool, {"lucene":0} node
    has_clause(0) → true → use l.value(0, elem_idx)   (elem_idx IS the flat per-root offset, level 1)
    AND-ed with score<50 evaluated on the SAME element before ∃ roll-up
    │
    ▼
    Correct, element-correlated, keyword clause accelerated by Lucene
```

### 2.2 Root Cause Analysis — Why Multi-Level Doesn't Delegate Today

Three independent gates each block it, ALL correctly (safe-by-construction), confirmed by direct code inspection this session:

#### 2.2.1 Gate 1 — `isItemOnArray` rejects a multi-hop chain

**File:** `OpenSearchNestedFieldRewriter.java`, `isItemOnArray` (`:803-808`), called from `tryDirectEqualityChildRewrite` (`:751-800`, specifically `:765`/`:768`).

```java
private static boolean isItemOnArray(RexNode node, int arrayCol) {
    if (!(node instanceof RexCall call) || !"ITEM".equals(call.getOperator().getName()) || call.getOperands().size() != 2) {
        return false;
    }
    return call.getOperands().get(0) instanceof RexInputRef ref && ref.getIndex() == arrayCol;
}
```

A chained `ITEM(ITEM($col,'variants'),'color')`'s outer operand-0 is another `ITEM` call, not a `RexInputRef` — fails, returns `false`, no child peer emitted for that conjunct. (Contrast `itemArrayCol`, `:1316-1341`, which WAS generalized to walk a chain to its root as part of the correctness work — `isItemOnArray` was deliberately left as the "single-hop only" check, since at the time multi-level Lucene delegation was explicitly out of scope.)

#### 2.2.2 Gate 2 — Rust UDF's nested descent discards the Lucene context

**File:** `nested_any_match_expr.rs`, `eval_bool`'s `{"nested":...}` branch, `:293`:

```rust
if let Some(field_name) = node.get("nested").and_then(|v| v.as_str()) {
    ...
    for inner_idx in inner_start..inner_end {
        let matched = eval_bool(inner_subtree, inner_struct, &inner_fields, inner_idx, None)?;
        //                                                                            ^^^^ always None
```

Even if a `{"lucene"}` node somehow reached this deep, it would find no bits, and correctly (per FR-2) evaluate its `"fallback"` — safe, but zero acceleration. This is documented as deliberate at `:249-250`.

#### 2.2.3 Gate 3 — the executor's hole-discovery doesn't look inside `"inner"`

**File:** `indexed_executor.rs`, `json_lucene_indices` (`:610-629`) — walks a JSON tree looking for `{"lucene":...}` holes to decide whether the child-split path should engage at all (`build_child_split`, guarded around `:454-456`). It does not currently recurse into a `"inner"` key, only `"args"`/`"fallback"`. Even if Gates 1 and 2 were both lifted, a `{"lucene"}` hole placed inside a `{"nested":...}` wrapper would be invisible to this scan, and `build_child_split` would decline (correctly, but again with zero acceleration) because it finds no holes matching its clause bookkeeping.

#### 2.2.4 What already works and needs no changes (confirmed by research)

- `NestedChildOrdinalMap` (`.../NestedChildOrdinalMap.java`) — already path-generic; `assign()` numbers whatever path strings it's asked to, with no assumption baked in about depth. **No change needed.**
- `collectChildDocs` (`LuceneFilterDelegationHandle.java:421-508`) — already reads `path` generically off `ScorerHandle.childPath` and calls `translator.childRow(path,docId)`/`childOffset(path,docId)`. **No change needed.**
- The FFM ABI (`ffm_callbacks.rs:64-65`, `CollectChildDocsFn`) — per-row `*const i32` + scalar `total_children`, unchanged shape. **No change needed.**
- Lucene ingest (`LuceneDocumentInput`, `VSRManager`) — per research finding #2, ordering (not intermediate-parent identity) is all that's structurally required, and ordering is already guaranteed by both writers sharing the same parse-order callbacks. **No change needed, PENDING §2.6's empirical validation.**

### 2.3 Design Alternatives Considered

| # | Alternative | Verdict |
|---|---|---|
| A | Compute intermediate-parent identity by writing a new per-level ordinal field at Lucene ingest time. | **Rejected as unnecessary** — research finding #2 shows position-implies-parentage already holds via the shared parse-order callback sequence; an ingest-format change would be strictly more invasive (touches on-disk format, needs a version/BWC story) for no correctness benefit over the chained-offset approach, UNLESS §2.6 disproves the ordering invariant, in which case THIS becomes the fallback design (see §2.6's contingency). |
| B | Chain each level's own `value_offsets` on the DataFusion/Rust side to compute the correct per-clause base, leaving Lucene's per-root flat offset untouched and reusing it as the innermost lookup key. | **Chosen** — this is what research finding #2 establishes as sufficient; smallest, most localized change; no ingest format change; reuses `NestedChildOrdinalMap` completely unmodified. |
| C | Only support multi-level Lucene delegation up to some fixed small depth (e.g. depth 2), treating depth ≥3 as still declined. | **Rejected** — the chained-offset design in B is depth-general by construction (same reasoning as the correctness plan's Component B decision to not special-case depth); artificially capping depth would add complexity (a depth check + two code paths) without a clear benefit, since B's cost doesn't scale with depth. |

### 2.4 Chosen Design

#### 2.4.1 Component A — Rewriter: multi-hop-aware child-peer construction

**File:** `OpenSearchNestedFieldRewriter.java`.

1. Generalize the entry check so `tryDirectEqualityChildRewrite` can recognize a multi-hop keyword-equality leaf, reusing the existing `ExprTreeBuilder.chainHops`/`countNestedBoundaries` machinery (already built for the correctness work) rather than duplicating chain-walking logic. Concretely: factor a shared helper (or reuse `ExprTreeBuilder` directly, passing it in) that, given a conjunct, returns either `null` (not eligible) or `(List<String> hops, int boundaries, String leafField, String value)` for a multi-hop keyword-equality shape.
2. When `boundaries > 0` (i.e. this is a multi-level leaf, not the existing single-level case), build the `NESTED_ANY_MATCH_CHILD` call carrying the FULL dotted field-name chain (e.g. `"variants.color"`, or however the Lucene serializer needs it split — see Component C) instead of the single leaf field name the depth-1 code path uses today. **Design question to resolve during implementation, not now:** should `NESTED_ANY_MATCH_CHILD`'s existing `(arrayCol, field, op, value, clauseIdx)` signature carry the leaf-only field name plus a separate "which nested level" descriptor, or the full dotted remainder as one string that the serializer re-splits? Lean towards the latter (minimize RexCall shape changes) unless implementation reveals a reason not to.
3. **Placement fix (addresses FR-4):** when building the multi-level `{"lucene": idx, "fallback": ...}` replacement node, it must go at the DEEPEST position — i.e., the ORIGINAL multi-level tree (with its `{"nested":hop,"inner":...}` wrappers already built by `ExprTreeBuilder.build()`) has its innermost comparison subtree replaced by `{"lucene":idx,"fallback":<that innermost comparison>}`, NOT the whole outer `{"nested":...}` tree replaced. This preserves the `"nested"` key at every level above the leaf, so `mergeSharedNestedPrefixes` (unchanged) continues to see and merge shared prefixes correctly — directly satisfying FR-4's second half.

#### 2.4.2 Component B — Lucene serializer: correct `_nested_path` targeting

**File:** `NestedAnyMatchChildSerializer.java` (or a new sibling if the multi-level shape diverges enough to warrant it — implementation decision).

Per FR-3 / research finding 4(a): the `_nested_path` filter term must be the deepest crossed level's own full path string (e.g. `"products.variants"` for a `color` leaf under `products.variants`), NOT the outer array column's path (`"products"`). This requires threading the full dotted chain (from Component A) through to serialization, then computing `deepestPath = arrayField.getFieldName() + "." + allHopsExceptLeaf.join(".")`.

**This is the one component the research report explicitly flags as a "silent wrong answer if done naively" — needs its own dedicated regression test (per FR-3/Acceptance Criteria #3), not just a happy-path test**, e.g.: assert that a query with the WRONG level deliberately hardcoded in a test double returns all-false (proving the test would have caught the bug), then assert the real implementation returns correct non-empty results.

#### 2.4.3 Component C — Rust: thread Lucene context through nested descent, per-clause coordinate spaces

**Files:** `nested_any_match_expr.rs`, `indexed_executor.rs`, `single_collector.rs`.

1. `nested_any_match_expr.rs:293` — change `None` to `lucene` (the one-line enablement research finding #3 identifies), so a `{"lucene"}` node reached via nested descent can actually consult bits, once they're correctly computed and supplied by C.3 below.
2. `indexed_executor.rs::json_lucene_indices` (`:610-629`) — extend to recurse into `"inner"`, accumulating the enclosing `"nested"` field-name chain per hole found, so each discovered clause carries its own depth/level descriptor (addresses Gate 3, §2.2.3).
3. `single_collector.rs`'s `ChildClause` (`:129-133`) — add a per-clause level/field-chain descriptor (from C.2's discovery). `evaluate_child_split` (`:395-472`) must then compute a PER-CLAUSE `child_base`/`total_children` (chained through that clause's own depth of `value_offsets` lookups), instead of the current single shared `child_base` used by all clauses — this is the plumbing cost flagged as the "real" cost center by research finding #5's sizing table, item 5-6.

**FR-7 (mixed-depth clauses) is a direct consequence of this per-clause design**: since each clause already gets its own independently-computed coordinate space, a single-level clause and a multi-level clause coexisting in one compound predicate should "just work" once C.3 is per-clause — but this must be an explicit test, not an assumption (per FR-7's own wording and research finding #5).

#### 2.4.4 Component D — Empirical validation of the ordering invariant (GATING, must happen FIRST)

See §2.6 — this is not really a "component" of the feature so much as a prerequisite gate, listed here for completeness of the design but sequenced first in §3.

### 2.5 Correctness Considerations / Edge Cases

| Case | Handling |
|---|---|
| A multi-level clause where an intermediate hop has ZERO elements (e.g. `products` has no `variants` for some root) | Per research finding #2's reasoning, an empty intermediate list yields `start == end` at that level — a valid, well-defined empty child-base range. No special-casing needed if C.3's chained-offset computation handles empty ranges naturally (must be covered by a test). |
| A multi-level clause mixed with a parent-level-only conjunct in the same AND | Unrelated to this feature — already handled by the existing `arrayConjuncts`/`parentConjuncts` split; not re-litigated here. |
| A multi-level clause where the SAME nested prefix has BOTH a Lucene-delegable equality leaf AND a non-delegable (range/other) leaf (e.g. `f6_004`: `specs.key="weight" AND specs.val>50`) | This is exactly the shared-prefix-merge scenario FR-4 exists for. `mergeSharedNestedPrefixes` must merge them under one `{"nested":"specs",...}` wrapper with the equality leaf as `{"lucene":i,"fallback":...}` and the range leaf as its ordinary comparison tree, both inside the SAME `"inner"` AND. Needs an explicit test (this is `f6_004`'s exact shape from the existing corpus — reuse it). |
| Segment merge reordering children within a root's block | The single highest-risk case in this entire plan — see §2.6. If it turns out to break the invariant, Alternative A (§2.3) becomes the fallback design, and this plan's Components A-C would need rework around whatever new ingest signal Alternative A introduces. |
| `child_grain_split=false` (default) | No change — this entire feature is gated behind the SAME existing flag; when off, multi-level predicates continue to use the pre-existing superset-pruning-peer path exactly as they do today (itself unaffected by this plan, per §1.6's explicit out-of-scope note). |

### 2.6 Risks & Mitigations — §1.3 Finding #5 Is the Gating Risk

| Risk | Mitigation |
|---|---|
| **(GATING) Lucene's block-order-implies-parentage invariant is unverified at depth ≥2 and untested under segment merge.** If violated, failure mode is silently wrong results. | **Before implementing Components A-C**, write and run a standalone verification: ingest a multi-level (depth ≥2, ideally depth 3 using the existing `c3` mapping) composite index with enough documents to force at least one segment merge (`_forcemerge` or enough docs + small merge policy threshold), then directly inspect (via a small test/tool, not production code) whether Lucene's post-merge ascending-docId order for a deep `_nested_path` within one root still matches Arrow's flattened value order for the same root. This can be a throwaway JUnit test against `NestedChildOrdinalMap.assign()`-style logic, or even a manual script — but it must produce a clear yes/no before Component A/B/C implementation starts. If it fails: STOP, do not proceed with this plan's design; re-open with Alternative A (§2.3) as the new starting point. |
| Component B's `_nested_path` targeting bug (FR-3) ships without a dedicated regression test and silently regresses later. | Acceptance Criteria #3 explicitly requires a test that would have caught this exact bug (wrong-level assertion), not just a happy-path test. |
| Component A's `{"lucene"}` placement fix interacts with `mergeSharedNestedPrefixes` in a way not caught by existing single-level tests (since single-level never has a `"nested"` key to hide). | Add the `f6_004`-shaped mixed-leaf-in-shared-prefix test called out in §2.5's edge-case table; run full existing corpus (not just new cases) per FR-6. |
| Mixed single-level + multi-level clauses in one predicate (FR-7) turn out to need more than "just works" from the per-clause design. | Explicit test required per FR-7/Acceptance Criteria #3; if it reveals more plumbing is needed, that's a scope-adjustment signal, not a silent gap. |
| This plan's scope creeps into fixing the object-inside-nested leaf-naming question (research finding, item #4, not-yet-verified) because it turns out to block Component A. | Per §1.6, explicitly out of scope unless implementation discovers it's a hard blocker for THIS feature — if so, stop and report back rather than silently expanding scope. |

---

## 3. Implementation Plan

### 3.1 Phase 0 — Gating Empirical Validation (MANDATORY, BLOCKS EVERYTHING ELSE)

| Step | Action | Verification |
|---|---|---|
| 1 | Design and run the block-order-vs-Arrow-order equivalence check from §2.6, at depth ≥2, under a forced segment merge. | Produces an unambiguous pass/fail. |
| 2 | Report the outcome back before writing any of Components A-D. | Classify into: (a) holds, proceed with this plan as designed; (b) fails, STOP and re-open with Alternative A as the new design's starting point; (c) holds for depth 2-3 but degrades at depth ≥N for some reason — re-scope depth limit into FR-1 if so. |

**This phase is the direct analog of the prior plan's §3.2 checkpoint gate — same discipline: no Phase 1 implementation work begins until this reports back and is classified.**

### 3.2 Phase 1 — Rewriter + Serializer (Components A, B)

| Step | Action | Verification |
|---|---|---|
| 1 | Implement Component A (§2.4.1): multi-hop-aware `tryDirectEqualityChildRewrite`, correct `{"lucene"}` placement at the deepest position. | Unit test: emitted JSON tree shape assertion (mirrors the correctness plan's own test style for `ExprTreeBuilder`). |
| 2 | Implement Component B (§2.4.2): correct `_nested_path` targeting in the serializer. | The dedicated wrong-level regression test from §2.6's mitigation table. |
| 3 | Run full existing engine test suite + corpus harness. | Zero regressions (FR-6). |

### 3.3 Phase 2 — Rust Plumbing (Component C)

| Step | Action | Verification |
|---|---|---|
| 1 | `eval_bool`'s `None`→`lucene` threading fix. | `cargo test --lib nested_any_match_expr` — existing tests still pass (they don't exercise this path yet, so this alone shouldn't change results). |
| 2 | `json_lucene_indices` recursion into `"inner"`. | New unit test: a `{"nested":...,"inner":{"lucene":...}}` tree is discovered correctly. |
| 3 | Per-clause `ChildClause` descriptor + per-clause `child_base`/`total_children` in `evaluate_child_split`. | New unit tests: single multi-level clause; the `f6_004`-shaped mixed-leaf-in-shared-prefix case; a mixed single-level+multi-level compound (FR-7). |

### 3.4 Phase 3 — End-to-End Verification

| Step | Action | Verification |
|---|---|---|
| 1 | Full regression: engine unit tests + corpus harness + `cargo test --lib`. | Matches §1.7's Definition of Done exactly. |
| 2 | Live profile-enabled verification against `c3` (depth 3) and `poc_deep7` (depth 2 and depth 7) for both a single-equality-leaf case and a compound (equality + range, shared prefix) case. | `backends=[lucene, datafusion]` observed; results match vanilla golden (same live-comparison technique as the existing depth-7 report). |
| 3 | Update or regenerate the HTML report to show the routing improvement. | Visual confirmation the routing badges have changed from `[datafusion]` to `[lucene, datafusion]` for the now-accelerated cases. |

---

## 4. Open Items Requiring Sign-Off Before Implementation Begins

1. **Confirm Phase 0's gating discipline is acceptable** — i.e., you agree no Component A/B/C code gets written until the block-order invariant is empirically validated, same as the prior plan's hard-stop checkpoint.
2. **Confirm scope**: child-grain split only (not the superset-pruning-peer path) — per §1.6.
3. **Confirm depth is NOT artificially capped** — Alternative B (chosen) is depth-general; confirm you don't want a depth limit for some other reason (e.g. wanting to ship depth-2/3 first and depth 7 later) before Phase 1 starts.
4. **Confirm the `NESTED_ANY_MATCH_CHILD` RexCall shape question in §2.4.1 step 2** is left as an implementation-time decision (full dotted remainder as one string vs. leaf+descriptor) rather than something you want decided now.
