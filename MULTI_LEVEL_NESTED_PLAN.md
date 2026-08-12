# Multi-Level (2–3+ Deep) Dotted Nested-Path Query Support

**Status:** Draft for review — no code written yet.
**Repos in scope:** `sql` (`/Users/shreanu/repos/sql`, branch `shreanu/nested-poc-search-rewrite`) and `mustang` (`/Users/shreanu/repos/mustang`, branch `ansh/nested-childgrain-split`).
**Methodology:** Requirements → Design → Implementation. No implementation begins until Requirements + Design are signed off. Phase 2's exact scope is intentionally re-confirmed against live evidence at a checkpoint inside Implementation, rather than fixed by upfront speculation (see §3.2).

---

## 1. Requirements

### 1.1 Background

The analytics engine ("Mustang") supports PPL queries against nested (array-of-struct) fields, e.g. `blogs5.comments: [{author, score}]`. Two operation classes exist:

| Operation class | Row-count effect | Example | Status |
|---|---|---|---|
| Per-element filter (1 nesting level) | Unchanged (parent grain) | `where comments.score > 4` | ✅ Done — verified correct incl. element-correlated AND ("Delta" bug fix), Lucene block-join delegation |
| Grain-change / group-by (1 nesting level) | Multiplies (child grain) | `stats count() by comments.author` | ✅ Done — `LogicalNestedScope`/`OpenSearchNestedScope`, capability-routed |
| Per-element filter, **2+ nesting levels** | Unchanged (parent grain) | `where products.variants.color = "red"` | ❌ **Broken — this document's scope** |

### 1.2 Problem Statement

**Reproduction** (confirmed live, this session, against index `c3` with mapping `products` (nested) → `variants` (nested) → leaf fields `color`, `price`; and `variants` → `specs` (nested) → leaf fields `key`, `val`):

```
PPL:    source=c3 | where products.variants.color = "red" | fields title
Result: 500 error
        "EQUAL function expects {[IP,IP],[COMPARABLE_TYPE,COMPARABLE_TYPE]},
         but got [STRUCT,STRING]"
```

By contrast, a single-level path on the same index works correctly:

```
PPL:    source=c3 | where products.pname = "Widget" | fields title
Result: {"columns":["title"],"rows":[["C1"]]}          ✅ correct
```

The failure is a **compile-time type-check error in query planning** — it happens before the row-level engine (DataFusion/Lucene) ever runs. No amount of engine-level fixing can help until this is addressed.

### 1.3 Functional Requirements

| ID | Requirement | Rationale |
|---|---|---|
| **FR-1** | A dotted path crossing N ≥ 2 nested-array boundaries (e.g. `products.variants.color`, `products.variants.specs.key`) SHALL compile to a valid logical plan without a type error. | This is the entry blocker; nothing else is testable until this holds. |
| **FR-2** | A multi-level nested filter SHALL return results matching vanilla OpenSearch `nested` query semantics — i.e., **element-correlated** existence at every boundary crossed, not first-element-only, and not a naive parent-grain flattening across boundaries. | Matches the correctness bar already established for the single-level "Delta" fix; a plausible-looking but wrong answer is worse than an error. |
| **FR-3** | A multi-level filter combined with conjuncts on other, unrelated columns (parent-level or different nested paths) SHALL correctly separate "this predicate touches the deep path" from "this predicate doesn't," and SHALL NOT silently pass an unclassified deep conjunct through unfiltered. | The existing single-level classifier (`containsItemOnArray`/`itemArrayCol`) hardcodes a `RexInputRef`-rooted check; a naive extension could misclassify a multi-level chain as "unrelated" rather than erroring, producing wrong (not failing) results — the most dangerous failure mode in this plan. |
| **FR-4** | Lucene delegation/acceleration for a multi-level predicate SHALL either (a) work correctly, or (b) be explicitly and deliberately skipped (falling back to DataFusion-only evaluation) — it SHALL NEVER be attempted in a way that silently produces wrong results. | The existing Rust UDF's own recursive-descent design already documents that Lucene split-bits do not cross a nested boundary; the Java side must honor that same boundary, not attempt cross-boundary delegation. |
| **FR-5** | The existing 106 non-deep corpus test cases (families F1–F3, F4 partial, F7, F8, in `mustang-nested-testkit/nested-tests/nqx_corpus.jsonl`) SHALL continue to pass with no regression. | This work must not destabilize the already-verified single-level path. |
| **FR-6** | All 14 `F6_deep` corpus test cases (index `c3`) SHALL pass, and `F6_deep` SHALL be removed from `run_corpus_routing.py`'s `SKIP_FAMILIES_ROUTING` exclusion set. | This is the concrete, pre-existing acceptance bar already defined in the test kit — not a new bar invented for this plan. |
| **FR-7** | Negation (`not (...)`) over a multi-level path is explicitly OUT of scope for "correct by default" — see §1.5. It must not crash, but its exact ∃¬ vs ¬∃ semantics may remain the same known-ambiguous state as the single-level case (see `F3_neg_grain` in the existing corpus). | Negation-grain ambiguity is a pre-existing, separately-tracked issue at the single level; multi-level shouldn't be held to a stricter bar than single-level already is. |

### 1.4 Non-Functional Requirements

| ID | Requirement |
|---|---|
| **NFR-1** | No changes to the Rust/DataFusion UDF (`nested_any_match_expr.rs`) are anticipated or in scope. It already supports the required recursive `{"nested":"<field>","inner":<subtree>}` shape to arbitrary depth, verified by `cargo test --lib nested_any_match_expr` (16/16 pass, depths 5–7). If Phase 2's checkpoint (§3.2) reveals this assumption is wrong, that is a signal to stop and re-plan, not to proceed with an ad hoc Rust patch. |
| **NFR-2** | Both repos' existing formatting/test conventions apply: `spotlessApply`/`spotlessJavaCheck` (mustang, per its `AGENTS.md`), and the sql-plugin's own equivalent, before any commit. |
| **NFR-3** | Every code change must be exercised by an automated test (unit test for the frontend resolver change; corpus-harness cases for the engine change) — no change should be verified by manual curl alone. |
| **NFR-4** | Changes should be minimal and localized to the specific methods identified in root-cause analysis (§2.2) — no speculative refactors of adjacent, working single-level code paths. |

### 1.5 Out of Scope

- Fixing the pre-existing `F3_neg_grain` ambiguous-negation semantics (∃¬ vs ¬∃) — tracked separately, applies equally at single and multi level.
- Fixing the pre-existing `F4_multi_clause` "lossy two-clause-looks-like-single" PPL syntax ambiguity — same reasoning.
- Extending Lucene delegation to work *across* nested boundaries (only "correctly disabled" is required — see FR-4). Making it actually accelerate multi-level predicates is a future performance project, not a correctness requirement here.
- The unrelated 7-level `poc_deep7` test index/dataset — that exercises a different scenario (arbitrary flat depth under one array, no additional array boundary per level) and already has its own ingest script; it is not part of this plan's acceptance criteria.

### 1.6 Acceptance Criteria (Definition of Done)

This work is complete when, on a single test run:

1. `./gradlew :sandbox:plugins:analytics-engine:test` (mustang) passes with zero new failures relative to the pre-existing baseline (4 known-unrelated `RuleProfilingListenerTests` failures excluded, per prior session's confirmed baseline).
2. The sql-plugin's own unit test suite passes, including a new depth-≥2 `QualifiedNameResolver` test.
3. `python3 run_corpus_routing.py` (no `--index`/`--family` filters) reports **0 correctness mismatches and 0 routing violations** for every family except the pre-existing, out-of-scope `F3_neg_grain`/`F4_multi_clause` cases enumerated in §1.5.
4. `F6_deep` no longer appears in `SKIP_FAMILIES_ROUTING`.
5. Manual verification of the two motivating queries from §1.2 returns correct, non-erroring results.

---

## 2. Design

### 2.1 Current Architecture — Single-Level Path (Baseline, Working)

```
PPL text: "where comments.score > 4"
    │
    ▼
[sql-plugin] QualifiedNameResolver.resolveFieldAccess()
    resolves base column `comments`, remainder = ["score"] (ONE segment)
    → ITEM($comments, 'score')                         (correct: 1 segment = no ambiguity)
    │
    ▼
[sql-plugin] PPLFuncImpTable ITEM resolver
    struct field lookup "score" on comments' component ROW → FOUND, typed INTEGER
    │
    ▼
[mustang] OpenSearchNestedFieldRewriter.ExprTreeBuilder.build()
    sees ITEM($comments, 'score'), operand-0 IS a RexInputRef, index == arrayCol
    → emits {"field": "score"}
    → wrapped into full predicate: {"op":">","args":[{"field":"score"},{"lit":4}]}
    │
    ▼
[mustang] NESTED_ANY_MATCH_EXPR UDF (Rust/DataFusion)
    per-element ∃ over comments[], element-correlated, returns bool per parent row
    │
    ▼
    Correct result, element-correlated, Lucene-delegatable for keyword-equality conjuncts
```

### 2.2 Root Cause Analysis — Multi-Level Path (Broken)

Two independent layers each contain a piece of the gap. Both were confirmed by direct code inspection this session (agents dispatched against both repos), not by inference.

#### 2.2.1 Layer 1 — sql-plugin: `QualifiedNameResolver` collapses the remainder

**File:** `sql/core/src/main/java/org/opensearch/sql/calcite/QualifiedNameResolver.java`
**Method:** `resolveFieldAccess(...)`, lines 290–300.

```java
private static RexNode resolveFieldAccess(
    CalcitePlanContext context, List<String> parts, int start, int length, RexNode field) {
  if (length == parts.size() - start) {
    return field;
  } else {
    int remainingStart = length + start;
    int remainingLength = parts.size() - remainingStart;
    String itemName = joinParts(parts, remainingStart, remainingLength);   // <-- joins ALL remaining segments with "."
    return createItemAccess(field, itemName, context);                    // <-- exactly ONE ItemAccess call
  }
}
```

For `products.variants.color`, the base-column resolver (`resolveFieldWithoutAlias`) finds `products` as the longest matching column prefix. The **remainder is `["variants", "color"]` — two segments** — and `joinParts` collapses them into the single string `"variants.color"`. One `ITEM($products, "variants.color")` call is emitted.

**Why this is wrong:** `products`'s component type has a field literally named `variants` (not `"variants.color"`). Downstream (Layer 1b below), the struct-field lookup for `"variants.color"` fails, and the ITEM call falls through to a generic, mistyped path.

**Why the single-level case accidentally works:** when the remainder has exactly one segment, `joinParts` is a no-op — `itemName` equals that one segment's real field name, and everything downstream works by coincidence, not by design. There is no case in this method today that walks segments one at a time; it is called exactly once per name resolution, always producing at most one `ItemAccess`.

#### 2.2.2 Layer 1b — sql-plugin: `PPLFuncImpTable`'s ITEM resolver mistypes the fallthrough

**File:** `sql/core/src/main/java/org/opensearch/sql/expression/function/PPLFuncImpTable.java`
**Lines:** 1373–1403.

```java
RelDataTypeField field = component.getField(fieldName, true, false);  // fieldName = "variants.color"
if (field != null) {
    // correctly-typed ITEM using the field's real type
    ...
}
return builder.makeCall(SqlStdOperatorTable.ITEM, array, key);          // <-- fallthrough: generic, untyped ITEM
```

`component.getField("variants.color", ...)` returns `null` (no field has that literal name). Control falls to the generic `SqlItemOperator` call, whose stock Calcite return-type inference ignores the string key entirely and types the result as the **whole `variants` component ROW** — mapped to `STRUCT` by `OpenSearchTypeFactory`. The subsequent `EQUAL(STRUCT, STRING)` type-check is what actually throws.

#### 2.2.3 Layer 2 — mustang: `ExprTreeBuilder` only recognizes single-hop `ITEM` on a bare column reference

**File:** `mustang/sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/planner/rules/OpenSearchNestedFieldRewriter.java`
**Method:** `ExprTreeBuilder.build(RexNode)`, leaf case at lines 862–876.

```java
if (node instanceof RexCall itemCall && "ITEM".equals(...) && operands.size() == 2) {
    RexNode arrayOperand = itemCall.getOperands().get(0);
    RexNode fieldNode   = itemCall.getOperands().get(1);
    if (arrayOperand instanceof RexInputRef ref && fieldNode instanceof RexLiteral lit && ...) {
        if (ref.getIndex() != arrayCol) return null;
        return Map.of("field", lit.getValueAs(String.class));
    }
    return null;   // <-- operand-0 of a CHAIN is a RexCall (another ITEM), not a RexInputRef — dies here
}
```

Even if Layer 1 emitted a correct chain `ITEM(ITEM($products,'variants'),'color')`, this method's `arrayOperand instanceof RexInputRef` check fails for the outer `ITEM` (whose operand-0 is the inner `ITEM` call, not a plain column ref). `build()` returns `null`, and the whole predicate falls back to the engine's generic (non-delegated) Correlate+Uncollect unnest path.

**What is unknown, and must not be assumed:** whether that generic fallback path produces *correct* element-correlated results for a doubly-nested column today, or silently reproduces the exact parent-grain "Delta" bug the single-level fix specifically exists to avoid. **This is why §3.2 defines an empirical checkpoint rather than assuming a scope for Phase 2.**

#### 2.2.4 What already works and needs no changes

- `OpenSearchSchemaBuilder.buildNestedStructType` (mustang, `sandbox/libs/analytics-api/.../schema/OpenSearchSchemaBuilder.java`) already recurses nested-in-nested types correctly into `ARRAY(ROW(...))` shapes — the type information a fix needs is already present in the schema, just unused by the two methods above.
- The Rust UDF (`nested_any_match_expr.rs`) already implements and unit-tests the arbitrary-depth recursive `{"nested":..., "inner":...}` shape — see NFR-1.

### 2.3 Design Alternatives Considered

| # | Alternative | Verdict |
|---|---|---|
| A | Fix only Layer 1 (frontend emits a correct ITEM chain) and hope the existing generic fallback in mustang already handles it correctly. | **Rejected as the sole fix** — "hope" is not a design; FR-2/FR-3 require verified correctness, not an assumption. Retained as the Phase 1 checkpoint's null hypothesis to test, not as the final design. |
| B | Fix Layer 1 only, and reject/error clearly on any multi-level predicate mustang's rewriter can't yet handle (rather than silently falling back). | Viable as an **intermediate, safe milestone** if Phase 2 turns out to be large — turns "silently maybe-wrong" into "loudly not-yet-supported," satisfying FR-3's "must not silently misclassify" bar even before Phase 2 lands. Considered as a fallback plan, not primary. |
| C | Fix both layers: frontend emits a real chain (Layer 1), AND the rewriter recursively descends the chain to emit the matching `{"nested":...,"inner":...}` shape (Layer 2), with explicit classification and Lucene-delegation guards. | **Chosen.** Only this fully satisfies FR-1 through FR-6. |

### 2.4 Chosen Design

#### 2.4.1 Component A — `QualifiedNameResolver.resolveFieldAccess` (sql-plugin)

Replace the single `joinParts` + one `createItemAccess` with a **per-segment walk**, re-inspecting the accumulated expression's type at each step:

```java
RexNode acc = field;
for (int i = remainingStart; i < parts.size(); i++) {
    RelDataType t = acc.getType();
    RelDataType comp = (t.getComponentType() != null) ? t.getComponentType() : t;  // unwrap ARRAY<ROW> if applicable
    if (comp.isStruct() && comp.getField(parts.get(i), true, false) != null) {
        acc = createItemAccess(acc, parts.get(i), context);    // one ITEM per boundary — builds the chain
    } else {
        // No such sub-field at this level (e.g. a genuine flattened `object`-type dotted name):
        // fall back to the flat joined remainder, preserving existing object-field behavior.
        return createItemAccess(acc, joinParts(parts, i, parts.size() - i), context);
    }
}
return acc;
```

**Design intent:** this is intentionally an incremental walk, not a full rewrite of the resolver's control flow — it changes exactly one method's body, preserving every other call site and the existing longest-prefix base-column resolution untouched. The fallback branch is what preserves backward compatibility with OpenSearch `object`-type fields that are genuinely flattened under dotted names (distinct from `nested` array-of-struct fields) — this is the same reasoning that keeps the existing `testFieldContainsDots` test passing.

**Supporting changes:**
- Remove two leftover debug `System.out.println` calls in `PPLFuncImpTable.java` (lines 1390, 1405), artifacts of the prior single-level fix that should not ship further.
- Add a new test to `CalcitePPLQualifiedNameResolutionTest` covering a depth-≥2 path, asserting the resulting `RexNode` is a nested `ITEM(ITEM(...))` chain rather than a single flat-keyed `ITEM`.

#### 2.4.2 Component B — `ExprTreeBuilder` recursive descent (mustang)

**Only implemented if the Phase 1 checkpoint (§3.2) shows it's needed** — see the empirical-gating rationale in §2.2.3 and §3.2. Design, ready to execute if triggered:

1. Give `ExprTreeBuilder` a running `RelDataType elementType`, initialized to `inputRowType.getField(arrayCol).getType().getComponentType()`, threaded alongside the existing `arrayCol` index.
2. In `build()`, generalize the leaf-detection logic to unwrap a **chain** of `ITEM` calls (operand-0 may itself be an `ITEM` call, not only a `RexInputRef`), tracking `elementType` as each hop is unwrapped.
3. **Placement constraint (critical, derived directly from the Rust UDF's contract):** the `{"nested": "<segment>", "inner": <subtree>}` wrapper must be built around a **boolean comparison subtree**, never around a bare leaf/value. The Rust UDF's `eval_bool` expects `inner` to itself be a predicate; the `deep_nested_missing_inner_errors` unit test confirms this errors loudly if violated. Concretely: the recursive wrapping happens in the **comparison-building branch** of `build()` (where `>`, `=`, etc. get assembled), not in the **leaf/field-reference branch** — the descent must know it's building a comparison one level down, then wrap that whole comparison, not wrap a half-built field reference.
4. For each array-typed segment crossed while descending, emit one `{"nested": "<segment>", "inner": ...}` wrapper around the next-level-down subtree; the innermost, final comparison is the base case.

#### 2.4.3 Component C — Conjunct classification fix (mustang)

**File/methods:** `containsItemOnArray`, `itemArrayCol` (same rewriter file).

These currently hardcode `ref.getIndex() == arrayCol` against a `RexInputRef`-rooted `ITEM`. They must be generalized to recognize a **chain** rooted the same way (i.e., walk to the chain's ultimate base column and compare that, rather than assuming the immediate operand-0 is already the base). This is called out separately from Component B because it is the one piece of this plan whose failure mode is **silent wrong answers, not a crash or a clean fallback** (FR-3) — a deep conjunct misclassified as "doesn't touch this array" gets dropped from the delegated-predicate handling and passed through as if it always matches, which is worse than either an error or a correct-but-unaccelerated fallback.

#### 2.4.4 Component D — Lucene-delegation guard (mustang)

**File/methods:** `tryDirectEqualityRewrite`, `tryDirectEqualityChildRewrite`.

Both must explicitly detect a multi-level (chain) path and `return null` — i.e., decline to attempt delegation — rather than either crashing or (worse) mis-delegating a predicate the Lucene child-scoped serializer and Rust split-bits mechanism cannot correctly express across a nested boundary (per the Rust UDF's own documented single-boundary limit on split-bits). This directly satisfies FR-4.

#### 2.4.5 Component E — Test kit extension (mustang test kit, not source)

- Add an ingest script (`nested-tests/create_c3.sh`, modeled on the existing `create_delta.sh`/`create_deep7.sh` pattern) for index `c3`, with a document set that satisfies all 14 `F6_deep` golden results already recorded in `nqx_corpus.jsonl`.
- Remove `F6_deep` from `run_corpus_routing.py`'s `SKIP_FAMILIES_ROUTING`.

### 2.5 Correctness Considerations / Edge Cases

| Case | Handling |
|---|---|
| Mixed dotted path where an intermediate segment is a flattened `object` (not `nested`) rather than an array | Component A's fallback branch (flat `joinParts` on the remainder) preserves today's behavior — no chain is built past an `object` boundary, since `comp.isStruct()` still holds but there's no array to recurse through; only genuinely `nested` (array) boundaries produce a chain. Needs a targeted test. |
| A multi-level predicate ANDed with an unrelated single-level or parent-level predicate | Must be independently, correctly classified per FR-3/Component C — verify explicitly with a corpus-style mixed-conjunct test case, not just the pure multi-level cases already in `F6_deep`. |
| Negation over a multi-level path | Explicitly out of scope for "correct," in scope for "must not crash" — see FR-7/§1.5. |
| A path that's multi-level but the *final* leaf comparison sits directly on a struct field one level in (no further nesting below), vs. one that itself crosses one more array boundary at the leaf | Both must be handled by the same general recursive-descent logic in Component B — there is no reason to special-case "exactly 2 levels" vs. "3+ levels"; the design in §2.4.2 is depth-general by construction, matching the Rust side's own depth-general design. |

### 2.6 Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Phase 2 turns out to be much larger than estimated (e.g. the generic fallback path has its own deep bugs beyond just "not delegated"). | The Phase 1 checkpoint (§3.2) is designed specifically to surface this *before* committing to a Phase 2 implementation plan — re-scope Phase 2 based on that evidence rather than the speculative design in §2.4.2–2.4.4. |
| Component A's fallback-to-flat-name branch accidentally breaks existing `object`-type field support. | NFR-3 requires the depth-≥2 test in Component A to include an `object`-type-field regression case alongside the new nested-chain case. |
| Fixing Component C (classification) introduces a subtle bug in the *single-level* classification path since both share the same methods. | FR-5 (no regression in the 106 existing corpus cases) is the direct guard; run the full corpus suite, not just `F6_deep`, after any Component C change. |

---

## 3. Implementation Plan

### 3.1 Phase 1 — sql-plugin frontend fix

**Scope:** Component A only (§2.4.1). Self-contained, one repo, one primary method.

| Step | Action | Verification |
|---|---|---|
| 1 | Implement the per-segment walk in `QualifiedNameResolver.resolveFieldAccess`. | Code review against §2.4.1 design. |
| 2 | Remove the two debug `System.out.println` calls in `PPLFuncImpTable.java`. | Grep for `System.out.println` in the touched file returns nothing new. |
| 3 | Add depth-≥2 chain test + `object`-type-field regression test to `CalcitePPLQualifiedNameResolutionTest`. | Test passes; confirms the emitted `RexNode` shape is a real `ITEM(ITEM(...))` chain, not a flat single ITEM. |
| 4 | Run full existing sql-plugin unit test suite. | Zero regressions. |
| 5 | `./gradlew publishToMavenLocal -Dsandbox.enabled=true` from the sql-plugin checkout. | Build succeeds; `~/.m2` artifact timestamp updates. |

### 3.2 Phase 1 → Phase 2 Checkpoint (Mandatory Gate)

After Phase 1 lands and is republished, **before writing any mustang code**, manually run against a live composite cluster with the newly-published sql-plugin jar:

```
source=c3 | where products.variants.color = "red" | fields title
source=c3 | where products.variants.specs.key = "weight" | fields title
```

Classify the observed outcome into exactly one of:

- **(a) Clean, correct result** matching the corpus's golden values → Phase 2 shrinks to Components C + D + E only (classification fix + delegation guard + test kit) — Component B is unnecessary because the existing generic fallback already produces correct results once it's reachable.
- **(b) Wrong (non-erroring) result** → confirms §2.2.3's stated risk; proceed with the full Phase 2 as designed (Components B + C + D + E).
- **(c) A new, different error** → STOP. Re-open root-cause analysis for this new failure mode before continuing; do not proceed on the existing Phase 2 design, since it was designed against the errors observed in §2.2, not this new one.

**This checkpoint's outcome, and which of (a)/(b)/(c) was observed, must be reported back before Phase 2 implementation starts.**

### 3.3 Phase 2 — mustang engine (scope confirmed by §3.2)

| Step | Action | Verification |
|---|---|---|
| 1 | (If triggered by outcome (b)) Implement Component B — recursive `ExprTreeBuilder` descent. | Manual test: motivating queries from §1.2/§3.2 return correct results. |
| 2 | Implement Component C — classification fix. | New mixed-conjunct test case (§2.5) passes; full existing corpus (F1–F3, F4, F7, F8) still passes with zero regressions. |
| 3 | Implement Component D — Lucene-delegation guard. | Profile-enabled query on a multi-level predicate shows no `NESTED_ANY_MATCH`/`NESTED_ANY_MATCH_CHILD` delegation attempted for the deep path; DataFusion-only routing confirmed via `full_plan` inspection, same technique used for the single-level routing checks. |
| 4 | Implement Component E — `c3` ingest script + un-skip `F6_deep`. | `python3 run_corpus_routing.py` run with no filters. |
| 5 | Full regression pass. | `./gradlew :sandbox:plugins:analytics-engine:test` (mustang) + full corpus harness — matches §1.6's Definition of Done exactly. |

### 3.4 Test Plan Summary

| Layer | Test type | Location |
|---|---|---|
| sql-plugin resolver | Unit test (RexNode shape assertion) | `CalcitePPLQualifiedNameResolutionTest` |
| mustang rewriter | Unit/manual (log trace inspection of emitted JSON tree) | Manual first; consider a dedicated `OpenSearchNestedFieldRewriter` test if Component B is triggered |
| End-to-end correctness | Corpus harness | `nested-tests/run_corpus_routing.py`, `F6_deep` family |
| End-to-end routing | Corpus harness (`--show-routing`) | Same script |
| Regression (single-level) | Corpus harness, full run | Same script, all families |
| Regression (engine unit tests) | Gradle test task | `:sandbox:plugins:analytics-engine:test` |

### 3.5 Dependencies & Sequencing Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ Phase 1: sql-plugin fix (Component A)                            │
│   resolveFieldAccess per-segment walk + tests + publish          │
└──────────────────────────────┬────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ CHECKPOINT (§3.2): manual test against live c3, classify outcome │
└──────┬─────────────────┬─────────────────┬─────────────────────┘
       │ (a) clean        │ (b) wrong        │ (c) new error
       ▼                  ▼                  ▼
 Phase 2 = C+D+E    Phase 2 = B+C+D+E    STOP, re-open root cause
       │                  │
       └────────┬─────────┘
                ▼
┌─────────────────────────────────────────────────────────────────┐
│ Phase 2: mustang fix (scoped components) + test kit extension    │
└──────────────────────────────┬────────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ Full regression: engine unit tests + full corpus harness         │
│ → Definition of Done (§1.6)                                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. Open Items Requiring Sign-Off Before Implementation Begins

1. Confirm branch targets: Phase 1 lands on `sql`'s `shreanu/nested-poc-search-rewrite`; Phase 2 lands on `mustang`'s `ansh/nested-childgrain-split` (or a new branch based on it) — confirm before starting.
2. Confirm §1.5's Out-of-Scope list is acceptable (i.e., negation-grain ambiguity and the multi-clause PPL-syntax ambiguity are genuinely out of scope for this specific effort).
3. Confirm the checkpoint gate in §3.2 is acceptable as a hard stop-and-report point, rather than a soft/optional check.
