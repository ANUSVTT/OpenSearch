#!/usr/bin/env python3
"""
Complete Minimal Query Set (Analytics/PPL) -- MULTI-LEVEL NESTING variant.

Same 23 scenarios as scenario-minimal-set-tests.py, but every "single-level nested" scenario
(5, 5a, 7, 7a, 9, 10, 11, 12, 13, 14, 15) is re-pointed at a NEW index, `cmts_deep`, where the
nested field sits ONE LEVEL DEEPER than in the original `cmts` (comments.replies.author /
comments.replies.score instead of comments.author / comments.score) -- same 8 documents, same
values, just wrapped in an extra nested hop, so every expected result is IDENTICAL to the
single-level report; only the field PATH and the query text change. Scenarios already at
multi-level (6, 8, 16, 17 on c3; 18/18a on ambig) are unchanged. Flat-only scenarios (1-4b) run
against cmts_deep too, for one consistent dataset across the whole report.

Generates scenario-minimal-set-multilevel-test-results.html in the same case-card style as the
single-level report, with each card additionally showing:
  - RelNode (raw)        -- pre-rewrite Calcite tree
  - RelNode (rewritten)  -- post nested-field-rewrite tree (NESTED_ANY_MATCH_EXPR/CHILD)
  - Logical plan          -- final annotated plan with backend routing (profile.full_plan)

New in this report: an "Indices & Data" section at the top, rendered BEFORE the scenario
cards, showing each index's mapping and its documents verbatim -- so a reader can see exactly
what data every case ran against without cross-referencing a separate script.

#10 (OR mixing a nested predicate with a flat predicate) previously CRASHED at multi-level
depth (a DataFusion type-coercion error: array_element/sum don't support the raw Struct type
the Correlate+Uncollect fallback produced once nesting was 2+ deep) -- worse than the
single-level version, which just silently dropped a row. FIXED: OpenSearchNestedFieldRewriter
now splits a top-level OR into array-referencing and pure-parent operands (mirroring the
existing AND-split) instead of falling back to unnest, so both the single-level silent-drop
and the multi-level crash are resolved by the same change. See case #10's note-box below.

Usage:
    python3 scenario-minimal-set-multilevel-tests.py
"""
import html
import json
import os
import re
import sys
import urllib.error
import urllib.request

BASE = os.environ.get("MUSTANG_BASE", "http://localhost:9000")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGFILE = os.environ.get(
    "MUSTANG_LOGFILE", os.path.join(SCRIPT_DIR, "build/testclusters/runTask-0/logs/runTask.log")
)
OUT_HTML = os.path.join(SCRIPT_DIR, "scenario-minimal-set-multilevel-test-results.html")
FAILURES_JSON = "/tmp/scenario_minimal_set_multilevel_failures.json"

COMPOSITE_SETTINGS = {
    "index.pluggable.dataformat.enabled": True,
    "index.pluggable.dataformat": "composite",
    "index.composite.primary_data_format": "parquet",
    "index.composite.secondary_data_formats": "lucene",
    "index.number_of_shards": 1,
    "index.number_of_replicas": 0,
}


# ─── HTTP helper ────────────────────────────────────────────────────────────

def req(method, path, body=None, ndjson=None, timeout=30):
    url = BASE + path
    headers = {}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    elif ndjson is not None:
        data = ndjson.encode()
        headers["Content-Type"] = "application/x-ndjson"
    r = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return json.load(resp), None
    except urllib.error.HTTPError as e:
        try:
            return None, json.load(e)
        except Exception:
            return None, {"error": {"reason": str(e)}}
    except Exception as e:
        return None, {"error": {"reason": str(e)}}


# ─── Test data setup ────────────────────────────────────────────────────────
# cmts_deep: a multi-level mirror of the single-level report's `cmts` -- comments is nested,
# and its `author`/`score` leaves are pushed ONE LEVEL DEEPER into a `replies` nested array,
# so every dotted path gains one extra hop (comments.replies.author instead of
# comments.author) while every document, value, and expected result stays byte-identical to
# the single-level report.

CMTS_DEEP_MAPPING = {
    "title": {"type": "keyword"},
    "views": {"type": "integer"},
    "comments": {"type": "nested", "properties": {
        "replies": {"type": "nested", "properties": {
            "author": {"type": "keyword"}, "score": {"type": "integer"},
        }},
    }},
}

CMTS_DEEP_DOCS = [
    {"title": "P1", "views": 60, "comments": [{"replies": [{"author": "alice", "score": 5}]},
                                               {"replies": [{"author": "bob", "score": 90}]}]},
    {"title": "P2", "views": 30, "comments": [{"replies": [{"author": "alice", "score": 99}]}]},
    {"title": "P3", "views": 70, "comments": [{"replies": [{"author": "bob", "score": 10}]},
                                               {"replies": [{"author": "carol", "score": 20}]}]},
    {"title": "P4", "views": 10, "comments": []},
    {"title": "P5", "views": 90},
    {"title": "P6", "views": 45, "comments": [{"replies": [{"author": "alice", "score": 5}]}]},
    {"title": "P7", "views": 55, "comments": [{"replies": [{"author": "bob", "score": 90}]}]},
    {"title": "P8", "views": 80, "comments": [{"replies": [{"author": "alice", "score": 5}]},
                                               {"replies": [{"author": "alice", "score": 90}]}]},
]

# c3: pre-existing 3-level nested corpus dataset (products -> variants -> specs), unchanged --
# scenarios 6/8/16/17 were already multi-level in the single-level report and stay as-is.
C3_MAPPING = {
    "title": {"type": "keyword"},
    "products": {"type": "nested", "properties": {
        "pname": {"type": "keyword"},
        "variants": {"type": "nested", "properties": {
            "color": {"type": "keyword"}, "price": {"type": "integer"},
            "specs": {"type": "nested", "properties": {
                "key": {"type": "keyword"}, "val": {"type": "integer"},
            }},
        }},
    }},
}

C3_DOCS = [
    {"title": "C1", "products": [{"pname": "Widget", "variants": [
        {"color": "red", "price": 200, "specs": [{"key": "weight", "val": 2}, {"key": "size", "val": 10}]}]}]},
    {"title": "C2", "products": [{"pname": "Sprocket", "variants": [
        {"color": "red", "price": 50, "specs": [{"key": "weight", "val": 2}, {"key": "size", "val": 10}]},
        {"color": "green", "price": 200, "specs": []}]}]},
    {"title": "C3", "products": []},
    {"title": "C4", "products": []},
    {"title": "C5", "products": [{"pname": "Thingamajig", "variants": [{"color": "red", "price": 100, "specs": []}]}]},
    {"title": "C6", "products": [{"pname": "Gadget", "variants": [
        {"color": "blue", "price": 100, "specs": [{"key": "weight", "val": 99}]},
        {"color": "red", "price": 100, "specs": [{"key": "weight", "val": 2}]}]}]},
    {"title": "C7", "products": [{"pname": "Doohickey", "variants": [
        {"color": "red", "price": 120, "specs": [{"key": "weight", "val": 60}]}]}]},
]

# ambig: sports-league domain, "name" is a real field at TWO different nesting depths --
# region.team.name (a team's own name) and region.team.player.name (a player's name, one
# level deeper). Unchanged from the single-level report -- already multi-level by design.
AMBIG_MAPPING = {
    "title": {"type": "keyword"},
    "region": {"type": "nested", "properties": {
        "team": {"type": "nested", "properties": {
            "name": {"type": "keyword"},
            "player": {"type": "nested", "properties": {"name": {"type": "keyword"}}},
        }},
    }},
}

AMBIG_DOCS = [
    {"title": "Acme Sports Network",
     "region": [{"team": [{"name": "Tigers", "player": [{"name": "Eagle"}]}]}]},
    {"title": "Metro Sports Group",
     "region": [{"team": [{"name": "Eagle", "player": [{"name": "Tigers"}]}]}]},
    {"title": "Neutral Sports Co",
     "region": [{"team": [{"name": "Sharks", "player": [{"name": "Ray"}]}]}]},
]

INDICES = [
    {
        "name": "cmts_deep",
        "description": "Multi-level mirror of the single-level report's cmts -- comments is nested, "
                        "and author/score are pushed one level deeper into a replies nested array "
                        "(comments.replies.author / comments.replies.score). Same 8 documents and "
                        "values as cmts, just one extra nesting hop.",
        "mapping": CMTS_DEEP_MAPPING,
        "docs": CMTS_DEEP_DOCS,
    },
    {
        "name": "c3",
        "description": "3-level nested corpus dataset (products -> variants -> specs), unchanged "
                        "from the single-level report -- already exercises multi-level nesting.",
        "mapping": C3_MAPPING,
        "docs": C3_DOCS,
    },
    {
        "name": "ambig",
        "description": "Sports-league domain testing the ambiguous-nested-name scenario: leaf field "
                        "\"name\" appears at two depths under the same ancestor region.team -- "
                        "directly (region.team.name, a team's own name) and one level deeper "
                        "(region.team.player.name, a player's name).",
        "mapping": AMBIG_MAPPING,
        "docs": AMBIG_DOCS,
    },
]


def create_index(name, mapping, docs):
    req("DELETE", f"/{name}")
    _, err = req("PUT", f"/{name}", body={"settings": COMPOSITE_SETTINGS, "mappings": {"properties": mapping}})
    if err:
        print(f"FATAL create {name}: {err}")
        sys.exit(1)
    lines = []
    for doc in docs:
        lines.append(json.dumps({"index": {}}))
        lines.append(json.dumps(doc))
    _, err = req("POST", f"/{name}/_bulk?refresh=true", ndjson="\n".join(lines) + "\n")
    if err:
        print(f"FATAL bulk {name}: {err}")
        sys.exit(1)
    req("POST", f"/{name}/_flush?force=true")
    print(f"  {name}: {len(docs)} docs")


def setup():
    for idx in INDICES:
        create_index(idx["name"], idx["mapping"], idx["docs"])


# ─── Log-scrape plan capture ────────────────────────────────────────────────

RAW_MARKER = "[TRACE-STEP] runAllOptimizations: START. rawRelNode="
REWRITTEN_MARKER_RE = re.compile(r"\[TRACE-STEP\] after nested-field rewrite \(changed=(true|false)\):")
TS_LINE_RE = re.compile(r"^\[20\d\d-\d\d-\d\dT")


def log_line_count():
    with open(LOGFILE, "rb") as f:
        return sum(1 for _ in f)


def extract_tree_block(lines, marker_substr=None, marker_regex=None):
    """Given a window of log lines, find the line containing marker_substr (or matching
    marker_regex) and collect the tree text that follows up to the next timestamped line."""
    for i, line in enumerate(lines):
        if (marker_substr and marker_substr in line) or (marker_regex and marker_regex.search(line)):
            tree_lines = []
            for later in lines[i + 1:]:
                if TS_LINE_RE.match(later):
                    break
                if later.strip() == "":
                    continue
                tree_lines.append(later.rstrip("\n"))
            return "\n".join(tree_lines)
    return None


def run_query_with_plans(query, endpoint="/_analytics/ppl/_explain"):
    """POSTs the query and returns (response_json, error, raw_relnode, rewritten_relnode,
    full_plan_lines) -- the last three come from scraping the log window this request wrote,
    keyed by line-count-before/after since the server handles one request at a time here."""
    before = log_line_count()
    resp, err = req("POST", endpoint, body={"query": query})
    after = log_line_count()
    with open(LOGFILE, "r", errors="replace") as f:
        f_lines = f.readlines()
    window = f_lines[before:after]
    raw_tree = extract_tree_block(window, marker_substr=RAW_MARKER)
    rewritten_tree = extract_tree_block(window, marker_regex=REWRITTEN_MARKER_RE)
    full_plan = None
    if resp is not None:
        full_plan = resp.get("profile", {}).get("full_plan")
    return resp, err, raw_tree, rewritten_tree, full_plan


# ─── Case definitions ───────────────────────────────────────────────────────
# Same 23 scenario numbers as the single-level report; queries re-pointed at cmts_deep's
# deeper path (comments.replies.author/score) where the single-level report used
# comments.author/score. Every expected result below was verified live against the running
# mustang server (matches the single-level report's expected values exactly, except #10 which
# now crashes instead of silently dropping a row).

CASES = []


def add(num, section, query, columns, rows, note=None, title_only=False):
    """title_only=True means `rows` is a list of expected title strings, extracted from the
    'title' column of the full row response -- used for the scenario-list queries that (as
    given, verbatim) have no `| fields title` clause and so return every column."""
    CASES.append({
        "num": num, "section": section, "query": query, "columns": columns, "rows": rows,
        "note": note, "title_only": title_only,
    })


add("1", "(A) FLAT field predicate", 'source=cmts_deep | where title = "P1"', ["title"], ["P1"], title_only=True)
add("2", "(A) FLAT field predicate", "source=cmts_deep | where views > 50", ["title"],
    ["P1", "P3", "P5", "P7", "P8"], title_only=True)
add("3", "(A) FLAT field predicate", 'source=cmts_deep | where title = "P1" and views > 50', ["title"], ["P1"],
    title_only=True)
add("4", "(A) FLAT field predicate", 'source=cmts_deep | where title = "P1" or title = "P2"', ["title"],
    ["P1", "P2"], title_only=True)
add("4a", "(A) FLAT field predicate", 'source=cmts_deep | where not (title = "P1")', ["title"],
    ["P2", "P3", "P4", "P5", "P6", "P7", "P8"], title_only=True)
add("4b", "(A) FLAT field predicate", 'source=cmts_deep | where not (title = "P1" and views > 50)', ["title"],
    ["P2", "P3", "P4", "P5", "P6", "P7", "P8"], title_only=True)

add("5", "(B) SINGLE nested equality, multi-level (comments.replies.author)",
    'source=cmts_deep | where comments.replies.author = "alice"', ["title"],
    ["P1", "P2", "P6", "P8"], title_only=True)
add("5a", "(B) SINGLE nested equality, multi-level (comments.replies.author)",
    'source=cmts_deep | where not (comments.replies.author = "alice")', ["title"],
    ["P1", "P3", "P7"], title_only=True,
    note="∃¬ reading (“some element fails”), not ¬∃ (“no element matches”) "
         "-- both are defensible readings of raw PPL not(nested); this is the documented, pre-existing "
         "F3_neg_grain ambiguity, unrelated to this report's scope. Same reading, same result set, as "
         "the single-level report's #5a.")
add("6", "(B) SINGLE nested equality, multi-level (c3, unchanged)",
    'source=c3 | where products.variants.color = "red"',
    ["title"], ["C1", "C2", "C5", "C6", "C7"], title_only=True)
add("7", "(B) nested + 1 flat conjunct, multi-level",
    'source=cmts_deep | where comments.replies.author = "alice" and views > 50',
    ["title"], ["P1", "P8"], title_only=True)
add("7a", "(B) nested + 1 flat conjunct (NOT on flat), multi-level",
    'source=cmts_deep | where comments.replies.author = "alice" and not (views > 50)', ["title"], ["P2", "P6"],
    title_only=True)
add("8", "(B) nested + 1 flat conjunct, multi-level (c3, unchanged)",
    'source=c3 | where products.variants.color = "red" and title = "C1"', ["title"], ["C1"], title_only=True)
add("9", "(B) nested + multiple flat conjuncts, multi-level",
    'source=cmts_deep | where comments.replies.author = "alice" and views > 50 and title = "P1"',
    ["title"], ["P1"], title_only=True)

add("10", "(B-negative) OR breaks the classification, multi-level",
    'source=cmts_deep | where comments.replies.author = "alice" or views > 50', ["title"],
    ["P1", "P2", "P3", "P5", "P6", "P7", "P8"], title_only=True,
    note="FIXED (was: BUG, worse at multi-level than single-level -- a hard crash instead of a "
         "silent wrong result). Root cause was: the OR forced a fallback to the Correlate+"
         "Uncollect unnest path because one operand (views>50) has no ITEM-on-array reference; "
         "at 2+ levels of nesting that fallback's intermediate row carried a raw multi-level "
         "Struct that DataFusion's array_element/sum had no signature for, crashing with a 500 "
         "'Internal error'. Fix: OpenSearchNestedFieldRewriter now splits a top-level OR into "
         "array-referencing and pure-parent operands (mirroring the existing AND-split), builds "
         "the array side into its own NESTED_ANY_MATCH_EXPR (correctly nesting the "
         "{\"nested\":\"replies\",...} wrapper for this multi-level path), and ORs it back with "
         "the parent operand(s) at the row level -- so the unnest fallback, and the crash it "
         "caused, are never reached. See RelNode (rewritten) below. Verified against real "
         "vanilla OpenSearch on the same data -- both now return the same 7 docs including P5.")

add("11", "Existence check, multi-level", "source=cmts_deep | where comments.replies.author is not null",
    ["title"], ["P1", "P2", "P3", "P6", "P7", "P8"], title_only=True)

add("12", "Aggregation -- parent doc id, multi-level",
    'source=cmts_deep | where comments.replies.author = "alice" | stats count() by title',
    ["count()", "title"], [[1, "P1"], [1, "P2"], [1, "P6"], [1, "P8"]])

add("13", "Aggregation -- nested field (group by), multi-level",
    "source=cmts_deep | stats count() by comments.replies.author",
    ["count()", "comments.replies.author"], [[5, "alice"], [3, "bob"], [1, "carol"]])
add("14", "Aggregation -- nested field (metric only), multi-level",
    "source=cmts_deep | stats avg(comments.replies.score)",
    ["avg(comments.replies.score)"], [[46.0]])
add("15", "Aggregation -- nested field (metric + group by), multi-level",
    "source=cmts_deep | stats avg(comments.replies.score) by comments.replies.author",
    ["avg(comments.replies.score)", "comments.replies.author"],
    [[40.8, "alice"], [63.333333333333336, "bob"], [20.0, "carol"]])
add("16", "Aggregation -- nested field, multi-level (group by) (c3, unchanged)",
    "source=c3 | stats count() by products.variants.color", ["count()", "products.variants.color"],
    [[4, "red"], [1, "blue"]])
add("17", "Aggregation -- nested field, multi-level (metric only) (c3, unchanged)",
    "source=c3 | stats avg(products.variants.price)", ["avg(products.variants.price)"], [[114.0]])

add("18", "Ambiguous nested-name resolution (unchanged)", 'source=ambig | where region.team.name = "Tigers"',
    ["title"], ["Acme Sports Network"], title_only=True,
    note="Shallow occurrence of leaf “name” (a TEAM's own name, directly under region.team). "
         "Must resolve to Acme only -- Metro's PLAYER is also named “Tigers” one level deeper "
         "(region.team.player.name), so any depth-confusion in the resolver would wrongly leak "
         "Metro in here.")
add("18a", "Ambiguous nested-name resolution (unchanged)",
    'source=ambig | where region.team.player.name = "Tigers"',
    ["title"], ["Metro Sports Group"], title_only=True,
    note="Deep occurrence of leaf “name” (a PLAYER's name, under sibling nested "
         "region.team.player). Must resolve to Metro only, independently of 18's shallow "
         "team-name “Tigers” (Acme's team, not Metro's player).")


# ─── Comparison ─────────────────────────────────────────────────────────────

def normalize(rows):
    def nv(v):
        if v is None:
            return "\x00null"
        if isinstance(v, float):
            return round(v, 4)
        return v
    return sorted([tuple(nv(v) for v in row) for row in rows], key=str)


def extract_titles(resp):
    """Pulls just the 'title' column's values out of a full (multi-column) row response."""
    columns = resp.get("columns", [])
    if "title" not in columns:
        return None
    idx = columns.index("title")
    return sorted(row[idx] for row in resp.get("rows", []))


def run_all():
    results = []
    ok = wrong = err_count = 0
    for case in CASES:
        resp, err, raw_tree, rewritten_tree, full_plan = run_query_with_plans(case["query"])
        if err:
            verdict = "ERROR"
            got_rows = None
            detail = json.dumps(err)[:300]
        elif case.get("title_only"):
            got_rows = resp.get("rows", [])
            act_titles = extract_titles(resp)
            exp_titles = sorted(case["rows"])
            verdict = "MATCH" if act_titles == exp_titles else "MISMATCH"
            detail = None
            case = {**case, "got_titles": act_titles}
        else:
            got_rows = resp.get("rows", [])
            exp_rows = normalize(case["rows"])
            act_rows = normalize(got_rows)
            if act_rows == exp_rows:
                verdict = "MATCH"
            else:
                verdict = "MISMATCH"
            detail = None
        if verdict == "MATCH":
            ok += 1
        elif verdict == "MISMATCH":
            wrong += 1
        else:
            err_count += 1
        results.append({
            **case, "verdict": verdict, "got_rows": got_rows, "detail": detail,
            "raw_tree": raw_tree, "rewritten_tree": rewritten_tree, "full_plan": full_plan,
        })
        print(f"  #{case['num']:<4} [{verdict}] {case['query']}")

    print("\n" + "=" * 90)
    print(f" TOTAL: {ok} MATCH / {wrong} MISMATCH / {err_count} ERROR   of {len(CASES)}")
    print("=" * 90)

    failures = [r for r in results if r["verdict"] != "MATCH"]
    with open(FAILURES_JSON, "w") as f:
        json.dump(
            [{"num": r["num"], "query": r["query"], "verdict": r["verdict"], "detail": r["detail"]} for r in failures],
            f, indent=1,
        )
    print(f"Saved {FAILURES_JSON} ({len(failures)} non-matching cases -- expected: only #10, the documented crash)")
    return results


# ─── HTML report ────────────────────────────────────────────────────────────

CSS = """
:root { color-scheme: light dark; }
body { font-family: -apple-system, "Segoe UI", Helvetica, sans-serif; margin: 20px; line-height: 1.5; font-size: 13px; }
h1 { font-size: 18px; } h2 { font-size: 15px; margin-top: 28px; } h3 { font-size: 14px; margin-top: 22px; color: #333; }
@media (prefers-color-scheme: dark) { h3 { color: #ccc; } }
table { border-collapse: collapse; margin: 10px 0 20px; width: 100%; }
th, td { border: 1px solid #8884; padding: 5px 9px; font-size: 12px; text-align: left; vertical-align: top; }
th { background: #f3f3f3; position: sticky; top: 0; }
@media (prefers-color-scheme: dark) { th { background: #2a2a2a; } pre { background: #1a1a1a; } }
code { font-family: ui-monospace, Menlo, monospace; font-size: 11px; }
pre { font-family: ui-monospace, Menlo, monospace; font-size: 11px; background: #f5f5f5; padding: 10px; border-radius: 4px; overflow-x: auto; margin: 6px 0; white-space: pre-wrap; word-wrap: break-word; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }
.badge-pass { background: #4caf50; color: white; }
.badge-fail { background: #f44336; color: white; }
.badge-sec { background: #6a4bc6; color: white; }
.badge-idx { background: #0277bd; color: white; }
details { margin: 4px 0; }
details summary { cursor: pointer; font-weight: bold; font-size: 12px; padding: 2px 0; }
details pre { margin-top: 6px; }
.section-header { background: #e3edf7; padding: 8px 12px; border-radius: 6px; margin-top: 20px; margin-bottom: 8px; }
@media (prefers-color-scheme: dark) { .section-header { background: #1c2c3c; } }
.note-box { background: #fff3e0; border-left: 4px solid #ff9800; padding: 10px 14px; border-radius: 4px; margin: 12px 0; }
@media (prefers-color-scheme: dark) { .note-box { background: #3a2a10; } }
.case-card { border: 1px solid #8884; border-radius: 6px; margin: 14px 0; padding: 12px 14px; }
.case-card.pass { border-left: 4px solid #4caf50; }
.case-card.fail { border-left: 4px solid #f44336; }
.case-title { font-weight: bold; font-size: 13px; }
.two-col { display: flex; gap: 16px; flex-wrap: wrap; }
.two-col > div { flex: 1; min-width: 260px; }
.result-box { padding: 6px 10px; border-radius: 4px; background: #f5f5f5; }
@media (prefers-color-scheme: dark) { .result-box { background: #1a1a1a; } }
.toc { background: #f0f0f0; padding: 10px 16px; border-radius: 6px; }
@media (prefers-color-scheme: dark) { .toc { background: #222; } }
.index-block { border: 1px solid #8884; border-radius: 6px; margin: 14px 0; padding: 12px 14px; }
.doc-entry { margin-bottom: 8px; padding: 8px; background: #fafafa; border-radius: 4px; border-left: 3px solid #ddd; }
@media (prefers-color-scheme: dark) { .doc-entry { background: #1e1e1e; border-left-color: #444; } }
"""


def esc(s):
    return html.escape(str(s), quote=True)


def render_mapping_table(mapping, prefix=""):
    """Recursively flattens a mapping's properties into (dotted path, type) rows for display."""
    rows = []
    for field_name, field_props in mapping.items():
        path = f"{prefix}.{field_name}" if prefix else field_name
        ftype = field_props.get("type", "object")
        rows.append((path, ftype))
        if "properties" in field_props:
            rows.extend(render_mapping_table(field_props["properties"], path))
    return rows


def render_index_block(idx):
    mapping_rows = render_mapping_table(idx["mapping"])
    mapping_table = "<table><thead><tr><th>Field path</th><th>Type</th></tr></thead><tbody>" + "".join(
        f"<tr><td><code>{esc(path)}</code></td><td><code>{esc(ftype)}</code></td></tr>" for path, ftype in mapping_rows
    ) + "</tbody></table>"
    docs_html = "".join(
        f'<div class="doc-entry"><pre>{esc(json.dumps(doc, indent=2))}</pre></div>' for doc in idx["docs"]
    )
    return f"""
<div class="index-block">
  <h3><code>{esc(idx['name'])}</code> <span class="badge badge-idx">{len(idx['docs'])} docs</span></h3>
  <p>{esc(idx['description'])}</p>
  <p><b>Mapping (flattened, dotted paths):</b></p>
  {mapping_table}
  <details><summary>Documents ({len(idx['docs'])})</summary>{docs_html}</details>
</div>
"""


def render_rows(rows):
    if rows is None:
        return "<code>(no result -- see error)</code>"
    if not rows:
        return "<code>[]</code>"
    return "<code>" + esc(json.dumps(rows)) + "</code>"


def render_case(r):
    passed = r["verdict"] == "MATCH"
    css_cls = "pass" if passed else "fail"
    badge = '<span class="badge badge-pass">MATCH</span>' if passed else f'<span class="badge badge-fail">{esc(r["verdict"])}</span>'
    note_html = ""
    if r.get("note"):
        note_html = f'<div class="note-box"><b>Note.</b> {esc(r["note"])}</div>'
    full_plan_text = "\n".join(r["full_plan"]) if r["full_plan"] else "(not captured)"
    raw_tree_text = r["raw_tree"] or "(not captured)"
    rewritten_tree_text = r["rewritten_tree"] or "(not captured)"
    detail_html = f'<p><b>Error detail:</b> <code>{esc(r["detail"])}</code></p>' if r["detail"] else ""
    if r.get("title_only"):
        expected_display = render_rows(sorted(r["rows"]))
        got_display = render_rows(r.get("got_titles"))
    else:
        expected_display = render_rows(r["rows"])
        got_display = render_rows(r["got_rows"])
    return f"""
<div class="case-card {css_cls}">
  <div class="case-title">#{esc(r['num'])} {badge}</div>
  <p><b>PPL query:</b></p><pre>{esc(r['query'])}</pre>
  {detail_html}
  <div class="two-col">
    <div><p><b>Expected (title):</b></p><div class="result-box">{expected_display}</div></div>
    <div><p><b>Got (title, extracted from full row response):</b></p><div class="result-box">{got_display}</div></div>
  </div>
  <details><summary>Got -- full row response (all columns)</summary><pre>{esc(json.dumps(r['got_rows']))}</pre></details>
  <details><summary>RelNode (raw, pre-rewrite)</summary><pre>{esc(raw_tree_text)}</pre></details>
  <details><summary>RelNode (rewritten, post nested-field-rewrite)</summary><pre>{esc(rewritten_tree_text)}</pre></details>
  <details><summary>Logical plan (final, annotated with backend routing)</summary><pre>{esc(full_plan_text)}</pre></details>
  {note_html}
</div>
"""


def write_html(results):
    sections = []
    seen = []
    for r in results:
        if r["section"] not in seen:
            seen.append(r["section"])
    ok = sum(1 for r in results if r["verdict"] == "MATCH")
    total = len(results)
    for sec in seen:
        sec_results = [r for r in results if r["section"] == sec]
        sec_ok = sum(1 for r in sec_results if r["verdict"] == "MATCH")
        cards = "\n".join(render_case(r) for r in sec_results)
        sections.append(f"""
<div class="section-header"><h2>{esc(sec)} <span class="badge badge-sec">{sec_ok}/{len(sec_results)}</span></h2></div>
{cards}
""")
    body = "\n".join(sections)
    index_blocks = "\n".join(render_index_block(idx) for idx in INDICES)
    html_doc = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Complete Minimal Query Set (Analytics/PPL) -- MULTI-LEVEL NESTING -- mustang-nested-testkit Results</title>
<style>{CSS}</style></head><body>
<h1>Complete Minimal Query Set (Analytics/PPL) &mdash; MULTI-LEVEL NESTING &mdash; mustang-nested-testkit Results</h1>
<p><b>Branch:</b> <code>shreanu/nested-poc-search-rewrite</code><br>
<b>Scope:</b> the same finalized 23-scenario minimal set as the single-level report, but every
single-level nested scenario is re-run against a NEW dataset (<code>cmts_deep</code>) where the
nested field sits one level deeper (<code>comments.replies.author</code> /
<code>comments.replies.score</code> instead of <code>comments.author</code> /
<code>comments.score</code>) -- same 8 documents and values, one extra nesting hop. Scenarios
already multi-level in the original report (6, 8, 16, 17 on <code>c3</code>; 18/18a on
<code>ambig</code>) are unchanged.</p>
<div class="toc"><b>Summary: {ok}/{total} passed.</b> The one non-matching case (#10) is a
documented correctness gap that is STRICTLY WORSE at multi-level depth than at single-level
(a hard crash instead of a silently wrong result) -- see its note-box below.</p>
</div>
<h2>Indices &amp; Data</h2>
<p>Every index used by the scenarios below, its field mapping (dotted paths, flattened), and its
documents verbatim.</p>
{index_blocks}
{body}
</body></html>
"""
    with open(OUT_HTML, "w") as f:
        f.write(html_doc)
    print(f"Wrote {OUT_HTML}")


def main():
    _, err = req("GET", "/")
    if err:
        print(f"ERROR: cannot connect to cluster at {BASE}: {err}")
        sys.exit(1)
    if not os.path.exists(LOGFILE):
        print(f"ERROR: log file not found at {LOGFILE} -- set MUSTANG_LOGFILE")
        sys.exit(1)

    print("Setting up indices...")
    setup()

    print(f"\nRunning {len(CASES)} cases...\n")
    results = run_all()
    write_html(results)


if __name__ == "__main__":
    main()
