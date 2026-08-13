#!/usr/bin/env python3
"""
Complete Minimal Query Set (Analytics/PPL) regression suite + HTML report.

Runs the 23-query "minimal scenario set" (finalized after Suresh/Ansh's team feedback:
NOT variants wherever applicable, plus the corrected "ambiguous nested-name" pair 18/18a —
same LEAF FIELD NAME recurring at two different depths within one nested hierarchy, e.g.
"c" at both a.b.c and a.b.x.c) against a live mustang server, and generates
scenario-minimal-set-test-results.html in the same case-card visual style as
multi-level-nested-poc-test-results.html, but with each card additionally showing:
  - RelNode (raw)        -- pre-rewrite Calcite tree
  - RelNode (rewritten)  -- post nested-field-rewrite tree (NESTED_ANY_MATCH_EXPR/CHILD)
  - Logical plan          -- final annotated plan with backend routing (profile.full_plan)

The two tree snapshots are NOT exposed as JSON profile fields today -- PlannerImpl logs them
at INFO level on every query ([TRACE-STEP] runAllOptimizations: START / after nested-field
rewrite) to the server's runTask.log. This script scrapes that log window (by line-count
before/after each request) rather than modifying engine Java, since the server is already
running and a log-scrape needs no rebuild/restart. See MULTI_LEVEL_NESTED_PLAN.md-adjacent
session notes for the alternative (real QueryProfile fields) if this ever needs to become a
permanent API instead of a one-off report.

Extends `cmts` with an additive `views` (integer) field -- required by scenarios 2/3/4b/7/
7a/9/10, absent from the pre-existing cmts mapping. Existing corpus cases against cmts only
reference title/comments and are unaffected. Adds a brand-new `ambig` index (a.b nested with
leaf "c", sibling nested a.b.x also with leaf "c") since no existing dataset has a repeated
leaf field name at different depths.

Usage:
    python3 scenario-minimal-set-tests.py
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
OUT_HTML = os.path.join(SCRIPT_DIR, "scenario-minimal-set-test-results.html")
FAILURES_JSON = "/tmp/scenario_minimal_set_failures.json"

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

CMTS_MAPPING = {
    "title": {"type": "keyword"},
    "views": {"type": "integer"},
    "comments": {"type": "nested", "properties": {
        "author": {"type": "keyword"}, "score": {"type": "integer"}}},
}

CMTS_DOCS = [
    {"title": "P1", "views": 60, "comments": [{"author": "alice", "score": 5}, {"author": "bob", "score": 90}]},
    {"title": "P2", "views": 30, "comments": [{"author": "alice", "score": 99}]},
    {"title": "P3", "views": 70, "comments": [{"author": "bob", "score": 10}, {"author": "carol", "score": 20}]},
    {"title": "P4", "views": 10, "comments": []},
    {"title": "P5", "views": 90},
    {"title": "P6", "views": 45, "comments": [{"author": "alice", "score": 5}]},
    {"title": "P7", "views": 55, "comments": [{"author": "bob", "score": 90}]},
    {"title": "P8", "views": 80, "comments": [{"author": "alice", "score": 5}, {"author": "alice", "score": 90}]},
]

# Sports-league domain: "name" is a real field at TWO different nesting depths --
# region.team.name (a team's own name) and region.team.player.name (a player's name,
# one level deeper). Same leaf field name, different depth, same ancestor (region.team) --
# the exact "ambiguous nested-name" shape from the Suresh/Ansh scenario, made concrete so a
# depth-confusion bug would be obviously wrong (a query for a TEAM name should never match on
# a PLAYER's name, and vice versa).
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
    # Team "Tigers" has a player nicknamed "Eagle" -- normal case, no name collision within this doc.
    {"title": "Acme Sports Network",
     "region": [{"team": [{"name": "Tigers", "player": [{"name": "Eagle"}]}]}]},
    # The trap doc: team "Eagle" has a player nicknamed "Tigers" -- the SAME word "Tigers" that
    # Acme uses as a TEAM name here appears as a PLAYER name instead. If the resolver ever
    # confused shallow region.team.name with deep region.team.player.name, a query for
    # team.name="Tigers" would wrongly also match this doc (via its player), or a query for
    # player.name="Tigers" would wrongly match Acme instead.
    {"title": "Metro Sports Group",
     "region": [{"team": [{"name": "Eagle", "player": [{"name": "Tigers"}]}]}]},
    # Neutral control -- no shared values with the other two docs at all.
    {"title": "Neutral Sports Co",
     "region": [{"team": [{"name": "Sharks", "player": [{"name": "Ray"}]}]}]},
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
    create_index("cmts", CMTS_MAPPING, CMTS_DOCS)
    create_index("ambig", AMBIG_MAPPING, AMBIG_DOCS)


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
# (num, section, query, columns, expected_rows, note)
# note is shown in a note-box for cases with a documented semantic caveat (not a bug we're
# tracking as a regression), or "BUG:..." for the one known correctness gap (case 10).

CASES = []


def add(num, section, query, columns, rows, note=None, title_only=False):
    """title_only=True means `rows` is a list of expected title strings, extracted from the
    'title' column of the full row response -- used for the scenario-list queries that (as
    given, verbatim) have no `| fields title` clause and so return every column."""
    CASES.append({
        "num": num, "section": section, "query": query, "columns": columns, "rows": rows,
        "note": note, "title_only": title_only,
    })


add("1", "(A) FLAT field predicate", 'source=cmts | where title = "P1"', ["title"], ["P1"], title_only=True)
add("2", "(A) FLAT field predicate", "source=cmts | where views > 50", ["title"],
    ["P1", "P3", "P5", "P7", "P8"], title_only=True)
add("3", "(A) FLAT field predicate", 'source=cmts | where title = "P1" and views > 50', ["title"], ["P1"],
    title_only=True)
add("4", "(A) FLAT field predicate", 'source=cmts | where title = "P1" or title = "P2"', ["title"],
    ["P1", "P2"], title_only=True)
add("4a", "(A) FLAT field predicate", 'source=cmts | where not (title = "P1")', ["title"],
    ["P2", "P3", "P4", "P5", "P6", "P7", "P8"], title_only=True)
add("4b", "(A) FLAT field predicate", 'source=cmts | where not (title = "P1" and views > 50)', ["title"],
    ["P2", "P3", "P4", "P5", "P6", "P7", "P8"], title_only=True)

add("5", "(B) SINGLE nested equality", 'source=cmts | where comments.author = "alice"', ["title"],
    ["P1", "P2", "P6", "P8"], title_only=True)
add("5a", "(B) SINGLE nested equality", 'source=cmts | where not (comments.author = "alice")', ["title"],
    ["P1", "P3", "P7"], title_only=True,
    note="∃¬ reading (“some element fails”), not ¬∃ (“no element matches”) "
         "-- both are defensible readings of raw PPL not(nested); this is the documented, pre-existing "
         "F3_neg_grain ambiguity, unrelated to this report's scope.")
add("6", "(B) SINGLE nested equality, multi-level", 'source=c3 | where products.variants.color = "red"',
    ["title"], ["C1", "C2", "C5", "C6", "C7"], title_only=True)
add("7", "(B) nested + 1 flat conjunct", 'source=cmts | where comments.author = "alice" and views > 50',
    ["title"], ["P1", "P8"], title_only=True)
add("7a", "(B) nested + 1 flat conjunct (NOT on flat)",
    'source=cmts | where comments.author = "alice" and not (views > 50)', ["title"], ["P2", "P6"],
    title_only=True)
add("8", "(B) nested + 1 flat conjunct, multi-level",
    'source=c3 | where products.variants.color = "red" and title = "C1"', ["title"], ["C1"], title_only=True)
add("9", "(B) nested + multiple flat conjuncts",
    'source=cmts | where comments.author = "alice" and views > 50 and title = "P1"', ["title"], ["P1"],
    title_only=True)

add("10", "(B-negative) OR breaks the classification",
    'source=cmts | where comments.author = "alice" or views > 50', ["title"],
    ["P1", "P2", "P3", "P5", "P6", "P7", "P8"], title_only=True,
    note="FIXED (was: BUG, mustang dropped P5). Root cause was: one OR operand (views>50) has "
         "no ITEM-on-array reference, so ExprTreeBuilder.build() returned null for that operand "
         "and the whole lambda rewrite bailed to the Correlate+Uncollect fallback -- an INNER "
         "join on comments that drops any parent with an absent/empty array BEFORE the OR "
         "filter ever evaluates the parent-only branch. Fix: OpenSearchNestedFieldRewriter now "
         "splits a top-level OR into array-referencing and pure-parent operands (mirroring the "
         "existing AND-split), builds the array side into its own NESTED_ANY_MATCH_EXPR, and ORs "
         "it back with the parent operand(s) at the row level -- valid because existential "
         "quantification distributes over OR. See RelNode (rewritten) below: the fix keeps the "
         "condition as a flat OR(NESTED_ANY_MATCH_EXPR(...), >(views,50)), never falling back to "
         "unnest. Verified against real vanilla OpenSearch on the same data -- both now return "
         "the same 7 docs including P5.")

add("11", "Existence check", "source=cmts | where comments.author is not null", ["title"],
    ["P1", "P2", "P3", "P6", "P7", "P8"], title_only=True)

add("12", "Aggregation -- parent doc id",
    'source=cmts | where comments.author = "alice" | stats count() by title', ["count()", "title"],
    [[1, "P1"], [1, "P2"], [1, "P6"], [1, "P8"]])

add("13", "Aggregation -- nested field (group by)", "source=cmts | stats count() by comments.author",
    ["count()", "comments.author"], [[5, "alice"], [3, "bob"], [1, "carol"]])
add("14", "Aggregation -- nested field (metric only)", "source=cmts | stats avg(comments.score)",
    ["avg(comments.score)"], [[46.0]])
add("15", "Aggregation -- nested field (metric + group by)",
    "source=cmts | stats avg(comments.score) by comments.author", ["avg(comments.score)", "comments.author"],
    [[40.8, "alice"], [63.333333333333336, "bob"], [20.0, "carol"]])
add("16", "Aggregation -- nested field, multi-level (group by)",
    "source=c3 | stats count() by products.variants.color", ["count()", "products.variants.color"],
    [[4, "red"], [1, "blue"]])
add("17", "Aggregation -- nested field, multi-level (metric only)",
    "source=c3 | stats avg(products.variants.price)", ["avg(products.variants.price)"], [[114.0]])

add("18", "Ambiguous nested-name resolution", 'source=ambig | where region.team.name = "Tigers"',
    ["title"], ["Acme Sports Network"], title_only=True,
    note="Shallow occurrence of leaf “name” (a TEAM's own name, directly under region.team). "
         "Must resolve to Acme only -- Metro's PLAYER is also named “Tigers” one level deeper "
         "(region.team.player.name), so any depth-confusion in the resolver would wrongly leak "
         "Metro in here.")
add("18a", "Ambiguous nested-name resolution", 'source=ambig | where region.team.player.name = "Tigers"',
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
    print(f"Saved {FAILURES_JSON} ({len(failures)} non-matching cases -- expected: only #10, the documented bug)")
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
"""


def esc(s):
    return html.escape(str(s), quote=True)


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
    html_doc = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Complete Minimal Query Set (Analytics/PPL) -- mustang-nested-testkit Results</title>
<style>{CSS}</style></head><body>
<h1>Complete Minimal Query Set (Analytics/PPL) &mdash; mustang-nested-testkit Results</h1>
<p><b>Branch:</b> <code>shreanu/nested-poc-search-rewrite</code><br>
<b>Scope:</b> the finalized 23-query minimal scenario set (flat predicates, single/multi-level
nested equality, NOT variants, the OR-classification-break case, existence checks,
parent-grain and nested-grain aggregation, and the ambiguous-nested-name pair 18/18a covering
the same leaf field name recurring at two depths within one nested hierarchy).</p>
<div class="toc"><b>Summary: {ok}/{total} passed.</b> The one non-matching case (#10) is a
documented correctness gap, not a report artifact -- see its note-box below. Cases 18/18a use
a brand-new <code>ambig</code> index built specifically for this report (leaf field
&ldquo;c&rdquo; present at both <code>a.b.c</code> and <code>a.b.x.c</code>); all other cases
run against <code>cmts</code> (extended with an additive <code>views</code> field) or
<code>c3</code>, both pre-existing.</p>
</div>
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
