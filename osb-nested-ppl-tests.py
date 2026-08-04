#!/usr/bin/env python3
"""
PPL translation of the opensearch-benchmark-workloads `nested` workload
(https://github.com/opensearch-project/opensearch-benchmark-workloads/tree/main/nested).

That workload's 7 search operations are native OpenSearch Query DSL (bool/match/
nested/range/inner_hits/date_histogram) run via `_search`. This suite re-expresses
the same 7 query SHAPES as PPL against Mustang's `/_analytics/ppl` endpoint, on a
small hand-built StackOverflow-shaped dataset (`osb-nested-ppl-data.json`) mirroring
OSB's qid/title/user/creationDate/tag/answers[user,date] document shape.

Three constraints forced dataset changes from the literal OSB shape (all
discovered live against Mustang, not assumed):
  - `tag` is a single value, not an array — Mustang's parquet primary format
    rejects multi-valued flat keyword fields ("Cannot accept multiple values for
    field: [tag] of type: [keyword]").
  - `answers.date` is an integer `date_epoch` (days-since-epoch), not a date type —
    date-typed fields inside a nested struct aren't supported yet by the POC nested
    writer ("unsupported struct-leaf vector type [TimeStampMilliVector]").
  - The top-level question-asker field is `asker`, not `user` — OSB's literal
    schema names BOTH the top-level asker and the nested answerer `user`. That's
    harmless in DSL (queries scope `answers.user` explicitly, no flattening), but
    after PPL `expand answers` the exploded child's unqualified `user` and the
    still-present parent `user` collide; `fields ... user` silently resolved to
    the PARENT's value (confirmed live) and dotted access post-expand errors with
    "Field [answers.user] not found." (fields are already flattened by then, no
    disambiguation exists). Renaming the parent field avoids the collision — a
    dataset-design lesson, not a Mustang bug.

Query-shape correspondence (verified live before building this generator):
  1. randomized-nested-queries          -> where tag=X and answers.date_epoch<=Y
  2. ..with-inner-hits (small/big)      -> where tag=X | expand answers | where date_epoch<=Y | head N
  3. randomized-term-queries            -> where tag=X (no nested)
  4. randomized-sorted-term-queries     -> where tag=X | sort [-] answers.date_epoch
  5. match-all                          -> source=... | fields ...
  6. nested-date-histo                  -> stats count() by span(answers.date_epoch, N)
     (works DIRECTLY on the dotted field — no `expand` needed, confirmed live;
     matches the general stats_by_child semantics: groups over ALL exploded children.)

Known semantic gap (not silently treated as equivalent — flagged for anyone reading
results): OSB's sorted-term-query DSL uses `mode:max` on the nested sort, so it
aggregates the WHOLE array. Our dotted-PPL sort instead uses the FIRST array
element (the vanilla quirk established earlier this session:
`AbstractCalciteIndexScan.pushDownSort` never calls `Utils.resolveNestedPath`).
The PPL translation below tests OUR actual first-element behavior, not a
faithful re-creation of the DSL's max-based semantics.

Expected results are computed in plain Python against the JSON dataset — same
independent-reference-semantics approach as own-nested-tests.py.

Usage:
    python3 osb-nested-ppl-tests.py            # generate cases file + run
    python3 osb-nested-ppl-tests.py --gen-only # only (re)write the cases file

Cluster must be running at BASE (default http://localhost:9000).
"""
import json
import os
import sys
import urllib.request
import urllib.error

BASE = os.environ.get("MUSTANG_BASE", "http://localhost:9000")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "osb-nested-ppl-data.json")
CASES_FILE = os.path.join(SCRIPT_DIR, "osb-nested-ppl-test-cases.txt")
INDEX = "sonested_ppl"

MAPPING = {
    "qid": {"type": "keyword"}, "title": {"type": "keyword"}, "asker": {"type": "keyword"},
    "creationDate": {"type": "date"}, "tag": {"type": "keyword"},
    "answers": {"type": "nested", "properties": {"user": {"type": "keyword"}, "date_epoch": {"type": "integer"}}},
}

SETTINGS = {
    "number_of_shards": 1, "number_of_replicas": 0,
    "index.pluggable.dataformat.enabled": True,
    "index.pluggable.dataformat": "composite",
    "index.composite.primary_data_format": "parquet",
    "index.composite.secondary_data_formats": "lucene",
}


# ─── HTTP helper ────────────────────────────────────────────────────────────

def req(method, path, body=None, ndjson=None, timeout=90):
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


def setup():
    with open(DATA_FILE) as f:
        data = json.load(f)
    req("DELETE", f"/{INDEX}")
    res, err = req("PUT", f"/{INDEX}", body={"settings": SETTINGS, "mappings": {"properties": MAPPING}})
    if err:
        print(f"FATAL create {INDEX}: {err}")
        sys.exit(1)
    lines = []
    for doc in data[INDEX]:
        lines.append(json.dumps({"index": {}}))
        lines.append(json.dumps(doc))
    res, err = req("POST", f"/{INDEX}/_bulk?refresh=true", ndjson="\n".join(lines) + "\n")
    if err or (res and res.get("errors")):
        print(f"FATAL bulk {INDEX}: {err or res}")
        sys.exit(1)
    req("POST", f"/{INDEX}/_flush?force=true")
    print(f"  {INDEX}: {len(data[INDEX])} docs")


# ─── Reference semantics ────────────────────────────────────────────────────

def load_data():
    with open(DATA_FILE) as f:
        return json.load(f)[INDEX]


def any_answer(doc, pred):
    return any(pred(a) for a in doc.get("answers", []))


def first_answer_field(doc, field):
    answers = doc.get("answers", [])
    return answers[0][field] if answers else None


CASES = []
_counter = [0]


def add(category, query, columns, rows, order_sensitive=False):
    _counter[0] += 1
    CASES.append((_counter[0], category, query, {"columns": columns, "rows": rows}, order_sensitive))


TAGS = ["java", "javascript", "linux", "ruby-on-rails"]
DATE_THRESHOLDS = [15400, 15420, 15440, 15460, 15480]


def gen_nested_range_queries():
    """OSB op 1 (randomized-nested-queries): match(tag) AND nested.range(answers.date<=X).
    PPL: where tag=X and answers.date_epoch<=Y | fields qid, title
    Tests the JOINT-AND-with-string-equality path we fixed (NlsString) combined with
    an existence-style range check — but here it's TWO independent conditions on
    DIFFERENT scopes (tag is parent-scalar, answers.date_epoch is per-element), so
    this is an AND of a parent predicate with an existence check, not a joint-element
    check (contrast with own-nested-tests.py's joint_and category, which is two
    conditions on the SAME element)."""
    data = load_data()
    for tag in TAGS:
        for thresh in DATE_THRESHOLDS:
            docs = [d for d in data if d["tag"] == tag and any_answer(d, lambda a, t=thresh: a["date_epoch"] <= t)]
            rows = sorted([[d["qid"], d["title"]] for d in docs], key=lambda r: str(r[0]))
            q = f'source={INDEX} | where tag = "{tag}" and answers.date_epoch <= {thresh} | fields qid, title'
            add("nested_range_and_tag", q, ["qid", "title"], rows)


def gen_term_only_queries():
    """OSB op 4 (randomized-term-queries): match(tag), no nested at all. Baseline/control."""
    data = load_data()
    for tag in TAGS:
        docs = [d for d in data if d["tag"] == tag]
        rows = sorted([[d["qid"], d["title"]] for d in docs], key=lambda r: str(r[0]))
        q = f'source={INDEX} | where tag = "{tag}" | fields qid, title'
        add("term_only", q, ["qid", "title"], rows)


def gen_inner_hits_style_expand():
    """OSB ops 2/3 (nested-queries-with-inner-hits, small + big size): match(tag) +
    nested.range with inner_hits materializing the matched children. PPL: filter on
    tag (parent), THEN expand to get one row per child, THEN filter children by the
    range — mirrors inner_hits' "show me which children matched" semantics rather
    than existence-only filtering."""
    data = load_data()
    for tag in TAGS:
        for thresh in DATE_THRESHOLDS:
            rows = []
            for d in data:
                if d["tag"] != tag:
                    continue
                for a in d.get("answers", []):
                    if a["date_epoch"] <= thresh:
                        rows.append([d["qid"], d["title"], a["user"], a["date_epoch"]])
            rows = sorted(rows, key=lambda r: str(r))
            q = (f'source={INDEX} | where tag = "{tag}" | expand answers '
                 f'| where date_epoch <= {thresh} | fields qid, title, user, date_epoch')
            add("inner_hits_style_expand", q, ["qid", "title", "user", "date_epoch"], rows)


def gen_sorted_term_queries():
    """OSB op 5 (randomized-sorted-term-queries): match(tag) + sort by nested
    answers.date (mode:max) DESC. [KNOWN GAP] Our dotted-PPL sort uses the FIRST
    array element, not the array's max — this tests OUR actual behavior, which is
    NOT a faithful translation of the DSL's mode:max semantics. Kept as a named,
    flagged divergence rather than silently treated as equivalent."""
    data = load_data()
    for tag in TAGS:
        docs = [d for d in data if d["tag"] == tag]
        first_key = lambda d: first_answer_field(d, "date_epoch")
        # nulls-last for DESC (docs with no answers), matches vanilla-established behavior
        docs_desc = sorted(docs, key=lambda d: (first_key(d) is None, -(first_key(d) or 0)))
        rows_desc = [[d["qid"], first_key(d)] for d in docs_desc]
        q_desc = f'source={INDEX} | where tag = "{tag}" | sort - answers.date_epoch | fields qid, answers.date_epoch'
        add("sorted_term_desc", q_desc, ["qid", "answers.date_epoch"], rows_desc, order_sensitive=True)

        # nulls-first for ASC
        docs_asc = sorted(docs, key=lambda d: (first_key(d) is not None, first_key(d) or 0))
        rows_asc = [[d["qid"], first_key(d)] for d in docs_asc]
        q_asc = f'source={INDEX} | where tag = "{tag}" | sort answers.date_epoch | fields qid, answers.date_epoch'
        add("sorted_term_asc", q_asc, ["qid", "answers.date_epoch"], rows_asc, order_sensitive=True)


def gen_match_all():
    """OSB op 6 (match-all): no filter, no nested — pure baseline."""
    data = load_data()
    rows = sorted([[d["qid"]] for d in data], key=lambda r: str(r[0]))
    add("match_all", f"source={INDEX} | fields qid", ["qid"], rows)

    rows2 = sorted([[d["qid"], d["title"], d["tag"]] for d in data], key=lambda r: str(r[0]))
    add("match_all", f"source={INDEX} | fields qid, title, tag", ["qid", "title", "tag"], rows2)


SPAN_WIDTHS = [10, 20, 30, 50]


def gen_date_histo():
    """OSB op 7 (nested-date-histo): nested agg -> date_histogram(answers.date, month).
    PPL: stats count() by span(answers.date_epoch, N) — confirmed live this works
    DIRECTLY on the dotted field (no `expand` required); groups over ALL exploded
    children the same way stats_by_child does for any other field."""
    data = load_data()
    for width in SPAN_WIDTHS:
        buckets = {}
        for d in data:
            for a in d.get("answers", []):
                bucket = (a["date_epoch"] // width) * width
                buckets[bucket] = buckets.get(bucket, 0) + 1
        rows = sorted([[c, b] for b, c in buckets.items()], key=lambda r: r[1])
        q = f"source={INDEX} | stats count() by span(answers.date_epoch, {width})"
        add("date_histo_span", q, ["count()", f"span(answers.date_epoch,{width})"], rows)


def gen_no_answers_edge_case():
    """Edge case unique to this dataset: a doc with an EMPTY answers array (qid
    10000105). Confirms existence-style dotted filters correctly exclude it (no
    element to match) and plain projection/stats-by-child yield null/absence
    correctly, not a crash."""
    data = load_data()
    no_ans_doc = next(d for d in data if not d.get("answers"))
    # existence filter must exclude the no-answers doc
    docs = [d for d in data if any_answer(d, lambda a: a["date_epoch"] > 0)]
    rows = sorted([[d["qid"]] for d in docs], key=lambda r: str(r[0]))
    add("no_answers_edge_case",
        f"source={INDEX} | where answers.date_epoch > 0 | fields qid", ["qid"], rows)
    assert no_ans_doc["qid"] not in [r[0] for r in rows], "no-answers doc must be excluded by existence filter"

    # plain projection on the no-answers doc's array field must be null, not error
    rows2 = sorted([[d["qid"], first_answer_field(d, "user")] for d in data], key=lambda r: str(r[0]))
    add("no_answers_edge_case",
        f"source={INDEX} | fields qid, answers.user", ["qid", "answers.user"], rows2)


def gen_all():
    gen_nested_range_queries()
    gen_term_only_queries()
    gen_inner_hits_style_expand()
    gen_sorted_term_queries()
    gen_match_all()
    gen_date_histo()
    gen_no_answers_edge_case()


def write_cases_file():
    with open(CASES_FILE, "w") as f:
        for num, cat, query, expected, order_sensitive in CASES:
            f.write(f"{num}|{cat}|{int(order_sensitive)}|{query}|{json.dumps(expected)}\n")
    print(f"Wrote {len(CASES)} cases to {CASES_FILE}")


# ─── Comparison / running ───────────────────────────────────────────────────

def normalize(rows, order_sensitive=False):
    def nv(v):
        if v is None:
            return "\x00null"
        if isinstance(v, float):
            return round(v, 4)
        if isinstance(v, (list, dict)):
            return json.dumps(v, sort_keys=True)
        return v
    tuples = [tuple(nv(v) for v in row) for row in rows]
    return tuples if order_sensitive else sorted(tuples, key=str)


def run_cases():
    ok = wrong = err_count = 0
    failures = []
    cat_stats = {}

    for i, (num, cat, query, expected, order_sensitive) in enumerate(CASES):
        exp_rows = normalize(expected["rows"], order_sensitive)
        cs = cat_stats.setdefault(cat, [0, 0, 0])
        actual, error = req("POST", "/_analytics/ppl", body={"query": query})
        if error:
            verdict, detail = "ERROR", error.get("error", {}).get("reason", "?")[:150]
        else:
            act_rows = normalize(actual.get("rows", []), order_sensitive)
            if act_rows == exp_rows:
                verdict, detail = "OK", ""
            else:
                verdict = "WRONG"
                detail = f"expected {json.dumps(exp_rows)[:150]} got {json.dumps(act_rows)[:150]}"

        if verdict == "OK":
            ok += 1
            cs[0] += 1
        elif verdict == "WRONG":
            wrong += 1
            cs[1] += 1
            failures.append((num, cat, query, verdict, detail))
        else:
            err_count += 1
            cs[2] += 1
            failures.append((num, cat, query, verdict, detail))

    print("\n" + "=" * 100)
    print(f" TOTAL: {ok} OK / {wrong} WRONG / {err_count} ERROR   of {len(CASES)}")
    print("=" * 100)
    print(f"\n {'category':<30} {'ok':>5} {'wrong':>6} {'error':>6}")
    for cat, (o, w, e) in cat_stats.items():
        print(f" {cat:<30} {o:>5} {w:>6} {e:>6}")

    if failures:
        print(f"\n{'=' * 100}\n FAILURES ({len(failures)}):\n{'=' * 100}")
        for num, cat, query, kind, detail in failures:
            print(f"  #{num:<4} [{cat}] {kind}: {query}")
            print(f"         -> {detail}")

    with open("/tmp/osb_nested_ppl_failures.json", "w") as f:
        json.dump([{"num": n, "cat": c, "query": q, "kind": k, "detail": d}
                   for n, c, q, k, d in failures], f, indent=1)
    print(f"\nSaved /tmp/osb_nested_ppl_failures.json")
    return err_count + wrong == 0


def main():
    gen_all()
    write_cases_file()

    if "--gen-only" in sys.argv:
        return

    _, err = req("GET", "/")
    if err:
        print(f"ERROR: Cannot connect to cluster at {BASE}: {err}")
        sys.exit(1)

    print(f"\nCluster reachable at {BASE}. Setting up index...")
    setup()

    print(f"\nRunning {len(CASES)} cases...\n")
    success = run_cases()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
