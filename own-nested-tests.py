#!/usr/bin/env python3
"""
Own nested-search regression suite (independent of ansh's ~1000-case suite and
our earlier 275-case suite — those are reference only, not reused here).

Three fresh indices/datasets (teams/members, orders/items, students/scores),
each with hand-picked discriminator docs (an element that individually fails a
joint AND, a doc with a low-first/high-later array for sort testing, etc).

Cases are generated systematically as field x operator x threshold grids per
category (not hand-typed one-by-one), which is what gets this suite to a
comparable scale (~300+ cases) to the reference suites without copying them.

Expected results are computed in plain Python against the JSON dataset using
the nested semantics we validated against a live vanilla OpenSearch server
earlier this session (existence = ANY element; compound conditions are JOINT
on a single element; NOT is pushed to the per-element predicate before the
ANY check; plain projection = first array element; stats...by <array>.field
groups over exploded children; a dotted filter followed by a PARENT-only
aggregate/group-by must not double count matching parents; sort on a dotted
nested field is a KNOWN QUIRK — plain field-sort on the first array element,
same value as plain projection, not the array's max/min).

Usage:
    python3 own-nested-tests.py            # generate cases file + run
    python3 own-nested-tests.py --gen-only # only (re)write own-nested-test-cases.txt

Cluster must be running at BASE (default http://localhost:9000 — our Mustang
dev port; vanilla runs on 9200 per this session's port convention).
"""
import json
import operator
import os
import sys
import urllib.request
import urllib.error

BASE = os.environ.get("MUSTANG_BASE", "http://localhost:9000")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "own-nested-test-data.json")
CASES_FILE = os.path.join(SCRIPT_DIR, "own-nested-test-cases.txt")

ARR = {"teams": "members", "orders": "items", "students": "scores"}
PARENT_KEY = {"teams": "team", "orders": "order", "students": "student"}

MAPPINGS = {
    "teams": {
        "team": {"type": "keyword"}, "budget": {"type": "integer"}, "active": {"type": "keyword"},
        "members": {"type": "nested", "properties": {
            "name": {"type": "keyword"}, "role": {"type": "keyword"},
            "hours": {"type": "integer"}, "rating": {"type": "integer"}}},
    },
    "orders": {
        "order": {"type": "keyword"}, "customer": {"type": "keyword"}, "total": {"type": "integer"},
        "items": {"type": "nested", "properties": {
            "product": {"type": "keyword"}, "qty": {"type": "integer"},
            "price": {"type": "integer"}, "category": {"type": "keyword"}}},
    },
    "students": {
        "student": {"type": "keyword"}, "grade_level": {"type": "integer"}, "honors": {"type": "keyword"},
        "scores": {"type": "nested", "properties": {
            "subject": {"type": "keyword"}, "score": {"type": "integer"}, "term": {"type": "keyword"}}},
    },
}

SETTINGS = {
    "number_of_shards": 1, "number_of_replicas": 0,
    "index.pluggable.dataformat.enabled": True,
    "index.pluggable.dataformat": "composite",
    "index.composite.primary_data_format": "parquet",
    "index.composite.secondary_data_formats": "lucene",
}

OPS = [(">", operator.gt), ("<", operator.lt), (">=", operator.ge),
       ("<=", operator.le), ("=", operator.eq), ("!=", operator.ne)]
OPS_MAP = dict(OPS)


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
    for index, props in MAPPINGS.items():
        req("DELETE", f"/{index}")
        res, err = req("PUT", f"/{index}", body={"settings": SETTINGS, "mappings": {"properties": props}})
        if err:
            print(f"FATAL create {index}: {err}")
            sys.exit(1)
        lines = []
        for doc in data[index]:
            lines.append(json.dumps({"index": {}}))
            lines.append(json.dumps(doc))
        res, err = req("POST", f"/{index}/_bulk?refresh=true", ndjson="\n".join(lines) + "\n")
        if err or (res and res.get("errors")):
            print(f"FATAL bulk {index}: {err or res}")
            sys.exit(1)
        req("POST", f"/{index}/_flush?force=true")
        print(f"  {index}: {len(data[index])} docs")


# ─── Reference semantics (computed directly on the JSON, independent of Mustang) ──

def load_data():
    with open(DATA_FILE) as f:
        return json.load(f)


def any_elem(doc, arr, pred):
    return any(pred(e) for e in doc.get(arr, []))


def first_elem_field(doc, arr, field):
    elems = doc.get(arr, [])
    return elems[0][field] if elems else None


def matching_parents(data, index, pred):
    """Parents where ANY element of the array satisfies pred (a function of the element dict)."""
    arr = ARR[index]
    return [d for d in data[index] if any_elem(d, arr, pred)]


# ─── Case generation ────────────────────────────────────────────────────────

CASES = []  # list of (num, category, index, ppl_query, expected_rows_dict)
_counter = [0]


def add(category, index, query, columns, rows):
    _counter[0] += 1
    CASES.append((_counter[0], category, index, query, {"columns": columns, "rows": rows}))


# Numeric nested fields per index, with a spread of thresholds spanning the
# actual data range (chosen so each yields a different-sized result set).
NUMERIC_FIELDS = {
    "teams": [("hours", [20, 30, 40]), ("rating", [3, 5, 8])],
    "orders": [("qty", [2, 5, 10]), ("price", [20, 50, 100])],
    "students": [("score", [60, 80, 90])],
}

# String nested fields per index, with their known distinct values.
STRING_FIELDS = {
    "teams": [("role", ["dev", "lead"])],
    "orders": [("category", ["tools", "elec", "hardware", "x", "y"])],
    "students": [("term", ["fall", "spring"]), ("subject", ["Math", "Sci", "Art"])],
}


def gen_existence():
    """Every (field, threshold, operator) combo — existence semantics: ANY array element matches."""
    data = load_data()
    for index, fields in NUMERIC_FIELDS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        for field, vals in fields:
            for val in vals:
                for opstr, opfn in OPS:
                    pred = lambda e, f=field, fn=opfn, v=val: fn(e[f], v)
                    parents = matching_parents(data, index, pred)
                    rows = sorted([[p[pk]] for p in parents])
                    q = f"source={index} | where {arr}.{field} {opstr} {val} | fields {pk}"
                    add("existence", index, q, [pk], rows)
    for index, fields in STRING_FIELDS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        for field, vals in fields:
            for val in vals:
                for opstr in ("=", "!="):
                    pred = (lambda e, f=field, v=val: e[f] == v) if opstr == "=" \
                        else (lambda e, f=field, v=val: e[f] != v)
                    parents = matching_parents(data, index, pred)
                    rows = sorted([[p[pk]] for p in parents])
                    q = f'source={index} | where {arr}.{field} {opstr} "{val}" | fields {pk}'
                    add("existence", index, q, [pk], rows)


# Numeric-field pairs (per index that has two numeric nested fields) for AND-joint tests.
NUMERIC_PAIR_FIELDS = {"teams": ("hours", "rating"), "orders": ("qty", "price")}
JOINT_GRID = {
    "teams": {"hours": [30, 40], "rating": [6, 8]},
    "orders": {"qty": [5, 10], "price": [20, 50]},
}
JOINT_OPS = ["<", ">"]


def gen_joint_and():
    """Two numeric conditions ANDed — must be satisfied JOINTLY by a single array element."""
    data = load_data()
    for index, (f1, f2) in NUMERIC_PAIR_FIELDS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        for v1 in JOINT_GRID[index][f1]:
            for op1 in JOINT_OPS:
                for v2 in JOINT_GRID[index][f2]:
                    for op2 in JOINT_OPS:
                        fn1, fn2 = OPS_MAP[op1], OPS_MAP[op2]
                        pred = lambda e, f1=f1, fn1=fn1, v1=v1, f2=f2, fn2=fn2, v2=v2: \
                            fn1(e[f1], v1) and fn2(e[f2], v2)
                        parents = matching_parents(data, index, pred)
                        rows = sorted([[p[pk]] for p in parents])
                        cond = f"{arr}.{f1} {op1} {v1} and {arr}.{f2} {op2} {v2}"
                        q = f"source={index} | where {cond} | fields {pk}"
                        add("joint_and", index, q, [pk], rows)


STRING_JOINT_SPECS = {
    "teams": {"numeric": [("hours", [30, 40]), ("rating", [6, 8])], "string": ("role", ["dev", "lead"])},
    "orders": {"numeric": [("qty", [3, 10]), ("price", [20, 50])], "string": ("category", ["tools", "elec"])},
    "students": {"numeric": [("score", [70, 90])], "string": ("term", ["fall", "spring"])},
}


def gen_string_numeric_joint():
    """A numeric comparison ANDed with a string equality — jointly on one element."""
    data = load_data()
    for index, spec in STRING_JOINT_SPECS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        sfield, svals = spec["string"]
        for field, vals in spec["numeric"]:
            for v1 in vals:
                for op1 in ("<", ">"):
                    for sval in svals:
                        fn1 = OPS_MAP[op1]
                        pred = lambda e, f=field, fn=fn1, v=v1, sf=sfield, sv=sval: \
                            fn(e[f], v) and e[sf] == sv
                        parents = matching_parents(data, index, pred)
                        rows = sorted([[p[pk]] for p in parents])
                        cond = f'{arr}.{field} {op1} {v1} and {arr}.{sfield} = "{sval}"'
                        q = f"source={index} | where {cond} | fields {pk}"
                        add("string_numeric_joint", index, q, [pk], rows)


NEGATION_SPECS = {
    "teams": [("hours", [30, 40], [">", "<", ">=", "<="]), ("rating", [5, 8], [">", "<", ">=", "<="])],
    "orders": [("price", [50, 100], [">", "<"]), ("qty", [3, 10], [">", "<"])],
    "students": [("score", [60, 90], [">", "<"])],
}


def gen_negation():
    """NOT(<predicate>) — negation is pushed to the per-element predicate before the ANY check."""
    data = load_data()
    for index, fields in NEGATION_SPECS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        for field, vals, ops in fields:
            for val in vals:
                for opstr in ops:
                    fn = OPS_MAP[opstr]
                    pred = lambda e, f=field, fn=fn, v=val: not fn(e[f], v)
                    parents = matching_parents(data, index, pred)
                    rows = sorted([[p[pk]] for p in parents])
                    cond = f"not ({arr}.{field} {opstr} {val})"
                    q = f"source={index} | where {cond} | fields {pk}"
                    add("negation", index, q, [pk], rows)


ARITH_SPECS = {
    "teams": ["hours", "rating"],
    "orders": ["qty", "price"],
    "students": ["score"],
}
ARITH_MODS = [2, 5, 10]
ARITH_REMS = [0, 1]


def gen_arithmetic():
    """MOD filter (field % mod = rem) — needs the compound-expr-tree UDF path, not the flat one."""
    data = load_data()
    for index, fields in ARITH_SPECS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        for field in fields:
            for mod in ARITH_MODS:
                for rem in ARITH_REMS:
                    pred = lambda e, f=field, m=mod, r=rem: e[f] % m == r
                    parents = matching_parents(data, index, pred)
                    rows = sorted([[p[pk]] for p in parents])
                    cond = f"{arr}.{field} % {mod} = {rem}"
                    q = f"source={index} | where {cond} | fields {pk}"
                    add("arithmetic", index, q, [pk], rows)


COMPOUND_ARITH_SPECS = {
    "teams": [("hours", [30, 40]), ("rating", [5, 8])],
    "orders": [("qty", [3, 10]), ("price", [20, 50])],
    "students": [("score", [70, 90])],
}
COMPOUND_ARITH_OPS = ["<", ">"]
COMPOUND_ARITH_MODS = [2, 5]


def gen_compound_arith():
    """Comparison AND mod-check on the SAME field — must both hold on one element (arithmetic + joint)."""
    data = load_data()
    for index, fields in COMPOUND_ARITH_SPECS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        for field, vals in fields:
            for val in vals:
                for opstr in COMPOUND_ARITH_OPS:
                    for mod in COMPOUND_ARITH_MODS:
                        fn = OPS_MAP[opstr]
                        pred = lambda e, f=field, fn=fn, v=val, m=mod: fn(e[f], v) and e[f] % m == 0
                        parents = matching_parents(data, index, pred)
                        rows = sorted([[p[pk]] for p in parents])
                        cond = f"{arr}.{field} {opstr} {val} and {arr}.{field} % {mod} = 0"
                        q = f"source={index} | where {cond} | fields {pk}"
                        add("compound_arith", index, q, [pk], rows)


THREE_CLAUSE_SPECS = {
    "teams": {"field": "hours", "ranges": [(10, 50), (20, 45)], "sfield": "role", "svals": ["lead", "dev"]},
    "orders": {"field": "qty", "ranges": [(1, 10), (2, 6)], "sfield": "category", "svals": ["tools", "elec"]},
    "students": {"field": "score", "ranges": [(50, 95), (60, 92)], "sfield": "term", "svals": ["spring", "fall"]},
}


def gen_three_clause_and():
    """Three ANDed clauses (range lo/hi + string exclusion) — all jointly on one element."""
    data = load_data()
    for index, spec in THREE_CLAUSE_SPECS.items():
        arr, pk, field = ARR[index], PARENT_KEY[index], spec["field"]
        for lo, hi in spec["ranges"]:
            for sval in spec["svals"]:
                pred = lambda e, f=field, lo=lo, hi=hi, sf=spec["sfield"], sv=sval: \
                    lo < e[f] < hi and e[sf] != sv
                parents = matching_parents(data, index, pred)
                rows = sorted([[p[pk]] for p in parents])
                cond = f'{arr}.{field} > {lo} and {arr}.{field} < {hi} and {arr}.{spec["sfield"]} != "{sval}"'
                q = f"source={index} | where {cond} | fields {pk}"
                add("three_clause_and", index, q, [pk], rows)


OR_SPECS = {
    "teams": [("hours", [(40, 15), (45, 10), (50, 20)]), ("rating", [(8, 2), (9, 3)])],
    "orders": [("price", [(100, 10), (200, 20)]), ("qty", [(10, 2), (15, 3)])],
    "students": [("score", [(94, 56), (90, 58), (92, 60)])],
}


def gen_or_filter():
    """field > hi OR field < lo — an OR of two per-element predicates (independent, not joint)."""
    data = load_data()
    for index, fields in OR_SPECS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        for field, pairs in fields:
            for hi, lo in pairs:
                pred = lambda e, f=field, hi=hi, lo=lo: e[f] > hi or e[f] < lo
                parents = matching_parents(data, index, pred)
                rows = sorted([[p[pk]] for p in parents])
                cond = f"{arr}.{field} > {hi} or {arr}.{field} < {lo}"
                q = f"source={index} | where {cond} | fields {pk}"
                add("or_filter", index, q, [pk], rows)


PLAIN_PROJ_FIELDS = {
    "teams": ["name", "role", "hours", "rating"],
    "orders": ["product", "category", "qty", "price"],
    "students": ["subject", "score", "term"],
}


def gen_plain_projection():
    """Plain `fields pk, arr.field` (no filter) — first array element only."""
    data = load_data()
    for index, fields in PLAIN_PROJ_FIELDS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        for field in fields:
            rows = sorted([[d[pk], first_elem_field(d, arr, field)] for d in data[index]],
                           key=lambda r: str(r[0]))
            q = f"source={index} | fields {pk}, {arr}.{field}"
            add("plain_projection", index, q, [pk, f"{arr}.{field}"], rows)
        # two-field combined projection (still first-element per field, independently)
        f1, f2 = fields[0], fields[1]
        rows = sorted([[d[pk], first_elem_field(d, arr, f1), first_elem_field(d, arr, f2)]
                        for d in data[index]], key=lambda r: str(r[0]))
        q = f"source={index} | fields {pk}, {arr}.{f1}, {arr}.{f2}"
        add("plain_projection", index, q, [pk, f"{arr}.{f1}", f"{arr}.{f2}"], rows)


STATS_BY_CHILD_FIELDS = {
    "teams": ["role"],
    "orders": ["category", "product"],
    "students": ["subject", "term"],
}


def gen_stats_by_child():
    """stats count() by arr.field — groups over EXPLODED children, all elements as rows."""
    data = load_data()
    for index, fields in STATS_BY_CHILD_FIELDS.items():
        arr = ARR[index]
        for field in fields:
            counts = {}
            for d in data[index]:
                for e in d.get(arr, []):
                    counts[e[field]] = counts.get(e[field], 0) + 1
            rows = sorted([[c, k] for k, c in counts.items()], key=lambda r: str(r[1]))
            q = f"source={index} | stats count() by {arr}.{field}"
            add("stats_by_child", index, q, ["count()", f"{arr}.{field}"], rows)


EXPLICIT_EXPAND_FIELDS = {
    "teams": ["name", "role"],
    "orders": ["product", "category"],
    "students": ["subject", "term"],
}


def gen_explicit_expand():
    """expand arr | fields pk, field — full per-child flatten, one row per child, NOT deduped."""
    data = load_data()
    for index, fields in EXPLICIT_EXPAND_FIELDS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        for field in fields:
            rows = sorted([[d[pk], e[field]] for d in data[index] for e in d.get(arr, [])],
                           key=lambda r: str(r))
            q = f"source={index} | expand {arr} | fields {pk}, {field}"
            add("explicit_expand", index, q, [pk, field], rows)


HEADLINE_SPECS = {
    "teams": {"filter_fields": [("hours", [20, 30, 40]), ("rating", [5, 7])], "pfield": "budget"},
    "orders": {"filter_fields": [("price", [20, 40, 100]), ("qty", [2, 5])], "pfield": "total"},
    "students": {"filter_fields": [("score", [60, 80, 90])], "pfield": "grade_level"},
}
HEADLINE_AGGFNS = ["sum", "avg"]


def gen_headline_dedup():
    """where <dotted> | stats agg(parent_field) — the headline dedup bug: parent must count ONCE
    per matching parent regardless of how many of its children matched, so the aggregate is not
    inflated by the per-child flatten."""
    data = load_data()
    for index, spec in HEADLINE_SPECS.items():
        arr, pfield = ARR[index], spec["pfield"]
        for field, vals in spec["filter_fields"]:
            for val in vals:
                pred = lambda e, f=field, v=val: e[f] > val
                parents = matching_parents(data, index, pred)
                pvals = [p[pfield] for p in parents]
                for aggfn in HEADLINE_AGGFNS:
                    if not pvals and aggfn != "sum":
                        continue  # avg on empty set is undefined — skip, not our semantics to test
                    result = sum(pvals) if aggfn == "sum" else round(sum(pvals) / len(pvals), 4)
                    cond = f"{arr}.{field} > {val}"
                    q = f"source={index} | where {cond} | stats {aggfn}({pfield})"
                    add("headline_dedup", index, q, [f"{aggfn}({pfield})"], [[result]])


COUNT_DEDUP_SPECS = {
    "teams": [("hours", [20, 30, 40]), ("rating", [5, 7, 8])],
    "orders": [("price", [20, 40, 100]), ("qty", [2, 5, 10])],
    "students": [("score", [60, 80, 90])],
}


def gen_count_dedup():
    """where <dotted> | stats count() — must count distinct matching PARENTS, not raw child rows."""
    data = load_data()
    for index, fields in COUNT_DEDUP_SPECS.items():
        arr = ARR[index]
        for field, vals in fields:
            for val in vals:
                pred = lambda e, f=field, v=val: e[f] > val
                n = len(matching_parents(data, index, pred))
                cond = f"{arr}.{field} > {val}"
                q = f"source={index} | where {cond} | stats count()"
                add("count_dedup", index, q, ["count()"], [[n]])


GROUPBY_PARENT_SPECS = {
    "teams": [("hours", [30, 40]), ("rating", [5, 8])],
    "orders": [("price", [40, 100]), ("qty", [3, 10])],
    "students": [("score", [70, 90])],
}


def gen_groupby_parent_with_filter():
    """where <dotted> | stats count() by <parent-key> — group-by-parent must also not double count."""
    data = load_data()
    for index, fields in GROUPBY_PARENT_SPECS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        for field, vals in fields:
            for val in vals:
                pred = lambda e, f=field, v=val: e[f] > val
                parents = matching_parents(data, index, pred)
                rows = sorted([[1, p[pk]] for p in parents], key=lambda r: str(r[1]))
                cond = f"{arr}.{field} > {val}"
                q = f"source={index} | where {cond} | stats count() by {pk}"
                add("groupby_parent_with_filter", index, q, ["count()", pk], rows)


SORT_FIELDS = {
    "teams": ["hours", "rating"],
    "orders": ["qty", "price"],
    "students": ["score"],
}


def gen_sort_nested():
    # [KNOWN QUIRK, established via live vanilla comparison earlier this session]
    # A dotted-nested ORDER BY is a plain field-sort with no nested/array-mode
    # handling in vanilla (AbstractCalciteIndexScan.pushDownSort never calls
    # Utils.resolveNestedPath) — the sort key AND the displayed value are both
    # the FIRST array element, same as a plain projection. It does not use the
    # array's max/min. Mustang intentionally mirrors this (not our bug to fix).
    data = load_data()
    for index, fields in SORT_FIELDS.items():
        arr, pk = ARR[index], PARENT_KEY[index]
        for field in fields:
            first_key = lambda d, f=field: first_elem_field(d, arr, f)

            docs_desc = sorted(data[index], key=lambda d: -first_key(d))
            rows_desc = [[d[pk], first_key(d)] for d in docs_desc]
            q_desc = f"source={index} | sort - {arr}.{field} | fields {pk}, {arr}.{field}"
            add("sort_nested_desc", index, q_desc, [pk, f"{arr}.{field}"], rows_desc)

            docs_asc = sorted(data[index], key=first_key)
            rows_asc = [[d[pk], first_key(d)] for d in docs_asc]
            q_asc = f"source={index} | sort {arr}.{field} | fields {pk}, {arr}.{field}"
            add("sort_nested_asc", index, q_asc, [pk, f"{arr}.{field}"], rows_asc)


def gen_all():
    gen_existence()
    gen_joint_and()
    gen_string_numeric_joint()
    gen_negation()
    gen_arithmetic()
    gen_compound_arith()
    gen_three_clause_and()
    gen_or_filter()
    gen_plain_projection()
    gen_stats_by_child()
    gen_explicit_expand()
    gen_headline_dedup()
    gen_count_dedup()
    gen_groupby_parent_with_filter()
    gen_sort_nested()


def write_cases_file():
    with open(CASES_FILE, "w") as f:
        for num, cat, index, query, expected in CASES:
            f.write(f"{num}|{cat}|{index}|{query}|{json.dumps(expected)}\n")
    print(f"Wrote {len(CASES)} cases to {CASES_FILE}")


# ─── Comparison / running ───────────────────────────────────────────────────

# Categories whose expected order is meaningful (sort queries) — compared
# order-sensitively. Every other category is compared as an order-independent
# set (PPL/vanilla don't guarantee row order without an explicit sort).
ORDER_SENSITIVE_CATEGORIES = {"sort_nested_desc", "sort_nested_asc"}


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

    for i, (num, cat, index, query, expected) in enumerate(CASES):
        order_sensitive = cat in ORDER_SENSITIVE_CATEGORIES
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
            failures.append((num, cat, index, query, verdict, detail))
        else:
            err_count += 1
            cs[2] += 1
            failures.append((num, cat, index, query, verdict, detail))

        done = i + 1
        if done % 50 == 0:
            print(f"  ...{done}/{len(CASES)} ({ok} ok)")

    print("\n" + "=" * 100)
    print(f" TOTAL: {ok} OK / {wrong} WRONG / {err_count} ERROR   of {len(CASES)}")
    print("=" * 100)
    print(f"\n {'category':<30} {'ok':>5} {'wrong':>6} {'error':>6}")
    for cat, (o, w, e) in cat_stats.items():
        print(f" {cat:<30} {o:>5} {w:>6} {e:>6}")

    if failures:
        print(f"\n{'=' * 100}\n FAILURES ({len(failures)}):\n{'=' * 100}")
        for num, cat, index, query, kind, detail in failures[:80]:
            print(f"  #{num:<4} [{cat}/{index}] {kind}: {query}")
            print(f"         -> {detail}")
        if len(failures) > 80:
            print(f"  ... and {len(failures) - 80} more")

    with open("/tmp/own_nested_failures.json", "w") as f:
        json.dump([{"num": n, "cat": c, "index": i, "query": q, "kind": k, "detail": d}
                   for n, c, i, q, k, d in failures], f, indent=1)
    print(f"\nSaved /tmp/own_nested_failures.json")
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

    print(f"\nCluster reachable at {BASE}. Setting up indices...")
    setup()

    print(f"\nRunning {len(CASES)} cases...\n")
    success = run_cases()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
