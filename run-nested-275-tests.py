#!/usr/bin/env python3
"""
Nested Document Search POC — 275-case test suite.

Runs all 275 tests against a running Mustang cluster (http://localhost:9200).
Tests cover: scalar projection/filter/aggregation, whole-array projection,
nested projection/filter/aggregation, mixed queries, dedup verification,
AND/OR predicates, aggregation variations, and edge cases.

Usage:
    python3 run-nested-275-tests.py

Prerequisites:
    - OpenSearch cluster running at localhost:9200 with analytics plugins
    - Start with: ./gradlew run -Dsandbox.enabled=true -PinstalledPlugins="[...]" ...
    - The script creates its own indices (blogs, ecommerce, logs, employees)

Output:
    - Console summary with pass/fail/error per category
    - /tmp/nested_275_failures.json with failure details
"""
import json
import os
import re
import time
import urllib.request
import urllib.error
import sys

BASE = "http://localhost:9200"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "nested-275-test-data.json")
CASES_FILE = os.path.join(SCRIPT_DIR, "nested-275-test-cases.txt")

NESTED_COL = {"blogs": "comments", "ecommerce": "reviews", "logs": "spans", "employees": "skills"}

MAPPINGS = {
    "blogs": {
        "title": {"type": "keyword"}, "views": {"type": "integer"}, "rating": {"type": "double"},
        "comments": {"type": "nested", "properties": {
            "author": {"type": "keyword"}, "score": {"type": "integer"}, "text": {"type": "keyword"}}},
    },
    "ecommerce": {
        "product_name": {"type": "keyword"}, "price": {"type": "double"},
        "category": {"type": "keyword"}, "in_stock": {"type": "integer"},
        "reviews": {"type": "nested", "properties": {
            "reviewer": {"type": "keyword"}, "rating": {"type": "integer"},
            "comment": {"type": "keyword"}, "helpful_votes": {"type": "integer"}}},
    },
    "logs": {
        "service": {"type": "keyword"}, "level": {"type": "keyword"},
        "http_status": {"type": "integer"}, "response_time": {"type": "integer"},
        "spans": {"type": "nested", "properties": {
            "operation": {"type": "keyword"}, "duration_ms": {"type": "integer"},
            "status": {"type": "keyword"}, "bytes_transferred": {"type": "integer"}}},
    },
    "employees": {
        "name": {"type": "keyword"}, "department": {"type": "keyword"},
        "salary": {"type": "integer"}, "years_exp": {"type": "integer"},
        "skills": {"type": "nested", "properties": {
            "name": {"type": "keyword"}, "level": {"type": "integer"},
            "years_used": {"type": "integer"}, "certified": {"type": "keyword"}}},
    },
}

SETTINGS = {
    "number_of_shards": 1, "number_of_replicas": 0,
    "index.pluggable.dataformat.enabled": True,
    "index.pluggable.dataformat": "composite",
    "index.composite.primary_data_format": "parquet",
    "index.composite.secondary_data_formats": "lucene",
}



# ─── HTTP helper ──────────────────────────────────────────────────────────────

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


# ─── Setup ────────────────────────────────────────────────────────────────────

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
            doc_clean = {k: v for k, v in doc.items() if v is not None}
            lines.append(json.dumps({"index": {}}))
            lines.append(json.dumps(doc_clean))
        res, err = req("POST", f"/{index}/_bulk?refresh=true", ndjson="\n".join(lines) + "\n")
        if err or res.get("errors"):
            print(f"FATAL bulk {index}: {err or res}")
            sys.exit(1)
        req("POST", f"/{index}/_flush?force=true")
        print(f"  {index}: {len(data[index])} docs")




# ─── Query translation ───────────────────────────────────────────────────────

def translate(query, index):
    col = NESTED_COL[index]
    dotted = col + "."
    if dotted not in query:
        return None
    parts = [p.strip() for p in query.split("|")]
    touches = any((p.startswith("where") or p.startswith("stats")) and dotted in p for p in parts)
    if not touches:
        return None
    out = []
    inserted = False
    for p in parts:
        out.append(p)
        if p.startswith("source=") and not inserted:
            out.append(f"expand {col}")
            inserted = True
    q = " | ".join(out)
    q = q.replace(dotted, "")
    return q


def normalize(rows):
    def nv(v):
        if v is None:
            return "\x00null"
        if isinstance(v, float):
            return round(v, 4)
        if isinstance(v, (list, dict)):
            return json.dumps(v, sort_keys=True)
        return v
    return sorted([tuple(nv(v) for v in row) for row in rows], key=str)




# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    # Check cluster is up
    _, err = req("GET", "/")
    if err:
        print(f"ERROR: Cannot connect to cluster at {BASE}")
        print(f"Start it with: ./gradlew run -Dsandbox.enabled=true -PinstalledPlugins=\"[...]\" ...")
        sys.exit(1)

    print("Setting up indices...")
    setup()
    time.sleep(2)

    # Load test cases
    if not os.path.exists(CASES_FILE):
        print(f"ERROR: Test cases file not found: {CASES_FILE}")
        sys.exit(1)
    with open(CASES_FILE) as f:
        lines = [l.rstrip("\n") for l in f if l.strip()]

    cases = []
    for line in lines:
        num, cat, idx, rest = line.split("|", 3)
        cut = rest.rfind("|{")
        query, result = rest[:cut].strip(), rest[cut + 1:]
        cases.append((int(num), cat, idx, query, json.loads(result)))

    print(f"\nRunning {len(cases)} cases (verbatim + expand-translated)...\n")
    ok = wrong = err_count = 0
    ok_verbatim = ok_translated = 0
    failures = []
    cat_stats = {}

    for num, cat, idx, query, expected in cases:
        exp_rows = normalize(expected.get("rows", []))
        cs = cat_stats.setdefault(cat, [0, 0, 0])

        # try verbatim first
        actual, error = req("POST", "/_analytics/ppl", body={"query": query})
        verdict = None
        detail = ""
        if not error and normalize(actual.get("rows", [])) == exp_rows:
            verdict = "OK"; ok_verbatim += 1
        else:
            # try translated
            tq = translate(query, idx)
            if tq:
                actual2, error2 = req("POST", "/_analytics/ppl", body={"query": tq})
                if not error2 and normalize(actual2.get("rows", [])) == exp_rows:
                    verdict = "OK"; ok_translated += 1
                elif error2:
                    verdict = "ERROR"
                    detail = "T:" + error2.get("error", {}).get("reason", "?")[:80]
                else:
                    verdict = "WRONG"
                    detail = f"T: expected {len(exp_rows)} rows, got {len(normalize(actual2.get('rows', [])))}"
                    detail += f" | exp={json.dumps(exp_rows)[:60]} got={json.dumps(normalize(actual2.get('rows',[])))[:60]}"
            elif error:
                verdict = "ERROR"
                detail = "V:" + error.get("error", {}).get("reason", "?")[:80]
            else:
                verdict = "WRONG"
                detail = f"V: expected {len(exp_rows)} rows, got {len(normalize(actual.get('rows', [])))}"

        if verdict == "OK":
            ok += 1; cs[0] += 1
        elif verdict == "WRONG":
            wrong += 1; cs[1] += 1
            failures.append((num, cat, idx, query, "WRONG", detail))
        else:
            err_count += 1; cs[2] += 1
            failures.append((num, cat, idx, query, "ERROR", detail))

        done = ok + wrong + err_count
        if done % 50 == 0:
            print(f"  ...{done}/{len(cases)} ({ok} ok)")

    print("\n" + "=" * 100)
    print(f" TOTAL: {ok} OK ({ok_verbatim} verbatim, {ok_translated} via expand) / {wrong} WRONG / {err_count} ERROR   of {len(cases)}")
    print("=" * 100)
    print(f"\n {'category':<15} {'ok':>5} {'wrong':>6} {'error':>6}")
    for cat, (o, w, e) in cat_stats.items():
        print(f" {cat:<15} {o:>5} {w:>6} {e:>6}")

    if failures:
        print(f"\n{'=' * 100}\n FAILURES ({len(failures)}):\n{'=' * 100}")
        for num, cat, idx, query, kind, detail in failures[:60]:
            print(f"  #{num:<4} [{cat}/{idx}] {kind}: {query[:95]}")
            print(f"         -> {detail[:150]}")
        if len(failures) > 60:
            print(f"  ... and {len(failures) - 60} more")

    with open("/tmp/nested_275_failures.json", "w") as f:
        json.dump([{"num": n, "cat": c, "index": i, "query": q, "kind": k, "detail": d}
                   for n, c, i, q, k, d in failures], f, indent=1)
    print(f"\nSaved /tmp/nested_275_failures.json")
    print(f"\nExit code: {0 if err_count + wrong == 0 else 1}")
    sys.exit(0 if err_count + wrong == 0 else 1)




if __name__ == "__main__":
    main()
