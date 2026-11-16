#!/usr/bin/env python3
"""
Migration helper: from the previous release (commit 0a97986) to this one. See MIGRATION.md.

    python migrate.py check [--refs refs.json]      read-only pre-flight: is this deployment ready?
    python migrate.py reindex refs.json [--keep N] [--force] [--skip-checks]
                                                     build the new index and swap the alias (no downtime)
    python migrate.py rollback                       point the alias back to the previous index version
    python migrate.py verify [--base-url URL] ...    smoke-test a running API after deploying it

`check` and `verify` only read. `reindex` is the one step that changes Elasticsearch, and it never
touches what is being served until the new index is complete (see es_indexer_textile.py).

Exit status: 0 when everything passed (warnings allowed), 1 when something failed.
"""
import argparse
import importlib.metadata
import json
import os
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Dict, List, NamedTuple, Optional

try:  # read .env like the application does
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

EXPECTED_DAPYTAINS = "1.0.0rc4"
EXPECTED_CONTEXT = "https://dtsapi.org/context/v1.0.json"
EXPECTED_DTS_VERSION = "1.0"
HIGH_WATERMARK_PERCENT = 90          # Elasticsearch's default: it stops allocating new shards past this
OK, WARN, FAIL = "ok", "warn", "fail"
SYMBOL = {OK: "✓", WARN: "!", FAIL: "✗"}


class Result(NamedTuple):
    level: str
    name: str
    detail: str = ""


def report(results: List[Result]) -> bool:
    """Print the results and return True when none failed."""
    for r in results:
        print(f"  {SYMBOL[r.level]} {r.name}" + (f"  — {r.detail}" if r.detail else ""))
    failed = [r for r in results if r.level == FAIL]
    warned = [r for r in results if r.level == WARN]
    print(f"\n  {len(results) - len(failed) - len(warned)} passed, {len(warned)} warning(s), {len(failed)} failed")
    return not failed


# ------------------------------------------------------------------------------------------------
# Elasticsearch state (pure functions on data the callers fetch, so they can be tested)
# ------------------------------------------------------------------------------------------------

def index_state(es, alias: str) -> Dict:
    """How `alias` is deployed: a plain index (previous release), an alias (this release), or absent."""
    if es.indices.exists_alias(name=alias):
        return {"kind": "alias", "indices": sorted(es.indices.get_alias(name=alias).keys())}
    if es.indices.exists(index=alias):
        return {"kind": "plain", "indices": [alias]}
    return {"kind": "missing", "indices": []}


def mapping_is_current(properties: Dict) -> List[str]:
    """What the new mapping has that `properties` lacks (empty list = up to date)."""
    missing = [f for f in ("date_start", "date_end", "position") if f not in properties]
    if "ngram" not in properties.get("title", {}).get("fields", {}):
        missing.append("title.ngram")
    for field in ("language", "publisher", "identifier"):
        if "text" not in properties.get(field, {}).get("fields", {}):
            missing.append(f"{field}.text")
    return missing


def human(size: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.0f} {unit}" if unit in ("B", "KiB") else f"{size:.1f} {unit}"
        size /= 1024


def disk_room(total: int, available: int, index_bytes: int, watermark: int = HIGH_WATERMARK_PERCENT) -> Result:
    """Will a second copy of the index fit below the disk watermark? (the re-index builds one next to the live one)"""
    used_after = (total - available + index_bytes) / total * 100 if total else 100
    detail = (f"{human(available)} free of {human(total)}; a second copy of the index (~{human(index_bytes)}) "
              f"would bring the disk to {used_after:.0f}% (Elasticsearch stops allocating shards at {watermark}%)")
    if (total - available) / total * 100 >= watermark:
        return Result(FAIL, "disk space", f"already past the {watermark}% watermark: " + detail)
    return Result(OK if used_after < watermark else FAIL, "disk space", detail)


def elasticsearch_checks(es, alias: str) -> List[Result]:
    results: List[Result] = []
    try:
        info = es.info()
    except Exception as error:  # connection, auth, TLS...
        return [Result(FAIL, "Elasticsearch reachable", str(error))]
    version = info["version"]["number"]
    results.append(Result(OK if int(version.split(".")[0]) >= 8 else FAIL, "Elasticsearch version", f"{version} (8.x needed)"))

    state = index_state(es, alias)
    if state["kind"] == "missing":
        results.append(Result(WARN, f"index {alias!r}", "does not exist: a first run will create it"))
        return results
    docs = es.count(index=alias)["count"]
    if state["kind"] == "plain":
        results.append(Result(WARN, f"index {alias!r}", f"still a plain index with {docs} documents: `reindex` converts it to an alias"))
    else:
        results.append(Result(OK, f"index {alias!r}", f"alias to {', '.join(state['indices'])} ({docs} documents)"))

    mapping = es.indices.get_mapping(index=alias)
    properties = next(iter(mapping.values()))["mappings"].get("properties", {})
    missing = mapping_is_current(properties)
    results.append(
        Result(OK, "mapping", "up to date") if not missing
        else Result(WARN, "mapping", "needs a re-index, missing: " + ", ".join(missing))
    )

    try:
        fs = es.nodes.stats(metric="fs")["nodes"]
        total = sum(n["fs"]["total"]["total_in_bytes"] for n in fs.values())
        available = sum(n["fs"]["total"]["available_in_bytes"] for n in fs.values())
        size = es.indices.stats(index=alias, metric="store")["_all"]["primaries"]["store"]["size_in_bytes"]
        results.append(disk_room(total, available, size))
    except Exception as error:
        results.append(Result(WARN, "disk space", f"could not be read: {error}"))
    return results


# ------------------------------------------------------------------------------------------------
# Local environment
# ------------------------------------------------------------------------------------------------

def environment_checks(refs: Optional[str]) -> List[Result]:
    results: List[Result] = []
    try:
        installed = importlib.metadata.version("dapytains")
        results.append(Result(
            OK if installed == EXPECTED_DAPYTAINS else FAIL, "dapytains",
            f"{installed}" + ("" if installed == EXPECTED_DAPYTAINS else f" installed, {EXPECTED_DAPYTAINS} expected: pip install -r requirements.txt"),
        ))
    except importlib.metadata.PackageNotFoundError:
        results.append(Result(FAIL, "dapytains", "not installed: pip install -r requirements.txt"))

    host = os.getenv("ELASTICSEARCH_HOST")
    results.append(Result(OK if host else FAIL, "ELASTICSEARCH_HOST", host or "not set"))
    if host and host.startswith("https") and os.getenv("ELASTICSEARCH_VERIFY_CERTS", "false").lower() != "true":
        results.append(Result(WARN, "ELASTICSEARCH_VERIFY_CERTS", "TLS certificates are not verified"))
    if not os.getenv("PUBLIC_BASE_URL") and not os.getenv("TRUSTED_PROXY_HOPS"):
        results.append(Result(WARN, "PUBLIC_BASE_URL", "not set: API links follow the address of each request. "
                                                       "Behind a reverse proxy, set it (see MIGRATION.md)"))
    else:
        results.append(Result(OK, "public URL", os.getenv("PUBLIC_BASE_URL") or f"{os.getenv('TRUSTED_PROXY_HOPS')} trusted proxy hop(s)"))

    db_path = os.getenv("DATABASE_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.db"))
    if not os.path.exists(db_path):
        results.append(Result(WARN, "database", f"{db_path} not found (nothing to migrate there: the schema did not change)"))
    else:
        try:
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as db:
                total, resources, unset = db.execute(
                    "SELECT COUNT(*), SUM(resource), SUM(nb_children IS NULL) FROM collections").fetchone()
            level = WARN if unset else OK
            results.append(Result(level, "database", f"{total} collections, {resources or 0} manuscripts"
                                  + (f"; nb_children is empty for {unset} (sorting by size will be off: re-run `flask data catalog ingest`)" if unset else "")))
        except sqlite3.Error as error:
            results.append(Result(WARN, "database", f"could not be read: {error}"))

    if refs:
        if not os.path.exists(refs):
            results.append(Result(FAIL, "refs file", f"{refs} not found: flask data prerender-to-json --out {refs}"))
        else:
            try:
                with open(refs, encoding="utf-8") as handle:
                    data = json.load(handle)
                docs = [d for item in data for d in (item if isinstance(item, list) else [item])]
                missing = {"collection", "ref", "content"} - set(docs[0]) if docs else {"(empty)"}
                results.append(Result(FAIL if missing else OK, "refs file",
                                      f"{len(docs)} pages" if not missing else f"missing fields: {', '.join(sorted(missing))}"))
            except (ValueError, OSError) as error:
                results.append(Result(FAIL, "refs file", str(error)))
    return results


def cmd_check(args) -> bool:
    print("Environment")
    ok_env = report(environment_checks(args.refs))
    print("\nElasticsearch")
    import es_indexer_textile as ix
    alias = args.index or ix.INDEX
    ok_es = report(elasticsearch_checks(ix.es, alias))
    return ok_env and ok_es


def cmd_reindex(args) -> bool:
    import es_indexer_textile as ix
    if not args.skip_checks and not cmd_check(argparse.Namespace(refs=args.refs_file, index=args.index)):
        print("\nFix the failed checks above, or pass --skip-checks to go ahead anyway.")
        return False
    print("\nRe-indexing (the live index keeps serving until the new one is complete)\n")
    argv = [args.refs_file, "--keep", str(args.keep)] + (["--force"] if args.force else []) + ([args.index] if args.index else [])
    return bool(ix.main(argv))


def cmd_rollback(args) -> bool:
    import es_indexer_textile as ix
    return bool(ix.main(["--rollback"] + ([args.index] if args.index else [])))


# ------------------------------------------------------------------------------------------------
# Smoke tests of a running API
# ------------------------------------------------------------------------------------------------

class Api:
    def __init__(self, base: str, origin: Optional[str] = None, timeout: int = 60):
        self.base, self.origin, self.timeout = base.rstrip("/"), origin, timeout

    def request(self, path: str, **params):
        url = f"{self.base}{path}" + ("?" + urllib.parse.urlencode(params) if params else "")
        headers = {"Origin": self.origin} if self.origin else {}
        try:
            return urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=self.timeout)
        except urllib.error.HTTPError as error:
            return error

    def json(self, path: str, **params):
        response = self.request(path, **params)
        return response.status if hasattr(response, "status") else response.code, json.load(response)

    def ndjson(self, path: str, **params) -> List[Dict]:
        return [json.loads(line) for line in self.request(path, **params).read().decode().splitlines() if line.strip()]


def run(name: str, check: Callable[[], Optional[str]]) -> Result:
    """Run one check: returning None or a detail string passes, raising AssertionError fails."""
    try:
        return Result(OK, name, check() or "")
    except AssertionError as error:
        return Result(FAIL, name, str(error))
    except Exception as error:  # connection refused, bad JSON...
        return Result(FAIL, name, f"{type(error).__name__}: {error}")


def api_checks(api: Api, text_query: str, expect_public_url: Optional[str]) -> List[Result]:
    shared: Dict = {}

    def entry_point():
        _, d = api.json("/")
        assert d["@context"] == EXPECTED_CONTEXT, f"@context is {d['@context']!r} (still DTS 1-alpha? is the new code running?)"
        assert d["dtsVersion"] == EXPECTED_DTS_VERSION, f"dtsVersion is {d['dtsVersion']!r}"
        assert "page" in d["collection"], f"collection template {d['collection']!r} does not advertise `page`"
        if expect_public_url:
            assert d["@id"].startswith(expect_public_url.rstrip("/")), f"links point to {d['@id']!r}, not {expect_public_url!r}"
        return f"DTS {d['dtsVersion']}, {d['collection']}"

    def root_collection():
        status, d = api.json("/collection/")
        assert status == 200 and d["member"], "the root collection has no members: is the catalog ingested?"
        assert d["view"]["@type"] == "Pagination" and d["view"]["first"].startswith("http"), "pagination links are not absolute"
        shared["first_member"] = d["member"][0]["@id"]
        return f"{len(d['member'])} members, {d['totalChildren']} in total"

    def page_out_of_range():
        status, d = api.json("/collection/", page=99999)
        assert status == 404 and "message" in d, f"expected a 404 message, got {status}"

    def child_count_extension():
        _, d = api.json("/collection/")
        members = d["member"]
        assert not any("nb_children" in m for m in members), "nb_children is still a top-level term (previous release)"
        counted = sum(1 for m in members if "nb_children" in m.get("extensions", {}))
        return f"{counted}/{len(members)} members carry extensions.nb_children"

    def facets():
        _, d = api.json("/manuscripts/facets/")
        assert d["languages"], "no languages: is the index empty?"
        assert d["min_year"] is not None, "no date_start/date_end in the index: re-index with the new mapping"
        shared["language"] = d["languages"][0]["code"]
        return f"languages {', '.join(l['code'] for l in d['languages'][:5])}; years {int(d['min_year'])}–{int(d['max_year'])}"

    def filtered_search():
        _, d = api.json("/manuscripts/", language=shared.get("language", "lat"))
        assert d["total"] > 0 and d["items"], "no manuscript for the most common language"
        shared["title"] = d["items"][0]["title"]
        return f"{d['total']} manuscripts"

    def accent_insensitive():
        import unicodedata
        word = max(shared["title"].replace("<mark class='dts-hit'>", "").replace("</mark>", "").split(), key=len)
        folded = "".join(c for c in unicodedata.normalize("NFD", word.lower()) if not unicodedata.combining(c))
        _, d = api.json("/manuscripts/", q=folded)
        assert d["total"] > 0, f"searching {folded!r} (from {word!r}) found nothing"
        return f"{word!r} ~ {folded!r}: {d['total']} found"

    def collections_search():
        _, d = api.json("/collections/search/")
        assert d["total"] > 0, "no collection found"
        return f"{d['total']} collections"

    def text_search():
        _, d = api.json("/search/", q=text_query, size=1)
        assert d["total"] > 0, f"no hit for {text_query!r}: pass --text-query with a word of your corpus"
        shared["has_text"] = True
        return f"{d['total']} pages"

    def concordance():
        assert shared.get("has_text"), "skipped: the text query matched nothing"
        _, d = api.json("/concordance/", q=text_query, format="json", size=2)
        assert d["rows"] and {"left", "keyword", "right", "ref"} <= set(d["rows"][0]), "no concordance rows"
        response = api.request("/concordance/", q=text_query, limit=3)
        body = response.read().decode("utf-8")
        assert body.startswith("﻿") and body.count("\n") >= 2, "the CSV export is empty"
        return f"{d['total_pages']} matching pages; CSV export ok"

    def statistics_stream():
        assert shared.get("has_text"), "skipped: the text query matched nothing"
        events = api.ndjson("/concordance/stats/", q=text_query, format="ndjson")
        assert events and events[-1]["type"] == "done", f"the stream did not finish: {[e['type'] for e in events]}"
        stats = events[-1]["stats"]
        assert stats["limit"] <= 100000, f"limit is {stats['limit']}"
        return f"{stats['occurrences']} occurrences, {stats['per10k']} per 10,000 tokens, limit {stats['limit']}"

    def statistics_cached():
        assert shared.get("has_text"), "skipped: the text query matched nothing"
        events = api.ndjson("/concordance/stats/", q=text_query, format="ndjson")
        assert events[-1]["stats"]["cached"] is True, "a repeated request was not served from the cache (is API_CACHE_DIR writable?)"

    def cors():
        response = api.request("/collection/")
        header = response.headers.get("Access-Control-Allow-Origin")
        assert header, f"no CORS header for origin {api.origin!r}: add it to ALLOWED_ORIGINS"
        return header

    checks = [
        ("DTS entry point (1.0, with page)", entry_point), ("root collection and pagination", root_collection),
        ("page past the end is a 404", page_out_of_range), ("child count in extensions", child_count_extension),
        ("catalog facets (new mapping)", facets), ("catalog search with a filter", filtered_search),
        ("accents ignored in catalog search", accent_insensitive), ("collection search", collections_search),
        (f"text search ({text_query!r})", text_search), ("concordance (JSON and CSV)", concordance),
        ("streamed statistics", statistics_stream), ("statistics cache", statistics_cached),
    ]
    if api.origin:
        checks.append((f"CORS for {api.origin}", cors))
    return [run(name, fn) for name, fn in checks]


def cmd_verify(args) -> bool:
    print(f"API at {args.base_url}\n")
    return report(api_checks(Api(args.base_url, args.origin), args.text_query, args.expect_public_url))


# ------------------------------------------------------------------------------------------------

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("check", help="read-only pre-flight")
    p.add_argument("--refs", help="the JSON file you will index (flask data prerender-to-json)")
    p.add_argument("--index", help="index alias (default: ELASTICSEARCH_INDEX)")
    p.set_defaults(func=cmd_check)

    p = sub.add_parser("reindex", help="build the new index and swap the alias")
    p.add_argument("refs_file")
    p.add_argument("--index")
    p.add_argument("--keep", type=int, default=1, help="previous versions to keep for rollback")
    p.add_argument("--force", action="store_true", help="go live even if the new index is much smaller")
    p.add_argument("--skip-checks", action="store_true")
    p.set_defaults(func=cmd_reindex)

    p = sub.add_parser("rollback", help="point the alias back to the previous version")
    p.add_argument("--index")
    p.set_defaults(func=cmd_rollback)

    p = sub.add_parser("verify", help="smoke-test the running API")
    p.add_argument("--base-url", default=os.getenv("VERIFY_BASE_URL", "http://localhost:5000"))
    p.add_argument("--text-query", default="et", help="a word that occurs in your texts")
    p.add_argument("--origin", help="a frontend origin to test CORS for, e.g. https://example.org")
    p.add_argument("--expect-public-url", help="the PUBLIC_BASE_URL links must start with")
    p.set_defaults(func=cmd_verify)

    args = parser.parse_args(argv)
    return 0 if args.func(args) else 1


if __name__ == "__main__":
    sys.exit(main())
