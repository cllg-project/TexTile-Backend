"""
Frequency statistics for a text search.

    GET /concordance/stats/?q=<query>[&mode=][&language=][&resource=][&within=]
                           [&limit=<max occurrences>][&refresh=1]
                           [&format=json|csv|ndjson]

The occurrences are the ones the concordance lists (same query, same matching modes, same
scope), counted by scanning the matching pages. Results:

- the number of occurrences, overall and per 10,000 tokens of the scope (every manuscript the
  filters select, with or without hits, so the figure is a relative frequency);
- the distinct forms that matched, which is what a wildcard, a fuzzy or a multi-word search
  (`deus,deum`) is really made of;
- the distribution by manuscript, by century and by language, with their own relative frequency.

Formats: `json` (default) and `csv` answer once the scan is done; `ndjson` streams one JSON object
per line while it runs: `start`, then `progress` snapshots (pages scanned out of the total, plus
the figures so far), then `done` (or `error`). A client can show them as they arrive.

A scan is bounded: at most `limit` occurrences (capped by STATS_MAX_OCCURRENCES, 100,000 by default;
the count is exact, never over), at most
STATS_MAX_SECONDS seconds, and at most STATS_MAX_CONCURRENT at once per process (a request over
that gets a 503). A bounded scan returns what it counted and says why it stopped
(`truncated_reason`: "occurrences" or "time"). Finished results are cached on disk, keyed by the
search and the index generation, so a re-index invalidates them; `refresh=1` recomputes. A scan
cut short by the time limit is not cached, since a faster run would count more.

Manuscript token counts come from the index (`tokens`); a manuscript without one counts as
unknown, not as zero, and is left out of the relative figures. Dates come from `date_start` /
`date_end` (the century is that of the middle of the range).
"""
import csv
import io
import json
import logging
import os
import time
from collections import Counter, defaultdict
from typing import Dict, Iterable, Iterator, List, Optional

from flask import Response, jsonify, request, stream_with_context

from . import concordance
from .concordance import ScopeError, _json_error, _spreadsheet_safe, extract_occurrences, has_phrase, scan_hits, scoped_query
from .config import INDEX
from .generation import index_generation
from ..cache import disk_cache, ttl_cache
from ..limits import STATS_LIMIT, busy_response

log = logging.getLogger(__name__)

TOP_FORMS = 100
TOP_MANUSCRIPTS = 100
# The most occurrences a statistics scan counts. Its own setting, not the CSV export's.
STATS_MAX_OCCURRENCES = int(os.getenv("STATS_MAX_OCCURRENCES", "100000"))
STATS_MAX_SECONDS = float(os.getenv("STATS_MAX_SECONDS", "60"))
PROGRESS_INTERVAL = float(os.getenv("STATS_PROGRESS_INTERVAL", "0.5"))
_PER = 10000


def per_10k(occurrences: int, tokens: Optional[int]) -> Optional[float]:
    return round(occurrences / tokens * _PER, 2) if tokens else None


def century(date_start: Optional[int], date_end: Optional[int]) -> Optional[int]:
    """Century (1 = years 1-100) of the middle of a date range, or None when undated."""
    if date_start is None and date_end is None:
        return None
    start = date_start if date_start is not None else date_end
    end = date_end if date_end is not None else date_start
    return (((start + end) // 2) - 1) // 100 + 1


SCOPE_PAGE = 1000


@ttl_cache(300)
def _scope_manuscripts(generation: str, scope_json: str) -> Dict[str, Dict]:
    """
    Every manuscript of the scope (hits or not): tokens, title, language, dates. Cached per index generation.

    Read page by page with a composite aggregation: a plain terms aggregation has to be given a
    fixed size, and silently drops every manuscript beyond it, which would skew the relative
    frequencies of any corpus larger than that.
    """
    out: Dict[str, Dict] = {}
    after = None
    while True:
        composite = {"size": SCOPE_PAGE, "sources": [{"collection": {"terms": {"field": "collection"}}}]}
        if after:
            composite["after"] = after
        res = concordance.es.search(index=INDEX, body={
            "size": 0,
            "query": json.loads(scope_json),
            "aggs": {"m": {
                "composite": composite,
                "aggs": {
                    "tokens": {"max": {"field": "tokens"}},
                    "sample": {"top_hits": {"size": 1, "_source": ["title", "language", "date_start", "date_end"]}},
                },
            }},
        })
        agg = res["aggregations"]["m"]
        for bucket in agg["buckets"]:
            src = bucket["sample"]["hits"]["hits"][0]["_source"]
            language = src.get("language")
            key = bucket["key"]["collection"]
            out[key] = {
                "title": src.get("title") or key,
                "tokens": int(bucket["tokens"]["value"]) if bucket["tokens"]["value"] is not None else None,
                "language": (language[0] if isinstance(language, list) and language else language) or "und",
                "date_start": src.get("date_start"),
                "date_end": src.get("date_end"),
                "pages": bucket["doc_count"],
            }
        after = agg.get("after_key")
        if not agg["buckets"] or not after:
            return out


def scope_manuscripts(filters_query: Dict) -> Dict[str, Dict]:
    return _scope_manuscripts(index_generation(), json.dumps(filters_query, sort_keys=True))


def scope_only_query(query: Dict) -> Dict:
    """The corpus restrictions of a scoped query, without the text match."""
    return {"bool": {"filter": query["bool"]["filter"]}} if "filter" in query.get("bool", {}) else {"match_all": {}}


def _marked_content(hit: Dict) -> str:
    highlight = hit.get("highlight", {})
    for field in ("content", "content.ngram"):
        found = next((f for f in highlight.get(field, []) if concordance.MARK_OPEN in f), "")
        if found:
            return found
    return ""


class Accumulator:
    """Counts occurrences page after page and can describe the figures so far at any moment."""

    def __init__(self, scope: Dict[str, Dict]):
        self.scope = scope
        self.forms: Counter = Counter()
        self.per_manuscript: Counter = Counter()
        self.pages_scanned = 0
        self.pages_with_hits = 0
        self.occurrences = 0

    def add(self, hit: Dict, merge_adjacent: bool, room: Optional[int] = None) -> None:
        """Count a page's occurrences, at most `room` of them (the first ones), so a limit is exact."""
        self.pages_scanned += 1
        found = extract_occurrences(_marked_content(hit), width=0, merge_adjacent=merge_adjacent)
        if room is not None:
            found = found[:max(0, room)]
        if not found:
            return
        self.pages_with_hits += 1
        collection = hit["_source"]["collection"]
        for occ in found:
            self.forms[occ.keyword.lower()] += 1
            self.per_manuscript[collection] += 1
            self.occurrences += 1

    def snapshot(self, truncated_reason: Optional[str] = None) -> Dict:
        total = self.occurrences
        scope_tokens = sum(m["tokens"] or 0 for m in self.scope.values())

        by_manuscript = []
        by_century: Dict[Optional[int], Dict] = defaultdict(lambda: {"occurrences": 0, "tokens": 0, "manuscripts": 0})
        by_language: Dict[str, Dict] = defaultdict(lambda: {"occurrences": 0, "tokens": 0, "manuscripts": 0})
        for key, m in self.scope.items():
            n = self.per_manuscript.get(key, 0)
            for bucket in (by_century[century(m["date_start"], m["date_end"])], by_language[m["language"]]):
                bucket["occurrences"] += n
                bucket["tokens"] += m["tokens"] or 0
                bucket["manuscripts"] += 1
            if n:
                by_manuscript.append({"collection": key, "title": m["title"], "occurrences": n,
                                      "tokens": m["tokens"], "per10k": per_10k(n, m["tokens"])})
        # Hits in a manuscript missing from the scope aggregate (should not happen) are not dropped
        for key, n in self.per_manuscript.items():
            if key not in self.scope:
                by_manuscript.append({"collection": key, "title": key, "occurrences": n, "tokens": None, "per10k": None})
        by_manuscript.sort(key=lambda r: (-r["occurrences"], r["title"]))

        def distribution(buckets, label):
            return [{label: k, **v, "per10k": per_10k(v["occurrences"], v["tokens"])} for k, v in buckets.items()]

        undated = by_century.get(None)
        return {
            "occurrences": total,
            "matching_pages": self.pages_with_hits,
            "manuscripts_with_hits": len(self.per_manuscript),
            "manuscripts_in_scope": len(self.scope),
            "tokens_in_scope": scope_tokens,
            "per10k": per_10k(total, scope_tokens),
            "truncated": truncated_reason is not None,
            "truncated_reason": truncated_reason,
            "undated_occurrences": undated["occurrences"] if undated else 0,
            "forms": [{"form": f, "occurrences": n, "share": round(n / total * 100, 1) if total else 0}
                      for f, n in self.forms.most_common(TOP_FORMS)],
            "distinct_forms": len(self.forms),
            "by_manuscript": by_manuscript[:TOP_MANUSCRIPTS],
            "by_century": sorted((r for r in distribution(by_century, "century") if r["century"] is not None),
                                 key=lambda r: r["century"]),
            "by_language": sorted(distribution(by_language, "language"), key=lambda r: (-r["occurrences"], r["language"])),
        }


def stats_events(query: Dict, pit_id: str, limit: int = STATS_MAX_OCCURRENCES, merge_adjacent: bool = True,
                 max_seconds: Optional[float] = None, total_pages: Optional[int] = None,
                 progress_interval: float = PROGRESS_INTERVAL, clock=time.monotonic) -> Iterator[Dict]:
    """
    Scan the matching pages, yielding `progress` events (about every `progress_interval` seconds,
    with the figures so far) and finally one `done` event. Stops early at `limit` occurrences or
    after `max_seconds`, and says so in the final figures. Always closes the point in time.
    """
    acc = Accumulator(scope_manuscripts(scope_only_query(query)))
    started = last_event = clock()
    reason = None

    hits = scan_hits(query, pit_id, source=["collection"])
    try:
        for hit in hits:
            acc.add(hit, merge_adjacent, room=limit - acc.occurrences)
            now = clock()
            if acc.occurrences >= limit:
                reason = "occurrences"
                break
            if max_seconds is not None and now - started > max_seconds:
                reason = "time"
                break
            if now - last_event >= progress_interval:
                last_event = now
                yield {"type": "progress", "pages": acc.pages_scanned, "total_pages": total_pages,
                       "occurrences": acc.occurrences, "stats": acc.snapshot()}
    finally:
        hits.close()
    yield {"type": "done", "pages": acc.pages_scanned, "total_pages": total_pages, "stats": acc.snapshot(reason)}


def compute_stats(query: Dict, pit_id: str, limit: int = STATS_MAX_OCCURRENCES, merge_adjacent: bool = True,
                  max_seconds: Optional[float] = None) -> Dict:
    """The final figures of a scan (see stats_events), without the intermediate events."""
    final = None
    for event in stats_events(query, pit_id, limit, merge_adjacent, max_seconds):
        final = event
    return final["stats"]


def stats_csv_rows(stats: Dict) -> Iterable[List]:
    """The statistics as one long table: dimension, key, occurrences, per10k, tokens."""
    yield ["dimension", "key", "occurrences", "per_10000_tokens", "tokens"]
    yield ["total", "all", stats["occurrences"], stats["per10k"], stats["tokens_in_scope"]]
    for r in stats["forms"]:
        yield ["form", r["form"], r["occurrences"], "", ""]
    for r in stats["by_manuscript"]:
        yield ["manuscript", r["title"], r["occurrences"], r["per10k"], r["tokens"]]
    for r in stats["by_century"]:
        yield ["century", r["century"], r["occurrences"], r["per10k"], r["tokens"]]
    for r in stats["by_language"]:
        yield ["language", r["language"], r["occurrences"], r["per10k"], r["tokens"]]


def _line(event: Dict) -> str:
    return json.dumps(event, ensure_ascii=False) + "\n"


def _response_for(stats: Dict, fmt: str):
    if fmt == "csv":
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\r\n")
        for row in stats_csv_rows(stats):
            writer.writerow([_spreadsheet_safe(c) for c in row])
        return Response(
            "﻿" + buffer.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": 'attachment; filename="term-statistics.csv"'},
        )
    return jsonify(stats)


def stats_route():
    try:
        q, mode, query = scoped_query(request.args)
    except ScopeError as error:
        return _json_error(error)

    fmt = request.args.get("format", "json")
    if fmt not in {"json", "csv", "ndjson"}:
        return jsonify({"error": "format must be json, csv or ndjson"}), 400
    limit = max(1, min(request.args.get("limit", STATS_MAX_OCCURRENCES, type=int), STATS_MAX_OCCURRENCES))
    cache_key = {
        "q": q, "mode": mode, "limit": limit, "generation": index_generation(),
        **{name: request.args.get(name) or "" for name in ("language", "resource", "within")},
    }

    meta = {"query": q, "mode": mode}
    cached = None if request.args.get("refresh") in ("1", "true") else disk_cache.get("stats", cache_key)
    if cached is not None:
        stats = {**meta, **cached, "cached": True}
        if fmt == "ndjson":
            return Response(_line({"type": "done", "stats": stats}), mimetype="application/x-ndjson")
        return _response_for(stats, fmt)

    if not STATS_LIMIT.try_acquire():
        return busy_response(STATS_LIMIT)

    def finish(stats: Dict) -> Dict:
        computed = {**stats, "computed_at": int(time.time()), "limit": limit, "max_seconds": STATS_MAX_SECONDS}
        if stats["truncated_reason"] != "time":  # a faster run would have counted more: not worth keeping
            disk_cache.set("stats", cache_key, computed)
        return {**meta, **computed, "cached": False}

    events = None
    try:
        total_pages = concordance.es.count(index=INDEX, body={"query": query})["count"]
        pit_id = concordance.es.open_point_in_time(index=INDEX, keep_alive="2m")["id"]
        events = stats_events(query, pit_id, limit, has_phrase(q), STATS_MAX_SECONDS, total_pages)
    except Exception:
        STATS_LIMIT.release()
        raise

    if fmt == "ndjson":
        def generate():
            try:
                yield _line({"type": "start", "total_pages": total_pages, "limit": limit, "max_seconds": STATS_MAX_SECONDS})
                for event in events:
                    if event["type"] == "progress":
                        event = {**event, "stats": {**meta, **event["stats"]}}
                    else:
                        event = {**event, "stats": finish(event["stats"])}
                    yield _line(event)
            except Exception as error:  # the response has started: report it in the stream
                log.exception("statistics failed")
                yield _line({"type": "error", "message": str(error)})
            finally:
                events.close()  # also when the client leaves: closes the point in time
                STATS_LIMIT.release()

        return Response(
            stream_with_context(generate()),
            mimetype="application/x-ndjson",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    try:
        final = None
        for event in events:
            final = event
        return _response_for(finish(final["stats"]), fmt)
    finally:
        events.close()
        STATS_LIMIT.release()
