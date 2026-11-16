"""
Concordance (keyword in context) export, streamed as CSV.

    GET /concordance/?q=<query>[&mode=exact|partial|fuzzy][&language=<code>]
                     [&resource=<manuscript id>][&within=<collection id>]
                     [&width=<characters of context>][&limit=<max rows>]        -> CSV (streamed)
    GET /concordance/?...&format=json[&page=1][&size=20]                         -> one page of rows

(Term statistics over the same query live in term_stats.py.)

The query is the same as the text search (same syntax, same matching modes), so a concordance
contains exactly the hits the search page shows. Elasticsearch returns each matching page with its
whole text highlighted; the keyword and its left/right context are cut out here. One row per
occurrence, in reading order (manuscript, then page order, then position in the page).

Rows are produced while Elasticsearch is paged through (point in time + search_after), so memory
stays flat however large the concordance is.
"""
import csv
import io
import logging
import os
import re
import unicodedata
from typing import Dict, Iterator, List, NamedTuple, Optional

from flask import Response, jsonify, request, stream_with_context

from .config import es, INDEX
from .hierarchy import descendants
from .query_builder import build_search_query
from ..limits import EXPORT_LIMIT, busy_response

log = logging.getLogger(__name__)

# Unicode private-use characters: cannot occur in transcriptions, unlike "<hit>"
MARK_OPEN = ""
MARK_CLOSE = ""

DEFAULT_WIDTH = 40
MAX_WIDTH = 200
BATCH_SIZE = 100
MAX_ROWS = int(os.getenv("CONCORDANCE_MAX_ROWS", "100000"))

HEADER = ["collection", "manuscript", "ref", "line", "left", "keyword", "right", "language", "date"]
_HIGHLIGHT = {
    "fields": {
        "content": {"number_of_fragments": 0},
        "content.ngram": {"number_of_fragments": 0},
    },
    "pre_tags": [MARK_OPEN],
    "post_tags": [MARK_CLOSE],
}


class Occurrence(NamedTuple):
    line: int       # 1-based line of the keyword within the page
    left: str
    keyword: str
    right: str


def has_phrase(q: str) -> bool:
    """Whether the query contains a quoted multi-word phrase (`"la bouche"`)."""
    parts = [p.strip() for p in q.split(",")]
    return any(len(p) > 2 and p.startswith('"') and p.endswith('"') and re.search(r"\s", p[1:-1]) for p in parts)


def extract_occurrences(marked: str, width: int = DEFAULT_WIDTH, merge_adjacent: bool = True) -> List[Occurrence]:
    """
    Cut keyword-in-context rows out of a text whose matches are wrapped in MARK_OPEN / MARK_CLOSE.

    With `merge_adjacent`, matches separated only by whitespace are merged, so a phrase
    ("la bouche") is one keyword even though Elasticsearch marks each word. Leave it off for
    queries without a phrase, where two neighbouring hits (`deus deus` for `deus,deum`) are two
    occurrences. Context never includes other marks, and line breaks inside the context or
    keyword become spaces.
    """
    plain_parts: List[str] = []
    spans: List[List[int]] = []
    pos = 0
    start: Optional[int] = None
    for char in marked:
        if char == MARK_OPEN:
            start = pos
        elif char == MARK_CLOSE:
            if start is not None and pos > start:
                spans.append([start, pos])
            start = None
        else:
            plain_parts.append(char)
            pos += 1
    text = "".join(plain_parts)

    merged: List[List[int]] = []
    for span in spans:
        if merge_adjacent and merged and not text[merged[-1][1]:span[0]].strip():
            merged[-1][1] = span[1]
        else:
            merged.append(span)

    def flat(value: str) -> str:
        return re.sub(r"\s*\n\s*", " ", value)

    out = []
    for begin, end in merged:
        out.append(Occurrence(
            line=text.count("\n", 0, begin) + 1,
            left=flat(text[max(0, begin - width):begin]),
            keyword=flat(text[begin:end]),
            right=flat(text[end:end + width]),
        ))
    return out


def _marked_content(hit: Dict) -> str:
    """The highlighted page text, preferring the exact field over the n-gram one."""
    highlight = hit.get("highlight", {})
    for field in ("content", "content.ngram"):
        for fragment in highlight.get(field, []):
            if MARK_OPEN in fragment:
                return fragment
    return ""


def _spreadsheet_safe(value) -> str:
    """Stop spreadsheets from running a cell as a formula (CSV injection)."""
    text = "" if value is None else str(value)
    return "'" + text if text and text[0] in "=+-@\t\r" else text


def _first(values) -> str:
    if isinstance(values, list):
        return values[0] if values else ""
    return values or ""


def rows_for_hit(hit: Dict, width: int, merge_adjacent: bool = True) -> Iterator[List[str]]:
    src = hit.get("_source", {})
    for occ in extract_occurrences(_marked_content(hit), width, merge_adjacent):
        yield [
            src.get("collection", ""),
            src.get("title", ""),
            src.get("ref", ""),
            occ.line,
            occ.left,
            occ.keyword,
            occ.right,
            ", ".join(src["language"]) if isinstance(src.get("language"), list) else (src.get("language") or ""),
            _first(src.get("created")),
        ]


def _csv_line(cells) -> str:
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\r\n").writerow(cells)
    return buffer.getvalue()


_SOURCE = ["collection", "title", "ref", "language", "created"]
# Reading order: manuscript, page order, then a unique tiebreaker (required for search_after)
_SORT = [
    {"collection": "asc"},
    {"position": {"order": "asc", "missing": "_last"}},
    {"_shard_doc": "asc"},
]


def scan_hits(query: Dict, pit_id: str, source: List[str] = None) -> Iterator[Dict]:
    """
    Every page matching `query`, highlighted, in reading order, fetched in batches
    (point in time + search_after). Always closes the point in time, even if the consumer stops early.
    """
    search_after = None
    try:
        while True:
            body = {
                "size": BATCH_SIZE,
                "query": query,
                "pit": {"id": pit_id, "keep_alive": "2m"},
                "sort": _SORT,
                "highlight": _HIGHLIGHT,
                "_source": source or _SOURCE,
                "track_total_hits": False,
            }
            if search_after:
                body["search_after"] = search_after
            res = es.search(body=body)
            pit_id = res.get("pit_id", pit_id)
            hits = res["hits"]["hits"]
            if not hits:
                return
            yield from hits
            search_after = hits[-1]["sort"]
    finally:
        try:
            es.close_point_in_time(id=pit_id)
        except Exception:  # the PIT expires by itself after keep_alive
            log.warning("could not close point in time", exc_info=True)


def concordance_rows(query: Dict, pit_id: str, width: int, limit: int, merge_adjacent: bool = True) -> Iterator[List]:
    """Yield concordance rows for `query`, at most `limit`."""
    sent = 0
    hits = scan_hits(query, pit_id)
    try:
        for hit in hits:
            for row in rows_for_hit(hit, width, merge_adjacent):
                yield row
                sent += 1
                if sent >= limit:
                    return
    finally:
        hits.close()  # closes the point in time now, not whenever the generator is collected


def _filename(q: str) -> str:
    ascii_q = unicodedata.normalize("NFKD", q).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^A-Za-z0-9]+", "-", ascii_q).strip("-").lower()[:40]
    return f"concordance-{slug}.csv" if slug else "concordance.csv"


class ScopeError(Exception):
    """A request that cannot be answered; carries the HTTP status."""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.message, self.status = message, status


def scope_filters(args) -> List[Dict]:
    """The corpus restrictions shared by the search, the concordance and the statistics."""
    filters = []
    if args.get("resource"):
        filters.append({"term": {"collection": args["resource"]}})
    if args.get("language"):
        filters.append({"term": {"language": args["language"]}})
    within = (args.get("within") or "").strip()
    if within:
        found = descendants(within)
        if found is None:
            raise ScopeError(f"Unknown collection {within!r}", 404)
        filters.append({"terms": {"collection": sorted(found[1])}})
    return filters


def scoped_query(args):
    """(q, mode, elasticsearch query) for a request, raising ScopeError when it cannot be answered."""
    q = (args.get("q") or "").strip()
    if not q:
        raise ScopeError("Query parameter 'q' is required")
    mode = args.get("mode", "exact")
    if mode not in {"exact", "fuzzy", "partial"}:
        mode = "exact"
    query = build_search_query(q, mode=mode).get("query", {})
    filters = scope_filters(args)
    if filters:
        query = {"bool": {"must": query, "filter": filters}}
    return q, mode, query


def _json_error(error: ScopeError):
    return jsonify({"error": error.message}), error.status


def concordance_page(query: Dict, page: int, size: int, width: int, merge_adjacent: bool = True) -> Dict:
    """One page of concordance rows: the occurrences of `size` matching pages, in reading order."""
    start = (page - 1) * size
    res = es.search(index=INDEX, body={
        "query": query,
        "from": start,
        "size": size,
        "sort": _SORT[:-1],  # no point in time here: the tiebreaker is only needed for search_after
        "highlight": _HIGHLIGHT,
        "_source": _SOURCE,
        "track_total_hits": True,
    })
    total = res["hits"]["total"]["value"]
    rows = [dict(zip(HEADER, row)) for hit in res["hits"]["hits"] for row in rows_for_hit(hit, width, merge_adjacent)]
    return {"rows": rows, "page": page, "size": size, "total_pages": total, "pages": max(1, -(-total // size))}


def concordance_route():
    try:
        q, mode, query = scoped_query(request.args)
    except ScopeError as error:
        return _json_error(error)
    width = max(5, min(request.args.get("width", DEFAULT_WIDTH, type=int), MAX_WIDTH))
    limit = max(1, min(request.args.get("limit", MAX_ROWS, type=int), MAX_ROWS))

    if request.args.get("format") == "json":
        page = max(1, request.args.get("page", 1, type=int))
        size = max(1, min(request.args.get("size", 20, type=int), 100))
        if (page - 1) * size >= 10000:  # Elasticsearch's default result window
            return jsonify({"error": "Page out of range; export the CSV for the full concordance"}), 400
        return jsonify(concordance_page(query, page, size, width, has_phrase(q)))

    if not EXPORT_LIMIT.try_acquire():
        return busy_response(EXPORT_LIMIT)
    try:
        matching_pages = es.count(index=INDEX, body={"query": query})["count"]
        pit_id = es.open_point_in_time(index=INDEX, keep_alive="2m")["id"]
    except Exception:
        EXPORT_LIMIT.release()
        raise

    def generate():
        try:
            yield "\ufeff"  # BOM: spreadsheets otherwise misread the UTF-8 diacritics
            yield _csv_line(HEADER)
            for row in concordance_rows(query, pit_id, width, limit, has_phrase(q)):
                yield _csv_line([_spreadsheet_safe(c) for c in row])
        finally:
            EXPORT_LIMIT.release()  # also when the client stops reading

    return Response(
        stream_with_context(generate()),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{_filename(q)}"',
            "X-Matching-Pages": str(matching_pages),  # lets clients show progress / size
            "X-Accel-Buffering": "no",                # nginx: do not buffer the stream
            "Cache-Control": "no-cache",
        },
    )
