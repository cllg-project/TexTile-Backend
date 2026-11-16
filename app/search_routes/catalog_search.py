import re

from flask import request, jsonify

from .config import es, INDEX
from .generation import index_generation
from .hierarchy import descendants
from .text import fold
from ..cache import ttl_cache

# Whole-word fields, lowercased and diacritics-folded at index time
# (see es_indexer_textile.py: folded_analyzer).
_WORD_FIELDS = [
    "title^3",
    "language.text^3",
    "publisher.text^2",
    "identifier.text^2",
    "created^2",
    "relation.text",
]
_YEAR_RE = re.compile(r"^\d{3,4}$")
_MIN_NGRAM = 4


_fold = fold  # kept under its old name for existing callers


def _escape_wildcard(text):
    return re.sub(r"([\\*?])", r"\\\1", text)


def _build_general_query(q):
    """
    Every whitespace-separated term of the query must match somewhere (AND),
    so "Smith 19" can't degrade to "Smith" when one term is too short for
    the n-gram analyzer. For each term, any of these is enough:
      - a whole-word match, accent/case-insensitive (francais == français)
      - a partial-word match on the title (n-grams, terms of 4+ characters)
      - for 3-4 digit terms, a year falling within the manuscript's date range
    Exact phrase matches on the title are boosted so "Smith-Lesouëf 19"
    ranks above "Smith-Lesouëf 14".
    """
    terms = q.split()
    must = []
    exact_boosts = []
    for term in terms:
        options = [{
            "multi_match": {
                "query": term,
                "fields": _WORD_FIELDS,
                "type": "best_fields",
            }
        }]
        is_short = len(term) < _MIN_NGRAM
        if is_short:
            # Too short for n-grams: fall back to a substring match on title words
            # ("19" also finds "190" and "119"), constant score so exact matches win.
            options.append({"constant_score": {
                "filter": {"wildcard": {"title": {"value": f"*{_escape_wildcard(_fold(term))}*"}}},
                "boost": 0.5,
            }})
        if len(term) >= _MIN_NGRAM:
            options.append({"match": {"title.ngram": {"query": term, "operator": "and"}}})
        if _YEAR_RE.match(term):
            year = int(term)
            options.append({
                "bool": {"filter": [
                    {"range": {"date_start": {"lte": year}}},
                    {"range": {"date_end": {"gte": year}}},
                ]}
            })
        must.append({"bool": {"should": options, "minimum_should_match": 1}})
        # Whole-word title match always outranks partial/substring matches.
        exact_boosts.append({"match": {"title": {"query": term, "boost": 10}}})

    return {
        "bool": {
            "must": must,
            "should": [
                {"match_phrase": {"title": {"query": q, "boost": 20}}},
                *exact_boosts,
            ],
        }
    }


def _filters_from_request():
    """Optional structured filters: language code, and a year interval that must overlap the manuscript's dates."""
    filters = []
    language = (request.args.get("language") or "").strip()
    if language:
        filters.append({"term": {"language": language}})
    start_year = request.args.get("start_year", type=int)
    stop_year = request.args.get("stop_year", type=int)
    if start_year is not None:
        filters.append({"range": {"date_end": {"gte": start_year}}})
    if stop_year is not None:
        filters.append({"range": {"date_start": {"lte": stop_year}}})
    return filters


def _within_filter():
    """
    `within=<collection id>` restricts results to the manuscripts under that collection.
    Returns (filter or None, unknown) where unknown is True when the collection doesn't exist.
    """
    within = (request.args.get("within") or "").strip()
    if not within:
        return None, False
    found = descendants(within)
    if found is None:
        return None, True
    _, resources = found
    return {"terms": {"collection": sorted(resources)}}, False


_SOURCE_FIELDS = [
    "collection", "title", "language", "created", "issued",
    "identifier", "publisher", "relation", "manifest_url", "tokens", "ocr_quality",
]
_MAX_PAGE_SIZE = 100
_MAX_WINDOW = 10000  # Elasticsearch's default index.max_result_window
_HIGHLIGHT = {
    "fields": {
        "title": {"number_of_fragments": 0},
        "title.ngram": {"number_of_fragments": 0},
        "language": {},
        "created": {},
        "publisher": {},
    },
    "pre_tags": ["<mark class='dts-hit'>"],
    "post_tags": ["</mark>"],
}


def _first(values):
    return values[0] if values else None


def _page_params():
    page = max(1, request.args.get("page", 1, type=int))
    size = max(1, min(request.args.get("size", 25, type=int), _MAX_PAGE_SIZE))
    return page, size


def search_manuscripts(query, page, size, highlight=None):
    """
    One result per manuscript (the best-scoring page of each), paginated by Elasticsearch.

    Results are ordered by relevance, then chronologically, then by identifier so paging is stable.
    `total` is the number of distinct manuscripts, not pages.
    """
    start = (page - 1) * size
    if start >= _MAX_WINDOW:
        return {"total": 0, "items": []}

    body = {
        "query": query,
        "collapse": {"field": "collection"},
        "sort": [
            {"_score": "desc"},
            {"date_start": {"order": "asc", "missing": "_last"}},
            {"collection": "asc"},
        ],
        "from": start,
        "size": min(size, _MAX_WINDOW - start),
        "_source": _SOURCE_FIELDS,
        "aggs": {"total": {"cardinality": {"field": "collection", "precision_threshold": 40000}}},
    }
    if highlight:
        body["highlight"] = highlight
    res = es.search(index=INDEX, body=body)

    hits = res["hits"]["hits"]
    total = res["aggregations"]["total"]["value"]
    if not hits:
        return {"total": total, "items": []}

    # Number of pages of each manuscript on this result page (independent of the query)
    ids = [h["_source"]["collection"] for h in hits]
    counts = es.search(index=INDEX, body={
        "size": 0,
        "query": {"terms": {"collection": ids}},
        "aggs": {"pages": {"terms": {"field": "collection", "size": len(ids)}}},
    })
    pages = {b["key"]: b["doc_count"] for b in counts["aggregations"]["pages"]["buckets"]}

    items = []
    for hit in hits:
        src = hit["_source"]
        hl = hit.get("highlight", {})
        items.append({
            "collection": src.get("collection"),
            "title": _first(hl.get("title")) or _first(hl.get("title.ngram")) or src.get("title") or src.get("collection"),
            "language": _first(hl.get("language")) or src.get("language"),
            "created": _first(hl.get("created")) or src.get("created"),
            "issued": src.get("issued"),
            "identifier": src.get("identifier"),
            "publisher": _first(hl.get("publisher")) or src.get("publisher"),
            "relation": src.get("relation"),
            "manifest_url": src.get("manifest_url"),
            "tokens": src.get("tokens"),
            "ocr_quality": src.get("ocr_quality"),
            "page_count": pages.get(src.get("collection"), 0),
        })
    return {"total": total, "items": items}


def manuscripts_route():
    """General catalog search: free text plus optional language, year and `within` filters."""
    q = (request.args.get("q") or "").strip()
    page, size = _page_params()
    filters = _filters_from_request()
    within, unknown = _within_filter()
    if within is not None:
        filters.append(within)

    if unknown or (not q and not filters):
        return jsonify({"total": 0, "items": []})

    if q:
        query = _build_general_query(q)
        query["bool"]["filter"] = filters
    else:
        query = {"bool": {"filter": filters}}
    return jsonify(search_manuscripts(query, page, size, highlight=_HIGHLIGHT if q else None))


# --- Legacy endpoints -------------------------------------------------------
# Kept so existing API clients keep working; all of them are thin wrappers over
# search_manuscripts() with the structured fields instead of wildcard matching.

def manuscripts_language_route():
    q = (request.args.get("q") or "").strip()
    page, size = _page_params()
    if not q:
        return jsonify({"total": 0, "items": []})

    query = {"bool": {"should": [
        {"term": {"language": q}},
        {"term": {"language": q.lower()}},
        {"match": {"language.text": q}},
    ], "minimum_should_match": 1}}
    return jsonify(search_manuscripts(query, page, size, highlight={
        "fields": {"language": {}}, "pre_tags": _HIGHLIGHT["pre_tags"], "post_tags": _HIGHLIGHT["post_tags"],
    }))


def manuscripts_date_route():
    page, size = _page_params()
    filters = []
    # type=int silently yields None on bad input; tell the client instead
    for name in ("start_year", "stop_year", "exact_start", "exact_stop"):
        raw = request.args.get(name)
        if raw not in (None, "") and request.args.get(name, type=int) is None:
            return jsonify({"error": "Invalid year format"}), 400
    start_year = request.args.get("start_year", type=int)
    stop_year = request.args.get("stop_year", type=int)
    exact_start = request.args.get("exact_start", type=int)
    exact_stop = request.args.get("exact_stop", type=int)

    if exact_start is not None:
        filters.append({"term": {"date_start": exact_start}})
    if exact_stop is not None:
        filters.append({"term": {"date_end": exact_stop}})
    if start_year is not None:
        filters.append({"range": {"date_end": {"gte": start_year}}})
    if stop_year is not None:
        filters.append({"range": {"date_start": {"lte": stop_year}}})

    if not filters:
        return jsonify({"error": "At least one date parameter is required"}), 400
    return jsonify(search_manuscripts({"bool": {"filter": filters}}, page, size))


def manuscripts_range_route():
    """Date range search for queries like '800-1400'."""
    q = (request.args.get("q") or "").strip()
    page, size = _page_params()
    if not q:
        return jsonify({"error": "Query parameter 'q' is required"}), 400

    match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", q)
    if not match:
        return jsonify({"error": "Invalid date range format. Use 'start-end' (e.g., '800-1400')"}), 400
    start_year, stop_year = int(match.group(1)), int(match.group(2))

    query = {"bool": {"filter": [
        {"range": {"date_end": {"gte": start_year}}},
        {"range": {"date_start": {"lte": stop_year}}},
    ]}}
    return jsonify(search_manuscripts(query, page, size))


@ttl_cache(60)
def _facets(generation: str) -> dict:
    """Computed once a minute at most, and again after a re-index (the generation is part of the key)."""
    body = {
        "size": 0,
        "aggs": {
            "languages": {
                "terms": {"field": "language", "size": 100},
                "aggs": {"manuscripts": {"cardinality": {"field": "collection"}}},
            },
            "min_year": {"min": {"field": "date_start"}},
            "max_year": {"max": {"field": "date_end"}},
        },
    }
    res = es.search(index=INDEX, body=body)
    aggs = res.get("aggregations", {})
    languages = sorted(
        ({"code": b["key"], "count": b["manuscripts"]["value"]} for b in aggs.get("languages", {}).get("buckets", [])),
        key=lambda x: (-x["count"], x["code"]),
    )
    return {
        "languages": languages,
        "min_year": aggs.get("min_year", {}).get("value"),
        "max_year": aggs.get("max_year", {}).get("value"),
    }


def manuscripts_facets_route():
    """Available values for the catalog filters: languages (with manuscript counts) and the date span."""
    return jsonify(_facets(index_generation()))
