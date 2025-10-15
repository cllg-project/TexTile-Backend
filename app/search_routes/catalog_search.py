from flask import request, jsonify

from .config import _get_document_title, es, INDEX


def manuscripts_route():
    q = (request.args.get("q") or "").strip()
    page = request.args.get("page", 1, type=int)
    size = request.args.get("size", 25, type=int)

    if not q:
        return jsonify({"total": 0, "items": []})

    # Build queries for different field types
    text_queries = []

    # Text fields (all available metadata fields)
    text_queries.append({
        "multi_match": {
            "query": q,
            "fields": [
                "language^3",
                "location^3",
                "content",
                "notes_scopecontent^2",
                "filename",
                "ark_portail",
                "manifest_url"
            ],
            "operator": "and",
            "type": "cross_fields"
        }
    })

    # Check if query contains numbers for date fields
    import re
    numbers = re.findall(r'\b\d{3,4}\b', q)  # Find 3-4 digit numbers (likely years)

    for num in numbers:
        year = int(num)
        text_queries.append({
            "bool": {
                "should": [
                    {"range": {"start_year": {"lte": year, "gte": year - 50}}},
                    {"range": {"stop_year": {"gte": year, "lte": year + 50}}},
                    {"term": {"start_year": year}},
                    {"term": {"stop_year": year}}
                ]
            }
        })

    # Combine all queries
    query = {
        "bool": {
            "should": text_queries,
            "minimum_should_match": 1
        }
    } if len(text_queries) > 1 else text_queries[0]

    body = {
        "size": 0,
        "query": query,
        "aggs": {
            "manuscripts": {
                "terms": {
                    "field": "collection.keyword",
                    "size": 1000  # Get all unique manuscripts
                },
                "aggs": {
                    "sample": {
                        "top_hits": {
                            "size": 1,
                            "_source": [
                                "collection", "language", "location", "start_year", "stop_year",
                                "ark_portail", "manifest_url", "tokens", "filename",
                                "notes_scopecontent", "distrib"
                            ],
                            "highlight": {
                                "fields": {
                                    "language": {},
                                    "location": {},
                                    "notes_scopecontent": {"fragment_size": 200, "number_of_fragments": 1},
                                    "filename": {}
                                },
                                "pre_tags": ["<mark class='dts-hit'>"],
                                "post_tags": ["</mark>"]
                            }
                        }
                    },
                    "page_count": {
                        "value_count": {
                            "field": "ref"
                        }
                    }
                }
            }
        }
    }

    res = es.search(index=INDEX, body=body)

    manuscripts = []
    aggs = res.get("aggregations", {}).get("manuscripts", {})

    for bucket in aggs.get("buckets", []):
        sample_hit = bucket["sample"]["hits"]["hits"][0]
        src = sample_hit["_source"]
        highlight = sample_hit.get("highlight", {})

        # Get highlighted metadata if available
        highlighted_language = (highlight.get("language", [None])[0]) or src.get("language", "")
        highlighted_location = (highlight.get("location", [None])[0]) or src.get("location", "")
        highlighted_notes = (highlight.get("notes_scopecontent", [None])[0]) or ""
        highlighted_filename = (highlight.get("filename", [None])[0]) or src.get("filename", "")

        # Get title from database
        title = _get_document_title(src)

        manuscripts.append({
            "collection": src.get("collection"),
            "title": title,
            "language": highlighted_language,
            "location": highlighted_location,
            "start_year": src.get("start_year"),
            "stop_year": src.get("stop_year"),
            "ark_portail": src.get("ark_portail"),
            "manifest_url": src.get("manifest_url"),
            "tokens": src.get("tokens"),
            "filename": highlighted_filename,
            "notes_scopecontent": highlighted_notes,
            "distrib": src.get("distrib", {}),
            "page_count": bucket["page_count"]["value"]
        })

    # Sort by relevance (ES already handles this in aggregation)
    total = len(manuscripts)

    # Apply pagination
    start_idx = (page - 1) * size
    end_idx = start_idx + size
    paginated_manuscripts = manuscripts[start_idx:end_idx]

    return jsonify({
        "total": total,
        "items": paginated_manuscripts
    })


def manuscripts_language_route():
    q = (request.args.get("q") or "").strip()
    page = request.args.get("page", 1, type=int)
    size = request.args.get("size", 25, type=int)

    if not q:
        return jsonify({"total": 0, "items": []})

    # Search specifically in language field
    # Handle both exact matches and partial matches for multi-language entries
    query = {
        "bool": {
            "should": [
                # Exact match
                {"term": {"language": q}},
                # Partial match for multi-language entries (e.g., "lat, fre" contains "lat")
                {"wildcard": {"language": f"*{q}*"}},
                # Text search in language field
                {"match": {"language": q}}
            ],
            "minimum_should_match": 1
        }
    }

    # Aggregate by collection to get manuscript-level results
    body = {
        "size": 0,
        "query": query,
        "aggs": {
            "manuscripts": {
                "terms": {
                    "field": "collection",
                    "size": 1000
                },
                "aggs": {
                    "sample": {
                        "top_hits": {
                            "size": 1,
                            "_source": [
                                "collection", "language", "location", "start_year", "stop_year",
                                "ark_portail", "manifest_url", "tokens", "filename",
                                "notes_scopecontent", "distrib"
                            ],
                            "highlight": {
                                "fields": {
                                    "language": {}
                                },
                                "pre_tags": ["<mark class='dts-hit'>"],
                                "post_tags": ["</mark>"]
                            }
                        }
                    },
                    "page_count": {
                        "value_count": {
                            "field": "ref"
                        }
                    }
                }
            }
        }
    }

    res = es.search(index=INDEX, body=body)

    manuscripts = []
    aggs = res.get("aggregations", {}).get("manuscripts", {})

    for bucket in aggs.get("buckets", []):
        sample_hit = bucket["sample"]["hits"]["hits"][0]
        src = sample_hit["_source"]
        highlight = sample_hit.get("highlight", {})

        highlighted_language = (highlight.get("language", [None])[0]) or src.get("language", "")

        # Get title from database
        title = _get_document_title(src)

        manuscripts.append({
            "collection": src.get("collection"),
            "title": title,
            "language": highlighted_language,
            "location": src.get("location"),
            "start_year": src.get("start_year"),
            "stop_year": src.get("stop_year"),
            "ark_portail": src.get("ark_portail"),
            "manifest_url": src.get("manifest_url"),
            "tokens": src.get("tokens"),
            "filename": src.get("filename"),
            "notes_scopecontent": src.get("notes_scopecontent"),
            "distrib": src.get("distrib", {}),
            "page_count": bucket["page_count"]["value"]
        })

    total = len(manuscripts)

    # Apply pagination
    start_idx = (page - 1) * size
    end_idx = start_idx + size
    paginated_manuscripts = manuscripts[start_idx:end_idx]

    return jsonify({
        "total": total,
        "items": paginated_manuscripts
    })


def manuscripts_date_route():
    start_year = request.args.get("start_year")
    stop_year = request.args.get("stop_year")
    exact_start = request.args.get("exact_start")
    exact_stop = request.args.get("exact_stop")
    page = request.args.get("page", 1, type=int)
    size = request.args.get("size", 25, type=int)

    if not any([start_year, stop_year, exact_start, exact_stop]):
        return jsonify({"error": "At least one date parameter is required"}), 400

    # Build date query
    date_filters = []

    if exact_start:
        try:
            exact_start_int = int(exact_start)
            date_filters.append({"term": {"start_year": exact_start_int}})
        except ValueError:
            return jsonify({"error": "exact_start must be a valid year"}), 400

    if exact_stop:
        try:
            exact_stop_int = int(exact_stop)
            date_filters.append({"term": {"stop_year": exact_stop_int}})
        except ValueError:
            return jsonify({"error": "exact_stop must be a valid year"}), 400

    # Handle range queries
    range_query = {}
    if start_year:
        try:
            start_year_int = int(start_year)
            range_query["start_year"] = {"gte": start_year_int}
        except ValueError:
            return jsonify({"error": "start_year must be a valid year"}), 400

    if stop_year:
        try:
            stop_year_int = int(stop_year)
            if "stop_year" not in range_query:
                range_query["stop_year"] = {}
            range_query["stop_year"]["lte"] = stop_year_int
        except ValueError:
            return jsonify({"error": "stop_year must be a valid year"}), 400

    # Add range filters if any
    for field, range_filter in range_query.items():
        date_filters.append({"range": {field: range_filter}})

    # Combine date filters
    if len(date_filters) == 1:
        query = date_filters[0]
    else:
        query = {
            "bool": {
                "should": date_filters,
                "minimum_should_match": 1
            }
        }

    # Aggregate by collection to get manuscript-level results
    body = {
        "size": 0,
        "query": query,
        "aggs": {
            "manuscripts": {
                "terms": {
                    "field": "collection",
                    "size": 1000
                },
                "aggs": {
                    "sample": {
                        "top_hits": {
                            "size": 1,
                            "_source": [
                                "collection", "language", "location", "start_year", "stop_year",
                                "ark_portail", "manifest_url", "tokens", "filename",
                                "notes_scopecontent", "distrib"
                            ]
                        }
                    },
                    "page_count": {
                        "value_count": {
                            "field": "ref"
                        }
                    }
                }
            }
        }
    }

    res = es.search(index=INDEX, body=body)

    manuscripts = []
    aggs = res.get("aggregations", {}).get("manuscripts", {})

    for bucket in aggs.get("buckets", []):
        sample_hit = bucket["sample"]["hits"]["hits"][0]
        src = sample_hit["_source"]

        # Get title from database
        title = _get_document_title(src)

        manuscripts.append({
            "collection": src.get("collection"),
            "title": title,
            "language": src.get("language"),
            "location": src.get("location"),
            "start_year": src.get("start_year"),
            "stop_year": src.get("stop_year"),
            "ark_portail": src.get("ark_portail"),
            "manifest_url": src.get("manifest_url"),
            "tokens": src.get("tokens"),
            "filename": src.get("filename"),
            "notes_scopecontent": src.get("notes_scopecontent"),
            "distrib": src.get("distrib", {}),
            "page_count": bucket["page_count"]["value"]
        })

    # Sort by start_year
    manuscripts.sort(key=lambda x: x.get("start_year") or 0)
    total = len(manuscripts)

    # Apply pagination
    start_idx = (page - 1) * size
    end_idx = start_idx + size
    paginated_manuscripts = manuscripts[start_idx:end_idx]

    return jsonify({
        "total": total,
        "items": paginated_manuscripts
    })

def manuscripts_range_route():
    """Simple date range search endpoint for queries like '800-1400'"""
    q = (request.args.get("q") or "").strip()
    page = request.args.get("page", 1, type=int)
    size = request.args.get("size", 25, type=int)

    if not q:
        return jsonify({"error": "Query parameter 'q' is required"}), 400

    # Parse range query (e.g., "800-1400")
    if '-' in q:
        try:
            parts = q.split('-', 1)
            start_year = int(parts[0].strip())
            stop_year = int(parts[1].strip())
        except (ValueError, IndexError):
            return jsonify({"error": "Invalid date range format. Use 'start-end' (e.g., '800-1400')"}), 400
    else:
        return jsonify({"error": "Invalid date range format. Use 'start-end' (e.g., '800-1400')"}), 400

    # Build date range query - handle cases where stop_year might be empty
    # Since most documents only have start_year, we'll search for documents where
    # start_year falls within the requested range
    query = {
        "range": {
            "start_year": {
                "gte": start_year,
                "lte": stop_year
            }
        }
    }

    # Aggregate by collection to get manuscript-level results
    body = {
        "size": 0,
        "query": query,
        "aggs": {
            "manuscripts": {
                "terms": {
                    "field": "collection",
                    "size": 1000
                },
                "aggs": {
                    "sample": {
                        "top_hits": {
                            "size": 1,
                            "_source": [
                                "collection", "language", "location", "start_year", "stop_year",
                                "ark_portail", "manifest_url", "tokens", "filename",
                                "notes_scopecontent", "distrib"
                            ]
                        }
                    },
                    "page_count": {
                        "value_count": {
                            "field": "ref"
                        }
                    }
                }
            }
        }
    }

    res = es.search(index=INDEX, body=body)

    manuscripts = []
    aggs = res.get("aggregations", {}).get("manuscripts", {})

    for bucket in aggs.get("buckets", []):
        hit = bucket.get("sample", {}).get("hits", {}).get("hits", [])
        if hit:
            src = hit[0].get("_source", {})
            
            # Get title from database
            title = _get_document_title(src)
            
            manuscripts.append({
                "collection": src.get("collection"),
                "title": title,
                "language": src.get("language"),
                "location": src.get("location"),
                "start_year": src.get("start_year"),
                "stop_year": src.get("stop_year"),
                "ark_portail": src.get("ark_portail"),
                "manifest_url": src.get("manifest_url"),
                "tokens": src.get("tokens"),
                "filename": src.get("filename"),
                "notes_scopecontent": src.get("notes_scopecontent"),
                "page_count": bucket.get("page_count", {}).get("value", 0)
            })

    # Sort by relevance (chronological order by start_year)
    manuscripts.sort(key=lambda x: x.get("start_year", 9999))
    total = len(manuscripts)

    # Apply pagination
    start_idx = (page - 1) * size
    end_idx = start_idx + size
    paginated_manuscripts = manuscripts[start_idx:end_idx]

    return jsonify({
        "total": total,
        "items": paginated_manuscripts
    })