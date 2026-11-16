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
                "title^3",
                "language^3",
                "content",
                "identifier^2",
                "publisher^2",
                "created^2",
                "relation",
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
        text_queries.append({"wildcard": {"created": f"*{num}*"}})

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
                    "field": "collection",
                    "size": 1000  # Get all unique manuscripts
                },
                "aggs": {
                    "sample": {
                        "top_hits": {
                            "size": 1,
                            "_source": [
                                "collection", "title", "language", "created", "issued",
                                "identifier", "publisher", "relation",
                                "manifest_url", "tokens", "ocr_quality"
                            ],
                            "highlight": {
                                "fields": {
                                    "title": {},
                                    "language": {},
                                    "created": {},
                                    "publisher": {}
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
        highlighted_title = (highlight.get("title", [None])[0]) or src.get("title", "")
        highlighted_language = (highlight.get("language", [None])[0]) or src.get("language", "")
        highlighted_created = (highlight.get("created", [None])[0]) or src.get("created", "")
        highlighted_publisher = (highlight.get("publisher", [None])[0]) or src.get("publisher", "")

        # Get title from database
        title = highlighted_title or _get_document_title(src)

        manuscripts.append({
            "collection": src.get("collection"),
            "title": title,
            "language": highlighted_language,
            "created": highlighted_created,
            "issued": src.get("issued"),
            "identifier": src.get("identifier"),
            "publisher": highlighted_publisher,
            "relation": src.get("relation"),
            "manifest_url": src.get("manifest_url"),
            "tokens": src.get("tokens"),
            "ocr_quality": src.get("ocr_quality"),
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
                                "collection", "title", "language", "created", "issued",
                                "identifier", "publisher", "relation",
                                "manifest_url", "tokens", "ocr_quality"
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
            "created": src.get("created"),
            "issued": src.get("issued"),
            "identifier": src.get("identifier"),
            "publisher": src.get("publisher"),
            "relation": src.get("relation"),
            "manifest_url": src.get("manifest_url"),
            "tokens": src.get("tokens"),
            "ocr_quality": src.get("ocr_quality"),
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
    date_queries = []

    if exact_start:
        date_queries.append({"wildcard": {"created": f"*{exact_start}*"}})

    if exact_stop:
        date_queries.append({"wildcard": {"created": f"*{exact_stop}*"}})

    # Handle range queries
    if start_year or stop_year:
        try:
            start_int = int(start_year) if start_year else 0
            stop_int = int(stop_year) if stop_year else 9999

            for year in range(start_int, min(stop_int + 1, start_int + 1000)):
                date_queries.append({"wildcard": {"created": f"*{year}*"}})
        except ValueError:
            return jsonify({"error": "Invalid year format"}), 400

    # Combine date filters
    if not date_queries:
        return jsonify({"error": "No valid date criteria"}), 400
    
    query = {
        "bool": {
            "should": date_queries,
            "minimum_should_match": 1
        }
    } if len(date_queries) > 1 else date_queries[0]

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
                                "collection", "title", "language", "created", "issued",
                                "identifier", "publisher", "relation",
                                "manifest_url", "tokens", "ocr_quality"
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
            "collection": src.get("collection"),
            "title": title,
            "language": src.get("language"),
            "created": src.get("created"),
            "issued": src.get("issued"),
            "identifier": src.get("identifier"),
            "publisher": src.get("publisher"),
            "relation": src.get("relation"),
            "manifest_url": src.get("manifest_url"),
            "tokens": src.get("tokens"),
            "ocr_quality": src.get("ocr_quality"),
            "page_count": bucket["page_count"]["value"]
        })

    # Sort by created year
    def extract_first_year(created_str):
        if not created_str:
            return 0
        if isinstance(created_str, list):
            created_str = created_str[0] if created_str else ""
        try:
            return int(created_str.split('/')[0])
        except (ValueError, IndexError):
            return 0
    
    manuscripts.sort(key=lambda x: extract_first_year(x.get("created")))
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

    # Build date range query
    date_queries = []
    for year in range(start_year, min(stop_year + 1, start_year + 1000)):
        date_queries.append({"wildcard": {"created": f"*{year}*"}})
    
    query = {
        "bool": {
            "should": date_queries,
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
                                "collection", "title", "language", "created", "issued",
                                "identifier", "publisher", "relation",
                                "manifest_url", "tokens", "ocr_quality"
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
                "created": src.get("created"),
                "issued": src.get("issued"),
                "identifier": src.get("identifier"),
                "publisher": src.get("publisher"),
                "relation": src.get("relation"),
                "manifest_url": src.get("manifest_url"),
                "tokens": src.get("tokens"),
                "ocr_quality": src.get("ocr_quality"),
                "page_count": bucket.get("page_count", {}).get("value", 0)
            })

    # Sort by relevance (chronological order by created year)
    def extract_first_year(manuscript):
        created = manuscript.get("created", "")
        if created and "/" in created:
            try:
                return int(created.split('/')[0])
            except (ValueError, IndexError):
                return 9999
        elif created:
            try:
                return int(created)
            except ValueError:
                return 9999
        return 9999
    
    manuscripts.sort(key=extract_first_year)
    total = len(manuscripts)

    # Apply pagination
    start_idx = (page - 1) * size
    end_idx = start_idx + size
    paginated_manuscripts = manuscripts[start_idx:end_idx]

    return jsonify({
        "total": total,
        "items": paginated_manuscripts
    })