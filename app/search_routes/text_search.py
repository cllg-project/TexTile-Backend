from flask import request, jsonify

from .config import _get_document_title, es, INDEX
from .query_builder import build_search_query


def get_vector_model():
    return None


def search_route():
    if request.args.get("format") == "csv":
        # Same query and filters as the search, exported as a streamed concordance
        from .concordance import concordance_route
        return concordance_route()
    q = (request.args.get("q") or "").strip()
    resource = request.args.get("resource")  # limits to one doc if present
    language = request.args.get("language")
    page = request.args.get("page", 1, type=int)
    size = request.args.get("size", 25, type=int)
    mode = request.args.get("mode", "exact")
    if mode not in {"exact", "fuzzy", "partial"}:
        mode = "exact"
    if not q:
        return jsonify({"total": 0, "items": []})

    query = build_search_query(q, mode=mode).get("query", {})

    filters = []
    if resource:
        filters.append({"term": {"collection": resource}})
    if language:
        filters.append({"term": {"language": language}})
    
    if filters:
        # wrap the query in a bool query with filters
        query = {
            "bool": {
                "must": query,
                "filter": filters if len(filters) > 1 else filters[0]
            }
        }

    body = {
        "from": (page - 1) * size,
        "size": size,
        "query": query,
        "highlight": {
            "fields": {
                "content": {"fragment_size": 160},
                "content.ngram": {"fragment_size": 160}
            },
            "pre_tags": ["<mark class='dts-hit'>"],
            "post_tags": ["</mark>"],
        },
        "_source": ["collection", "ref", "content", "title", "identifier", "publisher", "created", "relation"],
    }

    res = es.search(index=INDEX, body=body)
    total = res.get("hits", {}).get("total", {}).get("value", 0)
    items = []
    for h in res.get("hits", {}).get("hits", []):
        src = h.get("_source", {})

        # temporary part : get title from database
        title = _get_document_title(src)
        highlight = h.get("highlight", {})
        content = (highlight.get("content", []) + highlight.get("content.ngram", []))
        items.append({
            "collection": src.get("collection"),
            "ref": src.get("ref"),
            "title": title,
            "snippet": " <span class='text-muted'>[...]</span> ".join(content)
        })
    return jsonify({"total": total, "items": items})


def hybrid_search_route():
    """Hybrid search: combines traditional text + vector search"""
    q = (request.args.get("q") or "").strip()
    resource = request.args.get("resource")
    page = request.args.get("page", 1, type=int)
    size = request.args.get("size", 25, type=int)

    if not q:
        return jsonify({
            "total": 0,
            "traditional": {"total": 0, "items": []},
            "vector": {"total": 0, "items": []}
        })

    # Get traditional search results (reuse existing logic)
    traditional_params = request.args.to_dict()
    traditional_params["size"] = size // 2  # Split results

    # Simulate traditional search (you can optimize this)
    traditional_body = {
        "from": (page - 1) * (size // 2),
        "size": size // 2,
        "query": {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": q,
                        "fields": ["content^3", "ref"],
                        "operator": "and",
                    }
                },
                **({"filter": {"term": {"collection": resource}}} if resource else {}),
            }
        },
        "highlight": {
            "fields": {"content": {"fragment_size": 160, "number_of_fragments": 1}},
            "pre_tags": ["<mark class='text-hit'>"],
            "post_tags": ["</mark>"],
        },
        "_source": ["collection", "ref", "content", "title", "identifier", "publisher", "created", "relation"],
    }

    try:
        traditional_res = es.search(index=INDEX, body=traditional_body)
        traditional_items = []
        for h in traditional_res.get("hits", {}).get("hits", []):
            src = h.get("_source", {})

            # Get title from database
            title = _get_document_title(src)

            traditional_items.append({
                "collection": src.get("collection"),
                "ref": src.get("ref"),
                "title": title,
                "content": src.get("content", "")[:200] + "..." if len(src.get("content", "")) > 200 else src.get(
                    "content", ""),
                "snippet": (h.get("highlight", {}).get("content", [None])[0]) or "",
                "score": h["_score"],
                "search_type": "text"
            })
    except:
        traditional_items = []

    # Get vector search results if available
    vector_items = []
    # if VECTOR_SEARCH_AVAILABLE:
    #     try:
    #         vector_params = request.args.to_dict()
    #         vector_params["size"] = size // 2
    #         vector_params["min_score"] = 0.6  # Lower threshold for hybrid
    #
    #         # Create a mock request for vector search
    #         from werkzeug.test import EnvironBuilder
    #         with app.test_request_context('?' + '&'.join([f"{k}={v}" for k, v in vector_params.items()])):
    #             vector_response = vector_search_route()
    #             if vector_response.status_code == 200:
    #                 vector_data = vector_response.get_json()
    #                 vector_items = vector_data.get("items", [])
    #     except:
    #         pass

    return jsonify({
        "total": len(traditional_items) + len(vector_items),
        "traditional": {
            "total": len(traditional_items),
            "items": traditional_items
        },
        "vector": {
            "total": len(vector_items),
            "items": vector_items
        },
        "query": q
    })


# @app.route("/search/vector/")
# def vector_search_route():
#     """Vector/semantic search endpoint"""
#     if not VECTOR_SEARCH_AVAILABLE:
#         return jsonify({"error": "Vector search not available. Install sentence-transformers."}), 503
#
#     q = (request.args.get("q") or "").strip()
#     resource = request.args.get("resource")  # limit to one collection
#     page = int(request.args.get("page", 1))
#     size = int(request.args.get("size", 25))
#     min_score = float(request.args.get("min_score", 0.7))
#
#     if not q:
#         return jsonify({"total": 0, "items": []})
#
#     try:
#         # Get vector model and create query embedding
#         model = get_vector_model()
#         query_vector = model.encode([q])[0].tolist()
#
#         # Build Elasticsearch vector search query using knn (more efficient)
#         vector_query = {
#             "knn": {
#                 "field": "content_vector",
#                 "query_vector": query_vector,
#                 "k": size,
#                 "num_candidates": size * 2  # Search more candidates for better recall
#             },
#             "_source": ["collection", "ref", "content", "language", "location", "start_year", "filename", "notes_scopecontent"],
#             "highlight": {
#                 "fields": {"content": {"fragment_size": 160, "number_of_fragments": 1}},
#                 "pre_tags": ["<mark class='vector-hit'>"],
#                 "post_tags": ["</mark>"]
#             },
#             "size": size,
#             "from": (page - 1) * size
#         }
#
#         # Add resource filter if specified
#         if resource:
#             # For knn search, we need to add filter in a different way
#             vector_query["knn"]["filter"] = {
#                 "term": {"collection.keyword": resource}
#             }
#
#         # Search the vector index
#         res = es.search(index="documents_with_vectors", body=vector_query)
#
#         items = []
#         for h in res.get("hits", {}).get("hits", []):
#             score = h["_score"]  # knn score is already normalized
#             if score >= min_score:
#                 src = h.get("_source", {})
#                 content = src.get("content", "")
#
#                 # Generate semantic highlighting for vector matches
#                 # highlighted_content, matched_phrases = _generate_semantic_highlights(q, content, model)
#                 highlighted_content = content  # Use original content without semantic highlighting
#                 matched_phrases = []
#
#                 # Get title from database
#                 title = _get_document_title(src)
#
#                 item = {
#                     "collection": src.get("collection"),
#                     "ref": src.get("ref"),
#                     "title": title,
#                     "content": content[:200] + "..." if len(content) > 200 else content,
#                     "highlighted_content": highlighted_content[:300] + "..." if len(highlighted_content) > 300 else highlighted_content,
#                     "matched_phrases": matched_phrases,
#                     "score": round(score, 3),
#                     "search_type": "vector"
#                 }
#
#                 # Add metadata if available
#                 if src.get("language"):
#                     item["language"] = src.get("language")
#                 if src.get("location"):
#                     item["location"] = src.get("location")
#
#                 # Add traditional highlight if available (from ES highlighting)
#                 if h.get("highlight", {}).get("content"):
#                     item["snippet"] = h["highlight"]["content"][0]
#
#                 items.append(item)
#
#         return jsonify({
#             "total": len(items),
#             "items": items,
#             "query": q,
#             "search_type": "vector"
#         })
#
#     except Exception as e:
#         return jsonify({"error": f"Vector search error: {str(e)}"}), 500