from flask import current_app, jsonify, request, url_for

from . import hierarchy
from .text import fold
from dapytains.app.database import Collection


def manuscripts_count_route():
    """
    Get the total count of manuscripts (resources) from the database
    """
    try:
        # Count collections that are actual manuscripts
        total_count = Collection.query.filter(Collection.resource == True).count()
        
        return jsonify({
            "total_manuscripts": total_count,
            "source": "database"
        })
    except Exception as e:
        current_app.logger.exception("%s failed", request.path)
        return jsonify({
            "error": f"Database error: {str(e)}",
            "total_manuscripts": 0
        }), 500


def collections_list_route():
    """
    Get a lightweight list of top-level collections for autocomplete
    Returns collection names, identifiers, and URLs
    Supports optional 'q' parameter for filtering by title
    """
    
    try:
        # Get optional query parameter for filtering
        query = request.args.get('q', '').strip()
        
        # Get root collection first
        root_collection = Collection.query.filter(~Collection.parents.any()).first()
        
        if not root_collection:
            return jsonify({
                "collections": [],
                "total": 0
            })
        
        # Get first-level children of root collection
        base_query = Collection.query.filter(
            Collection.parents.any(id=root_collection.id)
        )
        
        # Apply search filter if query provided
        if query:
            base_query = base_query.filter(
                Collection.title.ilike(f'%{query}%')
            )
        
        collections = base_query.order_by(Collection.title).all()
        
        # Build lightweight response
        collection_list = []
        for collection in collections:
            # Relative to the public address (path prefix included)
            collection_url = f"{request.script_root}/collection/?id={collection.identifier}"
            
            collection_data = {
                "identifier": collection.identifier,
                "title": collection.title,
                "url": collection_url,
                "nb_children": getattr(collection, 'nb_children', None)
            }
            collection_list.append(collection_data)
        
        return jsonify({
            "collections": collection_list,
            "total": len(collection_list),
            "query": query if query else None
        })
        
    except Exception as e:
        current_app.logger.exception("%s failed", request.path)
        return jsonify({
            "error": f"Database error: {str(e)}",
            "collections": [],
            "total": 0
        }), 500


def manuscripts_list_route():
    """
    Lightweight, paginated list of manuscripts (resources) for the catalog autocomplete.
    Supports 'q' (title filter, case- and diacritics-insensitive), 'limit' and 'page'.
    """
    try:
        query = request.args.get('q', '').strip()
        limit = max(1, min(request.args.get('limit', 20, type=int), 200))
        page = max(1, request.args.get('page', 1, type=int))

        manuscripts = hierarchy.manuscripts()
        if query:
            needle = fold(query)
            manuscripts = [m for m in manuscripts if needle in fold(m.title)]

        total = len(manuscripts)
        start = (page - 1) * limit
        items = [
            {
                "identifier": m.identifier,
                "title": m.title,
                "url": f"{request.script_root}/collection/?id={m.identifier}",
            }
            for m in manuscripts[start:start + limit]
        ]

        return jsonify({
            "manuscripts": items,
            "total": total,
            "page": page,
            "limit": limit,
            "query": query if query else None
        })
    except Exception as e:
        current_app.logger.exception("%s failed", request.path)
        return jsonify({
            "error": f"Database error: {str(e)}",
            "manuscripts": [],
            "total": 0
        }), 500


def collections_search_route():
    """
    Paginated search over collections (not manuscripts), at any depth.
    Params: q (matches title and description, case/diacritics-insensitive; all terms must match),
            within (limits to a sub-tree), page, size.
    """
    try:
        query = request.args.get('q', '').strip()
        size = max(1, min(request.args.get('size', 20, type=int), 100))
        page = max(1, request.args.get('page', 1, type=int))

        found = hierarchy.collections()

        within = request.args.get('within', '').strip()
        if within:
            below = hierarchy.descendants(within)
            allowed = below[0] if below else set()
            found = [c for c in found if c.identifier in allowed]

        terms = [fold(t) for t in query.split()]
        if terms:
            found = [c for c in found if all(t in c.search_text for t in terms)]

        total = len(found)
        start = (page - 1) * size
        items = [
            {
                "identifier": c.identifier,
                "title": c.title,
                "description": c.description,
                "nb_children": c.nb_children,
                "total_children": c.child_count,
            }
            for c in found[start:start + size]
        ]
        return jsonify({"total": total, "items": items, "page": page, "size": size, "query": query or None})
    except Exception as e:
        current_app.logger.exception("%s failed", request.path)
        return jsonify({"error": f"Database error: {str(e)}", "total": 0, "items": []}), 500
