from flask import jsonify, url_for
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
    from flask import request
    
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
            # Build URL manually to avoid url_for issues
            collection_url = f"/collection/?id={collection.identifier}"
            
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
        return jsonify({
            "error": f"Database error: {str(e)}",
            "collections": [],
            "total": 0
        }), 500
