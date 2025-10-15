import os
from dapytains.app.database import Collection
from elasticsearch import Elasticsearch

VECTOR_SEARCH_AVAILABLE = False
es = Elasticsearch(
    os.getenv("ELASTICSEARCH_HOST", "https://localhost:9200"),
    basic_auth=(os.getenv("ELASTICSEARCH_USERNAME", "elastic"), os.getenv("ELASTICSEARCH_PASSWORD", "")),
    verify_certs=os.getenv("ELASTICSEARCH_VERIFY_CERTS", "false").lower() == "true"
)
INDEX = os.getenv("ELASTICSEARCH_INDEX", "documents")


def _get_document_title(src):
    """
    Get the document title from the database Collection table.
    Combines collection title with the document reference if available.
    """
    collection_identifier = src.get("collection", "")
    ref = src.get("ref", "")

    if not collection_identifier:
        return f"Document {ref}" if ref else "Untitled Document"

    try:
        # Query the database for the collection title
        collection = Collection.query.filter_by(identifier=collection_identifier).first()

        if collection and collection.title:
            # Combine collection title with document reference if available
            if ref:
                return f"{collection.title}, {ref}"
            else:
                # For manuscript-level results (no individual ref), return just the collection title
                return collection.title
        else:
            # Fallback if collection not found in DB
            if ref:
                return f"Document {ref}"
            else:
                return "Unknown Collection"

    except Exception as e:
        # Fallback in case of database error
        if ref:
            return f"Document {ref}"
        else:
            return "Unknown Collection"
