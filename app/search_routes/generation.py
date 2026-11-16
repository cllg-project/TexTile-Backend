from ..cache import ttl_cache
from . import concordance
from .config import INDEX


@ttl_cache(5)
def index_generation() -> str:
    """
    A token that changes whenever the searchable data does: the identity of the concrete index
    behind the alias (a new one after every re-index) plus its document count (incremental indexing).
    Cached results are keyed by it. Looked up at most every few seconds.
    """
    settings = concordance.es.indices.get_settings(index=INDEX, name="index.uuid")
    identities = sorted(f"{name}:{value['settings']['index']['uuid']}" for name, value in settings.items())
    count = concordance.es.count(index=INDEX)["count"]
    return f"{'|'.join(identities)}#{count}"
