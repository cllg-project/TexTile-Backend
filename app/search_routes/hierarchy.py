"""
The collection hierarchy, read once from the database and kept for a short while.

Everything that needs to walk it (the `within=` scope of the search, the concordance and the
statistics, the collection and manuscript lists) used to query the whole table on every
request. They now share one snapshot of plain data, rebuilt at most every
HIERARCHY_CACHE_SECONDS (default 60) per worker, so a catalog ingested by the CLI shows up
within that time.
"""
import os
from typing import Dict, List, NamedTuple, Optional, Set, Tuple

from dapytains.app.database import Collection, db, parent_child_association

from ..cache import ttl_cache
from .text import fold

HIERARCHY_CACHE_SECONDS = float(os.getenv("HIERARCHY_CACHE_SECONDS", "60"))


class Node(NamedTuple):
    id: int
    identifier: str
    title: str
    description: Optional[str]
    resource: bool
    nb_children: Optional[int]
    search_text: str          # title and description, folded, for substring search
    child_count: int


class Snapshot(NamedTuple):
    nodes: Dict[int, Node]
    by_identifier: Dict[str, int]
    children: Dict[int, List[int]]
    has_parent: Set[int]


@ttl_cache(HIERARCHY_CACHE_SECONDS)
def snapshot() -> Snapshot:
    children: Dict[int, List[int]] = {}
    has_parent: Set[int] = set()
    for parent_id, child_id in db.session.query(
        parent_child_association.c.parent_id, parent_child_association.c.child_id
    ):
        children.setdefault(parent_id, []).append(child_id)
        has_parent.add(child_id)

    nodes: Dict[int, Node] = {}
    for row in db.session.query(
        Collection.id, Collection.identifier, Collection.title, Collection.description,
        Collection.resource, Collection.nb_children,
    ):
        nodes[row.id] = Node(
            id=row.id, identifier=row.identifier, title=row.title or "", description=row.description,
            resource=bool(row.resource), nb_children=row.nb_children,
            search_text=fold(f"{row.title or ''} {row.description or ''}"),
            child_count=len(children.get(row.id, [])),
        )
    return Snapshot(nodes, {n.identifier: n.id for n in nodes.values()}, children, has_parent)


def descendants(identifier: str) -> Optional[Tuple[Set[str], Set[str]]]:
    """
    Identifiers of everything below a collection, at any depth.

    Returns (collection_identifiers, resource_identifiers), or None when the
    identifier is unknown. The collection itself is not included.
    """
    snap = snapshot()
    root = snap.by_identifier.get(identifier)
    if root is None:
        return None

    seen: Set[int] = {root}
    stack = [root]
    collections: Set[str] = set()
    resources: Set[str] = set()
    while stack:
        for child in snap.children.get(stack.pop(), []):
            if child in seen or child not in snap.nodes:
                continue
            seen.add(child)
            node = snap.nodes[child]
            (resources if node.resource else collections).add(node.identifier)
            stack.append(child)
    return collections, resources


def manuscripts() -> List[Node]:
    """All manuscripts (resources), by title."""
    return sorted((n for n in snapshot().nodes.values() if n.resource), key=lambda n: n.title)


def collections() -> List[Node]:
    """All collections below the root, by title."""
    snap = snapshot()
    return sorted((n for n in snap.nodes.values() if not n.resource and n.id in snap.has_parent), key=lambda n: n.title)
