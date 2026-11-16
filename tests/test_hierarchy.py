"""The cached collection hierarchy and the endpoints built on it; the cached facets."""
import pytest

from app.cache import clear_memory_caches
from app.search_routes import catalog_search as cs
from app.search_routes import concordance, hierarchy


@pytest.fixture
def tree(hierarchy):
    """root > parent > child1..3 (collections) plus two manuscripts under child1 and one under parent."""
    from dapytains.app.database import Collection, db
    with hierarchy.app_context():
        def add(identifier, title, parent, resource=True, description=None):
            node = Collection(identifier=identifier, title=title, resource=resource, description=description)
            node.parents.append(Collection.query.filter_by(identifier=parent).one())
            db.session.add(node)
        add("ms-1", "Bibliothèque nationale, Français 1", "child1")
        add("ms-2", "Smith-Lesouëf 19", "child1")
        add("ms-3", "Scandinave 19", "parent")
        db.session.commit()
    clear_memory_caches()
    return hierarchy


def test_descendants_at_any_depth(tree):
    with tree.app_context():
        collections, resources = hierarchy.descendants("parent")
        assert collections == {"child1", "child2", "child3"}
        assert resources == {"ms-1", "ms-2", "ms-3"}
        assert hierarchy.descendants("child1") == (set(), {"ms-1", "ms-2"})
        assert hierarchy.descendants("ms-1") == (set(), set())
        assert hierarchy.descendants("nope") is None


def test_the_snapshot_is_reused_until_it_expires_or_is_cleared(tree):
    from dapytains.app.database import Collection, db
    with tree.app_context():
        assert hierarchy.descendants("child2") == (set(), set())
        node = Collection(identifier="ms-new", title="New", resource=True)
        node.parents.append(Collection.query.filter_by(identifier="child2").one())
        db.session.add(node)
        db.session.commit()
        assert hierarchy.descendants("child2") == (set(), set())          # still the cached view
        clear_memory_caches()
        assert hierarchy.descendants("child2") == (set(), {"ms-new"})


def test_manuscripts_list_is_paginated_and_ignores_accents_and_case(tree):
    client = tree.test_client()
    data = client.get("/manuscripts/list/?q=FRANCAIS").get_json()
    assert [m["identifier"] for m in data["manuscripts"]] == ["ms-1"] and data["total"] == 1
    everything = client.get("/manuscripts/list/?limit=2&page=2").get_json()
    assert everything["total"] == 3 and len(everything["manuscripts"]) == 1
    assert everything["manuscripts"][0]["url"] == "/collection/?id=" + everything["manuscripts"][0]["identifier"]


def test_collections_search(tree):
    client = tree.test_client()
    everything = client.get("/collections/search/").get_json()
    assert {c["identifier"] for c in everything["items"]} == {"parent", "child1", "child2", "child3"}   # not the root, not manuscripts
    by_count = {c["identifier"]: c["total_children"] for c in everything["items"]}
    assert by_count["parent"] == 4 and by_count["child1"] == 2 and by_count["child2"] == 0
    assert [c["identifier"] for c in client.get("/collections/search/?q=CHILD 2").get_json()["items"]] == ["child2"]
    assert client.get("/collections/search/?q=zzz").get_json()["total"] == 0


def test_collections_search_within(tree):
    client = tree.test_client()
    inside = client.get("/collections/search/?within=parent").get_json()
    assert {c["identifier"] for c in inside["items"]} == {"child1", "child2", "child3"}
    assert client.get("/collections/search/?within=child1").get_json()["total"] == 0
    assert client.get("/collections/search/?within=nope").get_json()["total"] == 0


def test_collections_search_is_paginated(tree):
    data = tree.test_client().get("/collections/search/?size=3&page=2").get_json()
    assert data["total"] == 4 and len(data["items"]) == 1


def test_within_scopes_the_search(tree, monkeypatch):
    captured = {}
    monkeypatch.setattr(cs, "search_manuscripts", lambda query, page, size, highlight=None: captured.update(q=query) or {"total": 0, "items": []})
    tree.test_client().get("/manuscripts/?within=child1")
    assert {"terms": {"collection": ["ms-1", "ms-2"]}} in captured["q"]["bool"]["filter"]


# ---- facets ------------------------------------------------------------------

class _ES:
    def __init__(self):
        self.searches = 0

        class Indices:
            def get_settings(self_, index, name=None):
                return {"documents_v5_a": {"settings": {"index": {"uuid": self.uuid}}}}
        self.uuid, self.indices = "u1", Indices()

    def count(self, index, body=None):
        return {"count": 10}

    def search(self, index, body):
        self.searches += 1
        return {"aggregations": {
            "languages": {"buckets": [{"key": "lat", "manuscripts": {"value": 4}}, {"key": "fre", "manuscripts": {"value": 4}}]},
            "min_year": {"value": 1251}, "max_year": {"value": 1601},
        }}


def test_facets_are_cached_and_recomputed_after_a_reindex(client, monkeypatch):
    es = _ES()
    monkeypatch.setattr(cs, "es", es)
    monkeypatch.setattr(concordance, "es", es)

    first = client.get("/manuscripts/facets/").get_json()
    assert first["languages"] == [{"code": "fre", "count": 4}, {"code": "lat", "count": 4}]   # ties sorted by code
    assert (first["min_year"], first["max_year"]) == (1251, 1601)
    client.get("/manuscripts/facets/"); client.get("/manuscripts/facets/")
    assert es.searches == 1

    es.uuid = "u2"                                      # re-indexed
    clear_memory_caches()
    client.get("/manuscripts/facets/")
    assert es.searches == 2
