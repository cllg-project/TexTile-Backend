"""The DTS answers: JSON-LD context and version of dapytains 1.0, pagination, errors."""
import pytest
from dapytains.app.app import CONTEXT_URL, DTS_VERSION

from app import paginated_collection


@pytest.fixture
def client(hierarchy):
    return hierarchy.test_client()


def test_library_declares_dts_1_0():
    # Guards against pinning a dapytains that speaks another version of the specification
    assert CONTEXT_URL == "https://dtsapi.org/context/v1.0.json"
    assert DTS_VERSION == "1.0"


@pytest.mark.parametrize("url", ["/", "/collection/?id=parent", "/collection/?id=child1&nav=parents"])
def test_every_answer_declares_the_library_context_and_version(client, url):
    data = client.get(url).get_json()
    assert data["@context"] == CONTEXT_URL
    assert data["dtsVersion"] == DTS_VERSION
    assert "1-alpha" not in str(data)


def test_entry_point_advertises_the_page_parameter(client):
    data = client.get("/").get_json()
    assert data["@type"] == "EntryPoint"
    assert data["collection"].endswith("/collection/{?id}{&page,nav}")


def test_member_links_stay_templated_for_page_and_nav(client):
    data = client.get("/collection/?id=parent").get_json()
    assert data["collection"].endswith("/collection/?id=parent{&page,nav}")
    assert all(m["collection"].endswith("{&page,nav}") for m in data["member"])


def test_our_child_count_is_an_extension_not_a_top_level_term(hierarchy):
    # The DTS 1.0 context does not define `nb_children`; custom properties belong in `extensions`
    from dapytains.app.database import Collection, db
    with hierarchy.app_context():
        Collection.query.filter_by(identifier="parent").one().nb_children = 3
        db.session.commit()

    parent = hierarchy.test_client().get("/collection/?id=root").get_json()["member"][0]
    assert "nb_children" not in parent
    assert parent["extensions"] == {"nb_children": [3]}


def test_no_extensions_when_the_count_is_unknown(client):
    parent = client.get("/collection/?id=root").get_json()["member"][0]
    assert "extensions" not in parent and "nb_children" not in parent


def test_collection_answer_basics(client):
    data = client.get("/collection/?id=parent").get_json()
    assert data["@type"] == "Collection" and data["@id"] == "parent"
    assert data["totalChildren"] == 3 and data["totalParents"] == 1
    assert sorted(m["@id"] for m in data["member"]) == ["child1", "child2", "child3"]


def test_root_collection_when_no_id_is_given(client):
    assert client.get("/collection/").get_json()["@id"] == "root"


def test_parents_navigation(client):
    data = client.get("/collection/?id=child1&nav=parents").get_json()
    assert [m["@id"] for m in data["member"]] == ["parent"]


# ---- pagination --------------------------------------------------------------

def test_pagination_links_are_absolute_and_typed(client, monkeypatch):
    monkeypatch.setattr(paginated_collection, "PAGINATION_SIZE", 1)
    view = client.get("/collection/?id=parent&page=2").get_json()["view"]
    assert view["@type"] == "Pagination"
    for key in ("@id", "first", "previous", "next", "last"):
        assert view[key].startswith("http://localhost/collection/?id=parent"), key
    assert "page=2" in view["@id"]
    assert "page=1" in view["first"] and "page=1" in view["previous"]
    assert "page=3" in view["next"] and "page=3" in view["last"]


def test_first_and_last_pages_have_no_dangling_links(client, monkeypatch):
    monkeypatch.setattr(paginated_collection, "PAGINATION_SIZE", 1)
    first = client.get("/collection/?id=parent&page=1").get_json()["view"]
    last = client.get("/collection/?id=parent&page=3").get_json()["view"]
    assert "previous" not in first and "next" in first
    assert "next" not in last and "previous" in last


def test_pages_partition_the_members(client, monkeypatch):
    monkeypatch.setattr(paginated_collection, "PAGINATION_SIZE", 2)
    ids = []
    for page in (1, 2):
        members = client.get(f"/collection/?id=parent&page={page}").get_json()["member"]
        ids += [m["@id"] for m in members]
    assert sorted(ids) == ["child1", "child2", "child3"]


def test_sorting_parameters_are_kept_in_pagination_links(client):
    view = client.get("/collection/?id=parent&sort_by=title&sort_order=desc").get_json()["view"]
    assert "sort_by=title" in view["first"] and "sort_order=desc" in view["first"]


# ---- errors ------------------------------------------------------------------

def test_page_beyond_the_last_is_a_404(client):
    response = client.get("/collection/?id=parent&page=9")
    assert response.status_code == 404
    assert "last page is 1" in response.get_json()["message"]


def test_unknown_collection_is_a_404_message(client):
    response = client.get("/collection/?id=nope")
    assert response.status_code == 404 and "message" in response.get_json()


def test_bad_nav_is_a_400_message(client):
    response = client.get("/collection/?id=parent&nav=sideways")
    assert response.status_code == 400 and "message" in response.get_json()
