"""API links follow the public address of the website (PUBLIC_BASE_URL / TRUSTED_PROXY_HOPS)."""
import pytest
from flask import Flask, jsonify, request, url_for

from app.public_url import PublicURLMiddleware, configure_public_url


def make_app():
    app = Flask(__name__)

    @app.route("/collection/")
    def collection():
        return jsonify(root=request.url_root, link=url_for("collection", id="x", _external=True),
                       relative=url_for("collection", id="x"), script_root=request.script_root)

    return app


# ---- the middleware ----------------------------------------------------------

def test_links_use_the_public_host_not_the_internal_one():
    app = make_app()
    configure_public_url(app, "https://comma.example.org/")
    data = app.test_client().get("/collection/", base_url="http://internal:5000").get_json()
    assert data["root"] == "https://comma.example.org/"
    assert data["link"] == "https://comma.example.org/collection/?id=x"


def test_path_prefix_is_used_when_the_proxy_strips_it():
    app = make_app()
    configure_public_url(app, "https://example.org/dts/")
    data = app.test_client().get("/collection/").get_json()
    assert data["link"] == "https://example.org/dts/collection/?id=x"
    assert data["relative"] == "/dts/collection/?id=x"


def test_path_prefix_is_used_when_the_proxy_keeps_it():
    app = make_app()
    configure_public_url(app, "https://example.org/dts/")
    response = app.test_client().get("/dts/collection/")
    assert response.status_code == 200
    assert response.get_json()["link"] == "https://example.org/dts/collection/?id=x"


def test_prefix_lookalikes_are_not_stripped():
    app = make_app()
    configure_public_url(app, "https://example.org/dts")
    assert app.test_client().get("/dtsx/collection/").status_code == 404


@pytest.mark.parametrize("bad", ["example.org", "ftp://example.org/", "https://"])
def test_invalid_public_url_is_rejected_at_startup(bad):
    with pytest.raises(ValueError):
        PublicURLMiddleware(make_app().wsgi_app, bad)


def test_nothing_configured_follows_the_request():
    app = make_app()
    configure_public_url(app, "", 0)
    data = app.test_client().get("/collection/", base_url="http://dev.local:5000").get_json()
    assert data["root"] == "http://dev.local:5000/"


# ---- proxy headers -----------------------------------------------------------

FORWARDED = {"X-Forwarded-Proto": "https", "X-Forwarded-Host": "example.org", "X-Forwarded-Prefix": "/dts"}


def test_trusted_proxy_headers_are_honoured():
    app = make_app()
    configure_public_url(app, "", 1)
    data = app.test_client().get("/collection/", headers=FORWARDED).get_json()
    assert data["link"] == "https://example.org/dts/collection/?id=x"


def test_forwarded_headers_are_ignored_by_default():
    app = make_app()
    configure_public_url(app, "", 0)
    data = app.test_client().get("/collection/", headers=FORWARDED, base_url="http://internal:5000").get_json()
    assert data["root"] == "http://internal:5000/"


def test_forwarded_headers_cannot_override_an_explicit_public_url():
    app = make_app()
    configure_public_url(app, "https://real.example.org/", 1)
    data = app.test_client().get("/collection/", headers=FORWARDED).get_json()
    assert data["root"] == "https://real.example.org/"


# ---- the real DTS API --------------------------------------------------------

@pytest.fixture
def dts(monkeypatch, hierarchy):
    app = hierarchy

    def at(public_url):
        monkeypatch.setattr(app, "wsgi_app", PublicURLMiddleware(app.wsgi_app, public_url))
        return app.test_client()

    return at, app.test_client


def test_entry_point_links_follow_the_public_url(dts):
    at, _ = dts
    data = at("https://example.org/dts/").get("/").get_json()
    assert data["@id"] == "https://example.org/dts/"
    for key in ("collection", "document", "navigation"):
        assert data[key].startswith("https://example.org/dts/" + key + "/")


def test_collection_links_and_pagination_follow_the_public_url(dts):
    at, _ = dts
    data = at("https://example.org/dts/").get("/collection/?id=parent").get_json()
    assert data["collection"].startswith("https://example.org/dts/collection/?id=parent")
    assert data["member"][0]["collection"].startswith("https://example.org/dts/collection/?id=child")
    assert data["view"]["first"].startswith("https://example.org/dts/collection/?id=parent")
    assert data["view"]["last"].startswith("https://example.org/dts/collection/?id=parent")


def test_stats_routes_honour_the_prefix(dts):
    at, _ = dts
    data = at("https://example.org/dts/").get("/collections/list/").get_json()
    assert data["collections"] and all(c["url"].startswith("/dts/collection/?id=") for c in data["collections"])
