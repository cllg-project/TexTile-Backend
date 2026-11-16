"""migrate.py: the pre-flight and verification logic of the upgrade."""
import json
import sqlite3
from types import SimpleNamespace

import pytest

import es_indexer_textile as ix
import migrate as mg


# ---- mapping -----------------------------------------------------------------

def test_the_new_mapping_is_recognised_as_current():
    assert mg.mapping_is_current(ix.INDEX_SETTINGS["mappings"]["properties"]) == []


def test_the_previous_mapping_reports_what_is_missing():
    old = {"title": {"type": "text", "analyzer": "ngram_analyzer"}, "language": {"type": "keyword"},
           "publisher": {"type": "keyword"}, "identifier": {"type": "keyword"}, "created": {"type": "text"}}
    missing = mg.mapping_is_current(old)
    assert {"date_start", "date_end", "position", "title.ngram", "language.text", "publisher.text", "identifier.text"} <= set(missing)


# ---- index state -------------------------------------------------------------

class FakeIndices:
    def __init__(self, plain=(), aliases=None):
        self.plain, self.aliases = set(plain), aliases or {}

    def exists_alias(self, name):
        return name in self.aliases

    def get_alias(self, name):
        return {i: {} for i in self.aliases[name]}

    def exists(self, index):
        return index in self.plain or index in self.aliases


@pytest.mark.parametrize("indices,expected", [
    (FakeIndices(plain={"documents_v5"}), "plain"),                                  # the previous release
    (FakeIndices(aliases={"documents_v5": ["documents_v5_20261116"]}), "alias"),     # this release
    (FakeIndices(), "missing"),
])
def test_index_state(indices, expected):
    assert mg.index_state(SimpleNamespace(indices=indices), "documents_v5")["kind"] == expected


# ---- disk --------------------------------------------------------------------

GIB = 2 ** 30


def test_disk_room_ok_when_a_second_copy_fits():
    assert mg.disk_room(1000 * GIB, 500 * GIB, 10 * GIB).level == mg.OK


def test_disk_room_fails_when_the_second_copy_crosses_the_watermark():
    result = mg.disk_room(1000 * GIB, 120 * GIB, 50 * GIB)             # 88% used, 93% after the copy
    assert result.level == mg.FAIL and "93%" in result.detail


def test_disk_room_fails_when_already_past_the_watermark():
    result = mg.disk_room(1000 * GIB, 80 * GIB, 1 * GIB)               # the situation of a nearly full disk
    assert result.level == mg.FAIL and "already past" in result.detail


# ---- environment -------------------------------------------------------------

@pytest.fixture
def env(monkeypatch, tmp_path):
    for name in ("ELASTICSEARCH_HOST", "PUBLIC_BASE_URL", "TRUSTED_PROXY_HOPS", "ELASTICSEARCH_VERIFY_CERTS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "app.db"))
    return monkeypatch


def by_name(results):
    return {r.name: r for r in results}


def test_a_missing_elasticsearch_host_fails(env):
    assert by_name(mg.environment_checks(None))["ELASTICSEARCH_HOST"].level == mg.FAIL


def test_a_reverse_proxy_deployment_is_warned_about_links(env):
    env.setenv("ELASTICSEARCH_HOST", "http://es:9200")
    assert by_name(mg.environment_checks(None))["PUBLIC_BASE_URL"].level == mg.WARN
    env.setenv("PUBLIC_BASE_URL", "https://example.org/dts/")
    assert by_name(mg.environment_checks(None))["public URL"].level == mg.OK


def test_unverified_tls_is_flagged(env):
    env.setenv("ELASTICSEARCH_HOST", "https://es:9200")
    assert by_name(mg.environment_checks(None))["ELASTICSEARCH_VERIFY_CERTS"].level == mg.WARN


def make_db(path, rows):
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE collections (id INTEGER, resource BOOLEAN, nb_children INTEGER)")
        db.executemany("INSERT INTO collections VALUES (?, ?, ?)", rows)


def test_the_database_is_only_read_and_empty_child_counts_are_reported(env, tmp_path):
    env.setenv("ELASTICSEARCH_HOST", "http://es:9200")
    make_db(tmp_path / "app.db", [(1, 0, 2), (2, 0, None), (3, 1, None)])
    db = by_name(mg.environment_checks(None))["database"]
    assert db.level == mg.WARN and "3 collections, 1 manuscripts" in db.detail and "empty for 2" in db.detail


def test_a_complete_database_passes(env, tmp_path):
    env.setenv("ELASTICSEARCH_HOST", "http://es:9200")
    make_db(tmp_path / "app.db", [(1, 0, 2), (2, 1, 0)])
    assert by_name(mg.environment_checks(None))["database"].level == mg.OK


def test_the_refs_file_is_validated(env, tmp_path):
    env.setenv("ELASTICSEARCH_HOST", "http://es:9200")
    good, bad = tmp_path / "good.json", tmp_path / "bad.json"
    good.write_text(json.dumps([[{"collection": "c", "ref": "f1", "content": "x"}]]))
    bad.write_text(json.dumps([{"collection": "c"}]))
    assert by_name(mg.environment_checks(str(good)))["refs file"].level == mg.OK
    result = by_name(mg.environment_checks(str(bad)))["refs file"]
    assert result.level == mg.FAIL and "content" in result.detail and "ref" in result.detail
    assert by_name(mg.environment_checks(str(tmp_path / "nope.json")))["refs file"].level == mg.FAIL


def test_report_fails_only_on_failures(capsys):
    assert mg.report([mg.Result(mg.OK, "a"), mg.Result(mg.WARN, "b", "careful")]) is True
    assert mg.report([mg.Result(mg.OK, "a"), mg.Result(mg.FAIL, "b")]) is False
    assert "1 failed" in capsys.readouterr().out


# ---- verifying a running API -------------------------------------------------

class StubApi:
    """Canned answers, for a deployment of the new release (or of the previous one)."""

    def __init__(self, old=False):
        self.old, self.origin = old, None

    def json(self, path, **params):
        if path == "/":
            collection = "http://x/collection/{?id}{&nav}" if self.old else "http://x/collection/{?id}{&page,nav}"
            ctx = "https://distributed-text-services.github.io/specifications/context/1-alpha1.json" if self.old else mg.EXPECTED_CONTEXT
            return 200, {"@context": ctx, "dtsVersion": "1-alpha" if self.old else "1.0", "@id": "http://x/", "collection": collection}
        if path == "/collection/" and params.get("page"):
            return 404, {"message": "Page 99999 does not exist"}
        if path == "/collection/":
            member = {"@id": "m", "nb_children": 2} if self.old else {"@id": "m", "extensions": {"nb_children": [2]}}
            return 200, {"member": [member], "totalChildren": 1,
                         "view": {"@type": "Pagination", "first": "http://x/collection/?page=1"}}
        if path == "/manuscripts/facets/":
            return 200, {"languages": [{"code": "lat", "count": 4}], "min_year": 1251.0, "max_year": 1601.0}
        if path == "/manuscripts/":
            if params.get("q") == "lesouef":
                return 200, {"total": 4, "items": []}
            return 200, {"total": 4, "items": [{"title": "Smith-Lesouëf 19"}]}
        if path == "/collections/search/":
            return 200, {"total": 3}
        if path == "/search/":
            return 200, {"total": 9}
        if path == "/concordance/":
            return 200, {"rows": [{"left": "a", "keyword": "b", "right": "c", "ref": "f1"}], "total_pages": 9}
        raise AssertionError(path)

    def request(self, path, **params):
        return SimpleNamespace(read=lambda: "﻿header\r\nrow\r\n".encode(), headers={"Access-Control-Allow-Origin": "*"})

    def ndjson(self, path, **params):
        self.calls = getattr(self, "calls", 0) + 1
        return [{"type": "start"}, {"type": "done", "stats": {"occurrences": 5, "per10k": 1.2, "limit": 100000, "cached": self.calls > 1}}]


def test_a_correct_new_deployment_passes_every_check():
    results = mg.api_checks(StubApi(), "et", None)
    assert [r for r in results if r.level != mg.OK] == []
    assert len(results) == 12


def test_the_previous_release_fails_where_the_api_changed():
    failed = {r.name: r.detail for r in mg.api_checks(StubApi(old=True), "et", None) if r.level == mg.FAIL}
    assert "DTS entry point (1.0, with page)" in failed and "1-alpha" in failed["DTS entry point (1.0, with page)"]
    assert "child count in extensions" in failed and "top-level" in failed["child count in extensions"]


def test_the_expected_public_url_is_checked():
    assert [r for r in mg.api_checks(StubApi(), "et", "http://x") if r.level == mg.FAIL] == []
    failed = [r for r in mg.api_checks(StubApi(), "et", "https://example.org/dts") if r.level == mg.FAIL]
    assert [r.name for r in failed] == ["DTS entry point (1.0, with page)"] and "example.org" in failed[0].detail


def test_a_check_that_raises_is_a_failure_not_a_crash():
    class Broken(StubApi):
        def json(self, path, **params):
            raise ConnectionRefusedError("connection refused")
    results = mg.api_checks(Broken(), "et", None)
    assert all(r.level == mg.FAIL for r in results[:3]) and "ConnectionRefusedError" in results[0].detail


def test_cors_is_checked_when_an_origin_is_given():
    api = StubApi()
    api.origin = "https://example.org"
    assert mg.api_checks(api, "et", None)[-1].name == "CORS for https://example.org"


@pytest.mark.parametrize("size,expected", [(0, "0 B"), (900, "900 B"), (2048, "2 KiB"), (35 * 2**20, "35.0 MiB"), (3 * 2**30, "3.0 GiB")])
def test_human_sizes(size, expected):
    assert mg.human(size) == expected


def test_a_small_index_is_not_shown_as_zero_gib():
    assert "35.0 MiB" in mg.disk_room(1000 * GIB, 500 * GIB, 35 * 2**20).detail
