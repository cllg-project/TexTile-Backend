"""Zero-downtime re-indexing: versioned indices behind an alias (a fake client enforces Elasticsearch's alias rules)."""
import fnmatch
from datetime import datetime
from types import SimpleNamespace

import pytest
from elasticsearch import ApiError

import es_indexer_textile as ix


class NotFound(Exception):
    pass


class FakeIndices:
    def __init__(self, es):
        self.es = es

    def exists(self, index):
        return index in self.es.docs or index in self.es.aliases

    def exists_alias(self, name):
        return name in self.es.aliases

    def get_alias(self, name):
        return {index: {"aliases": {name: {}}} for index in sorted(self.es.aliases[name])}

    def get(self, index, allow_no_indices=True, ignore_unavailable=True):
        return {name: {} for name in self.es.docs if fnmatch.fnmatch(name, index)}

    def create(self, index, body=None, wait_for_active_shards=None):
        if index in self.es.docs or index in self.es.aliases:
            raise ValueError("index or alias already exists")
        self.es.docs[index] = 0
        self.es.created.append(index)

    def refresh(self, index):
        pass

    def delete(self, index, ignore_unavailable=False):
        if index not in self.es.docs:
            if ignore_unavailable:
                return
            raise NotFound(index)
        del self.es.docs[index]
        for members in self.es.aliases.values():
            members.discard(index)
        self.es.deleted.append(index)

    def update_aliases(self, actions):
        """Atomic: validated against the state, applied only if every action is valid."""
        docs, aliases = dict(self.es.docs), {a: set(m) for a, m in self.es.aliases.items()}
        for action in actions:
            (kind, spec), = action.items()
            if kind == "remove_index":
                del docs[spec["index"]]
                for members in aliases.values():
                    members.discard(spec["index"])
            elif kind == "remove":
                aliases[spec["alias"]].discard(spec["index"])
            elif kind == "add":
                if spec["alias"] in docs:
                    raise ValueError(f"an index named {spec['alias']!r} already exists")
                aliases.setdefault(spec["alias"], set()).add(spec["index"])
        self.es.docs = docs
        self.es.aliases = {a: m for a, m in aliases.items() if m}
        self.es.update_calls += 1


class FakeCluster:
    def __init__(self):
        self.status, self.calls = "yellow", []

    def health(self, index, wait_for_status, timeout):
        self.calls.append((index, wait_for_status, timeout))
        if self.status == "red-408":      # what the real client does: the timed-out wait is an HTTP 408 error
            raise ApiError("timed out", SimpleNamespace(status=408), {"status": "red", "timed_out": True})
        return {"status": self.status, "timed_out": self.status == "red"}


class FakeES:
    def __init__(self, docs=None, aliases=None):
        self.docs = dict(docs or {})                       # concrete index -> number of documents
        self.aliases = {a: set(m) for a, m in (aliases or {}).items()}
        self.created, self.deleted, self.update_calls = [], [], 0
        self.indices = FakeIndices(self)
        self.cluster = FakeCluster()

    def options(self, **kw):
        return self

    def count(self, index):
        names = self.aliases.get(index) or {index}
        return {"count": sum(self.docs[n] for n in names)}

    def bulk_into(self, index, n):
        self.docs[index] += n


@pytest.fixture(autouse=True)
def fake_bulk(monkeypatch):
    """Replace the real bulk helper: every action succeeds and lands in its index (unless told otherwise)."""
    state = {"fail_after": None, "client": None}

    def parallel_bulk(client, actions, chunk_size=100):
        for n, action in enumerate(actions):
            if state["fail_after"] is not None and n >= state["fail_after"]:
                raise RuntimeError("bulk failed")
            client.docs[action["_index"]] += 1 if action["_id"] not in client.seen.setdefault(action["_index"], set()) else 0
            client.seen[action["_index"]].add(action["_id"])
            yield True, {}

    monkeypatch.setattr(ix.helpers, "parallel_bulk", parallel_bulk)

    # Version names have one-second resolution: give each build in a test its own, always newer one
    from datetime import timedelta
    from itertools import count
    ticks = count(1)
    monkeypatch.setattr(
        ix, "versioned_name",
        lambda alias, now=None: f"{alias}_{(datetime(2030, 1, 1) + timedelta(seconds=next(ticks))).strftime(ix.TIMESTAMP)}",
    )
    FakeES.seen = None
    original_init = FakeES.__init__

    def init(self, *a, **k):
        original_init(self, *a, **k)
        self.seen = {}

    monkeypatch.setattr(FakeES, "__init__", init)
    return state


def docs(n, collection="c"):
    return [{"collection": collection, "ref": f"f{i}", "content": "x", "created": ["1200/1300"]} for i in range(n)]


def at(stamp):
    return datetime.strptime(stamp, ix.TIMESTAMP)


# ---- naming ------------------------------------------------------------------

def test_versioned_names_sort_chronologically(monkeypatch):
    monkeypatch.undo()                                          # the real function, not the test counter
    assert ix.versioned_name("documents_v5", at("20260101120000")) == "documents_v5_20260101120000"
    names = [ix.versioned_name("a", at(s)) for s in ("20251231235959", "20260101000000", "20260102000000")]
    assert names == sorted(names)


def test_only_real_versions_of_the_alias_are_recognised():
    assert ix._is_version_of("documents_v5", "documents_v5_20260101120000")
    assert not ix._is_version_of("documents_v5", "documents_v5_backup")
    assert not ix._is_version_of("documents_v5", "documents_v50_20260101120000")
    assert not ix._is_version_of("documents_v5", "other_20260101120000")


# ---- the swap ----------------------------------------------------------------

def test_first_run_creates_the_alias():
    es = FakeES()
    result = ix.build_and_swap(docs(3), "documents_v5", client=es)
    assert es.aliases == {"documents_v5": {result["new_index"]}}
    assert es.count("documents_v5")["count"] == 3 and result["previous"] == []


def test_migrating_a_legacy_plain_index_is_one_atomic_step():
    es = FakeES(docs={"documents_v5": 3})                       # how it used to be deployed
    assert ix.is_legacy_index(es, "documents_v5")
    result = ix.build_and_swap(docs(3), "documents_v5", client=es)
    assert not ix.is_legacy_index(es, "documents_v5")
    assert es.aliases == {"documents_v5": {result["new_index"]}}
    assert "documents_v5" not in es.docs                        # the plain index is gone
    assert es.update_calls == 1                                 # removed and aliased in the same request: no gap


def test_a_second_run_replaces_the_live_index_and_keeps_one_previous():
    es = FakeES()
    first = ix.build_and_swap(docs(3), "documents_v5", client=es)["new_index"]
    es.docs[first] = 3
    second = ix.build_and_swap(docs(4), "documents_v5", client=es)
    assert es.aliases["documents_v5"] == {second["new_index"]}
    assert second["previous"] == [first] and second["pruned"] == []
    assert first in es.docs                                     # kept: rollback target


def test_older_versions_are_pruned_beyond_the_keep_count(monkeypatch):
    es = FakeES(docs={f"documents_v5_2026010{i}000000": 3 for i in range(1, 5)},
                aliases={"documents_v5": {"documents_v5_20260104000000"}})
    pruned = ix.prune_versions(es, "documents_v5", keep=1)
    assert pruned == ["documents_v5_20260101000000", "documents_v5_20260102000000"]
    assert set(es.docs) == {"documents_v5_20260103000000", "documents_v5_20260104000000"}   # live + 1 kept


def test_pruning_never_touches_the_live_index_or_unrelated_indices():
    es = FakeES(docs={"documents_v5_20260101000000": 3, "documents_v5_backup": 1, "other_20260101000000": 1},
                aliases={"documents_v5": {"documents_v5_20260101000000"}})
    assert ix.prune_versions(es, "documents_v5", keep=0) == []
    assert set(es.docs) == {"documents_v5_20260101000000", "documents_v5_backup", "other_20260101000000"}


# ---- failures leave the live index alone ------------------------------------

def test_a_failed_build_leaves_the_live_index_untouched_and_cleans_up(fake_bulk):
    es = FakeES(docs={"documents_v5_20260101000000": 3}, aliases={"documents_v5": {"documents_v5_20260101000000"}})
    fake_bulk["fail_after"] = 2
    with pytest.raises(RuntimeError):
        ix.build_and_swap(docs(5), "documents_v5", client=es)
    assert es.aliases == {"documents_v5": {"documents_v5_20260101000000"}}
    assert es.docs == {"documents_v5_20260101000000": 3}        # the half-built index was deleted
    assert es.update_calls == 0


def test_a_failed_build_leaves_a_legacy_index_untouched(fake_bulk):
    es = FakeES(docs={"documents_v5": 3})
    fake_bulk["fail_after"] = 1
    with pytest.raises(RuntimeError):
        ix.build_and_swap(docs(5), "documents_v5", client=es)
    assert es.docs == {"documents_v5": 3} and es.aliases == {}


def test_an_empty_result_is_refused(monkeypatch):
    es = FakeES(docs={"documents_v5_20260101000000": 3}, aliases={"documents_v5": {"documents_v5_20260101000000"}})
    with pytest.raises(ix.IndexingError, match="empty"):
        ix.build_and_swap([], "documents_v5", client=es)
    assert es.aliases["documents_v5"] == {"documents_v5_20260101000000"}


def test_a_much_smaller_index_is_refused_unless_forced():
    es = FakeES(docs={"documents_v5_20260101000000": 100}, aliases={"documents_v5": {"documents_v5_20260101000000"}})
    with pytest.raises(ix.IndexingError, match="less than 50%"):
        ix.build_and_swap(docs(10), "documents_v5", client=es)
    assert es.aliases["documents_v5"] == {"documents_v5_20260101000000"}
    result = ix.build_and_swap(docs(10), "documents_v5", force=True, client=es)
    assert es.aliases["documents_v5"] == {result["new_index"]}


def test_a_somewhat_smaller_index_is_accepted():
    es = FakeES(docs={"documents_v5_20260101000000": 10}, aliases={"documents_v5": {"documents_v5_20260101000000"}})
    result = ix.build_and_swap(docs(6), "documents_v5", client=es)
    assert es.aliases["documents_v5"] == {result["new_index"]}


def test_documents_sharing_an_id_do_not_trip_the_size_check():
    es = FakeES()
    duplicated = docs(3) + docs(3)                              # same collection/ref: overwritten in the index
    result = ix.build_and_swap(duplicated, "documents_v5", client=es)
    assert result["documents"] == 3


def test_pages_are_numbered_in_reading_order_when_indexed():
    es = FakeES()
    batch = docs(3)
    ix.build_and_swap(batch, "documents_v5", client=es)
    assert [d["position"] for d in batch] == [0, 1, 2] and batch[0]["date_start"] == 1200


# ---- rollback ----------------------------------------------------------------

def test_rollback_points_the_alias_to_the_previous_version():
    es = FakeES(docs={"documents_v5_20260101000000": 3, "documents_v5_20260102000000": 4},
                aliases={"documents_v5": {"documents_v5_20260102000000"}})
    assert ix.rollback(es, "documents_v5") == "documents_v5_20260101000000"
    assert es.aliases == {"documents_v5": {"documents_v5_20260101000000"}}
    assert "documents_v5_20260102000000" in es.docs             # nothing is deleted by a rollback


def test_rollback_without_a_previous_version_says_so():
    es = FakeES(docs={"documents_v5_20260101000000": 3}, aliases={"documents_v5": {"documents_v5_20260101000000"}})
    with pytest.raises(ix.IndexingError, match="no previous version"):
        ix.rollback(es, "documents_v5")
    with pytest.raises(ix.IndexingError, match="does not exist"):
        ix.rollback(FakeES(), "documents_v5")


# ---- the alias rule the whole design relies on -------------------------------

def test_the_fake_enforces_that_an_alias_cannot_shadow_an_index():
    # guards the test double itself: without remove_index the legacy case must fail, as in Elasticsearch
    es = FakeES(docs={"documents_v5": 3, "documents_v5_20260101000000": 3})
    with pytest.raises(ValueError):
        es.indices.update_aliases([{"add": {"index": "documents_v5_20260101000000", "alias": "documents_v5"}}])


# ---- a create that times out may still have made the index -------------------

def test_a_create_that_fails_after_making_the_index_is_cleaned_up():
    es = FakeES(docs={"documents_v5_20260101000000": 3}, aliases={"documents_v5": {"documents_v5_20260101000000"}})
    original = es.indices.create

    def create_then_time_out(index, body=None, wait_for_active_shards=None):
        original(index, body)
        raise TimeoutError("read timed out")                  # the server did create it, the client gave up

    es.indices.create = create_then_time_out
    with pytest.raises(TimeoutError):
        ix.build_and_swap(docs(3), "documents_v5", client=es)
    assert set(es.docs) == {"documents_v5_20260101000000"}       # no half-built index left behind
    assert es.aliases == {"documents_v5": {"documents_v5_20260101000000"}}


def test_a_cleanup_that_fails_does_not_hide_the_original_error():
    es = FakeES()
    es.indices.create = lambda index, body=None, wait_for_active_shards=None: (_ for _ in ()).throw(TimeoutError("read timed out"))
    es.indices.delete = lambda index, ignore_unavailable=False: (_ for _ in ()).throw(ConnectionError("still down"))
    with pytest.raises(TimeoutError):
        ix.build_and_swap(docs(3), "documents_v5", client=es)


# ---- an index that cannot get shards (e.g. disk watermark) fails fast ----------

def test_an_index_without_active_shards_aborts_before_any_bulk_load(monkeypatch):
    es = FakeES(docs={"documents_v5_20260101000000": 3}, aliases={"documents_v5": {"documents_v5_20260101000000"}})
    es.cluster.status = "red"
    bulk_started = []
    monkeypatch.setattr(ix.helpers, "parallel_bulk", lambda *a, **k: bulk_started.append(1) or iter(()))
    with pytest.raises(ix.IndexingError, match="no active shards.*disk"):
        ix.build_and_swap(docs(3), "documents_v5", client=es)
    assert bulk_started == []                                   # nothing was sent to a red index
    assert set(es.docs) == {"documents_v5_20260101000000"}      # the unusable index was removed
    assert es.aliases == {"documents_v5": {"documents_v5_20260101000000"}} and es.update_calls == 0


def test_the_wait_is_bounded():
    es = FakeES()
    ix.build_and_swap(docs(2), "documents_v5", client=es)
    (_index, status, timeout), = es.cluster.calls
    assert status == "yellow" and timeout.endswith("s")


def test_the_real_clients_408_on_a_red_index_becomes_the_clear_message():
    es = FakeES(docs={"documents_v5_20260101000000": 3}, aliases={"documents_v5": {"documents_v5_20260101000000"}})
    es.cluster.status = "red-408"
    with pytest.raises(ix.IndexingError, match="no active shards"):
        ix.build_and_swap(docs(3), "documents_v5", client=es)
    assert set(es.docs) == {"documents_v5_20260101000000"}


def test_main_reports_a_refused_publish_cleanly(tmp_path, monkeypatch, capsys):
    import json
    es = FakeES()
    es.cluster.status = "red-408"
    monkeypatch.setattr(ix, "es", es)
    monkeypatch.setattr(ix, "check_elasticsearch_connection", lambda: True)
    refs = tmp_path / "refs.json"
    refs.write_text(json.dumps(docs(3)))
    assert ix.main([str(refs)]) is False                       # exit status 1, no traceback
    assert "Not published" in capsys.readouterr().out


def test_main_does_not_claim_to_keep_a_version_it_just_removed(tmp_path, monkeypatch, capsys):
    import json
    # after a rollback the live index is older than another version: pruning removes the old live one
    es = FakeES(docs={"documents_v5_20260101000000": 3, "documents_v5_20260102000000": 3},
                aliases={"documents_v5": {"documents_v5_20260101000000"}})
    monkeypatch.setattr(ix, "es", es)
    monkeypatch.setattr(ix, "check_elasticsearch_connection", lambda: True)
    monkeypatch.setattr(ix, "test_search", lambda alias: None)
    refs = tmp_path / "refs.json"
    refs.write_text(json.dumps(docs(3)))
    assert ix.main([str(refs), "--keep", "1"]) is True
    out = capsys.readouterr().out
    assert "removed old versions: documents_v5_20260101000000" in out
    assert "kept for rollback: documents_v5_20260101000000" not in out
    assert "documents_v5_20260102000000" in es.docs                 # the newest other version is the one kept
