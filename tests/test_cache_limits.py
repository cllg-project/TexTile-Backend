"""The disk cache, the in-process cache, the concurrency limit and the index generation."""
import os
import time

import pytest

from app import cache as cache_module
from app.cache import DiskCache, clear_memory_caches, ttl_cache
from app.limits import ConcurrencyLimit, RETRY_AFTER_SECONDS, busy_response


# ---- DiskCache ---------------------------------------------------------------

@pytest.fixture
def disk(tmp_path):
    return DiskCache(str(tmp_path / "c"), ttl=100, max_entries=3)


def test_roundtrip_and_miss(disk):
    assert disk.get("stats", {"q": "a"}) is None
    disk.set("stats", {"q": "a"}, {"n": 1, "forms": ["dñs", "ꝑ"]})
    assert disk.get("stats", {"q": "a"}) == {"n": 1, "forms": ["dñs", "ꝑ"]}
    assert disk.get("stats", {"q": "b"}) is None


def test_key_order_does_not_matter_but_values_and_namespaces_do(disk):
    disk.set("stats", {"a": 1, "b": 2}, "x")
    assert disk.get("stats", {"b": 2, "a": 1}) == "x"
    assert disk.get("stats", {"a": 1, "b": 3}) is None
    assert disk.get("other", {"a": 1, "b": 2}) is None


def test_entries_expire(disk):
    disk.set("s", "k", "v")
    path = disk._path("s", "k")
    os.utime(path, (time.time() - 101, time.time() - 101))
    assert disk.get("s", "k") is None
    assert not os.path.exists(path)


def test_oldest_entries_are_evicted(disk):
    base = time.time() - 50                            # old, but inside the 100 s lifetime
    for i in range(3):
        disk.set("s", i, i)
        os.utime(disk._path("s", i), (base + i, base + i))
    disk.set("s", "new", "new")
    assert disk.get("s", 0) is None                  # the oldest went
    assert [disk.get("s", i) for i in (1, 2)] == [1, 2] and disk.get("s", "new") == "new"


def test_no_temporary_files_are_left_behind(disk):
    disk.set("s", "k", "v")
    assert [f for f in os.listdir(disk.directory) if f.endswith(".tmp")] == []


def test_corrupted_entry_is_a_miss(disk):
    disk.set("s", "k", "v")
    with open(disk._path("s", "k"), "w") as handle:
        handle.write("{not json")
    assert disk.get("s", "k") is None


def test_a_cache_that_cannot_write_does_not_break_the_caller(tmp_path):
    blocked = tmp_path / "file"
    blocked.write_text("a file, not a directory")
    broken = DiskCache(str(blocked / "sub"))
    broken.set("s", "k", "v")                         # must not raise
    assert broken.get("s", "k") is None


def test_clear(disk):
    disk.set("s", "k", "v")
    disk.clear()
    assert disk.get("s", "k") is None


# ---- ttl_cache ---------------------------------------------------------------

def test_ttl_cache_memoises_until_it_expires(monkeypatch):
    calls, now = [], [100.0]
    monkeypatch.setattr(cache_module.time, "monotonic", lambda: now[0])

    @ttl_cache(10)
    def compute(x):
        calls.append(x)
        return x * 2

    assert compute(2) == 4 and compute(2) == 4 and calls == [2]
    assert compute(3) == 6 and calls == [2, 3]        # arguments are part of the key
    now[0] += 11
    assert compute(2) == 4 and calls == [2, 3, 2]     # expired


def test_clear_memory_caches_empties_every_registered_cache():
    calls = []

    @ttl_cache(1000)
    def compute():
        calls.append(1)
        return 1

    compute(); compute()
    assert len(calls) == 1
    clear_memory_caches()
    compute()
    assert len(calls) == 2


# ---- concurrency limit -------------------------------------------------------

def test_the_limit_refuses_beyond_its_maximum_and_frees_slots():
    limit = ConcurrencyLimit("things", 2)
    assert limit.try_acquire() and limit.try_acquire()
    assert limit.try_acquire() is False
    limit.release()
    assert limit.try_acquire() is True


def test_busy_response_is_a_503_with_retry_after(client):
    from app import app
    with app.test_request_context():
        response = busy_response(ConcurrencyLimit("statistics", 1))
    assert response.status_code == 503
    assert response.headers["Retry-After"] == str(RETRY_AFTER_SECONDS)
    assert "statistics" in response.get_json()["error"]


# ---- index generation --------------------------------------------------------

class _Indices:
    def __init__(self):
        self.uuid, self.calls = "uuid-1", 0

    def get_settings(self, index, name=None):
        self.calls += 1
        return {"documents_v5_a": {"settings": {"index": {"uuid": self.uuid}}}}


class _ES:
    def __init__(self):
        self.indices, self.docs = _Indices(), 100

    def count(self, index, body=None):
        return {"count": self.docs}


def test_generation_changes_with_the_index_and_its_size(monkeypatch):
    from app.search_routes import concordance, generation
    es = _ES()
    monkeypatch.setattr(concordance, "es", es)
    first = generation.index_generation()
    assert "documents_v5_a:uuid-1" in first and first.endswith("#100")

    es.indices.uuid = "uuid-2"                         # re-indexed: another concrete index behind the alias
    clear_memory_caches()
    assert generation.index_generation() != first
    es.docs = 101                                      # documents added to the same index
    clear_memory_caches()
    assert generation.index_generation().endswith("#101")


def test_generation_is_not_looked_up_on_every_call(monkeypatch):
    from app.search_routes import concordance, generation
    es = _ES()
    monkeypatch.setattr(concordance, "es", es)
    generation.index_generation(); generation.index_generation(); generation.index_generation()
    assert es.indices.calls == 1
