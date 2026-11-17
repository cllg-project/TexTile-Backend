"""Streamed statistics: events, caching, limits, and that resources are always released."""
import json
from urllib.parse import urlencode

import pytest

from app import limits
from app.cache import clear_memory_caches
from app.search_routes import concordance as cc
from app.search_routes import term_stats as ts
from test_term_stats import FakeES, O, C, page  # the shared fake Elasticsearch


@pytest.fixture
def es(monkeypatch):
    def install(*pages, **attrs):
        fake = FakeES(list(pages))
        for name, value in attrs.items():
            setattr(fake, name, value)
        monkeypatch.setattr(cc, "es", fake)
        return fake
    return install


def events(client, **params):
    response = client.get("/concordance/stats/?" + urlencode({"q": "x", "format": "ndjson", **params}))
    assert response.mimetype == "application/x-ndjson"
    return [json.loads(line) for line in response.get_data(as_text=True).splitlines()]


def free_slots(limit):
    """How many slots of a ConcurrencyLimit are currently free."""
    taken = 0
    while limit.try_acquire():
        taken += 1
    for _ in range(taken):
        limit.release()
    return taken


TWO_PAGES = [[page("ms-a", f"{O}x{C}"), page("ms-c", f"{O}x{C} {O}x{C}")]]


# ---- the stream --------------------------------------------------------------

def test_stream_starts_and_ends_with_the_right_events(client, es):
    es(*TWO_PAGES)
    out = events(client)
    assert [e["type"] for e in out][0] == "start" and out[-1]["type"] == "done"
    assert out[0]["total_pages"] == 7                                    # known up front, for a progress bar
    done = out[-1]["stats"]
    assert done["occurrences"] == 3 and done["cached"] is False and done["truncated"] is False
    assert done["query"] == "x" and "computed_at" in done


def test_progress_events_carry_the_figures_so_far(monkeypatch, es):
    es(*TWO_PAGES)
    fake_time = iter(range(100))                                        # one tick per clock reading
    out = list(ts.stats_events({"match_all": {}}, "pit-1", merge_adjacent=False, total_pages=2, progress_interval=0, clock=lambda: next(fake_time)))
    progress = [e for e in out if e["type"] == "progress"]
    assert [p["pages"] for p in progress] == [1, 2] and progress[0]["total_pages"] == 2
    assert [p["occurrences"] for p in progress] == [1, 3]
    [form] = progress[0]["stats"]["forms"]
    assert (form["form"], form["occurrences"], form["share"]) == ("x", 1, 100.0)
    assert out[-1]["type"] == "done" and out[-1]["stats"]["occurrences"] == 3


def test_no_progress_events_are_sent_faster_than_the_interval(es):
    es(*TWO_PAGES)
    out = list(ts.stats_events({"match_all": {}}, "pit-1", progress_interval=3600))
    assert [e["type"] for e in out] == ["done"]


def test_errors_during_the_scan_are_reported_in_the_stream_and_release_everything(client, es):
    fake = es(*TWO_PAGES, fail_scan_at=0)
    out = events(client)
    assert out[-1]["type"] == "error" and "went away" in out[-1]["message"]
    assert fake.closed == ["pit-1"]
    assert free_slots(limits.STATS_LIMIT) == limits.STATS_LIMIT.maximum


# ---- the limits --------------------------------------------------------------

def test_stops_at_the_occurrence_limit_and_says_so(client, es):
    es([page("ms-a", " a ".join([f"{O}x{C}"] * 5))], [page("ms-a", f"{O}x{C}")])
    done = events(client, limit=3)[-1]["stats"]
    assert done["truncated"] is True and done["truncated_reason"] == "occurrences"


def test_the_limit_parameter_cannot_exceed_the_server_maximum(client, es, monkeypatch):
    monkeypatch.setattr(ts, "STATS_MAX_OCCURRENCES", 2)
    es([page("ms-a", " a ".join([f"{O}x{C}"] * 5))])
    assert events(client, limit=1000000)[-1]["stats"]["truncated_reason"] == "occurrences"


def test_stops_at_the_time_budget_and_says_so(es):
    es([page("ms-a", f"{O}x{C}"), page("ms-a", f"{O}x{C}"), page("ms-a", f"{O}x{C}")])
    ticks = iter([0, 5, 20, 40, 60])                                    # start, then the clock after each page
    out = list(ts.stats_events({"match_all": {}}, "pit-1", max_seconds=10, clock=lambda: next(ticks), progress_interval=1e9))
    final = out[-1]["stats"]
    assert final["truncated_reason"] == "time" and final["occurrences"] == 2   # counted while inside the budget


def test_a_scan_cut_short_by_time_is_not_cached(client, es, monkeypatch):
    fake = es(TWO_PAGES[0], TWO_PAGES[0])
    monkeypatch.setattr(ts, "STATS_MAX_SECONDS", -1)                     # already over budget after the first page
    assert events(client)[-1]["stats"]["truncated_reason"] == "time"
    events(client)
    assert len(fake.scans) >= 2                                          # the second request scanned again


def test_too_many_heavy_requests_get_a_503_before_anything_is_opened(client, es):
    fake = es(*TWO_PAGES)
    held = [limits.STATS_LIMIT.try_acquire() for _ in range(limits.STATS_LIMIT.maximum)]
    try:
        response = client.get("/concordance/stats/?q=x")
        assert response.status_code == 503 and response.headers["Retry-After"]
        assert fake.scans == [] and fake.closed == []
    finally:
        for _ in held:
            limits.STATS_LIMIT.release()
    assert client.get("/concordance/stats/?q=x").status_code == 200     # and it recovers


def test_the_slot_is_released_after_every_kind_of_answer(client, es):
    es(*TWO_PAGES)
    for fmt in ("json", "csv", "ndjson"):
        client.get("/concordance/stats/?" + urlencode({"q": "x", "format": fmt, "refresh": 1})).get_data()
    assert free_slots(limits.STATS_LIMIT) == limits.STATS_LIMIT.maximum


def test_exports_are_limited_too(client, es):
    fake = es(*TWO_PAGES)
    held = [limits.EXPORT_LIMIT.try_acquire() for _ in range(limits.EXPORT_LIMIT.maximum)]
    try:
        response = client.get("/concordance/?q=x")
        assert response.status_code == 503 and fake.scans == []
    finally:
        for _ in held:
            limits.EXPORT_LIMIT.release()
    client.get("/concordance/?q=x").get_data()
    assert free_slots(limits.EXPORT_LIMIT) == limits.EXPORT_LIMIT.maximum


def test_unknown_format_is_a_400(client, es):
    es()
    assert client.get("/concordance/stats/?q=x&format=xml").status_code == 400


# ---- the cache ---------------------------------------------------------------

def test_a_second_identical_request_is_served_from_the_cache(client, es):
    fake = es(*TWO_PAGES)
    first = events(client)
    scans = len(fake.scans)
    second = events(client)
    assert len(fake.scans) == scans                                      # nothing scanned
    assert [e["type"] for e in second] == ["done"]                       # a cached answer is a single event
    assert second[0]["stats"]["cached"] is True
    assert second[0]["stats"]["occurrences"] == first[-1]["stats"]["occurrences"]


def test_json_and_csv_share_the_cache_with_the_stream(client, es):
    fake = es(*TWO_PAGES)
    events(client)
    scans = len(fake.scans)
    assert client.get("/concordance/stats/?q=x").get_json()["cached"] is True
    assert client.get("/concordance/stats/?q=x&format=csv").status_code == 200
    assert len(fake.scans) == scans


def test_refresh_recomputes(client, es):
    fake = es(TWO_PAGES[0], TWO_PAGES[0])
    events(client)
    scans = len(fake.scans)
    assert events(client, refresh=1)[-1]["stats"]["cached"] is False
    assert len(fake.scans) > scans


@pytest.mark.parametrize("change", [{"q": "y"}, {"mode": "partial"}, {"language": "lat"}, {"limit": 5}])
def test_a_different_search_is_not_served_from_another_ones_cache(client, es, change):
    fake = es(TWO_PAGES[0], TWO_PAGES[0], TWO_PAGES[0])
    events(client)
    scans = len(fake.scans)
    assert events(client, **change)[-1]["stats"]["cached"] is False
    assert len(fake.scans) > scans


def test_reindexing_invalidates_the_cache(client, es):
    fake = es(TWO_PAGES[0], TWO_PAGES[0])
    events(client)
    fake.indices.get_settings = lambda index, name=None: {"documents_v5_new": {"settings": {"index": {"uuid": "uuid-2"}}}}
    clear_memory_caches()                                                # the generation is looked up every few seconds
    assert events(client)[-1]["stats"]["cached"] is False


def test_the_scope_figures_are_reused_between_searches(client, es):
    fake = es(TWO_PAGES[0], TWO_PAGES[0])
    events(client, q="x")
    events(client, q="y")
    assert len(fake.aggs) == 1                                           # same scope, same index: aggregated once


# ---- the 100,000 hits maximum ------------------------------------------------

def test_the_default_maximum_is_100000_occurrences():
    assert ts.STATS_MAX_OCCURRENCES == 100000


def test_the_statistics_maximum_does_not_depend_on_the_csv_export_maximum(monkeypatch):
    # they are separate settings: raising or lowering one must not move the other
    monkeypatch.setattr(cc, "MAX_ROWS", 5)
    assert ts.STATS_MAX_OCCURRENCES == 100000


def test_the_count_is_exact_when_the_limit_falls_inside_a_page(client, es):
    es([page("ms-a", " a ".join([f"{O}x{C}"] * 5))], [page("ms-a", f"{O}x{C}")])
    done = events(client, limit=3)[-1]["stats"]
    assert done["occurrences"] == 3                                     # not 5: the page is cut at the limit
    assert done["truncated"] is True and done["truncated_reason"] == "occurrences" and done["limit"] == 3
    [form] = done["forms"]
    assert (form["form"], form["occurrences"], form["share"]) == ("x", 3, 100.0)
    assert done["by_manuscript"][0]["occurrences"] == 3


def test_a_result_below_the_maximum_is_not_marked_truncated(client, es):
    es(*TWO_PAGES)
    done = events(client, limit=100)[-1]["stats"]
    assert done["occurrences"] == 3 and done["truncated"] is False and done["truncated_reason"] is None


def test_the_server_maximum_applies_to_a_huge_request(client, es, monkeypatch):
    monkeypatch.setattr(ts, "STATS_MAX_OCCURRENCES", 2)
    es([page("ms-a", " a ".join([f"{O}x{C}"] * 9))])
    done = events(client, limit=10**9)[-1]["stats"]
    assert done["occurrences"] == 2 and done["limit"] == 2


# ---- a scope of any size: no silent cut-off ----------------------------------

def test_every_manuscript_of_a_large_scope_is_counted(client, es):
    # more manuscripts than one aggregation page (and than the 10,000 a fixed terms size would have allowed)
    big = {f"ms-{i:05d}": (1000, "lat", 1200, 1300) for i in range(2500)}
    fake = es(TWO_PAGES[0], scope=big, scope_page_size=1000)
    done = events(client)[-1]["stats"]
    assert done["manuscripts_in_scope"] == 2500
    assert done["tokens_in_scope"] == 2500 * 1000                      # the figures the relative frequency divides by
    assert len(fake.aggs) == 3                                         # read in pages of 1000: 1000 + 1000 + 500
    assert all("after" not in a["aggs"]["m"]["composite"] for a in fake.aggs[:1])
    assert fake.aggs[1]["aggs"]["m"]["composite"]["after"] == {"collection": "ms-00999"}


def test_the_scope_is_read_in_pages_of_a_bounded_size(client, es):
    fake = es(TWO_PAGES[0])
    events(client)
    assert fake.aggs[0]["aggs"]["m"]["composite"]["size"] == ts.SCOPE_PAGE <= 1000


def test_an_empty_scope_ends_the_paging(client, es):
    es(TWO_PAGES[0], scope={})
    assert events(client)[-1]["stats"]["manuscripts_in_scope"] == 0
