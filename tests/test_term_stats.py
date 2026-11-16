"""Term statistics and the JSON concordance view (Elasticsearch is faked)."""
import csv
import io

import pytest

from app.search_routes import concordance as cc
from app.search_routes import term_stats as ts

O, C = cc.MARK_OPEN, cc.MARK_CLOSE


# ---- pure helpers ------------------------------------------------------------

@pytest.mark.parametrize("q,expected", [
    ('"la bouche"', True), ('deus,"la bouche"', True), ('"la bou*"', True),
    ('"deus"', False), ("deus,deum", False), ("la bouche", False), ('""', False), ("reg*", False),
])
def test_has_phrase(q, expected):
    assert cc.has_phrase(q) is expected


def test_neighbouring_hits_stay_separate_unless_it_is_a_phrase():
    text = f"{O}deus{C} {O}deum{C}"
    assert [o.keyword for o in cc.extract_occurrences(text, 0, merge_adjacent=False)] == ["deus", "deum"]
    assert [o.keyword for o in cc.extract_occurrences(text, 0, merge_adjacent=True)] == ["deus deum"]


@pytest.mark.parametrize("start,end,expected", [
    (1275, 1300, 13), (1401, 1500, 15), (1502, 1601, 16), (1300, 1350, 14),
    (1200, 1200, 12), (100, 100, 1), (101, 101, 2), (1250, None, 13), (None, 1250, 13), (None, None, None),
])
def test_century_of_the_middle_of_the_range(start, end, expected):
    assert ts.century(start, end) == expected


def test_per_10k():
    assert ts.per_10k(5, 10000) == 5.0
    assert ts.per_10k(1, 3) == 3333.33
    assert ts.per_10k(5, None) is None and ts.per_10k(5, 0) is None


# ---- a small fake corpus -----------------------------------------------------

SCOPE = {  # collection -> (tokens, language, date_start, date_end)
    "ms-a": (10000, "lat", 1275, 1300),     # 13th century
    "ms-b": (30000, "lat", 1301, 1350),     # 14th century (nothing found in it)
    "ms-c": (None, "fre", 1401, 1450),      # 15th century, token count unknown
    "ms-d": (5000, "und", None, None),      # undated
}


def page(collection, content):
    """A matching page. Elasticsearch returns the whole field as ONE highlighted fragment."""
    return {"_source": {"collection": collection, "title": f"Title {collection}"},
            "highlight": {"content": [content]}, "sort": [collection, 0, 0]}


class _Indices:
    def get_settings(self, index, name=None):
        return {"documents_v5_20260101": {"settings": {"index": {"uuid": "uuid-1"}}}}


class FakeES:
    def __init__(self, pages):
        self.pages, self.scans, self.aggs, self.closed = pages, [], [], []
        self.indices = _Indices()

    def open_point_in_time(self, index, keep_alive):
        return {"id": "pit-1"}

    def close_point_in_time(self, id):
        self.closed.append(id)

    def search(self, body, index=None):
        if "aggs" in body:                                            # the scope aggregation (composite, paged)
            self.aggs.append(body)
            composite = body["aggs"]["m"]["composite"]
            entries = list(getattr(self, "scope", SCOPE).items())
            keys = [key for key, _ in entries]
            after = composite.get("after", {}).get("collection")
            start = keys.index(after) + 1 if after else 0
            chunk = entries[start:start + min(composite["size"], getattr(self, "scope_page_size", 10**9))]
            buckets = [{
                "key": {"collection": key}, "doc_count": 3,
                "tokens": {"value": tokens},
                "sample": {"hits": {"hits": [{"_source": {
                    "title": f"Title {key}", "language": [lang], "date_start": start_year, "date_end": end_year}}]}},
            } for key, (tokens, lang, start_year, end_year) in chunk]
            agg = {"buckets": buckets}
            if start + len(chunk) < len(entries):
                agg["after_key"] = {"collection": chunk[-1][0]}
            return {"aggregations": {"m": agg}}
        if "pit" in body:                                             # scan_hits
            n = len(self.scans)
            self.scans.append(body)
            if getattr(self, "fail_scan_at", None) == n:
                raise RuntimeError("elasticsearch went away")
            return {"pit_id": "pit-1", "hits": {"hits": self.pages[n] if n < len(self.pages) else []}}
        self.last_page_body = body                                    # concordance_page
        hits = self.pages[0]
        return {"hits": {"total": {"value": 7}, "hits": hits}}

    def count(self, index, body=None):
        return {"count": 7}


@pytest.fixture
def es(monkeypatch):
    def install(*pages):
        fake = FakeES(list(pages))
        monkeypatch.setattr(cc, "es", fake)
        return fake
    return install


def stats(client, **params):
    from urllib.parse import urlencode
    return client.get("/concordance/stats/?" + urlencode({"q": "dominus", **params}))


# ---- statistics --------------------------------------------------------------

def test_counts_forms_and_relative_frequencies(client, es):
    fake = es([
        page("ms-a", f"{O}Dominus{C} et {O}dominus{C} puis {O}dominus{C}"),
        page("ms-c", f"{O}Dominus{C}"),
    ])
    data = stats(client).get_json()

    assert data["occurrences"] == 4
    assert data["matching_pages"] == 2
    assert data["manuscripts_with_hits"] == 2 and data["manuscripts_in_scope"] == 4
    assert data["tokens_in_scope"] == 45000                      # unknown token counts do not add anything
    assert data["per10k"] == 0.89                                # 4 / 45000 * 10000
    # forms are case-folded but keep their diacritics
    assert data["forms"] == [{"form": "dominus", "occurrences": 4, "share": 100.0}]
    assert data["distinct_forms"] == 1 and data["truncated"] is False
    assert fake.closed == ["pit-1"]


def test_distribution_by_manuscript(client, es):
    es([page("ms-a", f"{O}x{C} a {O}x{C} b {O}x{C}"), page("ms-c", f"{O}x{C}")])
    rows = stats(client, q="x").get_json()["by_manuscript"]
    assert [(r["collection"], r["occurrences"]) for r in rows] == [("ms-a", 3), ("ms-c", 1)]
    assert rows[0]["per10k"] == 3.0 and rows[0]["tokens"] == 10000
    assert rows[1]["tokens"] is None and rows[1]["per10k"] is None   # unknown, not zero


def test_distribution_by_century_includes_centuries_without_hits(client, es):
    es([page("ms-a", f"{O}x{C}")])
    centuries = {c["century"]: c for c in stats(client, q="x").get_json()["by_century"]}
    assert sorted(centuries) == [13, 14, 15]                      # the undated manuscript is not a century
    assert centuries[13]["occurrences"] == 1 and centuries[13]["per10k"] == 1.0
    assert centuries[14]["occurrences"] == 0 and centuries[14]["per10k"] == 0.0   # present in the corpus, absent here
    assert centuries[15]["per10k"] is None                        # token count unknown


def test_distribution_by_language(client, es):
    es([page("ms-a", f"{O}x{C}"), page("ms-c", f"{O}x{C}")])
    languages = {l["language"]: l for l in stats(client, q="x").get_json()["by_language"]}
    assert languages["lat"]["occurrences"] == 1 and languages["lat"]["manuscripts"] == 2
    assert languages["fre"]["occurrences"] == 1
    assert languages["und"]["occurrences"] == 0


def test_limit_marks_the_result_as_truncated(client, es):
    fake = es([page("ms-a", " a ".join([f"{O}x{C}"] * 5))], [page("ms-a", f"{O}x{C}")])
    data = ts.compute_stats({"match_all": {}}, "pit-1", limit=3)
    assert data["occurrences"] == 3 and data["truncated"] is True   # exactly the limit, never over; scanning stops
    assert len(fake.scans) == 1 and fake.closed == ["pit-1"]


def test_neighbouring_hits_are_counted_separately_without_a_phrase(client, es):
    es([page("ms-a", f"{O}deus{C} {O}deum{C}")])
    data = stats(client, q="deus,deum").get_json()
    assert data["occurrences"] == 2 and {f["form"] for f in data["forms"]} == {"deus", "deum"}
    es([page("ms-a", f"{O}la{C} {O}bouche{C}")])
    assert stats(client, q='"la bouche"').get_json()["occurrences"] == 1


def test_scope_filters_apply_to_the_denominator_too(client, es):
    fake = es([page("ms-a", f"{O}x{C}")])
    stats(client, q="x", language="lat")
    assert fake.aggs[0]["query"] == {"bool": {"filter": [{"term": {"language": "lat"}}]}}


def test_no_scope_means_match_all(client, es):
    fake = es()
    stats(client, q="x")
    assert fake.aggs[0]["query"] == {"match_all": {}}


def test_nothing_found(client, es):
    es()
    data = stats(client, q="zzz").get_json()
    assert data["occurrences"] == 0 and data["forms"] == [] and data["by_manuscript"] == []
    assert data["per10k"] == 0.0


def test_csv_export(client, es):
    es([page("ms-a", f"{O}dominus{C}")])
    response = stats(client, format="csv")
    assert response.mimetype == "text/csv" and "term-statistics.csv" in response.headers["Content-Disposition"]
    text = response.get_data(as_text=True)
    assert text.startswith("﻿")
    rows = list(csv.reader(io.StringIO(text[1:])))
    assert rows[0] == ["dimension", "key", "occurrences", "per_10000_tokens", "tokens"]
    assert ["total", "all", "1", "0.22", "45000"] in rows
    assert ["form", "dominus", "1", "", ""] in rows
    assert any(r[0] == "century" and r[1] == "13" for r in rows)


def test_csv_cells_cannot_become_formulas(client, es):
    es([page("ms-a", f"{O}=evil{C}")])
    rows = list(csv.reader(io.StringIO(stats(client, format="csv").get_data(as_text=True)[1:])))
    assert ["form", "'=evil", "1", "", ""] in rows


def test_q_is_required(client, es):
    fake = es()
    response = client.get("/concordance/stats/")
    assert response.status_code == 400 and fake.scans == [] and fake.aggs == []


def test_unknown_collection_is_a_404(hierarchy, es):
    es()
    assert hierarchy.test_client().get("/concordance/stats/?q=x&within=nope").status_code == 404


# ---- the JSON concordance view -----------------------------------------------

def json_page(client, **params):
    from urllib.parse import urlencode
    return client.get("/concordance/?" + urlencode({"q": "x", "format": "json", **params}))


def test_json_page_rows_and_paging(client, es):
    fake = es([page("ms-a", f"abc {O}x{C} def")])
    data = json_page(client, page=3, size=2, width=10).get_json()
    assert data["rows"][0]["keyword"] == "x" and data["rows"][0]["left"] == "abc " and data["rows"][0]["right"] == " def"
    assert data["rows"][0]["manuscript"] == "Title ms-a"
    assert (data["page"], data["size"], data["total_pages"], data["pages"]) == (3, 2, 7, 4)
    assert fake.last_page_body["from"] == 4 and fake.last_page_body["size"] == 2
    assert fake.closed == []                                       # a plain search: no point in time to leak


def test_json_page_size_is_capped(client, es):
    fake = es([])
    json_page(client, size=100000)
    assert fake.last_page_body["size"] == 100


def test_json_page_beyond_the_search_window_points_to_the_csv(client, es):
    es([])
    response = json_page(client, page=2000, size=100)
    assert response.status_code == 400 and "CSV" in response.get_json()["error"]


def test_json_q_is_required(client, es):
    es([])
    assert client.get("/concordance/?format=json").status_code == 400
