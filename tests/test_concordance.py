"""Concordance export: context extraction (pure) and the streamed CSV endpoint (fake Elasticsearch)."""
import csv
import io

import pytest

from app.search_routes import concordance as cc

O, C = cc.MARK_OPEN, cc.MARK_CLOSE


def mark(text, *words):
    """Wrap each given word in highlight marks, like Elasticsearch does."""
    for w in words:
        text = text.replace(w, f"{O}{w}{C}")
    return text


# ---- context extraction ------------------------------------------------------

def test_keyword_and_context():
    [occ] = cc.extract_occurrences(mark("et dixit dominus ad moysen", "dominus"), width=8)
    assert (occ.left, occ.keyword, occ.right) == ("t dixit ", "dominus", " ad moys")
    assert occ.line == 1


def test_context_is_limited_to_the_width_and_never_includes_marks():
    text = mark("aaa dominus bbb dominus ccc", "dominus")
    first, second = cc.extract_occurrences(text, width=100)
    assert first.right == " bbb dominus ccc"        # the next hit appears as plain text
    assert O not in first.right and C not in first.right
    assert second.left == "aaa dominus bbb "


def test_start_and_end_of_the_page_are_handled():
    [occ] = cc.extract_occurrences(mark("dominus", "dominus"), width=40)
    assert (occ.left, occ.keyword, occ.right) == ("", "dominus", "")


def test_a_phrase_marked_word_by_word_is_one_keyword():
    # Elasticsearch marks each word of a phrase separately
    [occ] = cc.extract_occurrences(f"a {O}la{C} {O}bouche{C} b", width=5)
    assert occ.keyword == "la bouche"


def test_separate_matches_are_not_merged():
    occs = cc.extract_occurrences(f"{O}deus{C} et {O}deum{C}", width=5)
    assert [o.keyword for o in occs] == ["deus", "deum"]


def test_line_numbers_and_line_breaks():
    text = f"premiere ligne\nseconde {O}dominus{C} ici\ntroisieme"
    [occ] = cc.extract_occurrences(text, width=40)
    assert occ.line == 2
    assert "\n" not in occ.left + occ.right        # context flows across lines as spaces
    assert occ.right.endswith("ici troisieme")


def test_no_marks_no_rows():
    assert cc.extract_occurrences("nothing highlighted here") == []
    assert cc.extract_occurrences("") == []


def test_medieval_characters_are_preserved():
    [occ] = cc.extract_occurrences(f"ꝑ {O}dñs{C} ⁊ ſicut", width=10)
    assert (occ.left, occ.keyword, occ.right) == ("ꝑ ", "dñs", " ⁊ ſicut")


@pytest.mark.parametrize("value,expected", [
    ("=SUM(A1)", "'=SUM(A1)"), ("+1", "'+1"), ("-que", "'-que"), ("@x", "'@x"),
    ("dominus", "dominus"), ("", ""), (None, ""), (12, "12"),
])
def test_cells_cannot_run_as_spreadsheet_formulas(value, expected):
    assert cc._spreadsheet_safe(value) == expected


def test_filename_is_ascii_and_safe():
    assert cc._filename('"la bouche"') == "concordance-la-bouche.csv"
    assert cc._filename("dñs/../x") == "concordance-dns-x.csv"
    assert cc._filename("???") == "concordance.csv"


def test_neighbouring_hits_are_separate_occurrences_without_a_phrase():
    # `deus,deum` on "deus deum": two hits, not one merged keyword
    occs = cc.extract_occurrences(f"{O}deus{C} {O}deum{C}", width=5, merge_adjacent=False)
    assert [o.keyword for o in occs] == ["deus", "deum"]


@pytest.mark.parametrize("q,expected", [
    ('"la bouche"', True), ('deus, "la bouche"', True), ('"bouche"', False),
    ("la bouche", False), ("deus,deum", False), ('""', False),
])
def test_has_phrase_only_for_quoted_multi_word_queries(q, expected):
    assert cc.has_phrase(q) is expected


@pytest.mark.parametrize("language,expected", [
    (["lat", "", "fro"], "lat, fro"),
    (["lat", None], "lat"),       # ", ".join used to raise TypeError on None
    ([None, ""], ""),
    ([], ""),
    ("lat", "lat"),
    (None, ""),
])
def test_language_column_tolerates_empty_and_missing_values(language, expected):
    hit_ = {"_source": {"language": language}, "highlight": {"content": [f"{O}x{C}"]}}
    [row] = cc.rows_for_hit(hit_, 5)
    assert row[7] == expected


# ---- the streamed endpoint ---------------------------------------------------

class FakeES:
    def __init__(self, batches, matching=7, fail_on_call=None):
        self.batches, self.matching, self.fail_on_call = batches, matching, fail_on_call
        self.searches, self.closed, self.count_bodies = [], [], []

    def count(self, index, body):
        self.count_bodies.append(body)
        return {"count": self.matching}

    def open_point_in_time(self, index, keep_alive):
        return {"id": "pit-1"}

    def search(self, body):
        self.searches.append(body)
        n = len(self.searches) - 1
        if self.fail_on_call == n:
            raise RuntimeError("elasticsearch went away")
        return {"pit_id": "pit-1", "hits": {"hits": self.batches[n] if n < len(self.batches) else []}}

    def close_point_in_time(self, id):
        self.closed.append(id)


def hit(ref, content, collection="c1", title="Manuscript", n=0):
    return {
        "_source": {"collection": collection, "title": title, "ref": ref, "language": ["lat"], "created": ["1200/1300"]},
        "highlight": {"content": [content]},
        "sort": [collection, n, n],
    }


@pytest.fixture
def fake(monkeypatch):
    def install(*batches, **kw):
        es = FakeES(list(batches), **kw)
        monkeypatch.setattr(cc, "es", es)
        return es
    return install


def parse(response):
    text = response.get_data(as_text=True)
    assert text.startswith("﻿")
    return list(csv.reader(io.StringIO(text[1:])))


def test_csv_content_and_headers(client, fake):
    es = fake([hit("f1", f"a {O}dominus{C} b", n=1), hit("f2", f"{O}dominus{C} c\nd {O}dominus{C}", n=2)])
    response = client.get("/concordance/?q=dominus&width=10")

    assert response.status_code == 200
    assert response.mimetype == "text/csv"
    assert response.headers["X-Matching-Pages"] == "7"
    assert response.headers["Content-Disposition"] == 'attachment; filename="concordance-dominus.csv"'

    rows = parse(response)
    assert rows[0] == cc.HEADER
    assert [(r[2], r[3], r[5]) for r in rows[1:]] == [("f1", "1", "dominus"), ("f2", "1", "dominus"), ("f2", "2", "dominus")]
    assert rows[1][:2] == ["c1", "Manuscript"] and rows[1][7:] == ["lat", "1200/1300"]
    assert es.closed == ["pit-1"]


def test_rows_come_in_reading_order(client, fake):
    es = fake([hit("f1", f"{O}x{C}")])
    client.get("/concordance/?q=x").get_data()      # the response streams: nothing runs until it is read
    sort = es.searches[0]["sort"]
    assert sort[0] == {"collection": "asc"}
    assert "position" in sort[1] and "_shard_doc" in sort[-1]      # stable paging needs a unique tiebreaker


def test_it_pages_through_every_batch_with_search_after(client, fake):
    es = fake([hit("f1", f"{O}x{C}", n=1)], [hit("f2", f"{O}x{C}", n=2)])
    rows = parse(client.get("/concordance/?q=x"))
    assert [r[2] for r in rows[1:]] == ["f1", "f2"]
    assert "search_after" not in es.searches[0]
    assert es.searches[1]["search_after"] == ["c1", 1, 1]
    assert es.closed == ["pit-1"]


def test_limit_stops_early_and_still_closes_the_point_in_time(client, fake):
    es = fake([hit("f1", f"{O}x{C} {O}x{C}", n=1), hit("f2", f"{O}x{C}", n=2)], [hit("f3", f"{O}x{C}", n=3)])
    rows = parse(client.get("/concordance/?q=x&limit=2"))
    assert len(rows) == 1 + 2
    assert len(es.searches) == 1                                    # did not fetch more than needed
    assert es.closed == ["pit-1"]


def test_point_in_time_is_closed_when_elasticsearch_fails_mid_stream(client, fake):
    es = fake([hit("f1", f"{O}x{C}", n=1)], fail_on_call=1)
    with pytest.raises(RuntimeError):
        client.get("/concordance/?q=x").get_data()
    assert es.closed == ["pit-1"]


def test_a_failure_to_close_the_point_in_time_does_not_break_the_download(client, fake):
    es = fake([hit("f1", f"{O}x{C}")])
    es.close_point_in_time = lambda id: (_ for _ in ()).throw(RuntimeError("already expired"))
    assert len(parse(client.get("/concordance/?q=x"))) == 2


def test_hits_without_highlights_produce_no_rows(client, fake):
    page = {"_source": {"collection": "c", "ref": "f"}, "highlight": {}, "sort": [1]}
    fake([page])
    assert parse(client.get("/concordance/?q=x")) == [cc.HEADER]


def test_formula_like_text_is_neutralised_in_the_file(client, fake):
    fake([hit("f1", f"=cmd {O}x{C}", title="=HYPERLINK(1)")])
    rows = parse(client.get("/concordance/?q=x"))
    assert rows[1][1] == "'=HYPERLINK(1)" and rows[1][4].startswith("'=cmd")


def test_filters_and_mode_reach_elasticsearch(client, fake):
    es = fake()
    client.get("/concordance/?q=reg*&mode=partial&language=lat&resource=c1").get_data()
    query = es.searches[0]["query"]
    assert {"term": {"collection": "c1"}} in query["bool"]["filter"]
    assert {"term": {"language": "lat"}} in query["bool"]["filter"]
    assert es.count_bodies[0]["query"] == query                     # the size header counts the same hits


def test_width_and_limit_are_clamped(client, fake):
    fake([hit("f1", f"{'a' * 500}{O}x{C}{'b' * 500}")])
    [_, row] = parse(client.get("/concordance/?q=x&width=100000&limit=10000000"))
    assert len(row[4]) == cc.MAX_WIDTH and len(row[6]) == cc.MAX_WIDTH


def test_q_is_required(client, fake):
    es = fake()
    response = client.get("/concordance/")
    assert response.status_code == 400 and "error" in response.get_json()
    assert es.searches == []


def test_unknown_collection_is_a_404(hierarchy, fake):
    es = fake()
    response = hierarchy.test_client().get("/concordance/?q=x&within=nope")
    assert response.status_code == 404
    assert es.searches == []


def test_within_restricts_to_the_collections_manuscripts(hierarchy, fake):
    es = fake()
    hierarchy.test_client().get("/concordance/?q=x&within=parent").get_data()
    assert any("terms" in f and "collection" in f["terms"] for f in es.searches[0]["query"]["bool"]["filter"])


def test_search_csv_export_uses_the_concordance(client, fake):
    # /search/?format=csv used to crash after the header row
    fake([hit("f1", f"a {O}x{C} b")])
    rows = parse(client.get("/search/?q=x&format=csv"))
    assert rows[0] == cc.HEADER and rows[1][5] == "x"


def test_json_page_has_rows_and_paging_metadata(client, fake, monkeypatch):
    seen = {}

    class PagedES(FakeES):
        def search(self, index=None, body=None):
            seen.update(body)
            return {"hits": {"total": {"value": 45}, "hits": [hit("f1", f"a {O}x{C} b", n=1)]}}

    monkeypatch.setattr(cc, "es", PagedES([]))
    data = client.get("/concordance/?q=x&format=json&page=2&size=20").get_json()
    assert seen["from"] == 20 and seen["size"] == 20
    assert data["pages"] == 3 and data["page"] == 2
    assert data["rows"][0]["keyword"] == "x" and data["rows"][0]["ref"] == "f1"


def test_json_page_beyond_the_result_window_is_rejected(client, fake):
    es = fake()
    response = client.get("/concordance/?q=x&format=json&page=101&size=100")
    assert response.status_code == 400 and es.searches == []


def paged(monkeypatch, total):
    seen = {}

    class PagedES(FakeES):
        def search(self, index=None, body=None):
            seen.update(body)
            return {"hits": {"total": {"value": total}, "hits": []}}

    monkeypatch.setattr(cc, "es", PagedES([]))
    return seen


def test_json_page_defaults_to_50_matching_pages(client, monkeypatch):
    seen = paged(monkeypatch, 120)
    data = client.get("/concordance/?q=x&format=json").get_json()
    assert seen["size"] == 50 and data["size"] == 50 and data["pages"] == 3 and data["capped"] is False


@pytest.mark.parametrize("size,pages", [(100, 100), (50, 200), (25, 400), (30, 333)])
def test_pages_offered_never_exceed_the_result_window(client, monkeypatch, size, pages):
    paged(monkeypatch, 45000)
    data = client.get(f"/concordance/?q=x&format=json&size={size}").get_json()
    assert data["pages"] == pages and data["capped"] is True
    assert data["pages"] * size <= cc.RESULT_WINDOW


def test_every_offered_page_can_be_opened_and_the_next_cannot(client, monkeypatch):
    # size 30: page 333 ends at 9,990 (fits), page 334 would end at 10,020 (Elasticsearch would refuse it)
    seen = paged(monkeypatch, 45000)
    assert client.get("/concordance/?q=x&format=json&size=30&page=333").status_code == 200
    assert seen["from"] == 9960
    assert client.get("/concordance/?q=x&format=json&size=30&page=334").status_code == 400


def test_a_result_that_fits_the_window_is_not_capped(client, monkeypatch):
    paged(monkeypatch, 10000)
    data = client.get("/concordance/?q=x&format=json&size=100").get_json()
    assert data["pages"] == 100 and data["capped"] is False


# ---- indexing: reading order -------------------------------------------------

def test_positions_number_pages_per_manuscript():
    from es_indexer_textile import add_positions
    docs = [{"collection": "a", "ref": "f1"}, {"collection": "b", "ref": "f1"},
            {"collection": "a", "ref": "f2"}, {"collection": "a", "ref": "f3"}, "not a dict"]
    add_positions(docs)
    assert [d["position"] for d in docs[:4]] == [0, 0, 1, 2]
