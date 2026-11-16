"""Catalog search: query building and the HTTP layer (Elasticsearch is faked)."""
import pytest

from app.search_routes import catalog_search as cs


# ---- query building ---------------------------------------------------------

def test_fold_removes_diacritics_and_case():
    assert cs._fold("Français") == "francais"
    assert cs._fold("Smith-Lesouëf") == "smith-lesouef"


def _must_terms(query):
    return [m["bool"]["should"] for m in query["bool"]["must"]]


def test_every_term_is_required():
    # "Smith 19" must not degrade to "Smith": both terms become must-clauses
    assert len(cs._build_general_query("Smith 19")["bool"]["must"]) == 2


def test_short_terms_fall_back_to_substring_not_ngrams():
    options = _must_terms(cs._build_general_query("19"))[0]
    kinds = [next(iter(o)) for o in options]
    assert "constant_score" in kinds          # "19" also finds "190" / "119"
    assert not any("title.ngram" in str(o) for o in options)


def test_long_terms_use_ngrams_for_partial_words():
    options = _must_terms(cs._build_general_query("biblio"))[0]
    assert any("title.ngram" in str(o) for o in options)


def test_years_match_date_ranges():
    options = _must_terms(cs._build_general_query("1290"))[0]
    assert any("date_start" in str(o) and "date_end" in str(o) for o in options)


def test_wildcard_characters_in_user_input_are_escaped():
    options = _must_terms(cs._build_general_query("a*"))[0]
    wildcard = next(o for o in options if "constant_score" in o)["constant_score"]["filter"]["wildcard"]["title"]["value"]
    assert wildcard == r"*a\**"


def test_exact_phrase_is_boosted():
    should = cs._build_general_query("Smith 19")["bool"]["should"]
    assert should[0]["match_phrase"]["title"]["boost"] > should[1]["match"]["title"]["boost"]


# ---- HTTP layer -------------------------------------------------------------

@pytest.fixture
def captured(monkeypatch):
    calls = []

    def fake(query, page, size, highlight=None):
        calls.append({"query": query, "page": page, "size": size, "highlight": highlight})
        return {"total": 0, "items": []}

    monkeypatch.setattr(cs, "search_manuscripts", fake)
    return calls


def test_empty_search_returns_nothing_without_hitting_elasticsearch(client, captured):
    assert client.get("/manuscripts/").get_json() == {"total": 0, "items": []}
    assert captured == []


def test_filters_without_text_are_a_valid_search(client, captured):
    client.get("/manuscripts/?language=lat&start_year=1200&stop_year=1300")
    filters = captured[0]["query"]["bool"]["filter"]
    assert {"term": {"language": "lat"}} in filters
    assert {"range": {"date_end": {"gte": 1200}}} in filters
    assert {"range": {"date_start": {"lte": 1300}}} in filters
    assert captured[0]["highlight"] is None


def test_page_size_is_capped(client, captured):
    client.get("/manuscripts/?q=smith&size=100000&page=0")
    assert captured[0]["size"] == cs._MAX_PAGE_SIZE and captured[0]["page"] == 1


@pytest.mark.parametrize("url", [
    "/manuscripts/range/?q=abc",
    "/manuscripts/range/",
    "/manuscripts/date/",
    "/manuscripts/date/?start_year=abc",
])
def test_legacy_endpoints_reject_bad_input(client, captured, url):
    assert client.get(url).status_code == 400
    assert captured == []


def test_range_endpoint_uses_overlap_on_numeric_fields(client, captured):
    client.get("/manuscripts/range/?q=1200-1300")
    filters = captured[0]["query"]["bool"]["filter"]
    assert {"range": {"date_end": {"gte": 1200}}} in filters
    assert {"range": {"date_start": {"lte": 1300}}} in filters
