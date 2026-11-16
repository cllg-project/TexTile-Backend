# TexTile API

[![Tests](https://github.com/cllg-project/TexTile-Backend/actions/workflows/tests.yml/badge.svg)](https://github.com/cllg-project/TexTile-Backend/actions/workflows/tests.yml)

The backend of **TexTile**, the web reader for **CoMMA**, *a Large-scale Corpus of Multilingual Medieval
Archives*. It serves transcribed medieval manuscripts through a [DTS 1.0](https://dtsapi.org) API and adds what
a reader needs on top of it: catalog search, full-text search, concordances and frequency statistics.

Python (Flask) on top of [MyDapytains](https://github.com/distributed-text-services/MyDapytains), SQLite and
Elasticsearch 8. The web interface lives in the [TexTile](https://github.com/cllg-project/TexTile) repository.

**Contents**
[Features](#features) ·
[How it fits together](#how-it-fits-together) ·
[Quick start](#quick-start) ·
[API reference](#api-reference) ·
[Configuration](#configuration) ·
[Indexing](#indexing-and-re-indexing) ·
[Deployment](#deployment) ·
[Limits and caching](#limits-and-caching) ·
[Development](#development) ·
[Troubleshooting](#troubleshooting) ·
[Upgrading](#upgrading)

## Features

- **DTS 1.0**: collections, navigation and documents (TEI XML or rendered HTML), with collections paginated
  and sortable.
- **Catalog search**: find manuscripts by title, shelfmark, publisher, language or date. Accents and case are
  ignored (`francais` finds `français`), every word must match (`smith 19` finds *Smith-Lesouëf 19*), and a
  year finds the manuscripts whose date range contains it. Filter by language and dates, or search inside one
  collection.
- **Full-text search** over the transcribed pages, with exact, partial and fuzzy matching, phrases, wildcards
  and highlighted snippets.
- **Concordances**: every occurrence of a search with its left and right context, in reading order, as a
  streamed CSV or as JSON pages.
- **Statistics**: how often the searched forms occur, overall and per 10,000 tokens, which forms matched, and
  how they are distributed by manuscript, century and language. Streamed while it is computed.
- **Built to run**: re-indexing without downtime, caching, limits on heavy requests, threaded gunicorn,
  links that follow your public address behind a reverse proxy, 220+ tests and CI.

## How it fits together

```
TEI XML files ──► flask data catalog build ──► catalog.xml
                                                  │  flask data catalog ingest
                                                  ▼
   rendered pages ◄── flask data prerender ──  SQLite (app.db)        ┌──────────────┐
   (HTML, on disk)         generate            collections,           │ TexTile web  │
        │                                      citation structure     │ interface    │
        │ flask data prerender-to-json                  │             └──────┬───────┘
        ▼                                               ▼                    │ HTTP
     refs.json ──► es_indexer_textile.py ──►  Elasticsearch ◄───── Flask API (this repository)
                    (zero-downtime)           one document per page
```

| Store | Holds | Used for |
|---|---|---|
| **SQLite** (`app.db`) | collection hierarchy, metadata, citation structure | DTS collections and navigation, the catalog tree, `within=` |
| **Disk cache** (`PRERENDER_DIR`) | every page already rendered to HTML | DTS documents, served without transforming TEI per request |
| **Elasticsearch** | one document per page, with its manuscript's metadata | catalog search, full-text search, concordances, statistics |

Only the web API needs to run in production. The `flask` commands that build the stores are run when the
data changes.

## Quick start

A local setup with the five example manuscripts (about 2,900 pages).

**You need** Python 3.10 (the version of the Docker image), `git` (MyDapytains is installed from GitHub),
and Docker, or any Elasticsearch 8.x you can reach.

```bash
# 1. Python environment
python -m venv env && source env/bin/activate
pip install -r requirements.txt

# 2. Elasticsearch (single node, no security: for development only)
docker run -d --name textile-es -p 9200:9200 \
  -e discovery.type=single-node -e xpack.security.enabled=false \
  -e ES_JAVA_OPTS="-Xms512m -Xmx512m" \
  docker.elastic.co/elasticsearch/elasticsearch:8.11.0

# 3. Configuration: copy the example, then adjust the paths
cp .env.example .env
```

In `.env`, at least:

```ini
ELASTICSEARCH_HOST=http://localhost:9200
ELASTICSEARCH_INDEX=documents_v5          # set it explicitly: the API and the indexer have different defaults
PRERENDER_DIR=/absolute/path/to/cache     # where rendered pages are stored
ALLOWED_ORIGINS=http://localhost:5173     # the web interface's address (see "CORS" below)
```

```bash
# 4. Build the data (details in example_data/README.md)
export FLASK_APP=app
flask data catalog build "./example_data/*/*.xml" \
      --mapping example_data/example_mapping.json \
      --external-metadata example_data/example_metadata.json \
      --output example_data/catalog.xml
flask db create
flask data catalog ingest example_data/catalog.xml
flask data prerender generate --media-type html --workers 8     # a few minutes: renders every page
flask data prerender-to-json --out example_data/refs.json --workers 4
python es_indexer_textile.py example_data/refs.json             # builds the index, swaps the alias

# 5. Run
flask run -p 5000
```

Check it:

```bash
curl localhost:5000/                                   # the DTS entry point
curl "localhost:5000/manuscripts/?q=smith+19"          # catalog search
python migrate.py verify --text-query et               # 13 checks of the whole stack
```

## API reference

Everything is JSON unless stated otherwise. Examples use `localhost:5000`.

| Area | Endpoint | Purpose |
|---|---|---|
| DTS | `GET /` | entry point with the URI templates |
| | `GET /collection/` | collections and manuscripts, paginated |
| | `GET /navigation/` | the citable units of a manuscript |
| | `GET /document/` | a page or range, as TEI XML or HTML |
| Catalog | `GET /manuscripts/` | search manuscripts, with filters |
| | `GET /collections/search/` | search collections at any depth |
| | `GET /manuscripts/facets/` | languages and date span, for the filters |
| | `GET /manuscripts/list/`, `/collections/list/`, `/manuscripts/count/` | lightweight lists and counts |
| Text | `GET /search/` | full-text search over the pages |
| | `GET /concordance/` | keyword in context: CSV or JSON |
| | `GET /concordance/stats/` | frequency statistics: JSON, CSV or streamed |
| Other | `GET /health` | health check (database) |

### DTS

Answers follow **DTS 1.0** as produced by MyDapytains `1.0.0rc4`: `@context` is
`https://dtsapi.org/context/v1.0.json` and `dtsVersion` is `1.0`, both taken from the library.

```http
GET /collection/?id=<collection id>&nav=children|parents&page=1&sort_by=default|title|nb_children&sort_order=asc|desc
GET /navigation/?resource=<id>&ref=<ref>|start=<ref>&end=<ref>&tree=<tree>&down=<int>
GET /document/?resource=<id>&ref=<ref>|start=<ref>&end=<ref>&tree=<tree>&mediaType=html
```

- `id` is optional: without it, `/collection/` returns the root. `nav=parents` lists a collection's parents.
- `/collection/` is **paginated** (`PAGINATION_SIZE`, 20 by default). The answer has a `view` of type
  `Pagination` with absolute `first`, `previous`, `next` and `last` links, and a page past the end is a `404`.
  The collection URI template advertises it: `collection/{?id}{&page,nav}`.
- Our own number of children per member is in `extensions.nb_children`, an array like every `extensions` value.
- `/document/` without `ref` returns the whole TEI file; `mediaType=html` returns the pre-rendered HTML.
- All links in the answers are built from `PUBLIC_BASE_URL` when it is set
  (see [Behind a reverse proxy](#behind-a-reverse-proxy)).

### Catalog search

```http
GET /manuscripts/?q=<text>&language=<code>&start_year=<y>&stop_year=<y>&within=<collection id>&page=1&size=25
GET /collections/search/?q=<text>&within=<collection id>&page=1&size=20
GET /manuscripts/facets/
```

| Parameter | Meaning |
|---|---|
| `q` | every word must match (title, shelfmark, publisher, language, date); accents and case are ignored |
| `language` | an ISO code from the facets (`lat`, `fre`...) |
| `start_year`, `stop_year` | keep manuscripts whose date range overlaps the interval |
| `within` | only the manuscripts below this collection, at any depth |
| `page`, `size` | pagination (`size` up to 100) |

- A short number also matches longer ones, the exact one ranked first: `19` finds `19`, then `119` and `190`.
  A year such as `1290` finds the manuscripts whose date range contains it.
- With no `q`, the filters alone are a valid search.
- One result per manuscript, paginated by Elasticsearch; `total` counts manuscripts. Each result has
  `collection`, `title` (highlighted), `language`, `created`, `publisher`, `identifier`, `manifest_url`, `tokens`,
  `page_count`...
- Older endpoints still work, as thin wrappers over the same code: `/manuscripts/language/?q=`,
  `/manuscripts/range/?q=800-1400`, `/manuscripts/date/?start_year=&stop_year=&exact_start=&exact_stop=`.

### Full-text search

```http
GET /search/?q=<query>&mode=exact|partial|fuzzy&language=<code>&resource=<manuscript id>&page=1&size=25
```

Returns `total` and `items` with the page's `collection`, `ref`, `title` and a highlighted `snippet`.

| Syntax | Means |
|---|---|
| `deus,deum` | any of the comma-separated alternatives |
| `"la bouche"` | an exact phrase |
| `reg*` | words starting with `reg` (exact mode only) |

`mode=partial` also matches inside words, `mode=fuzzy` tolerates small spelling differences.
The web interface rewrites `v`→`u` and `j`→`i` outside quotes before sending a query; the API does not.

### Concordance

```http
GET /concordance/?q=<query>&mode=&language=&resource=&within=&width=40&limit=100000          # CSV, streamed
GET /concordance/?q=<query>&format=json&page=1&size=20                                      # JSON
```

The same query, modes and filters as `/search/` (`/search/?format=csv` is the same export), so a concordance
contains exactly the hits the search shows. One row per occurrence, in reading order (manuscript, page,
position in the page):

`collection, manuscript, ref, line, left, keyword, right, language, date`

- **CSV** is streamed: it starts downloading at once and uses constant memory, however many lines there are.
  The `X-Matching-Pages` header gives the number of matching pages up front. It is UTF-8 with a BOM so
  spreadsheets read the diacritics, and cells starting with `= + - @` are prefixed with `'` so a spreadsheet
  never runs them as formulas.
- `width` is the context in characters on each side (5–200). `limit` is capped by `CONCORDANCE_MAX_ROWS`.
- **JSON** pages through the *matching pages* (`size` per page, up to 100) and reports `total_pages` and
  `pages`. Elasticsearch's 10,000-result window applies; the CSV has no such limit.
- A quoted phrase is one keyword (`"la bouche"`); for `deus,deum`, two neighbouring hits are two occurrences.

### Statistics

```http
GET /concordance/stats/?q=<query>&mode=&language=&resource=&within=&limit=&format=json|csv|ndjson&refresh=1
```

Counts the occurrences of a search and returns:

- `occurrences` and `per10k`: occurrences per 10,000 tokens of **all** manuscripts in the searched scope, with
  or without hits, so it is a relative frequency;
- `forms`: the distinct forms that matched, which is what a wildcard, fuzzy or `deus,deum` search is made of;
- `by_manuscript`, `by_century` (century of the middle of the date range; centuries without hits are listed
  with 0) and `by_language`, each with its own `per10k`.

A manuscript whose token count is unknown has a `null` relative frequency, never 0. The numbers agree with the
concordance of the same search.

**Streaming**: `format=ndjson` sends one JSON object per line while it counts: `start` (with the total number of
matching pages), then `progress` events about twice a second (pages scanned and the figures so far), then `done`
or `error`. The web interface uses it for its loading indicator. Closing the connection stops the scan.

**Bounds**: at most `STATS_MAX_OCCURRENCES` (**100,000**) occurrences are counted, exactly, and at most
`STATS_MAX_SECONDS` seconds are spent. The result says if it stopped early (`truncated`, `truncated_reason`
`occurrences` or `time`, and `limit`). `limit=` can lower the maximum, never raise it.

## Configuration

Settings come from environment variables, read from a `.env` file if there is one
(`.env.example` for development, `.env.production.example` for production).

**Elasticsearch and data**

| Variable | Meaning | Default |
|---|---|---|
| `ELASTICSEARCH_HOST` | Elasticsearch URL | API `https://localhost:9200`, indexer `http://dts_dts_elasticsearch:9200` |
| `ELASTICSEARCH_USERNAME`, `ELASTICSEARCH_PASSWORD` | credentials | `elastic`, empty |
| `ELASTICSEARCH_VERIFY_CERTS` | verify TLS certificates; keep `true` in production | `false` |
| `ELASTICSEARCH_INDEX` | the index **alias** the API reads and the indexer publishes to | API `documents`, indexer `documents_v5` |
| `DATABASE_PATH` | SQLite database | `app.db` next to the code (`/app/app.db` in the image) |
| `PRERENDER_DIR` | where rendered pages are stored | `/tmp/prerender` |
| `USE_DISK_CACHE`, `VERBOSE_CACHE` | page cache on/off, and its logging | `true`, `0` |

The API and the indexer have different defaults for the host and the index: **set both explicitly** in `.env`.

**Serving**

| Variable | Meaning | Default |
|---|---|---|
| `ALLOWED_ORIGINS` | comma-separated origins allowed by CORS (the web interface's address) | `http://localhost:3000`, `:8080`, `:5173` |
| `PUBLIC_BASE_URL` | public address of the API; every generated link is built from it | the request's address |
| `TRUSTED_PROXY_HOPS` | trust `X-Forwarded-*` from this many proxies (ignored when `PUBLIC_BASE_URL` is set) | `0` |
| `PAGINATION_SIZE` | members per page of `/collection/` | `20` |
| `WEB_CONCURRENCY`, `GUNICORN_THREADS`, `GUNICORN_TIMEOUT`, `LOG_LEVEL`, `BIND` | gunicorn: workers, threads per worker, timeout (s), log level, address | `min(4, 2×CPU+1)`, `8`, `120`, `info`, `0.0.0.0:5000` |

**Limits and caches** (see [Limits and caching](#limits-and-caching))

| Variable | Meaning | Default |
|---|---|---|
| `STATS_MAX_OCCURRENCES` | most occurrences one statistics scan counts | `100000` |
| `STATS_MAX_SECONDS` | time budget of one statistics scan | `60` |
| `STATS_MAX_CONCURRENT`, `EXPORT_MAX_CONCURRENT` | scans and CSV exports at once, per worker | `2`, `4` |
| `CONCORDANCE_MAX_ROWS` | most rows of one CSV export | `100000` |
| `API_CACHE_DIR` | saved statistics, shared by the workers | `<PRERENDER_DIR>/api-cache` |
| `API_CACHE_TTL`, `API_CACHE_MAX_ENTRIES` | their lifetime (s) and count | `86400`, `2000` |
| `HIERARCHY_CACHE_SECONDS` | how long the collection hierarchy is kept in memory | `60` |

## Indexing and re-indexing

`ELASTICSEARCH_INDEX` is an **alias**. `es_indexer_textile.py` builds a new index named
`<alias>_<timestamp>`, checks it, and only then repoints the alias in one atomic step: searches keep using the
old index until that instant, and a failed build changes nothing.

```bash
python es_indexer_textile.py refs.json            # build, check, swap
python es_indexer_textile.py refs.json --keep 2   # keep two previous versions for rollback
python es_indexer_textile.py --rollback           # point the alias back to the previous version
```

- The new index must hold every document and not be less than half the size of the live one (`--force`
  overrides that second check).
- A re-index needs room for a **second copy** of the index. If Elasticsearch cannot allocate the new index's
  shards, usually because the disk is past its watermark, the indexer stops after 30 s, says so, and removes it.
- On a deployment that still has a plain index named like the alias, the first run converts it to an alias in
  the same atomic request.
- Re-index whenever the mapping or the data changes. The statistics cache and the facets refresh by themselves.

Each page is one document with its manuscript's metadata, in an index mapped for accent-insensitive search
(`title`, `.text` sub-fields), date ranges (`date_start`, `date_end`) and reading order (`position`).

## Deployment

### Docker

The image holds no configuration: pass it when you run it.

```bash
docker build -f Dockerfile.api -t textile-api .
docker run --env-file .env -p 5000:5000 textile-api
```

It runs `gunicorn -c gunicorn.conf.py app:app`: threaded workers, so a long statistics stream or CSV export
occupies one thread instead of freezing a worker. The request timeout is a heartbeat for threaded workers, not a
cap on how long a request may run. Mount a volume for `PRERENDER_DIR` (the rendered pages) and for
`API_CACHE_DIR` if you want the saved statistics to survive a restart.

### Behind a reverse proxy

The DTS answers contain links (the entry point, the URI templates, the pagination). They are built from the
address the API is reached at, so behind a proxy they would point to the internal address. Set
`PUBLIC_BASE_URL` to the public one:

```ini
PUBLIC_BASE_URL=https://example.org/dts/      # served under a path prefix
```

The proxy may strip the `/dts` prefix or keep it; both work. The web interface's `VITE_DTS_BASE_URL` must be
the same address. If your proxy sets `X-Forwarded-Proto/Host/Prefix`, `TRUSTED_PROXY_HOPS=1` does the same
without hard-coding the address.

### CORS

The browser only lets the web interface call the API if its exact origin (scheme, host and port) is in
`ALLOWED_ORIGINS`, for example `https://reader.example.org`.

### Production checklist

- `ELASTICSEARCH_VERIFY_CERTS=true` and a real `ELASTICSEARCH_PASSWORD`.
- `PUBLIC_BASE_URL` set, `ALLOWED_ORIGINS` set to the real frontend.
- Disk space for a second copy of the index during re-indexing.
- `API_CACHE_DIR` writable by the API's user (if it is not, results are simply not cached).
- `/health` wired to your health check; `FLASK_DEBUG` off.

## Limits and caching

Searching is cheap; counting and exporting are not, so they are bounded.

| Operation | Bound | When exceeded |
|---|---|---|
| statistics scan | `STATS_MAX_OCCURRENCES` occurrences, `STATS_MAX_SECONDS` seconds | stops, returns what it counted, says why |
| statistics and exports at once | `STATS_MAX_CONCURRENT`, `EXPORT_MAX_CONCURRENT` per worker | `503` with `Retry-After` |
| CSV export | `CONCORDANCE_MAX_ROWS` rows | stops at the limit |
| JSON concordance | Elasticsearch's 10,000-result window | `400`, use the CSV |

| Cache | Holds | Invalidated |
|---|---|---|
| disk (`API_CACHE_DIR`) | finished statistics, shared by all workers | by itself on a re-index; `refresh=1`; after `API_CACHE_TTL` |
| memory | the collection hierarchy (`HIERARCHY_CACHE_SECONDS`), the facets, the per-manuscript token counts | by expiry; re-index for the last two |

A statistics scan that was cut short by the time limit is not cached, since a faster run would count more.

## Development

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest                      # 220+ tests, no Elasticsearch needed (a fake client is used)
flask run --debug -p 5000
```

CI (`.github/workflows/tests.yml`) runs the tests on Python 3.10 and builds the Docker image on every push and
pull request.

```
app/
  __init__.py               the Flask app: DTS routes, search routes, CLI, public-URL handling
  cli.py                    flask db | data catalog | data prerender | ...
  paginated_collection.py   the paginated /collection/ and the DTS 1.0 templates
  public_url.py             PUBLIC_BASE_URL / TRUSTED_PROXY_HOPS
  cache.py, limits.py       disk and memory caches, concurrency limits
  prerendering.py           TEI to HTML pages (XSLT) and their disk cache
  utils/catalog_building.py builds catalog.xml from the TEI files
  search_routes/
    catalog_search.py       /manuscripts/, facets, legacy endpoints
    text_search.py          /search/
    query_builder.py        the full-text query (modes, phrases, wildcards)
    concordance.py          keyword in context, CSV and JSON
    term_stats.py           frequency statistics, streaming
    hierarchy.py            the cached collection hierarchy, `within=`
    manuscript_stats.py     lists, counts and collection search
es_indexer_textile.py       builds the index and swaps the alias
migrate.py                  checks and helpers for upgrading (see MIGRATION.md)
gunicorn.conf.py            production server settings
example_data/               five manuscripts to try everything on
tests/
```

`es_indexer.py` is an older indexer that enriches pages from a `counts.json`; `update_extent_in_db.py` and
`update_source_in_db.py` are one-off scripts that fill in the extent and the IIIF manifest of manuscripts that
are already ingested. Use `es_indexer_textile.py` for indexing.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Re-indexing stops with "no active shards" | Elasticsearch has no room for a new index: free disk space (it stops allocating shards past 90% full). Nothing live changed. |
| The browser reports a CORS error | Add the web interface's exact origin to `ALLOWED_ORIGINS`. |
| Links in answers point to `localhost` or an internal host | Set `PUBLIC_BASE_URL`. |
| `/manuscripts/` or `/concordance/` answer HTTP 400 `No mapping found for [date_start]` | The index predates this version: re-index. |
| `503` from `/concordance/…` | More heavy requests than the limit allow: retry after the delay in `Retry-After`. |
| Search finds nothing after changing data | The data has to be indexed again: `flask data prerender-to-json`, then the indexer. |
| `flask` cannot find the app | `export FLASK_APP=app`, from the repository root. |

## Upgrading

Moving from an earlier version: see [MIGRATION.md](MIGRATION.md) and `python migrate.py check`.

## Citing

> Clérice, T., Gabay, S., Vlachou-Efstathiou, M., Pinche, A., Sagot, B. (2026).
> *CoMMA, a Large-scale Corpus of Multilingual Medieval Archives.* LREC 2026, Palma de Mallorca.
> <https://inria.hal.science/hal-05299220>

## Related projects

- [TexTile](https://github.com/cllg-project/TexTile): the web interface for this API.
- [MyDapytains](https://github.com/distributed-text-services/MyDapytains): the DTS implementation this is built on.
- [DTS specification](https://dtsapi.org).

## License

MIT, © 2025 Inria. See [LICENSE.MD](LICENSE.MD).
